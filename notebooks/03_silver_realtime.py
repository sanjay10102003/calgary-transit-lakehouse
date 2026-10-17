# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver: positions and stop predictions
# MAGIC Both read Bronze as a stream so each run only handles new rows.
# MAGIC
# MAGIC Positions: parse, drop repeats (the feed resends a vehicle's last ping if it hasn't moved), and send
# MAGIC rows that fail the checks to a quarantine table with the reason.
# MAGIC
# MAGIC Trip updates: one row per upcoming stop, MERGEd so only the latest prediction per trip/stop is kept.
# MAGIC `arrival.time` of 0 means unknown, so it's treated as null instead of 1970.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

TRIP_SCHEMA = """struct<trip_id:string, route_id:string, direction_id:int,
                        start_time:string, start_date:string, schedule_relationship:string>"""
VEH_DESC_SCHEMA = "struct<id:string, label:string>"

VEHICLE_ENTITY_SCHEMA = f"""
struct<id:string,
  vehicle:struct<
    trip:{TRIP_SCHEMA},
    vehicle:{VEH_DESC_SCHEMA},
    position:struct<latitude:double, longitude:double, bearing:double, speed:double>,
    current_stop_sequence:int, stop_id:string, current_status:string,
    timestamp:string, congestion_level:string, occupancy_status:string>>
"""

TRIP_UPDATE_ENTITY_SCHEMA = f"""
struct<id:string,
  trip_update:struct<
    trip:{TRIP_SCHEMA},
    vehicle:{VEH_DESC_SCHEMA},
    stop_time_update:array<struct<
      stop_sequence:int, stop_id:string, schedule_relationship:string,
      arrival:struct<delay:int, time:string, uncertainty:int>,
      departure:struct<delay:int, time:string, uncertainty:int>>>,
    timestamp:string, delay:int>>
"""


def service_date_expr(start_date_col, event_ts_col):
    """Trip start_date if the feed provides it, else the local date shifted back a few hours
    (trips running after midnight belong to the previous service day)."""
    fallback = F.to_date(F.from_utc_timestamp(event_ts_col - F.expr(f"INTERVAL {SERVICE_DAY_SHIFT_H} HOURS"), LOCAL_TZ))
    return F.coalesce(F.to_date(start_date_col, "yyyyMMdd"), fallback)

# COMMAND ----------

# MAGIC %md ## Vehicle positions

# COMMAND ----------

POS = f"{SILVER}.vehicle_positions"
POS_Q = f"{SILVER}.quarantine_vehicle_positions"

parsed = (
    spark.readStream.table(f"{BRONZE}.raw_vehicle_positions")
    .withColumn("e", F.from_json("entity_json", VEHICLE_ENTITY_SCHEMA))
    .select(
        F.col("e.vehicle.vehicle.id").alias("vehicle_id"),
        F.col("e.vehicle.vehicle.label").alias("vehicle_label"),
        F.col("e.vehicle.trip.trip_id").alias("trip_id"),
        F.col("e.vehicle.trip.route_id").alias("route_id"),
        F.col("e.vehicle.trip.direction_id").alias("direction_id"),
        F.col("e.vehicle.trip.start_date").alias("start_date"),
        F.col("e.vehicle.position.latitude").alias("lat"),
        F.col("e.vehicle.position.longitude").alias("lon"),
        F.col("e.vehicle.position.bearing").alias("bearing"),
        F.col("e.vehicle.position.speed").alias("speed_mps"),
        F.col("e.vehicle.current_stop_sequence").alias("current_stop_sequence"),
        F.col("e.vehicle.stop_id").alias("stop_id"),
        F.col("e.vehicle.current_status").alias("current_status"),
        F.col("e.vehicle.occupancy_status").alias("occupancy_status"),
        # fall back to the feed time if the vehicle has none
        F.coalesce(F.timestamp_seconds(F.col("e.vehicle.timestamp").cast("bigint")), F.col("feed_ts")).alias("event_ts"),
        "feed_ts", "poll_ts", "_ingested_at",
    )
    .withColumn("service_date", service_date_expr(F.col("start_date"), F.col("event_ts")))
    .withWatermark("event_ts", "30 minutes")
    .dropDuplicates(["vehicle_id", "event_ts"])
)

b = CALGARY_BBOX
dq_reason = (
    F.when(F.col("vehicle_id").isNull(), "missing_vehicle_id")
    .when(F.col("lat").isNull() | F.col("lon").isNull(), "missing_position")
    .when(~F.col("lat").between(b["lat_min"], b["lat_max"]) | ~F.col("lon").between(b["lon_min"], b["lon_max"]),
          "outside_calgary")
    .when(F.col("event_ts") > F.col("poll_ts") + F.expr("INTERVAL 5 MINUTES"), "timestamp_in_future")
    .when(F.col("event_ts") < F.col("poll_ts") - F.expr("INTERVAL 1 HOUR"), "stale_ping")
)


def write_positions(batch_df, batch_id):
    checked = batch_df.withColumn("dq_reason", dq_reason)
    good = checked.where("dq_reason IS NULL").drop("dq_reason")
    bad = checked.where("dq_reason IS NOT NULL")
    # txnVersion so a retried batch doesn't write twice
    (good.write.format("delta").mode("append")
         .option("txnAppId", "silver_positions").option("txnVersion", batch_id).saveAsTable(POS))
    (bad.write.format("delta").mode("append")
        .option("txnAppId", "silver_positions_q").option("txnVersion", batch_id).saveAsTable(POS_Q))


(parsed.writeStream
    .foreachBatch(write_positions)
    .option("checkpointLocation", f"{CHECKPOINTS}/silver_positions")
    .trigger(availableNow=True)
    .start()
    .awaitTermination())

for t in (POS, POS_Q):
    print(f"{t}: {spark.table(t).count():,} rows" if table_exists(t) else f"{t}: no rows yet")

# COMMAND ----------

# MAGIC %md ## Trip updates

# COMMAND ----------

PRED = f"{SILVER}.stop_predictions"

if not table_exists(PRED):
    spark.sql(f"""
        CREATE TABLE {PRED} (
            service_date DATE, trip_id STRING, route_id STRING, direction_id INT, vehicle_id STRING,
            stop_sequence INT, stop_id STRING, schedule_relationship STRING,
            predicted_arrival_ts TIMESTAMP, arrival_delay_s INT,
            predicted_departure_ts TIMESTAMP, departure_delay_s INT,
            update_ts TIMESTAMP, first_seen_ts TIMESTAMP, last_seen_ts TIMESTAMP, n_updates BIGINT
        )""")

updates = (
    spark.readStream.table(f"{BRONZE}.raw_trip_updates")
    .withColumn("e", F.from_json("entity_json", TRIP_UPDATE_ENTITY_SCHEMA))
    .where(F.col("e.trip_update").isNotNull())
    .select(
        F.col("e.trip_update.trip.trip_id").alias("trip_id"),
        F.col("e.trip_update.trip.route_id").alias("route_id"),
        F.col("e.trip_update.trip.direction_id").alias("direction_id"),
        F.col("e.trip_update.trip.start_date").alias("start_date"),
        F.col("e.trip_update.vehicle.id").alias("vehicle_id"),
        F.coalesce(F.timestamp_seconds(F.col("e.trip_update.timestamp").cast("bigint")), F.col("feed_ts")).alias("update_ts"),
        F.explode_outer("e.trip_update.stop_time_update").alias("stu"),
    )
    .where(F.col("stu").isNotNull() & F.col("trip_id").isNotNull())
    .select(
        "trip_id", "route_id", "direction_id", "vehicle_id", "update_ts", "start_date",
        F.col("stu.stop_sequence").alias("stop_sequence"),
        F.col("stu.stop_id").alias("stop_id"),
        F.col("stu.schedule_relationship").alias("schedule_relationship"),
        F.timestamp_seconds(F.when(F.col("stu.arrival.time").cast("bigint") > 0, F.col("stu.arrival.time").cast("bigint"))).alias("predicted_arrival_ts"),
        F.col("stu.arrival.delay").alias("arrival_delay_s"),
        F.timestamp_seconds(F.when(F.col("stu.departure.time").cast("bigint") > 0, F.col("stu.departure.time").cast("bigint"))).alias("predicted_departure_ts"),
        F.col("stu.departure.delay").alias("departure_delay_s"),
    )
    .withColumn("service_date", service_date_expr(F.col("start_date"), F.col("update_ts")))
    # stop_sequence isn't always filled in
    .withColumn("stop_key", F.coalesce(F.col("stop_sequence").cast("string"), F.concat(F.lit("id:"), F.col("stop_id"))))
)


def merge_predictions(batch_df, batch_id):
    keys = ["service_date", "trip_id", "stop_key"]
    # merge fails if two source rows hit the same target row, so keep the newest per key
    w = Window.partitionBy(*keys).orderBy(F.col("update_ts").desc())
    agg = batch_df.groupBy(*keys).agg(F.min("update_ts").alias("first_seen_ts"), F.count("*").alias("n_updates"))
    latest = (batch_df.withColumn("rn", F.row_number().over(w)).where("rn = 1").drop("rn")
              .join(agg, keys)
              .withColumn("last_seen_ts", F.col("update_ts")))

    target_key = "coalesce(cast(t.stop_sequence AS STRING), concat('id:', t.stop_id))"
    cols = ["route_id", "direction_id", "vehicle_id", "stop_sequence", "stop_id", "schedule_relationship",
            "predicted_arrival_ts", "arrival_delay_s", "predicted_departure_ts", "departure_delay_s", "update_ts"]
    (DeltaTable.forName(spark, PRED).alias("t")
        .merge(latest.alias("s"),
               f"t.service_date = s.service_date AND t.trip_id = s.trip_id AND {target_key} = s.stop_key")
        .whenMatchedUpdate(
            condition="s.update_ts > t.update_ts",
            set={**{c: f"s.{c}" for c in cols},
                 "last_seen_ts": "s.last_seen_ts",
                 "n_updates": "t.n_updates + s.n_updates"})
        .whenNotMatchedInsert(values={
            **{c: f"s.{c}" for c in cols},
            "service_date": "s.service_date", "trip_id": "s.trip_id",
            "first_seen_ts": "s.first_seen_ts", "last_seen_ts": "s.last_seen_ts", "n_updates": "s.n_updates"})
        .execute())


(updates.writeStream
    .foreachBatch(merge_predictions)
    .option("checkpointLocation", f"{CHECKPOINTS}/silver_predictions")
    .trigger(availableNow=True)
    .start()
    .awaitTermination())

print(f"{PRED}: {spark.table(PRED).count():,} rows")
