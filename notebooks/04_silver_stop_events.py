# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver: stop events
# MAGIC Works out when each vehicle actually reached each stop, two ways:
# MAGIC 1. the last prediction we saw before the stop dropped out of the trip update
# MAGIC 2. the closest GPS ping within 75 m of the stop
# MAGIC
# MAGIC Prediction wins if both exist. Comparing every ping to every stop is too slow, so pings and stops are
# MAGIC bucketed into ~100 m grid cells and joined on (trip, cell) first.
# MAGIC
# MAGIC The last couple of service days are rebuilt each run with replaceWhere to pick up late data.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from pyspark.sql import functions as F
from pyspark.sql.window import Window

dbutils.widgets.text("lookback_days", "2")
LOOKBACK = int(dbutils.widgets.get("lookback_days"))

EVENTS = f"{SILVER}.stop_events"
today_local = datetime.now(ZoneInfo(LOCAL_TZ)).date()
# first run builds everything
start_date = today_local - timedelta(days=LOOKBACK) if table_exists(EVENTS) else datetime(2000, 1, 1).date()
print(f"Rebuilding stop events for service_date >= {start_date}")

in_window = F.col("service_date") >= F.lit(start_date)
sched = spark.table(f"{SILVER}.scheduled_stop_times").where(in_window)
preds = spark.table(f"{SILVER}.stop_predictions").where(in_window)
pos = spark.table(f"{SILVER}.vehicle_positions").where(in_window).where("trip_id IS NOT NULL")

# only trips we actually saw
seen_trips = (preds.select("service_date", "trip_id")
              .unionByName(pos.select("service_date", "trip_id")).distinct())
sched = sched.join(seen_trips, ["service_date", "trip_id"], "left_semi")

# COMMAND ----------

# MAGIC %md ## Method 1: trip update predictions

# COMMAND ----------

pred_join = (
    sched.alias("s").join(
        preds.alias("p"),
        (F.col("s.service_date") == F.col("p.service_date"))
        & (F.col("s.trip_id") == F.col("p.trip_id"))
        & (
            (F.col("s.stop_sequence") == F.col("p.stop_sequence"))
            | (F.col("p.stop_sequence").isNull() & (F.col("s.stop_id") == F.col("p.stop_id")))
        ),
    )
    .select(
        "s.service_date", "s.trip_id", "s.stop_sequence",
        F.coalesce(
            # the feed sends time=0 for unknown
            F.when(F.year("p.predicted_arrival_ts") >= 2020, F.col("p.predicted_arrival_ts")),
            F.when(F.year("p.predicted_departure_ts") >= 2020, F.col("p.predicted_departure_ts")),
            F.timestamp_seconds(F.unix_timestamp("s.scheduled_arrival_ts")
                                + F.coalesce("p.arrival_delay_s", "p.departure_delay_s")),
        ).alias("pred_ts"),
        F.col("p.last_seen_ts").alias("pred_last_seen_ts"),
        F.col("p.n_updates").alias("pred_n_updates"),
        F.col("p.schedule_relationship").alias("schedule_relationship"),
        F.col("p.vehicle_id").alias("pred_vehicle_id"),
    )
    # only count it if the predicted time had passed while the stop was still in the feed
    .withColumn("pred_observed",
                F.col("pred_ts").isNotNull()
                & (F.col("pred_ts") <= F.col("pred_last_seen_ts") + F.expr("INTERVAL 2 MINUTES")))
    # stop_id-only matches can hit twice on loop routes
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("service_date", "trip_id", "stop_sequence").orderBy(F.col("pred_last_seen_ts").desc())))
    .where("rn = 1").drop("rn")
)

# COMMAND ----------

# MAGIC %md ## Method 2: closest GPS ping

# COMMAND ----------

def haversine_m(lat1, lon1, lat2, lon2):
    r = 6371000.0
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = F.sin(dlat / 2) ** 2 + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2
    return 2 * r * F.asin(F.sqrt(a))


neighbours = F.array(F.lit(-1), F.lit(0), F.lit(1))

stop_cells = (
    sched.where("stop_lat IS NOT NULL")
    .select("service_date", "trip_id", "stop_sequence", "stop_lat", "stop_lon", "scheduled_arrival_ts")
    # explode has to be its own column, spark won't allow it inside arithmetic
    .withColumn("d_lat", F.explode(neighbours))
    .withColumn("d_lon", F.explode(neighbours))
    .withColumn("cell_lat", F.floor(F.col("stop_lat") / GRID_LAT_DEG) + F.col("d_lat"))
    .withColumn("cell_lon", F.floor(F.col("stop_lon") / GRID_LON_DEG) + F.col("d_lon"))
    .drop("d_lat", "d_lon")
)

ping_cells = (
    pos.select("service_date", "trip_id", "vehicle_id", "event_ts", "lat", "lon")
    .withColumn("cell_lat", F.floor(F.col("lat") / GRID_LAT_DEG))
    .withColumn("cell_lon", F.floor(F.col("lon") / GRID_LON_DEG))
)

pairs = (
    stop_cells.join(ping_cells, ["service_date", "trip_id", "cell_lat", "cell_lon"])
    .withColumn("dist_m", haversine_m(F.col("stop_lat"), F.col("stop_lon"), F.col("lat"), F.col("lon")))
    .where(F.col("dist_m") <= STOP_MATCH_RADIUS_M)
    # out-and-back routes pass stops they serve later, so limit to -20/+90 min of schedule
    .where((F.unix_timestamp("event_ts") - F.unix_timestamp("scheduled_arrival_ts")).between(-1200, 5400))
)

w_closest = Window.partitionBy("service_date", "trip_id", "stop_sequence").orderBy("dist_m", "event_ts")
geo = (
    pairs.withColumn("rn", F.row_number().over(w_closest)).where("rn = 1")
    .select("service_date", "trip_id", "stop_sequence",
            F.col("event_ts").alias("geo_ts"), F.col("dist_m").alias("geo_dist_m"),
            F.col("vehicle_id").alias("geo_vehicle_id"))
)

# COMMAND ----------

# MAGIC %md ## Combine and compute delay

# COMMAND ----------

keys = ["service_date", "trip_id", "stop_sequence"]
events = (
    sched.join(pred_join, keys, "left").join(geo, keys, "left")
    .withColumn("actual_arrival_ts",
                F.when(F.col("pred_observed"), F.col("pred_ts")).otherwise(F.col("geo_ts")))
    .withColumn("detection_method",
                F.when(F.col("pred_observed"), "trip_update")
                 .when(F.col("geo_ts").isNotNull(), "geo")
                 .otherwise(None))
    .withColumn("delay_s",
                (F.unix_timestamp("actual_arrival_ts") - F.unix_timestamp("scheduled_arrival_ts")).cast("int"))
    .withColumn("methods_agree_60s",
                F.when(F.col("pred_observed") & F.col("geo_ts").isNotNull(),
                       F.abs(F.unix_timestamp("pred_ts") - F.unix_timestamp("geo_ts")) <= 60))
    .withColumn("is_skipped", F.col("schedule_relationship") == "SKIPPED")
    .withColumn("dq_flag",
                F.when(F.col("delay_s") < -1800, "too_early")
                 .when(F.col("delay_s") > 7200, "too_late"))
    .withColumn("on_time",
                F.when(F.col("delay_s").isNotNull(),
                       F.col("delay_s").between(ON_TIME_EARLY_S, ON_TIME_LATE_S)))
    .withColumn("vehicle_id", F.coalesce("pred_vehicle_id", "geo_vehicle_id"))
    .withColumn("computed_at", F.current_timestamp())
    .select("service_date", "trip_id", "route_id", "direction_id", "stop_sequence", "stop_id",
            "stop_lat", "stop_lon", "feed_version", "vehicle_id",
            "scheduled_arrival_ts", "actual_arrival_ts", "delay_s", "on_time", "detection_method",
            "pred_ts", "pred_n_updates", "geo_ts", "geo_dist_m", "methods_agree_60s",
            "is_skipped", "dq_flag", "computed_at")
)

writer = events.write.format("delta")
if table_exists(EVENTS):
    writer.mode("overwrite").option("replaceWhere", f"service_date >= '{start_date}'").saveAsTable(EVENTS)
else:
    writer.saveAsTable(EVENTS)

# COMMAND ----------

# MAGIC %md ## Data quality

# COMMAND ----------

e = spark.table(EVENTS).where(in_window)
m = e.agg(
    F.count("*").alias("scheduled_stops"),
    F.count("actual_arrival_ts").alias("observed"),
    F.sum(F.when(F.col("dq_flag").isNotNull(), 1).otherwise(0)).alias("dq_failed"),
    F.sum(F.when(F.col("methods_agree_60s").isNotNull(), 1).otherwise(0)).alias("both_methods"),
    F.sum(F.when(F.col("methods_agree_60s"), 1).otherwise(0)).alias("agree"),
).first()

pos_good = spark.table(f"{SILVER}.vehicle_positions").where(in_window).count()
quarantine = (spark.table(f"{SILVER}.quarantine_vehicle_positions").where(in_window).count()
              if table_exists(f"{SILVER}.quarantine_vehicle_positions") else 0)

m = {k: int(v or 0) for k, v in m.asDict().items()}

def rate(ok, total):
    return float(ok) / total if total else None

checks = [
    ("stop_coverage", m["scheduled_stops"], m["scheduled_stops"] - m["observed"], rate(m["observed"], m["scheduled_stops"])),
    ("delay_in_plausible_range", m["observed"], m["dq_failed"], rate(m["observed"] - m["dq_failed"], m["observed"])),
    ("geo_vs_trip_update_agree_60s", m["both_methods"], m["both_methods"] - m["agree"], rate(m["agree"], m["both_methods"])),
    ("positions_passed_dq", pos_good + quarantine, quarantine, rate(pos_good, pos_good + quarantine)),
]
dq = (spark.createDataFrame(checks, "check_name string, total long, failed long, pass_rate double")
      .withColumn("window_start", F.lit(start_date))
      .withColumn("run_ts", F.current_timestamp()))
dq.write.mode("append").saveAsTable(f"{GOLD}.dq_results")
display(dq)
