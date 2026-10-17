# Databricks notebook source
# MAGIC %md
# MAGIC ## Gold
# MAGIC `fact_stop_event` has one row per trip per stop. Dimensions for route, stop, date and hour, plus the
# MAGIC smaller tables the dashboard reads from.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

events = spark.table(f"{SILVER}.stop_events")
current_version = (spark.table(f"{SILVER}.schedule_versions").where("is_current")
                   .select("feed_version").first().feed_version)

# COMMAND ----------

# MAGIC %md ## Dimensions

# COMMAND ----------

ROUTE_TYPE = {0: "CTrain (LRT)", 1: "Subway", 2: "Rail", 3: "Bus", 4: "Ferry", 5: "Cable tram"}
route_type_name = F.create_map(*[x for k, v in ROUTE_TYPE.items() for x in (F.lit(k), F.lit(v))])

dim_route = (
    spark.table(f"{SILVER}.routes").where(F.col("feed_version") == current_version)
    .select(
        "route_id", "route_short_name", "route_long_name", "route_type",
        F.coalesce(route_type_name[F.col("route_type")], F.lit("Other")).alias("mode"),
        F.concat_ws(" - ", "route_short_name", "route_long_name").alias("route_label"),
    )
)
dim_route.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{GOLD}.dim_route")

dim_stop = (
    spark.table(f"{SILVER}.stops").where(F.col("feed_version") == current_version)
    .select("stop_id", "stop_code", "stop_name", "stop_lat", "stop_lon")
)
dim_stop.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{GOLD}.dim_stop")

assert events.limit(1).count() == 1, "no stop events yet"
bounds = events.agg(F.min("service_date").alias("lo"), F.max("service_date").alias("hi")).first()
dim_date = (
    spark.sql(f"SELECT explode(sequence(DATE'{bounds.lo}', DATE'{bounds.hi}')) AS service_date")
    .select(
        F.date_format("service_date", "yyyyMMdd").cast("int").alias("date_key"),
        "service_date",
        F.date_format("service_date", "EEEE").alias("day_name"),
        F.dayofweek("service_date").isin(1, 7).alias("is_weekend"),
        F.weekofyear("service_date").alias("week_of_year"),
        F.month("service_date").alias("month"),
    )
)
dim_date.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{GOLD}.dim_date")

dim_time = spark.range(24).select(
    F.col("id").cast("int").alias("hour"),
    F.when(F.col("id").between(6, 8), "AM peak")
     .when(F.col("id").between(9, 14), "Midday")
     .when(F.col("id").between(15, 17), "PM peak")
     .when(F.col("id").between(18, 21), "Evening")
     .otherwise("Night").alias("time_band"),
)
dim_time.write.mode("overwrite").saveAsTable(f"{GOLD}.dim_time")

# COMMAND ----------

# MAGIC %md ## Facts

# COMMAND ----------

fact = (
    events.where("actual_arrival_ts IS NOT NULL AND dq_flag IS NULL AND NOT coalesce(is_skipped, false)")
    .select(
        F.date_format("service_date", "yyyyMMdd").cast("int").alias("date_key"),
        "service_date", "route_id", "direction_id", "trip_id", "stop_sequence", "stop_id", "vehicle_id",
        F.hour(F.from_utc_timestamp("scheduled_arrival_ts", LOCAL_TZ)).alias("hour"),
        F.from_utc_timestamp("scheduled_arrival_ts", LOCAL_TZ).alias("scheduled_arrival_local"),
        F.from_utc_timestamp("actual_arrival_ts", LOCAL_TZ).alias("actual_arrival_local"),
        "delay_s", (F.col("delay_s") / 60.0).alias("delay_min"), "on_time",
        (F.col("delay_s") > ON_TIME_LATE_S).alias("is_late"),
        (F.col("delay_s") < ON_TIME_EARLY_S).alias("is_early"),
        "detection_method",
    )
)
fact.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{GOLD}.fact_stop_event")

w_first = Window.partitionBy("service_date", "trip_id").orderBy("stop_sequence")
w_last = Window.partitionBy("service_date", "trip_id").orderBy(F.col("stop_sequence").desc())
trip_summary = (
    spark.table(f"{GOLD}.fact_stop_event")
    .withColumn("is_first", F.row_number().over(w_first) == 1)
    .withColumn("is_last", F.row_number().over(w_last) == 1)
    .groupBy("date_key", "service_date", "route_id", "direction_id", "trip_id")
    .agg(
        F.count("*").alias("stops_observed"),
        F.max(F.when(F.col("is_first"), F.col("delay_s"))).alias("start_delay_s"),
        F.max(F.when(F.col("is_last"), F.col("delay_s"))).alias("end_delay_s"),
        F.max("delay_s").alias("max_delay_s"),
        F.avg(F.col("on_time").cast("int")).alias("on_time_rate"),
    )
    .withColumn("delay_growth_s", F.col("end_delay_s") - F.col("start_delay_s"))
)
trip_summary.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{GOLD}.fact_trip_summary")

# COMMAND ----------

# MAGIC %md ## Dashboard aggregates

# COMMAND ----------

f = spark.table(f"{GOLD}.fact_stop_event")
r = spark.table(f"{GOLD}.dim_route")
t = spark.table(f"{GOLD}.dim_time")
d = spark.table(f"{GOLD}.dim_date")

def otp(df):
    return df.agg(
        F.count("*").alias("arrivals"),
        F.round(100 * F.avg(F.col("on_time").cast("int")), 1).alias("on_time_pct"),
        F.round(100 * F.avg(F.col("is_late").cast("int")), 1).alias("late_pct"),
        F.round(F.avg("delay_min"), 2).alias("avg_delay_min"),
        F.round(F.percentile_approx("delay_min", 0.9), 2).alias("p90_delay_min"),
    )

# per route
otp(f.join(r, "route_id").groupBy("route_id", "route_short_name", "route_label", "mode")) \
    .where("arrivals >= 50") \
    .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{GOLD}.agg_route_scorecard")

# per route and hour
otp(f.join(r, "route_id").join(t, "hour").join(d.select("date_key", "is_weekend"), "date_key")
     .groupBy("route_short_name", "mode", "hour", "time_band", "is_weekend")) \
    .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{GOLD}.agg_otp_by_hour")

# per day
otp(f.join(r, "route_id").groupBy("service_date", "mode")) \
    .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{GOLD}.agg_daily_trend")

# hotspots, needs 30+ arrivals so one bad bus doesn't skew it
otp(f.join(spark.table(f"{GOLD}.dim_stop"), "stop_id")
     .groupBy("stop_id", "stop_name", "stop_lat", "stop_lon")) \
    .where("arrivals >= 30") \
    .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{GOLD}.agg_delay_hotspots")

# avg delay by how far into the trip (tenths)
max_seq = Window.partitionBy("service_date", "trip_id")
(f.withColumn("progress_bucket",
              F.least(F.lit(9), F.floor(10 * (F.col("stop_sequence") - F.min("stop_sequence").over(max_seq))
                                         / (F.max("stop_sequence").over(max_seq) - F.min("stop_sequence").over(max_seq) + 1))))
  .join(r, "route_id")
  .groupBy("mode", "progress_bucket")
  .agg(F.round(F.avg("delay_min"), 2).alias("avg_delay_min"), F.count("*").alias("arrivals"))
  .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{GOLD}.agg_delay_propagation"))

# COMMAND ----------

# MAGIC %md ## Live vehicles

# COMMAND ----------

pos = spark.table(f"{SILVER}.vehicle_positions").where(F.col("event_ts") >= F.current_timestamp() - F.expr("INTERVAL 15 MINUTES"))
w_latest = Window.partitionBy("vehicle_id").orderBy(F.col("event_ts").desc())
latest_delay = (
    spark.table(f"{SILVER}.stop_events").where("actual_arrival_ts IS NOT NULL")
    .withColumn("rn", F.row_number().over(Window.partitionBy("service_date", "trip_id").orderBy(F.col("stop_sequence").desc())))
    .where("rn = 1").select("service_date", "trip_id", (F.col("delay_s") / 60.0).alias("current_delay_min"))
)
live = (
    pos.withColumn("rn", F.row_number().over(w_latest)).where("rn = 1")
    .join(latest_delay, ["service_date", "trip_id"], "left")
    .join(r.select("route_id", "route_short_name", "mode"), "route_id", "left")
    .select("vehicle_id", "vehicle_label", "route_short_name", "mode", "trip_id", "lat", "lon", "bearing",
            F.from_utc_timestamp("event_ts", LOCAL_TZ).alias("last_seen_local"),
            F.round("current_delay_min", 1).alias("current_delay_min"),
            F.when(F.col("current_delay_min") > 5, "Late (>5 min)")
             .when(F.col("current_delay_min") < -1, "Early")
             .when(F.col("current_delay_min").isNotNull(), "On time")
             .otherwise("Unknown").alias("status"))
)
live.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{GOLD}.live_vehicles")

# COMMAND ----------

# MAGIC %md ## Pipeline health

# COMMAND ----------

health = spark.sql(f"""
  SELECT 'bronze.raw_vehicle_positions' AS table_name, count(*) AS row_count, max(_ingested_at) AS last_update FROM {BRONZE}.raw_vehicle_positions
  UNION ALL SELECT 'bronze.raw_trip_updates', count(*), max(_ingested_at) FROM {BRONZE}.raw_trip_updates
  UNION ALL SELECT 'silver.vehicle_positions', count(*), max(_ingested_at) FROM {SILVER}.vehicle_positions
  UNION ALL SELECT 'silver.stop_predictions', count(*), max(last_seen_ts) FROM {SILVER}.stop_predictions
  UNION ALL SELECT 'silver.scheduled_stop_times', count(*), NULL FROM {SILVER}.scheduled_stop_times
  UNION ALL SELECT 'silver.stop_events', count(*), max(computed_at) FROM {SILVER}.stop_events
  UNION ALL SELECT 'gold.fact_stop_event', count(*), NULL FROM {GOLD}.fact_stop_event
""").withColumn("checked_at", F.current_timestamp())
health.write.mode("overwrite").saveAsTable(f"{GOLD}.pipeline_health")
display(health)
