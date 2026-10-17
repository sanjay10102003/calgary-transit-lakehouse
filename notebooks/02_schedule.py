# Databricks notebook source
# MAGIC %md
# MAGIC ## Schedule (static GTFS)
# MAGIC Loads any new schedule zips into Bronze, one table per GTFS file, each row tagged with `feed_version`.
# MAGIC Versions get valid_from/valid_to dates so each service day uses the schedule that applied that day.
# MAGIC Then expands everything into `scheduled_stop_times`: one row per service date, trip and stop, in UTC.
# MAGIC GTFS times can be past 24:00 (25:30:00 is 1:30 AM the next day).

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

import os
import zipfile
from datetime import date, timedelta

from pyspark.sql import functions as F
from pyspark.sql.window import Window

STATIC_DIR = f"{LANDING}/gtfs_static"
VERSIONS = f"{BRONZE}.gtfs_feed_versions"

# COMMAND ----------

# MAGIC %md ## Load new schedule zips

# COMMAND ----------

def clean_columns(df):
    """GTFS CSVs sometimes carry a UTF-8 BOM or stray spaces in the header."""
    return df.toDF(*[c.strip().lstrip("﻿") for c in df.columns])


def load_version(zip_path: str, version: str, first_seen: date) -> list[str]:
    target = f"{GTFS_EXTRACT}/{version}"
    os.makedirs(target, exist_ok=True)
    with zipfile.ZipFile(zip_path) as z:
        z.extractall(target)

    loaded = []
    for fname in sorted(os.listdir(target)):
        if not fname.endswith(".txt"):
            continue
        name = fname[:-4]
        df = (spark.read.option("header", True).option("inferSchema", False)  # strings only in bronze
              .csv(f"{target}/{fname}"))
        df = clean_columns(df).withColumn("feed_version", F.lit(version))
        table = f"{BRONZE}.gtfs_{name}"
        if table_exists(table):
            spark.sql(f"DELETE FROM {table} WHERE feed_version = '{version}'")
            df.write.mode("append").option("mergeSchema", True).saveAsTable(table)
        else:
            df.write.saveAsTable(table)
        loaded.append(f"{name} ({df.count():,} rows)")

    row = spark.createDataFrame([(version, zip_path, first_seen)], "feed_version string, zip_path string, first_seen date")
    row = row.withColumn("loaded_at", F.current_timestamp())
    row.write.mode("append").saveAsTable(VERSIONS)
    return loaded


already = set()
if table_exists(VERSIONS):
    already = {r.feed_version for r in spark.table(VERSIONS).select("feed_version").collect()}

zips = sorted(f for f in os.listdir(STATIC_DIR) if f.endswith(".zip")) if os.path.exists(STATIC_DIR) else []
new = [z for z in zips if z[:-4] not in already]
print(f"Found {len(zips)} schedule zip(s), {len(new)} new")

for z in new:
    version = z[:-4]                                    # gtfs_YYYYMMDD_<hash>
    first_seen = date(int(version[5:9]), int(version[9:11]), int(version[11:13]))
    print(f"Loading {version} ...")
    for line in load_version(f"{STATIC_DIR}/{z}", version, first_seen):
        print("   ", line)

assert table_exists(VERSIONS), "no schedule yet, run poller.py --static"

# COMMAND ----------

# MAGIC %md ## Schedule versions
# MAGIC The first version loaded also covers any earlier dates.

# COMMAND ----------

w = Window.orderBy("first_seen", "loaded_at")
versions = (
    spark.table(VERSIONS)
    .withColumn("rn", F.row_number().over(w))
    .withColumn("valid_from", F.when(F.col("rn") == 1, F.lit("1900-01-01").cast("date")).otherwise(F.col("first_seen")))
    .withColumn("valid_to", F.coalesce(F.lead("first_seen").over(w), F.lit("9999-12-31").cast("date")))
    .withColumn("is_current", F.col("valid_to") == F.lit("9999-12-31").cast("date"))
    .select("feed_version", "valid_from", "valid_to", "is_current", "first_seen", "loaded_at")
)
versions.write.mode("overwrite").saveAsTable(f"{SILVER}.schedule_versions")
display(spark.table(f"{SILVER}.schedule_versions"))

# COMMAND ----------

# MAGIC %md ## Typed reference tables

# COMMAND ----------

stops = (spark.table(f"{BRONZE}.gtfs_stops")
         .select("feed_version", "stop_id", "stop_code", "stop_name",
                 F.col("stop_lat").cast("double").alias("stop_lat"),
                 F.col("stop_lon").cast("double").alias("stop_lon")))
stops.write.mode("overwrite").saveAsTable(f"{SILVER}.stops")

routes_cols = spark.table(f"{BRONZE}.gtfs_routes").columns
routes = (spark.table(f"{BRONZE}.gtfs_routes")
          .select("feed_version", "route_id",
                  F.col("route_short_name") if "route_short_name" in routes_cols else F.lit(None).alias("route_short_name"),
                  F.col("route_long_name") if "route_long_name" in routes_cols else F.lit(None).alias("route_long_name"),
                  F.col("route_type").cast("int").alias("route_type")))
routes.write.mode("overwrite").saveAsTable(f"{SILVER}.routes")

trips_cols = spark.table(f"{BRONZE}.gtfs_trips").columns
trips = (spark.table(f"{BRONZE}.gtfs_trips")
         .select("feed_version", "trip_id", "route_id", "service_id",
                 (F.col("direction_id").cast("int") if "direction_id" in trips_cols else F.lit(None).cast("int")).alias("direction_id"),
                 (F.col("trip_headsign") if "trip_headsign" in trips_cols else F.lit(None).cast("string")).alias("trip_headsign")))
trips.write.mode("overwrite").saveAsTable(f"{SILVER}.trips")

# COMMAND ----------

# MAGIC %md ## Build scheduled_stop_times for missing dates

# COMMAND ----------

SCHED = f"{SILVER}.scheduled_stop_times"
today = date.today()

# from the first realtime data (or 2 days ago) to tomorrow
start = today - timedelta(days=2)
if table_exists(f"{BRONZE}.raw_trip_updates"):
    first_rt = spark.table(f"{BRONZE}.raw_trip_updates").agg(F.min("poll_ts")).first()[0]
    if first_rt:
        start = min(start, first_rt.date() - timedelta(days=1))
end = today + timedelta(days=1)

dates = spark.sql(f"SELECT explode(sequence(DATE'{start}', DATE'{end}')) AS service_date")
if table_exists(SCHED):
    dates = dates.join(spark.table(SCHED).select("service_date").distinct(), "service_date", "left_anti")
# no .cache() on serverless, the list is tiny anyway
date_list = [r.service_date for r in dates.collect()]
dates = spark.createDataFrame([(d,) for d in date_list], "service_date date")
print(f"Service dates to build: {[d.isoformat() for d in date_list]}")

# COMMAND ----------

def gtfs_seconds(col):
    """'25:30:00' -> 91800 seconds after midnight of the service date."""
    parts = F.split(F.trim(col), ":")
    return parts[0].cast("int") * 3600 + parts[1].cast("int") * 60 + parts[2].cast("int")


def local_to_utc(service_date_col, seconds_col):
    """Service-date midnight (local) + seconds, converted to a UTC timestamp."""
    local_wall_clock = F.timestamp_seconds(F.unix_timestamp(F.to_timestamp(service_date_col)) + seconds_col)
    return F.to_utc_timestamp(local_wall_clock, LOCAL_TZ)


if date_list:
    # which schedule version applies to each date
    date_version = dates.join(
        spark.table(f"{SILVER}.schedule_versions"),
        (F.col("service_date") >= F.col("valid_from")) & (F.col("service_date") < F.col("valid_to")),
    ).select("service_date", "feed_version")

    # calendar.txt is the weekly pattern, calendar_dates.txt adds/removes specific days
    empty = spark.createDataFrame([], "feed_version string, service_date date, service_id string")
    active = empty
    if table_exists(f"{BRONZE}.gtfs_calendar"):
        cal = spark.table(f"{BRONZE}.gtfs_calendar")
        day_flags = F.array("sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday")
        active = (cal.join(date_version, "feed_version")
                  .where(F.col("service_date").between(F.to_date("start_date", "yyyyMMdd"), F.to_date("end_date", "yyyyMMdd")))
                  .where(F.element_at(day_flags, F.dayofweek("service_date")) == "1")
                  .select("feed_version", "service_date", "service_id"))
    added, removed = empty, empty
    if table_exists(f"{BRONZE}.gtfs_calendar_dates"):
        cd = (spark.table(f"{BRONZE}.gtfs_calendar_dates")
              .withColumn("service_date", F.to_date("date", "yyyyMMdd"))
              .join(date_version, ["feed_version", "service_date"]))
        added = cd.where("exception_type = '1'").select("feed_version", "service_date", "service_id")
        removed = cd.where("exception_type = '2'").select("feed_version", "service_date", "service_id")
    services = active.unionByName(added).distinct().join(removed, ["feed_version", "service_date", "service_id"], "left_anti")

    st = spark.table(f"{BRONZE}.gtfs_stop_times")
    arr_s = gtfs_seconds(F.coalesce(F.nullif(F.col("arrival_time"), F.lit("")), F.col("departure_time")))
    dep_s = gtfs_seconds(F.coalesce(F.nullif(F.col("departure_time"), F.lit("")), F.col("arrival_time")))

    scheduled = (
        services
        .join(spark.table(f"{SILVER}.trips"), ["feed_version", "service_id"])
        .join(st.select("feed_version", "trip_id", "stop_id",
                        F.col("stop_sequence").cast("int").alias("stop_sequence"),
                        arr_s.alias("arr_s"), dep_s.alias("dep_s")),
              ["feed_version", "trip_id"])
        .join(spark.table(f"{SILVER}.stops").select("feed_version", "stop_id", "stop_lat", "stop_lon"),
              ["feed_version", "stop_id"], "left")
        .where(F.col("arr_s").isNotNull())
        .select(
            "service_date", "feed_version", "trip_id", "route_id", "direction_id",
            "stop_sequence", "stop_id", "stop_lat", "stop_lon",
            local_to_utc("service_date", F.col("arr_s")).alias("scheduled_arrival_ts"),
            local_to_utc("service_date", F.col("dep_s")).alias("scheduled_departure_ts"),
        )
    )
    scheduled.write.mode("append").saveAsTable(SCHED)

display(spark.table(SCHED).groupBy("service_date", "feed_version").count().orderBy("service_date"))
