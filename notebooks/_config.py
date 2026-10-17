# Databricks notebook source
# MAGIC %md
# MAGIC Shared names and settings, pulled in with `%run ./_config`.

# COMMAND ----------

CATALOG = "workspace"
BRONZE = f"{CATALOG}.transit_bronze"
SILVER = f"{CATALOG}.transit_silver"
GOLD = f"{CATALOG}.transit_gold"

LANDING = "/Volumes/workspace/transit_bronze/landing"
CHECKPOINTS = f"{LANDING}/_checkpoints"
GTFS_EXTRACT = f"{LANDING}/_gtfs_extracted"

LOCAL_TZ = "America/Edmonton"

# rough box around Calgary, anything outside is bad GPS
CALGARY_BBOX = {"lat_min": 50.80, "lat_max": 51.25, "lon_min": -114.40, "lon_max": -113.80}

# on time = no more than 1 min early or 5 min late
ON_TIME_EARLY_S = -60
ON_TIME_LATE_S = 300

# Geospatial stop matching
STOP_MATCH_RADIUS_M = 75     # metres
GRID_LAT_DEG = 0.001         # ~111 m
GRID_LON_DEG = 0.0015        # ~105 m at this latitude

# trips after midnight belong to the previous service day, so when there's no start_date
# shift back a few hours before taking the date
SERVICE_DAY_SHIFT_H = 4

spark.conf.set("spark.sql.session.timeZone", "UTC")

def table_exists(name: str) -> bool:
    return spark.catalog.tableExists(name)
