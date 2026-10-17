# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze: real-time feeds
# MAGIC Auto Loader picks up new files from `landing/<feed>/` and appends them to `raw_<feed>`. The checkpoint
# MAGIC tracks which files are done, so rerunning doesn't duplicate anything.
# MAGIC
# MAGIC The entity is kept as a JSON string here and parsed in Silver, so new fields in the feed can't break
# MAGIC ingestion. `availableNow` processes what's there and stops, which is what serverless supports anyway.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from pyspark.sql import functions as F

ENVELOPE_SCHEMA = """
    feed STRING,
    feed_ts BIGINT,
    poll_ts STRING,
    gtfs_rt_version STRING,
    entity_id STRING,
    entity_json STRING
"""

FEEDS = ["vehicle_positions", "trip_updates", "alerts"]


def ingest(feed: str):
    source = f"{LANDING}/{feed}/"
    target = f"{BRONZE}.raw_{feed}"
    checkpoint = f"{CHECKPOINTS}/bronze_{feed}"

    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.partitionColumns", "")   # don't add a date column from the folder names
        .schema(ENVELOPE_SCHEMA)
        .load(source)
        .withColumn("feed_ts", F.timestamp_seconds("feed_ts"))
        .withColumn("poll_ts", F.to_timestamp("poll_ts"))
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    before = spark.table(target).count() if table_exists(target) else 0
    query = (
        stream.writeStream
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .toTable(target)
    )
    query.awaitTermination()
    after = spark.table(target).count()
    print(f"{feed:<18} +{after - before:,} rows  ->  {target} ({after:,} total)")


for feed in FEEDS:
    landed = {f.name.rstrip("/") for f in dbutils.fs.ls(LANDING)}
    if feed in landed:
        ingest(feed)
    else:
        print(f"{feed:<18} no files landed yet, skipping")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'vehicle_positions' AS feed, count(*) AS rows, max(feed_ts) AS latest_feed_ts FROM workspace.transit_bronze.raw_vehicle_positions
# MAGIC UNION ALL
# MAGIC SELECT 'trip_updates', count(*), max(feed_ts) FROM workspace.transit_bronze.raw_trip_updates
