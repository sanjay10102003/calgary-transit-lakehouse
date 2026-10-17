# Databricks notebook source
# MAGIC %md
# MAGIC ## Liquid clustering benchmark
# MAGIC Times a few typical queries, clusters the big Silver tables, runs OPTIMIZE, and times them again.
# MAGIC Results go to `transit_gold.benchmarks`. Each query runs 3 times and I keep the median; serverless
# MAGIC caching still makes this noisy.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

import statistics
import time

from pyspark.sql import functions as F

QUERIES = {
    "route_day_delays": f"""
        SELECT stop_sequence, avg(delay_s) FROM {SILVER}.stop_events
        WHERE service_date = (SELECT max(service_date) FROM {SILVER}.stop_events) AND route_id = (
            SELECT route_id FROM {SILVER}.stop_events GROUP BY route_id ORDER BY count(*) DESC LIMIT 1)
        GROUP BY stop_sequence""",
    "vehicle_track": f"""
        SELECT event_ts, lat, lon FROM {SILVER}.vehicle_positions
        WHERE service_date = (SELECT max(service_date) FROM {SILVER}.vehicle_positions)
          AND vehicle_id = (SELECT vehicle_id FROM {SILVER}.vehicle_positions LIMIT 1)
        ORDER BY event_ts""",
    "schedule_lookup": f"""
        SELECT count(*) FROM {SILVER}.scheduled_stop_times
        WHERE service_date = current_date() AND route_id = (SELECT route_id FROM {SILVER}.routes LIMIT 1)""",
    "hourly_otp": f"""
        SELECT hour(scheduled_arrival_ts), avg(cast(on_time AS int)) FROM {SILVER}.stop_events
        WHERE service_date >= date_sub(current_date(), 3) GROUP BY 1""",
}

CLUSTERING = {
    f"{SILVER}.stop_events": ["service_date", "route_id"],
    f"{SILVER}.vehicle_positions": ["service_date", "vehicle_id"],
    f"{SILVER}.scheduled_stop_times": ["service_date", "route_id"],
}


def run_all(phase: str):
    rows = []
    for name, sql in QUERIES.items():
        times = []
        for _ in range(3):
            t0 = time.perf_counter()
            spark.sql(sql).collect()
            times.append(time.perf_counter() - t0)
        rows.append((phase, name, statistics.median(times)))
        print(f"{phase:<7} {name:<18} {statistics.median(times):.2f}s")
    return rows


def table_stats(table):
    d = spark.sql(f"DESCRIBE DETAIL {table}").first()
    return d.numFiles, d.sizeInBytes

# COMMAND ----------

before = run_all("before")
files_before = {t: table_stats(t) for t in CLUSTERING}

# COMMAND ----------

for table, cols in CLUSTERING.items():
    print(f"Clustering {table} by {cols} ...")
    spark.sql(f"ALTER TABLE {table} CLUSTER BY ({', '.join(cols)})")
    spark.sql(f"OPTIMIZE {table}")

# COMMAND ----------

after = run_all("after")
files_after = {t: table_stats(t) for t in CLUSTERING}

# COMMAND ----------

results = []
for (_, name, b), (_, _, a) in zip(before, after):
    results.append((name, round(b, 3), round(a, 3), round(100 * (b - a) / b, 1)))
bench = (spark.createDataFrame(results, "query string, before_s double, after_s double, improvement_pct double")
         .withColumn("measured_at", F.current_timestamp()))
bench.write.mode("append").saveAsTable(f"{GOLD}.benchmarks")
display(bench)

for t in CLUSTERING:
    print(f"{t}: files {files_before[t][0]} -> {files_after[t][0]}, size {files_before[t][1]/1e6:.0f} MB -> {files_after[t][1]/1e6:.0f} MB")
