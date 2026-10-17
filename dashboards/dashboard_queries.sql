-- Datasets behind the Calgary Transit Live dashboard. Each query is one dataset.

-- 1. KPI tiles
SELECT
  count(*)                                              AS arrivals_measured,
  round(100 * avg(cast(on_time AS int)), 1)             AS on_time_pct,
  round(avg(delay_min), 2)                              AS avg_delay_min,
  count(DISTINCT route_id)                              AS routes_covered
FROM workspace.transit_gold.fact_stop_event;

-- 2. Live map
SELECT vehicle_id, route_short_name, mode, lat, lon, current_delay_min, status, last_seen_local
FROM workspace.transit_gold.live_vehicles;

-- 3. Route scorecard
SELECT route_short_name AS route, route_label, mode, arrivals, on_time_pct, late_pct, avg_delay_min, p90_delay_min
FROM workspace.transit_gold.agg_route_scorecard
ORDER BY on_time_pct ASC;

-- 4. On-time % by hour, weekday vs weekend
SELECT hour,
       CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
       round(sum(on_time_pct * arrivals) / sum(arrivals), 1) AS on_time_pct,
       sum(arrivals) AS arrivals
FROM workspace.transit_gold.agg_otp_by_hour
GROUP BY ALL
ORDER BY hour;

-- 5. Peak vs off-peak by mode
SELECT time_band, mode,
       round(sum(on_time_pct * arrivals) / sum(arrivals), 1) AS on_time_pct
FROM workspace.transit_gold.agg_otp_by_hour
GROUP BY ALL;

-- 6. Delay hotspots
SELECT stop_name, stop_lat, stop_lon, arrivals, avg_delay_min, late_pct
FROM workspace.transit_gold.agg_delay_hotspots
ORDER BY avg_delay_min DESC
LIMIT 50;

-- 7. Delay vs how far along the trip
SELECT progress_bucket * 10 AS pct_of_trip_completed, mode, avg_delay_min
FROM workspace.transit_gold.agg_delay_propagation
ORDER BY progress_bucket;

-- 8. Daily trend
SELECT service_date, mode, on_time_pct, arrivals
FROM workspace.transit_gold.agg_daily_trend
ORDER BY service_date;

-- 9. Data health
SELECT table_name, row_count, last_update FROM workspace.transit_gold.pipeline_health;

-- 10. Data quality over time
SELECT run_ts, check_name, total, failed, round(100 * pass_rate, 1) AS pass_pct
FROM workspace.transit_gold.dq_results
ORDER BY run_ts DESC, check_name;


-- Overall counts
SELECT
  (SELECT count(*) FROM workspace.transit_bronze.raw_vehicle_positions)  AS raw_position_events,
  (SELECT count(*) FROM workspace.transit_bronze.raw_trip_updates)       AS raw_trip_update_events,
  (SELECT count(*) FROM workspace.transit_silver.scheduled_stop_times)   AS scheduled_stop_rows,
  (SELECT count(*) FROM workspace.transit_gold.fact_stop_event)          AS measured_arrivals,
  (SELECT count(DISTINCT route_id) FROM workspace.transit_gold.fact_stop_event) AS routes,
  (SELECT count(DISTINCT stop_id)  FROM workspace.transit_gold.fact_stop_event) AS stops,
  (SELECT min(poll_ts) FROM workspace.transit_bronze.raw_vehicle_positions) AS data_from,
  (SELECT max(poll_ts) FROM workspace.transit_bronze.raw_vehicle_positions) AS data_to;
