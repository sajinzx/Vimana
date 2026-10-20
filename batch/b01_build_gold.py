"""b01: build the gold Delta tables with Spark SQL (CTEs, window functions, ranking)."""

from __future__ import annotations

from pyspark.sql import SparkSession

from streaming.j1_clean_enrich import delta_path
from streaming.spark_session import build_spark
from vimana_common.fuel import fuel_rate_kg_per_min
from vimana_common.settings import fuel_config

NARROWBODY_RATE = fuel_rate_kg_per_min("narrowbody")
CO2 = float(fuel_config()["co2_per_kg_fuel"])


def register_views(spark: SparkSession) -> dict[str, bool]:
    """Register silver tables as temp views; returns which ones exist."""
    present = {}
    for view, rel in {
        "events": "silver/events",
        "tracks": "silver/tracks",
        "metar": "silver/metar",
        "airport_metrics": "silver/airport_metrics",
        "dq": "silver/dq",
    }.items():
        try:
            spark.read.format("delta").load(delta_path(rel)).createOrReplaceTempView(view)
            present[view] = True
        except Exception:  # noqa: BLE001 - table not produced yet
            present[view] = False
    return present


AIRPORT_HOURLY = """
WITH ev AS (
  SELECT airport, type, icao24, ts,
         date_trunc('hour', timestamp_millis(ts)) AS hour,
         CAST(get_json_object(details, '$.duration_min') AS DOUBLE) AS duration_min,
         CAST(get_json_object(details, '$.extra_co2_kg') AS DOUBLE) AS co2_kg
  FROM events
),
hourly AS (
  SELECT airport, hour,
         COUNT_IF(type = 'LANDED') AS arrivals,
         COUNT_IF(type = 'HOLDING_START') AS holds,
         COUNT_IF(type = 'GO_AROUND') AS go_arounds,
         COALESCE(SUM(CASE WHEN type = 'HOLDING_END' THEN duration_min END), 0.0) AS holding_min,
         COALESCE(SUM(CASE WHEN type = 'LANDED' THEN co2_kg END), 0.0) AS co2_kg
  FROM ev GROUP BY airport, hour
),
traffic AS (
  SELECT airport, date_trunc('hour', timestamp_millis(window_end)) AS hour,
         AVG(CAST(get_json_object(metrics_json, '$.traffic_40nm') AS DOUBLE)) AS avg_traffic
  FROM airport_metrics GROUP BY airport, date_trunc('hour', timestamp_millis(window_end))
)
SELECT h.airport, h.hour, h.arrivals, h.holds, h.go_arounds, h.holding_min, h.co2_kg,
       t.avg_traffic,
       h.holds - LAG(h.holds) OVER (PARTITION BY h.airport ORDER BY h.hour) AS holds_vs_prev_hour,
       AVG(h.holding_min) OVER (PARTITION BY h.airport ORDER BY h.hour
                                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS holding_min_3h_avg
FROM hourly h LEFT JOIN traffic t ON h.airport = t.airport AND h.hour = t.hour
"""

HOLDING_EVENTS = f"""
WITH marks AS (
  SELECT airport, icao24, callsign, type, ts, details,
         LEAD(type) OVER (PARTITION BY airport, icao24 ORDER BY ts) AS next_type,
         LEAD(ts) OVER (PARTITION BY airport, icao24 ORDER BY ts) AS next_ts
  FROM events WHERE type IN ('HOLDING_START', 'HOLDING_END')
)
SELECT airport, icao24, callsign, SUBSTRING(callsign, 1, 3) AS airline,
       ts AS start_ts, next_ts AS end_ts,
       (next_ts - ts) / 60000.0 AS duration_min,
       CAST(get_json_object(details, '$.fix_lat') AS DOUBLE) AS fix_lat,
       CAST(get_json_object(details, '$.fix_lon') AS DOUBLE) AS fix_lon,
       CAST(get_json_object(details, '$.alt_ft') AS DOUBLE) AS alt_ft,
       (next_ts - ts) / 60000.0 * {NARROWBODY_RATE} AS fuel_kg,
       (next_ts - ts) / 60000.0 * {NARROWBODY_RATE} * {CO2} AS co2_kg
FROM marks WHERE type = 'HOLDING_START' AND next_type = 'HOLDING_END'
"""

AIRLINE_STATS = """
WITH landed AS (
  SELECT SUBSTRING(callsign, 1, 3) AS airline,
         CAST(get_json_object(details, '$.holding_min') AS DOUBLE) AS holding_min
  FROM events WHERE type = 'LANDED' AND callsign IS NOT NULL
),
agg AS (
  SELECT airline, COUNT(*) AS flights,
         100.0 * AVG(CASE WHEN holding_min > 0 THEN 1.0 ELSE 0.0 END) AS pct_held,
         AVG(holding_min) AS avg_holding_min
  FROM landed GROUP BY airline
)
SELECT *, RANK() OVER (ORDER BY avg_holding_min DESC) AS rank_by_holding FROM agg
"""

WEATHER_IMPACT = """
WITH landed AS (
  SELECT airport, icao24, ts,
         CAST(get_json_object(details, '$.transit_min') AS DOUBLE) AS transit_min
  FROM events WHERE type = 'LANDED'
),
unimpeded AS (
  SELECT airport, PERCENTILE_APPROX(transit_min, 0.2) AS base_min FROM landed GROUP BY airport
),
labelled AS (
  SELECT l.airport, l.icao24, l.ts, GREATEST(l.transit_min - u.base_min, 0.0) AS additional_min
  FROM landed l JOIN unimpeded u ON l.airport = u.airport
),
joined AS (
  SELECT lb.*, m.visibility_m, m.fog,
         ROW_NUMBER() OVER (PARTITION BY lb.icao24, lb.ts ORDER BY m.obs_ts DESC) AS rn
  FROM labelled lb JOIN metar m ON m.station = lb.airport AND m.obs_ts <= lb.ts
),
latest AS (SELECT * FROM joined WHERE rn = 1)
SELECT CASE WHEN visibility_m < 800 THEN '<800'
            WHEN visibility_m < 1500 THEN '800-1500'
            WHEN visibility_m < 5000 THEN '1500-5000'
            ELSE '>5000' END AS visibility_bucket,
       fog, COUNT(*) AS flights, AVG(additional_min) AS avg_additional_min,
       PERCENTILE_APPROX(additional_min, 0.5) AS median_additional_min
FROM latest GROUP BY 1, 2
"""

DAILY_KPIS = """
WITH ev AS (
  SELECT to_date(timestamp_millis(ts)) AS day, type, icao24,
         CAST(get_json_object(details, '$.extra_co2_kg') AS DOUBLE) AS co2_kg
  FROM events
),
e AS (
  SELECT day, COUNT(DISTINCT icao24) AS flights_tracked,
         COUNT_IF(type = 'HOLDING_START') AS holds,
         COUNT_IF(type = 'GO_AROUND') AS go_arounds,
         COUNT_IF(type = 'DIVERSION_SUSPECTED') AS diversions,
         COALESCE(SUM(CASE WHEN type = 'LANDED' THEN co2_kg END), 0.0) AS holding_co2_kg
  FROM ev GROUP BY day
),
m AS (SELECT date AS day, COUNT(*) AS messages_processed FROM tracks GROUP BY date)
SELECT e.*, m.messages_processed FROM e LEFT JOIN m ON e.day = m.day
"""

DATA_QUALITY = """
SELECT to_date(timestamp_millis(ts)) AS day, 'VALID' AS reason, SUM(valid) AS records
FROM dq GROUP BY 1
UNION ALL
SELECT to_date(timestamp_millis(d.ts)) AS day, r.key AS reason, SUM(r.value) AS records
FROM dq d LATERAL VIEW EXPLODE(from_json(d.invalid_by_reason, 'map<string,long>')) r AS key, value
GROUP BY 1, 2
"""


def write_gold(spark: SparkSession, name: str, sql: str) -> int:
    df = spark.sql(sql)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        delta_path(f"gold/{name}")
    )
    n = df.count()
    print(f"{name}: {n} rows")
    return n


def main() -> None:
    spark = build_spark("vimana-b01")
    spark.sparkContext.setLogLevel("WARN")
    present = register_views(spark)
    print("silver tables present:", present)
    plan = [
        ("gold_airport_hourly", AIRPORT_HOURLY, ["events"]),
        ("gold_holding_events", HOLDING_EVENTS, ["events"]),
        ("gold_airline_stats", AIRLINE_STATS, ["events"]),
        ("gold_weather_impact", WEATHER_IMPACT, ["events", "metar"]),
        ("gold_daily_kpis", DAILY_KPIS, ["events", "tracks"]),
        ("gold_data_quality", DATA_QUALITY, ["dq"]),
    ]
    for name, sql, needs in plan:
        if all(present[n] for n in needs):
            write_gold(spark, name, sql)
        else:
            print(f"skip {name}: missing {[n for n in needs if not present[n]]}")
    spark.stop()


if __name__ == "__main__":
    main()
