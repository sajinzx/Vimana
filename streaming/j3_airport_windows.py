"""J3: sliding-window airport metrics.

Two aggregations (traffic from adsb.clean, event counts from flight.events)
write the same airport_live row. Each cell's timestamp is the window end, so
HBase cell versions form the per-minute time series and a late update of a
window overwrites its own version (idempotent).
"""

from __future__ import annotations

from typing import Any

import orjson
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from streaming.j1_clean_enrich import checkpoint, delta_append, kafka_options, write_kafka
from streaming.j2_flight_state import read_clean
from streaming.sinks.hbase_sink import write_live
from vimana_common.airports import load_airports
from vimana_common.settings import app_config

TRIGGER = "10 seconds"
EVENT_SCHEMA = (
    "event_id string, type string, icao24 string, callsign string, airport string, ts long"
)


def read_events(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .options(**kafka_options())
        .option("subscribe", "flight.events")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
        .select("e.*")
        .withColumn("event_time", (F.col("ts") / 1000).cast("timestamp"))
    )


def traffic_windows(spark: SparkSession) -> DataFrame:
    w = app_config()["windows"]["traffic"]
    clean = read_clean(spark).filter(
        F.col("airport").isNotNull()
        & ~F.col("on_ground")
        & (F.col("dist_nm") <= app_config()["detectors"]["terminal_radius_nm"])
    )
    return (
        clean.withWatermark("event_time", w["watermark"])
        .groupBy(F.window("event_time", w["duration"], w["slide"]), "airport")
        .agg(
            F.approx_count_distinct("icao24").alias("traffic_40nm"),
            F.approx_count_distinct(F.when(F.col("alt_agl_ft") < 5000, F.col("icao24"))).alias(
                "low_alt_count"
            ),
            F.avg("gs_kt").alias("avg_gs_kt"),
            F.count("*").alias("msg_count"),
        )
    )


def event_windows(spark: SparkSession) -> DataFrame:
    w = app_config()["windows"]
    ev = read_events(spark).filter(F.col("airport").isNotNull())
    return (
        ev.withWatermark("event_time", w["traffic"]["watermark"])
        .groupBy(F.window("event_time", w["events"]["duration"], w["events"]["slide"]), "airport")
        .agg(
            F.count(F.when(F.col("type") == "LANDED", 1)).alias("arrivals_15m"),
            F.count(F.when(F.col("type") == "HOLDING_START", 1)).alias("holds_15m"),
            F.count(F.when(F.col("type") == "GO_AROUND", 1)).alias("go_arounds_15m"),
        )
    )


def latest_complete(rows: list[dict[str, Any]], slide_ms: int = 60_000) -> list[dict[str, Any]]:
    """Keep windows that end no later than one slide after the newest window start.

    A sliding window that ends in the future only covers part of its span, so
    publishing it as the "latest" value would understate the metric.
    """
    if not rows:
        return []
    cutoff = max(r["w_start"] for r in rows) + slide_ms
    return [r for r in rows if r["w_end"] <= cutoff]


def _flatten(batch: DataFrame) -> list[dict[str, Any]]:
    out = []
    for r in batch.collect():
        d = r.asDict()
        w = d.pop("window")
        d["w_start"] = int(w["start"].timestamp() * 1000)
        d["w_end"] = int(w["end"].timestamp() * 1000)
        out.append(d)
    return latest_complete(out)


def congestion_alerts(row: dict[str, Any]) -> list[dict[str, Any]]:
    cfg = app_config()["windows"]
    apt = load_airports().get(row["airport"])
    alerts = []
    if apt and row.get("traffic_40nm") is not None:
        if row["traffic_40nm"] > apt.capacity_per_10min * cfg["congestion_traffic_factor"]:
            alerts.append("traffic above capacity")
    if (row.get("holds_15m") or 0) >= cfg["congestion_holds_15m"]:
        alerts.append("multiple aircraft holding")
    return [
        {
            "alert_id": f"{row['airport']}|{row['w_end']}|CONGESTION",
            "kind": "CONGESTION",
            "severity": "warning",
            "airport": row["airport"],
            "ts": row["w_end"],
            "message": f"Congestion at {row['airport']}: {reason}",
            "details": {k: row.get(k) for k in ("traffic_40nm", "holds_15m")},
        }
        for reason in alerts
    ]


def publish(batch: DataFrame, rows: list[dict[str, Any]], batch_id: int, app_id: str) -> None:
    if not rows:
        return
    spark = batch.sparkSession
    write_live(
        (r["airport"], {k: v for k, v in r.items() if k != "airport"}, r["w_end"]) for r in rows
    )
    ddl = "key string, value string"
    metrics = [(r["airport"], orjson.dumps(r).decode()) for r in rows]
    write_kafka(spark.createDataFrame(metrics, ddl), "airport.metrics")
    alerts = [(a["airport"], orjson.dumps(a).decode()) for r in rows for a in congestion_alerts(r)]
    if alerts:
        write_kafka(spark.createDataFrame(alerts, ddl), "alerts")
    delta_append(
        batch.sparkSession.createDataFrame(
            [(r["airport"], r["w_end"], orjson.dumps(r).decode()) for r in rows],
            "airport string, window_end long, metrics_json string",
        ),
        "silver/airport_metrics",
        app_id,
        batch_id,
    )


def process_traffic(batch: DataFrame, batch_id: int) -> None:
    if not batch.isEmpty():
        publish(batch, _flatten(batch), batch_id, "j3_traffic")


def process_events(batch: DataFrame, batch_id: int) -> None:
    if not batch.isEmpty():
        publish(batch, _flatten(batch), batch_id, "j3_events")


def start_window_queries(spark: SparkSession) -> list[StreamingQuery]:
    a = (
        traffic_windows(spark)
        .writeStream.outputMode("update")
        .foreachBatch(process_traffic)
        .option("checkpointLocation", checkpoint("j3_traffic"))
        .trigger(processingTime=TRIGGER)
        .queryName("j3_traffic")
        .start()
    )
    b = (
        event_windows(spark)
        .writeStream.outputMode("update")
        .foreachBatch(process_events)
        .option("checkpointLocation", checkpoint("j3_events"))
        .trigger(processingTime=TRIGGER)
        .queryName("j3_events")
        .start()
    )
    return [a, b]
