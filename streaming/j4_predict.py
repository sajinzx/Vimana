"""J4: stream-stream join of terminal entries with weather, plus live prediction.

ENTERED_TERMINAL events are left-outer joined to the METARs observed in the
preceding 90 minutes. Each entry is scored with the current MLlib model, or a
per-airport median baseline until a model has been trained.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import orjson
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from streaming.j1_clean_enrich import (
    checkpoint,
    delta_append,
    kafka_options,
    metar_stream,
    write_kafka,
)
from streaming.sinks.hbase_sink import prediction_mutation, write_rows
from vimana_common.features import FEATURE_COLUMNS, build_features
from vimana_common.hbase_client import connect
from vimana_common.settings import app_config, get_settings

DEFAULT_BASELINE_MIN = 4.0
ENTRY_SCHEMA = (
    "event_id string, type string, icao24 string, callsign string, airport string, ts long, "
    "details struct<entry_bearing:double, alt_ft:double, gs_kt:double, category:string>"
)
WX_FIELDS = ["visibility_m", "ceiling_ft", "wind_kt", "wind_dir_deg", "fog", "low_vis"]


def models_dir() -> Path:
    return Path(get_settings().data_dir) / "models" / "delay"


def entries_stream(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .options(**kafka_options())
        .option("subscribe", "flight.events")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        .select(F.from_json(F.col("value").cast("string"), ENTRY_SCHEMA).alias("e"))
        .select("e.*")
        .filter(F.col("type") == "ENTERED_TERMINAL")
        .select(
            F.col("event_id").alias("e_id"),
            F.col("icao24").alias("e_icao24"),
            F.col("callsign").alias("e_callsign"),
            F.col("airport").alias("e_airport"),
            F.col("ts").alias("e_ts"),
            F.col("details.entry_bearing").alias("e_bearing"),
            F.col("details.alt_ft").alias("e_alt_ft"),
            F.col("details.gs_kt").alias("e_gs_kt"),
            F.col("details.category").alias("e_category"),
            (F.col("ts") / 1000).cast("timestamp").alias("e_time"),
        )
    )


def joined_stream(spark: SparkSession) -> DataFrame:
    entries = entries_stream(spark).withWatermark("e_time", "10 minutes")
    wx = metar_stream(spark).select(
        F.col("station").alias("w_station"),
        F.col("obs_time").alias("w_time"),
        *[F.col(c).alias(f"w_{c}") for c in WX_FIELDS],
    )
    wx = wx.withWatermark("w_time", "3 hours")
    cond = F.expr(
        """
        e_airport = w_station AND
        w_time BETWEEN e_time - INTERVAL 90 MINUTES AND e_time
        """
    )
    return entries.join(wx, cond, "leftOuter")


class ModelHolder:
    """Loads the current PipelineModel and reloads it when _SUCCESS changes."""

    def __init__(self) -> None:
        self._mtime: float | None = None
        self._model: Any = None
        self.version = "baseline"

    def get(self) -> Any:
        marker = models_dir() / "current" / "_SUCCESS"
        if not marker.exists():
            self._model, self._mtime, self.version = None, None, "baseline"
            return None
        mtime = marker.stat().st_mtime
        if mtime != self._mtime:
            from pyspark.ml import PipelineModel

            cur = models_dir() / "current"
            self._model = PipelineModel.load(str(cur / "pipeline"))
            vfile = cur / "version.txt"
            self.version = vfile.read_text().strip() if vfile.exists() else "unknown"
            self._mtime = mtime
        return self._model


def load_baseline() -> dict[str, float]:
    path = models_dir() / "baseline.json"
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return {}


def fetch_live(airports: set[str]) -> dict[str, dict[str, float]]:
    """Latest airport_live counters per airport."""
    out: dict[str, dict[str, float]] = {}
    conn = connect()
    try:
        table = conn.table("airport_live")
        for a in airports:
            row = table.row(a.encode(), columns=[b"c"])
            out[a] = {k.decode().split(":", 1)[1]: float(v) for k, v in row.items()}
    finally:
        conn.close()
    return out


def latest_per_entry(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep the most recent METAR (by observation time) for each entry event."""
    best: dict[str, dict[str, Any]] = {}
    for r in rows:
        cur = best.get(r["e_id"])
        if cur is None or (
            r["w_time"] is not None and (cur["w_time"] is None or r["w_time"] > cur["w_time"])
        ):
            best[r["e_id"]] = r
    return list(best.values())


def entry_features(r: dict[str, Any], live: dict[str, float] | None) -> dict[str, Any]:
    wx = None
    if r.get("w_station") is not None:
        wx = {c: r.get(f"w_{c}") for c in WX_FIELDS}
    entry = {
        "airport": r["e_airport"],
        "ts": r["e_ts"],
        "entry_bearing": r["e_bearing"],
        "category": r["e_category"],
        "alt_ft": r["e_alt_ft"],
        "gs_kt": r["e_gs_kt"],
    }
    return build_features(entry, wx, live)


def score(
    spark: SparkSession,
    feats: list[dict[str, Any]],
    holder: ModelHolder,
    baseline: dict[str, float],
) -> tuple[list[float], str]:
    model = holder.get()
    if model is None:
        return [float(baseline.get(f["airport"], DEFAULT_BASELINE_MIN)) for f in feats], "baseline"
    pdf = spark.createDataFrame([{c: f[c] for c in FEATURE_COLUMNS} for f in feats])
    preds = [
        float(max(r["prediction"], 0.0))
        for r in model.transform(pdf).select("prediction").collect()
    ]
    return preds, holder.version


def make_batch_processor():
    holder = ModelHolder()

    def process(batch: DataFrame, batch_id: int) -> None:
        if batch.isEmpty():
            return
        spark = batch.sparkSession
        rows = latest_per_entry([r.asDict() for r in batch.collect()])
        live = fetch_live({r["e_airport"] for r in rows})
        feats = [entry_features(r, live.get(r["e_airport"])) for r in rows]
        preds, version = score(spark, feats, holder, load_baseline())
        threshold = app_config()["windows"]["alert_prediction_min"]

        out, kafka_rows, alerts = [], [], []
        for r, f, p in zip(rows, feats, preds, strict=True):
            rec = {
                "icao24": r["e_icao24"],
                "callsign": r["e_callsign"],
                "airport": r["e_airport"],
                "entry_ts": r["e_ts"],
                "pred_min": round(p, 2),
                "model_version": version,
                "wx_missing": f["wx_missing"],
                "features": json.dumps(f, separators=(",", ":")),
            }
            out.append(rec)
            kafka_rows.append((rec["icao24"], orjson.dumps({**rec, "features": f}).decode()))
            if p >= threshold:
                alert = {
                    "alert_id": f"{rec['airport']}|{rec['icao24']}|{rec['entry_ts']}|DELAY",
                    "kind": "LONG_DELAY_PREDICTED",
                    "severity": "warning",
                    "airport": rec["airport"],
                    "icao24": rec["icao24"],
                    "callsign": rec["callsign"],
                    "ts": rec["entry_ts"],
                    "message": f"{rec['callsign']} predicted +{p:.0f} min at {rec['airport']}",
                }
                alerts.append((rec["airport"], orjson.dumps(alert).decode()))

        ddl = "key string, value string"
        write_rows("predictions", (prediction_mutation(o) for o in out))
        write_kafka(spark.createDataFrame(kafka_rows, ddl), "predictions")
        if alerts:
            write_kafka(spark.createDataFrame(alerts, ddl), "alerts")
        delta_append(
            spark.createDataFrame(
                [
                    (
                        o["icao24"],
                        o["airport"],
                        o["entry_ts"],
                        o["pred_min"],
                        o["model_version"],
                        o["features"],
                    )
                    for o in out
                ],
                "icao24 string, airport string, entry_ts long, pred_min double, "
                "model_version string, features string",
            ),
            "silver/predictions",
            "j4_predictions",
            batch_id,
        )

    return process


def start_predict_query(spark: SparkSession) -> StreamingQuery:
    return (
        joined_stream(spark)
        .writeStream.outputMode("append")
        .foreachBatch(make_batch_processor())
        .option("checkpointLocation", checkpoint("j4_predict"))
        .trigger(processingTime="5 seconds")
        .queryName("j4_predict")
        .start()
    )
