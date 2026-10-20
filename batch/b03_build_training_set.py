"""b03: build the delay-model training set from silver events, METAR and metrics."""

from __future__ import annotations

import json
from pathlib import Path

from batch.training_data import build_training_frame
from streaming.j1_clean_enrich import delta_path
from streaming.spark_session import build_spark
from vimana_common.settings import get_settings


def models_dir() -> Path:
    return Path(get_settings().data_dir) / "models" / "delay"


def main() -> None:
    spark = build_spark("vimana-b03")
    spark.sparkContext.setLogLevel("WARN")
    load = lambda rel: spark.read.format("delta").load(delta_path(rel)).toPandas()  # noqa: E731
    events = load("silver/events")
    metar = load("silver/metar")
    metrics = load("silver/airport_metrics")

    frame, unimpeded, baseline = build_training_frame(events, metar, metrics)
    if frame.empty:
        raise SystemExit("no completed arrivals yet; run the stack longer (make seed)")
    print(f"training rows: {len(frame)}; airports: {sorted(frame['airport'].unique())}")

    spark.createDataFrame(frame).write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(delta_path("gold/training_set"))

    out = models_dir()
    out.mkdir(parents=True, exist_ok=True)
    (out / "unimpeded.json").write_text(json.dumps(unimpeded, indent=2))
    (out / "baseline.json").write_text(json.dumps(baseline, indent=2))
    print("wrote unimpeded.json and baseline.json to", out)
    spark.stop()


if __name__ == "__main__":
    main()
