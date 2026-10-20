"""b04: train the terminal-area additional-time model (Spark MLlib + MLflow).

GBTRegressor with a time-based train/test split and TrainValidationSplit,
compared with a per-airport median baseline and linear regression. The model
is saved under DATA_DIR/models/delay/v{timestamp} and `current` is repointed
atomically so the streaming job hot-reloads it.
"""

from __future__ import annotations

import json
import os
import shutil
import time
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from batch.b03_build_training_set import models_dir
from streaming.j1_clean_enrich import delta_path
from streaming.spark_session import build_spark
from vimana_common.features import CATEGORICAL, NUMERIC

LABEL = "additional_min"
MIN_ROWS = 50


def build_pipeline(estimator) -> Pipeline:
    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in CATEGORICAL
    ]
    encoder = OneHotEncoder(
        inputCols=[f"{c}_idx" for c in CATEGORICAL],
        outputCols=[f"{c}_oh" for c in CATEGORICAL],
        handleInvalid="keep",
    )
    assembler = VectorAssembler(
        inputCols=[f"{c}_oh" for c in CATEGORICAL] + NUMERIC,
        outputCol="features",
        handleInvalid="keep",
    )
    return Pipeline(stages=[*indexers, encoder, assembler, estimator])


def time_split(df: DataFrame, test_fraction: float = 0.2) -> tuple[DataFrame, DataFrame]:
    """Last 20 % of the time range is the test set (no random split, no leakage)."""
    lo, hi = df.agg(F.min("entry_ts"), F.max("entry_ts")).first()
    cutoff = hi - (hi - lo) * test_fraction
    return df.filter(F.col("entry_ts") < cutoff), df.filter(F.col("entry_ts") >= cutoff)


def evaluate(pred: DataFrame) -> dict:
    def metric(name: str, df: DataFrame = pred) -> float:
        return RegressionEvaluator(
            labelCol=LABEL, predictionCol="prediction", metricName=name
        ).evaluate(df)

    out = {"mae": metric("mae"), "rmse": metric("rmse"), "r2": metric("r2"), "n": pred.count()}
    out["mae_by_airport"] = {
        r["airport"]: r["mae"]
        for r in pred.groupBy("airport")
        .agg(F.avg(F.abs(F.col(LABEL) - F.col("prediction"))).alias("mae"))
        .collect()
    }
    out["mae_by_fog"] = {
        str(int(r["fog"])): r["mae"]
        for r in pred.groupBy("fog")
        .agg(F.avg(F.abs(F.col(LABEL) - F.col("prediction"))).alias("mae"))
        .collect()
    }
    return out


def baseline_predictions(train: DataFrame, test: DataFrame) -> DataFrame:
    med = train.groupBy("airport").agg(F.percentile_approx(LABEL, 0.5).alias("prediction"))
    overall = train.agg(F.percentile_approx(LABEL, 0.5)).first()[0]
    return test.join(med, "airport", "left").withColumn(
        "prediction", F.coalesce(F.col("prediction"), F.lit(overall))
    )


def importances_by_name(model, sample: DataFrame) -> dict[str, float]:
    assembled = model.transform(sample.limit(1))
    meta = assembled.schema["features"].metadata["ml_attr"]["attrs"]
    attrs = sorted((a for group in meta.values() for a in group), key=lambda a: a["idx"])
    names = [a["name"] for a in attrs]
    imp = model.stages[-1].featureImportances.toArray()
    ranked = {n: float(v) for n, v in zip(names, imp, strict=False) if v > 0}
    return dict(sorted(ranked.items(), key=lambda kv: -kv[1])[:25])


def publish(version: str, src: Path) -> None:
    """Atomically repoint models/delay/current to the new version."""
    root = models_dir()
    staging = root / "current.new"
    if staging.exists():
        shutil.rmtree(staging)
    shutil.copytree(src, staging)
    (staging / "_SUCCESS").write_text(version)
    old = root / "current.old"
    cur = root / "current"
    if old.exists():
        shutil.rmtree(old)
    if cur.exists():
        os.replace(cur, old)
    os.replace(staging, cur)
    if old.exists():
        shutil.rmtree(old)


def main() -> None:
    spark = build_spark("vimana-b04")
    spark.sparkContext.setLogLevel("WARN")
    df = spark.read.format("delta").load(delta_path("gold/training_set")).cache()
    n = df.count()
    if n < MIN_ROWS:
        raise SystemExit(f"only {n} training rows; need at least {MIN_ROWS} (run make seed)")
    train, test = time_split(df)
    print(f"train={train.count()} test={test.count()}")

    gbt = GBTRegressor(
        labelCol=LABEL,
        featuresCol="features",
        maxIter=80,
        maxDepth=5,
        stepSize=0.1,
        subsamplingRate=0.8,
        seed=7,
    )
    pipe = build_pipeline(gbt)
    grid = (
        ParamGridBuilder()
        .addGrid(gbt.maxDepth, [4, 5, 6])
        .addGrid(gbt.maxIter, [60, 100])
        .addGrid(gbt.stepSize, [0.05, 0.1])
        .build()
    )
    tvs = TrainValidationSplit(
        estimator=pipe,
        estimatorParamMaps=grid,
        evaluator=RegressionEvaluator(labelCol=LABEL, metricName="mae"),
        trainRatio=0.8,
        seed=7,
    )
    model = tvs.fit(train).bestModel
    gbt_metrics = evaluate(model.transform(test))
    lin_model = build_pipeline(
        LinearRegression(labelCol=LABEL, featuresCol="features", regParam=0.1)
    ).fit(train)
    lin_metrics = evaluate(lin_model.transform(test))
    base_metrics = evaluate(baseline_predictions(train, test))
    improvement = (
        100 * (1 - gbt_metrics["mae"] / base_metrics["mae"]) if base_metrics["mae"] else 0.0
    )
    print(
        f"MAE gbt={gbt_metrics['mae']:.3f} linear={lin_metrics['mae']:.3f} "
        f"baseline={base_metrics['mae']:.3f}"
    )
    print(f"improvement over per-airport median: {improvement:.1f}%")

    version = "v" + time.strftime("%Y%m%d%H%M%S")
    vdir = models_dir() / version
    model.write().overwrite().save(str(vdir / "pipeline"))
    best = model.stages[-1]
    metrics = {
        "version": version,
        "trained_at": int(time.time() * 1000),
        "train_rows": train.count(),
        "test_rows": test.count(),
        "gbt": gbt_metrics,
        "linear_regression": lin_metrics,
        "baseline_median": base_metrics,
        "improvement_over_baseline_pct": round(improvement, 2),
        "params": {
            "maxDepth": best.getMaxDepth(),
            "maxIter": best.getMaxIter(),
            "stepSize": best.getStepSize(),
        },
        "feature_importances": importances_by_name(model, test),
        "test_predictions": [
            {"actual": r[LABEL], "predicted": r["prediction"]}
            for r in model.transform(test).select(LABEL, "prediction").limit(500).collect()
        ],
    }
    (vdir / "metrics.json").write_text(json.dumps(metrics, indent=2))
    (vdir / "version.txt").write_text(version)

    try:
        import mlflow

        mlflow.set_tracking_uri("file:/mlruns")
        mlflow.set_experiment("vimana-delay")
        with mlflow.start_run(run_name=version):
            mlflow.log_params(metrics["params"])
            for name, m in (
                ("gbt", gbt_metrics),
                ("linear", lin_metrics),
                ("baseline", base_metrics),
            ):
                for k in ("mae", "rmse", "r2"):
                    mlflow.log_metric(f"{name}_{k}", m[k])
            mlflow.log_metric("improvement_pct", improvement)
            mlflow.log_artifact(str(vdir / "metrics.json"))
    except Exception as exc:  # noqa: BLE001 - tracking is optional
        print("mlflow logging skipped:", exc)

    publish(version, vdir)
    print("published", version)
    spark.stop()


if __name__ == "__main__":
    main()
