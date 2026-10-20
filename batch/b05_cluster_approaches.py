"""b05: cluster approach paths per airport with MLlib KMeans and flag anomalies."""

from __future__ import annotations

import json

import numpy as np
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler
from pyspark.ml.linalg import Vectors

from batch.trajectory import anomaly_flags, extract_approaches, vector_to_latlon
from streaming.j1_clean_enrich import delta_path
from streaming.spark_session import build_spark
from vimana_common.airports import load_airports

K_RANGE = range(3, 11)
MIN_FLIGHTS = 12


def cluster_airport(spark, airport: str, items: list[dict]):
    """Best-silhouette KMeans for one airport; returns (k, silhouette, labels, scaled, centers)."""
    df = spark.createDataFrame([(Vectors.dense(i["vector"].tolist()),) for i in items], ["raw"])
    scaler = StandardScaler(inputCol="raw", outputCol="features", withMean=True, withStd=True).fit(
        df
    )
    scaled = scaler.transform(df).cache()
    evaluator = ClusteringEvaluator(featuresCol="features", metricName="silhouette")
    best = None
    for k in K_RANGE:
        if k >= len(items):
            break
        model = KMeans(k=k, seed=7, featuresCol="features", maxIter=50).fit(scaled)
        score = evaluator.evaluate(model.transform(scaled))
        if best is None or score > best[0]:
            best = (score, k, model)
    score, k, model = best
    feats = np.array([r["features"].toArray() for r in scaled.collect()])
    labels = np.array(
        [r["prediction"] for r in model.transform(scaled).select("prediction").collect()]
    )
    return k, score, labels, feats, np.array(model.clusterCenters())


def main() -> None:
    spark = build_spark("vimana-b05")
    spark.sparkContext.setLogLevel("WARN")
    tracks = (
        spark.read.format("delta")
        .load(delta_path("silver/tracks"))
        .select("icao24", "airport", "ts", "lat", "lon", "dist_nm", "on_ground")
        .toPandas()
    )
    airports = load_airports()
    coords = {a.icao: (a.lat, a.lon) for a in airports.values()}
    approaches = extract_approaches(tracks, coords)
    print(f"landed approaches: {len(approaches)}")

    cluster_rows, anomaly_rows = [], []
    for icao, apt in airports.items():
        items = [a for a in approaches if a["airport"] == icao]
        if len(items) < MIN_FLIGHTS:
            print(f"{icao}: {len(items)} approaches, need {MIN_FLIGHTS}; skipped")
            continue
        k, score, labels, feats, centers = cluster_airport(spark, icao, items)
        raw = np.array([i["vector"] for i in items])
        dist, flags = anomaly_flags(feats, labels, centers)
        print(f"{icao}: k={k} silhouette={score:.3f} anomalies={int(flags.sum())}")
        for c in range(k):
            members = raw[labels == c]
            cluster_rows.append(
                (
                    icao,
                    int(c),
                    int(k),
                    int(len(members)),
                    float(score),
                    json.dumps(vector_to_latlon(members.mean(axis=0), apt.lat, apt.lon)),
                )
            )
        for item, lab, d, flag in zip(items, labels, dist, flags, strict=True):
            if flag:
                anomaly_rows.append(
                    (
                        item["flight_id"],
                        icao,
                        int(lab),
                        float(d),
                        json.dumps(vector_to_latlon(item["vector"], apt.lat, apt.lon)),
                    )
                )

    if cluster_rows:
        spark.createDataFrame(
            cluster_rows,
            "airport string, cluster int, k int, size int, silhouette double, path_json string",
        ).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
            delta_path("gold/gold_approach_clusters")
        )
        spark.createDataFrame(
            anomaly_rows or [("", "", 0, 0.0, "[]")],
            "flight_id string, airport string, cluster int, distance double, path_json string",
        ).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
            delta_path("gold/gold_approach_anomalies")
        )
    spark.stop()


if __name__ == "__main__":
    main()
