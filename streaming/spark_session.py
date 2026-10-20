"""SparkSession factory shared by all streaming queries."""

from __future__ import annotations

from pyspark.sql import SparkSession

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"


def build_spark(app_name: str) -> SparkSession:
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[4]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.driver.memory", "1536m")
        .config("spark.jars.ivy", "/opt/ivy")
        .config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
    )
    return configure_spark_with_delta_pip(builder, extra_packages=[KAFKA_PACKAGE]).getOrCreate()
