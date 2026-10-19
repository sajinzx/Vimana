"""Build a SparkSession once at image build time to fill the Ivy cache."""

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = (
    SparkSession.builder.appName("prewarm")
    .master("local[1]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.ivy", "/opt/ivy")
)
spark = configure_spark_with_delta_pip(
    builder, extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"]
).getOrCreate()
spark.range(1).count()
spark.stop()
