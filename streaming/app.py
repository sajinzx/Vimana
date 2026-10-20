"""Streaming entrypoint: one SparkSession running all streaming queries."""

from __future__ import annotations

from streaming.j1_clean_enrich import start_clean_query, start_metar_query
from streaming.spark_session import build_spark
from vimana_common.logging import get_logger


def main() -> None:
    log = get_logger("streaming")
    spark = build_spark("vimana-streaming")
    spark.sparkContext.setLogLevel("WARN")
    queries = [start_clean_query(spark), start_metar_query(spark)]
    log.info("started", queries=[q.name for q in queries])
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
