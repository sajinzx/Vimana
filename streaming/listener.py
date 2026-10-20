"""StreamingQueryListener publishing query progress to pipeline.metrics."""

from __future__ import annotations

import json

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQueryListener

from vimana_common.settings import get_settings


class ProgressPublisher(StreamingQueryListener):
    def __init__(self) -> None:
        self._producer = None

    def _send(self, key: str, payload: dict) -> None:
        if self._producer is None:
            from confluent_kafka import Producer

            self._producer = Producer({"bootstrap.servers": get_settings().kafka_bootstrap})
        self._producer.produce(
            "pipeline.metrics", key=key.encode(), value=json.dumps(payload).encode()
        )
        self._producer.poll(0)

    def onQueryStarted(self, event) -> None:  # noqa: N802
        pass

    def onQueryProgress(self, event) -> None:  # noqa: N802
        p = json.loads(event.progress.json)
        sources = p.get("sources") or []
        metrics = (sources[0].get("metrics") or {}) if sources else {}
        payload = {
            "name": p.get("name"),
            "batchId": p.get("batchId"),
            "ts": p.get("timestamp"),
            "numInputRows": p.get("numInputRows"),
            "inputRowsPerSecond": p.get("inputRowsPerSecond"),
            "processedRowsPerSecond": p.get("processedRowsPerSecond"),
            "triggerExecutionMs": (p.get("durationMs") or {}).get("triggerExecution"),
            "stateRows": sum(s.get("numRowsTotal", 0) for s in p.get("stateOperators") or []),
            "avgOffsetsBehindLatest": metrics.get("avgOffsetsBehindLatest"),
            "maxOffsetsBehindLatest": metrics.get("maxOffsetsBehindLatest"),
        }
        self._send(str(p.get("name")), payload)

    def onQueryIdle(self, event) -> None:  # noqa: N802
        pass

    def onQueryTerminated(self, event) -> None:  # noqa: N802
        pass


def register_listener(spark: SparkSession) -> None:
    spark.streams.addListener(ProgressPublisher())
