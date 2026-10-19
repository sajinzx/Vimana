"""happybase connection helpers shared by streaming sinks and the backend."""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import happybase

from vimana_common.settings import get_settings


def connect(retries: int = 5, delay_s: float = 1.0) -> happybase.Connection:
    s = get_settings()
    last: Exception | None = None
    for _ in range(retries):
        try:
            return happybase.Connection(
                s.hbase_host, s.hbase_thrift_port, transport="buffered", protocol="binary"
            )
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(delay_s)
    raise ConnectionError(f"cannot reach HBase Thrift: {last}")


def make_pool(size: int = 8) -> happybase.ConnectionPool:
    s = get_settings()
    return happybase.ConnectionPool(
        size, host=s.hbase_host, port=s.hbase_thrift_port, transport="buffered", protocol="binary"
    )


@contextmanager
def table_batch(table_name: str, batch_size: int = 1000) -> Iterator[Any]:
    """One connection and one batched writer, closed on exit (use per Spark partition)."""
    conn = connect()
    try:
        with conn.table(table_name).batch(batch_size=batch_size) as batch:
            yield batch
    finally:
        conn.close()


def encode_row(values: dict[str, Any]) -> dict[bytes, bytes]:
    """Encode {"family:qualifier": value} into UTF-8 bytes, skipping None."""
    return {k.encode(): str(v).encode() for k, v in values.items() if v is not None}
