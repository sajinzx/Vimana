"""HBase rowkey builders.

Keys are salted (avoid hotspotting on monotonically increasing keys) and use a
reversed timestamp so a prefix scan returns the newest rows first.
"""

from __future__ import annotations

import zlib
from datetime import UTC

SALT_BUCKETS = 16
MAX_TS = 9_999_999_999_999


def salt(key: str) -> str:
    return f"{zlib.crc32(key.encode()) % SALT_BUCKETS:02d}"


def rev_ts(ts_ms: int) -> str:
    return f"{MAX_TS - ts_ms:013d}"


def unrev_ts(rev: str) -> int:
    return MAX_TS - int(rev)


def pad_ts(ts_ms: int) -> str:
    """13-digit zero-padded timestamp so string comparison matches numeric order."""
    return f"{ts_ms:013d}"


def track_prefix(icao24: str) -> bytes:
    return f"{salt(icao24)}|{icao24}|".encode()


def track_rowkey(icao24: str, ts_ms: int) -> bytes:
    return f"{salt(icao24)}|{icao24}|{rev_ts(ts_ms)}".encode()


def parse_track_rowkey(key: bytes) -> tuple[str, int]:
    _, icao24, rev = key.decode().split("|")
    return icao24, unrev_ts(rev)


def state_rowkey(icao24: str) -> bytes:
    return icao24.encode()


def cell_rowkey(h3_r5: str, ts_ms: int) -> bytes:
    """Row per H3 cell per minute: <h3>|yyyyMMddHHmm (UTC)."""
    from datetime import datetime

    minute = datetime.fromtimestamp(ts_ms / 1000, tz=UTC).strftime("%Y%m%d%H%M")
    return f"{h3_r5}|{minute}".encode()


def airport_rowkey(icao: str) -> bytes:
    return icao.upper().encode()


def event_prefix(icao: str) -> bytes:
    return f"{icao.upper()}|".encode()


def event_rowkey(icao: str, ts_ms: int, icao24: str, etype: str) -> bytes:
    return f"{icao.upper()}|{rev_ts(ts_ms)}|{icao24}|{etype}".encode()


def prediction_rowkey(icao24: str, entry_ts_ms: int) -> bytes:
    return f"{icao24}|{pad_ts(entry_ts_ms)}".encode()
