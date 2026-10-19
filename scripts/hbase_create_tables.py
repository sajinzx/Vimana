"""Create the Vimana HBase tables (idempotent).

flight_track is pre-split and created by infra/hbase/presplit.sh. If it is
missing here (for example when running outside Docker) it is created without
splits so the rest of the system still works.
"""

from __future__ import annotations

import os
import sys
import time

import happybase

HOST = os.environ.get("HBASE_HOST", "hbase")
PORT = int(os.environ.get("HBASE_THRIFT_PORT", "9090"))

DAY = 86400

TABLES: dict[str, dict[str, dict]] = {
    "flight_track": {
        "pos": dict(max_versions=1, time_to_live=7 * DAY),
        "ctx": dict(max_versions=1, time_to_live=7 * DAY),
        "wx": dict(max_versions=1, time_to_live=7 * DAY),
    },
    "flight_state": {"s": dict(max_versions=1, time_to_live=1 * DAY)},
    "airspace_cell": {"a": dict(max_versions=1, time_to_live=1 * DAY)},
    "airport_live": {
        "c": dict(max_versions=1440, time_to_live=2 * DAY),
        "w": dict(max_versions=200, time_to_live=7 * DAY),
    },
    "events": {"e": dict(max_versions=1, time_to_live=30 * DAY)},
    "predictions": {"p": dict(max_versions=1, time_to_live=30 * DAY)},
}


def connect(retries: int = 30, delay_s: float = 2.0) -> happybase.Connection:
    for attempt in range(1, retries + 1):
        try:
            conn = happybase.Connection(HOST, PORT, transport="buffered", protocol="binary")
            conn.tables()
            return conn
        except Exception as exc:  # noqa: BLE001
            print(f"hbase not ready ({attempt}/{retries}): {exc}", flush=True)
            time.sleep(delay_s)
    raise SystemExit("could not connect to HBase Thrift")


def main() -> int:
    conn = connect()
    existing = {t.decode() for t in conn.tables()}
    for name, families in TABLES.items():
        if name in existing:
            print(f"exists  {name}")
            continue
        conn.create_table(name, families)
        print(f"created {name}")
    print("tables:", sorted(t.decode() for t in conn.tables()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
