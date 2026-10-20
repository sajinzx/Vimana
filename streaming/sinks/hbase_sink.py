"""HBase writers used from Spark foreachPartition.

The *_mutation builders are pure so they can be unit tested without HBase.
All keys are deterministic from event time and id, so replaying a
micro-batch rewrites the same cells (idempotent).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from vimana_common.hbase_client import connect, encode_row
from vimana_common.rowkeys import (
    airport_rowkey,
    cell_rowkey,
    event_rowkey,
    pad_ts,
    prediction_rowkey,
    state_rowkey,
    track_rowkey,
)


def track_mutation(row: Mapping[str, Any]) -> tuple[bytes, dict[bytes, bytes]]:
    """flight_track row for one canonical position."""
    key = track_rowkey(row["icao24"], int(row["ts"]))
    data = encode_row(
        {
            "pos:lat": row.get("lat"),
            "pos:lon": row.get("lon"),
            "pos:alt_ft": row.get("alt_ft"),
            "pos:gs_kt": row.get("gs_kt"),
            "pos:track": row.get("track_deg"),
            "pos:vr_fpm": row.get("vr_fpm"),
            "pos:gnd": int(bool(row.get("on_ground"))),
            "ctx:callsign": row.get("callsign"),
            "ctx:airport": row.get("airport"),
            "ctx:dist_nm": row.get("dist_nm"),
            "ctx:h3_r7": row.get("h3_r7"),
            "ctx:src": row.get("source"),
        }
    )
    return key, data


def cell_mutation(row: Mapping[str, Any]) -> tuple[bytes, dict[bytes, bytes]] | None:
    """airspace_cell entry: one dynamic column per aircraft, value = altitude."""
    if not row.get("h3_r5") or row.get("on_ground") or row.get("alt_ft") is None:
        return None
    key = cell_rowkey(row["h3_r5"], int(row["ts"]))
    return key, {f"a:{row['icao24']}".encode(): str(int(row["alt_ft"])).encode()}


def write_tracks_partition(rows: Iterable[Any]) -> None:
    """foreachPartition body: one connection, batched puts to both tables."""
    conn = connect()
    try:
        with (
            conn.table("flight_track").batch(batch_size=1000) as tracks,
            conn.table("airspace_cell").batch(batch_size=1000) as cells,
        ):
            for r in rows:
                d = r.asDict()
                key, data = track_mutation(d)
                tracks.put(key, data)
                cell = cell_mutation(d)
                if cell:
                    cells.put(*cell)
    finally:
        conn.close()


def metar_mutation(row: Mapping[str, Any]) -> tuple[bytes, dict[bytes, bytes], int]:
    """airport_live weather columns; cell timestamp = observation time."""
    data = encode_row(
        {
            "w:raw": row.get("raw"),
            "w:vis_m": row.get("visibility_m"),
            "w:wind_dir": row.get("wind_dir_deg"),
            "w:wind_kt": row.get("wind_kt"),
            "w:ceiling_ft": row.get("ceiling_ft"),
            "w:fog": None if row.get("fog") is None else int(bool(row["fog"])),
            "w:category": row.get("flight_category"),
        }
    )
    return airport_rowkey(row["station"]), data, int(row["obs_ts"])


def write_metar_partition(rows: Iterable[Any]) -> None:
    conn = connect()
    try:
        table = conn.table("airport_live")
        for r in rows:
            key, data, ts = metar_mutation(r.asDict())
            table.put(key, data, timestamp=ts)
    finally:
        conn.close()


def event_mutation(row: Mapping[str, Any]) -> tuple[bytes, dict[bytes, bytes]]:
    key = event_rowkey(row["airport"], int(row["ts"]), row["icao24"], row["type"])
    data = encode_row(
        {
            "e:type": row["type"],
            "e:icao24": row["icao24"],
            "e:callsign": row.get("callsign"),
            "e:ts": row["ts"],
            "e:details": row.get("details"),
        }
    )
    return key, data


def state_mutation(row: Mapping[str, Any]) -> tuple[bytes, dict[bytes, bytes]]:
    data = encode_row(
        {
            "s:callsign": row.get("callsign"),
            "s:lat": row.get("lat"),
            "s:lon": row.get("lon"),
            "s:alt_ft": row.get("alt_ft"),
            "s:gs_kt": row.get("gs_kt"),
            "s:track": row.get("track_deg"),
            "s:vr_fpm": row.get("vr_fpm"),
            "s:phase": row.get("phase"),
            "s:airport": row.get("airport"),
            "s:category": row.get("category"),
            "s:holding": int(bool(row.get("holding"))),
            "s:last_seen": pad_ts(int(row["last_seen"])),
        }
    )
    return state_rowkey(row["icao24"]), data


def prediction_mutation(row: Mapping[str, Any]) -> tuple[bytes, dict[bytes, bytes]]:
    key = prediction_rowkey(row["icao24"], int(row["entry_ts"]))
    data = encode_row(
        {
            "p:airport": row["airport"],
            "p:pred_min": row["pred_min"],
            "p:model_version": row["model_version"],
            "p:features": row.get("features"),
        }
    )
    return key, data


LIVE_COLUMNS = {
    "traffic_40nm": "c:traffic_40nm",
    "low_alt_count": "c:low_alt_count",
    "avg_gs_kt": "c:avg_gs_kt",
    "arrivals_15m": "c:arrivals_15m",
    "holds_15m": "c:holds_15m",
    "go_arounds_15m": "c:go_arounds_15m",
}


def live_mutation(
    airport: str, metrics: Mapping[str, Any], ts_ms: int
) -> tuple[bytes, dict[bytes, bytes], int]:
    """airport_live counters; cell timestamp = window end so versions form a series."""
    data = encode_row({LIVE_COLUMNS[k]: v for k, v in metrics.items() if k in LIVE_COLUMNS})
    return airport_rowkey(airport), data, ts_ms


def write_rows(table: str, mutations: Iterable[tuple[bytes, dict[bytes, bytes]]]) -> None:
    """Driver-side batched write for low-volume tables (events, predictions)."""
    conn = connect()
    try:
        with conn.table(table).batch(batch_size=500) as b:
            for key, data in mutations:
                b.put(key, data)
    finally:
        conn.close()


def write_state_partition(rows: Iterable[Any]) -> None:
    conn = connect()
    try:
        with conn.table("flight_state").batch(batch_size=500) as b:
            for r in rows:
                b.put(*state_mutation(r.asDict()))
    finally:
        conn.close()


def write_live(items: Iterable[tuple[str, Mapping[str, Any], int]]) -> None:
    """Write airport_live metrics with explicit cell timestamps."""
    conn = connect()
    try:
        table = conn.table("airport_live")
        for airport, metrics, ts_ms in items:
            key, data, ts = live_mutation(airport, metrics, ts_ms)
            if data:
                table.put(key, data, timestamp=ts)
    finally:
        conn.close()
