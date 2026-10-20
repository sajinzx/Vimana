"""HBase writers used from Spark foreachPartition.

The *_mutation builders are pure so they can be unit tested without HBase.
All keys are deterministic from event time and id, so replaying a
micro-batch rewrites the same cells (idempotent).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from vimana_common.hbase_client import connect, encode_row
from vimana_common.rowkeys import airport_rowkey, cell_rowkey, track_rowkey


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
