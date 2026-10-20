"""Read access to the HBase tables used by the API (blocking happybase calls)."""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from typing import Any, Protocol

from vimana_common.airports import load_airports
from vimana_common.hbase_client import make_pool
from vimana_common.rowkeys import (
    event_prefix,
    pad_ts,
    prediction_rowkey,
    track_prefix,
    unrev_ts,
)


def _s(row: dict[bytes, bytes]) -> dict[str, str]:
    return {k.decode(): v.decode() for k, v in row.items()}


def _f(v: str | None) -> float | None:
    try:
        return None if v is None else float(v)
    except ValueError:
        return None


class Repo(Protocol):
    def ping(self) -> bool: ...
    def airport_latest(self, icao: str) -> dict[str, str]: ...
    def airport_history(self, icao: str, hours: int) -> dict[str, list[list[float]]]: ...
    def live_aircraft(self) -> list[dict[str, Any]]: ...
    def aircraft(self, icao24: str) -> dict[str, Any] | None: ...
    def track(self, icao24: str, minutes: int) -> list[dict[str, Any]]: ...
    def events(
        self, airport: str | None, etype: str | None, limit: int
    ) -> list[dict[str, Any]]: ...
    def predictions(self, airport: str | None, limit: int) -> list[dict[str, Any]]: ...
    def prediction_for(self, icao24: str) -> dict[str, Any] | None: ...
    def density(self, minutes: int) -> list[dict[str, Any]]: ...
    def set_actual(self, icao24: str, entry_ts: int, actual_min: float) -> None: ...


class HBaseRepo:
    def __init__(self, pool_size: int = 8) -> None:
        self.pool = make_pool(pool_size)
        self._ref_ms = int(time.time() * 1000)

    def ping(self) -> bool:
        try:
            with self.pool.connection() as c:
                c.tables()
            return True
        except Exception:  # noqa: BLE001
            return False

    # -- airports ---------------------------------------------------------
    def airport_latest(self, icao: str) -> dict[str, str]:
        with self.pool.connection() as c:
            row = _s(c.table("airport_live").row(icao.upper().encode()))
        return {k.split(":", 1)[1]: v for k, v in row.items()}

    def airport_history(self, icao: str, hours: int) -> dict[str, list[list[float]]]:
        cols = [
            "c:traffic_40nm",
            "c:low_alt_count",
            "c:arrivals_15m",
            "c:holds_15m",
            "c:go_arounds_15m",
            "w:vis_m",
        ]
        out: dict[str, list[list[float]]] = {}
        with self.pool.connection() as c:
            t = c.table("airport_live")
            for col in cols:
                cells = t.cells(icao.upper().encode(), col, versions=1440, include_timestamp=True)
                pts = [[ts, f] for v, ts in cells if (f := _f(v.decode())) is not None]
                latest = max((p[0] for p in pts), default=0)
                keep = [p for p in pts if p[0] >= latest - hours * 3_600_000]
                out[col.split(":")[1]] = sorted(keep)
        return out

    # -- aircraft ---------------------------------------------------------
    @staticmethod
    def _state(icao24: str, d: dict[str, str]) -> dict[str, Any]:
        return {
            "icao24": icao24,
            "callsign": d.get("s:callsign"),
            "lat": _f(d.get("s:lat")),
            "lon": _f(d.get("s:lon")),
            "alt_ft": _f(d.get("s:alt_ft")),
            "gs_kt": _f(d.get("s:gs_kt")),
            "track": _f(d.get("s:track")),
            "vr_fpm": _f(d.get("s:vr_fpm")),
            "phase": d.get("s:phase"),
            "airport": d.get("s:airport"),
            "category": d.get("s:category"),
            "holding": d.get("s:holding") == "1",
            "last_seen": int(d.get("s:last_seen") or 0),
        }

    def _scan_state(self, cutoff: int | None) -> list[dict[str, Any]]:
        kwargs: dict[str, Any] = {}
        if cutoff is not None:
            kwargs["filter"] = (
                f"SingleColumnValueFilter('s','last_seen',>=,'binary:{pad_ts(cutoff)}',true,true)"
            )
        with self.pool.connection() as c:
            return [
                self._state(key.decode(), _s(data))
                for key, data in c.table("flight_state").scan(**kwargs)
            ]

    def live_aircraft(self, window_ms: int = 120_000) -> list[dict[str, Any]]:
        rows = self._scan_state(self._ref_ms - window_ms)
        if rows:
            self._ref_ms = max(self._ref_ms, max(r["last_seen"] for r in rows))
            return rows
        # the simulation clock may differ from wall time: resync on the newest sample
        everything = self._scan_state(None)
        if everything:
            self._ref_ms = max(r["last_seen"] for r in everything)
        return [r for r in everything if r["last_seen"] >= self._ref_ms - window_ms]

    def aircraft(self, icao24: str) -> dict[str, Any] | None:
        with self.pool.connection() as c:
            row = c.table("flight_state").row(icao24.encode())
        return self._state(icao24, _s(row)) if row else None

    def track(self, icao24: str, minutes: int) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        with self.pool.connection() as c:
            for key, data in c.table("flight_track").scan(
                row_prefix=track_prefix(icao24), limit=max(minutes * 12, 50)
            ):
                d = _s(data)
                out.append(
                    {
                        "ts": unrev_ts(key.decode().split("|")[2]),
                        "lat": _f(d.get("pos:lat")),
                        "lon": _f(d.get("pos:lon")),
                        "alt_ft": _f(d.get("pos:alt_ft")),
                        "gs_kt": _f(d.get("pos:gs_kt")),
                    }
                )
        if not out:
            return out
        newest = out[0]["ts"]  # reverse-timestamp keys return newest first
        return sorted(
            (p for p in out if p["ts"] >= newest - minutes * 60_000), key=lambda p: p["ts"]
        )

    # -- events / predictions ----------------------------------------------
    def events(self, airport: str | None, etype: str | None, limit: int) -> list[dict[str, Any]]:
        airports = [airport.upper()] if airport else list(load_airports())
        out: list[dict[str, Any]] = []
        with self.pool.connection() as c:
            t = c.table("events")
            for a in airports:
                scan_limit = limit * 5 if etype else limit
                for _, data in t.scan(row_prefix=event_prefix(a), limit=scan_limit):
                    d = _s(data)
                    if etype and d.get("e:type") != etype:
                        continue
                    out.append(
                        {
                            "airport": a,
                            "type": d.get("e:type"),
                            "icao24": d.get("e:icao24"),
                            "callsign": d.get("e:callsign"),
                            "ts": int(d.get("e:ts", 0)),
                            "details": json.loads(d.get("e:details") or "{}"),
                        }
                    )
        out.sort(key=lambda e: e["ts"], reverse=True)
        return out[:limit]

    @staticmethod
    def _prediction(key: bytes, d: dict[str, str]) -> dict[str, Any]:
        icao24, entry = key.decode().split("|")
        actual, pred = _f(d.get("p:actual_min")), _f(d.get("p:pred_min"))
        return {
            "icao24": icao24,
            "entry_ts": int(entry),
            "airport": d.get("p:airport"),
            "pred_min": pred,
            "model_version": d.get("p:model_version"),
            "actual_min": actual,
            "error_min": None if actual is None or pred is None else round(pred - actual, 2),
        }

    def predictions(self, airport: str | None, limit: int) -> list[dict[str, Any]]:
        out = []
        with self.pool.connection() as c:
            for key, data in c.table("predictions").scan():
                p = self._prediction(key, _s(data))
                if airport and p["airport"] != airport.upper():
                    continue
                out.append(p)
        out.sort(key=lambda p: p["entry_ts"], reverse=True)
        return out[:limit]

    def prediction_for(self, icao24: str) -> dict[str, Any] | None:
        with self.pool.connection() as c:
            rows = list(c.table("predictions").scan(row_prefix=f"{icao24}|".encode()))
        if not rows:
            return None
        key, data = max(rows, key=lambda r: r[0])
        return self._prediction(key, _s(data))

    def set_actual(self, icao24: str, entry_ts: int, actual_min: float) -> None:
        with self.pool.connection() as c:
            c.table("predictions").put(
                prediction_rowkey(icao24, entry_ts), {b"p:actual_min": str(actual_min).encode()}
            )

    # -- density -----------------------------------------------------------
    def density(self, minutes: int) -> list[dict[str, Any]]:
        """Aircraft per H3 res-5 cell over the newest `minutes` of cell rows."""
        rows: list[tuple[str, str, set[str]]] = []
        with self.pool.connection() as c:
            for key, data in c.table("airspace_cell").scan(limit=50_000):
                cell, minute = key.decode().split("|")
                rows.append((cell, minute, {k.decode().split(":", 1)[1] for k in data}))
        if not rows:
            return []
        newest = max(datetime.strptime(m, "%Y%m%d%H%M").replace(tzinfo=UTC) for _, m, _ in rows)
        counts: dict[str, set[str]] = {}
        for cell, minute, aircraft in rows:
            t = datetime.strptime(minute, "%Y%m%d%H%M").replace(tzinfo=UTC)
            if (newest - t).total_seconds() <= minutes * 60:
                counts.setdefault(cell, set()).update(aircraft)
        return [{"h3": h, "count": len(s)} for h, s in counts.items()]
