"""WebSocket broadcaster: one HBase read per tick, shared by all clients."""

from __future__ import annotations

import asyncio
import time
from typing import Any

import orjson
from fastapi import WebSocket
from fastapi.concurrency import run_in_threadpool

from app.deps import AppState
from vimana_common.airports import load_airports, terminal_radius_nm  # noqa: F401
from vimana_common.geo import haversine_nm
from vimana_common.settings import airports_config

TICK_S = 2.0
RING_NM = float(airports_config()["outer_radius_nm"])
AIRCRAFT_FIELDS = (
    "icao24",
    "callsign",
    "lat",
    "lon",
    "alt_ft",
    "gs_kt",
    "track",
    "phase",
    "airport",
    "holding",
)


def filter_for(aircraft: list[dict[str, Any]], airport: str | None) -> list[dict[str, Any]]:
    """Aircraft inside an airport's 100 NM ring (all aircraft when no filter)."""
    if not airport:
        return aircraft
    apt = load_airports().get(airport.upper())
    if apt is None:
        return []
    return [
        a
        for a in aircraft
        if a["lat"] is not None
        and a["lon"] is not None
        and haversine_nm(apt.lat, apt.lon, a["lat"], a["lon"]) <= RING_NM
    ]


def snapshot_builder(ts: int, slim: list[dict[str, Any]], kpis: dict[str, Any]):
    def build(airport: str | None) -> dict[str, Any]:
        return {"type": "snapshot", "ts": ts, "aircraft": filter_for(slim, airport), "kpis": kpis}

    return build


class LiveHub:
    def __init__(self, state: AppState) -> None:
        self.state = state
        self.clients: dict[WebSocket, str | None] = {}

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self.clients[ws] = None

    def disconnect(self, ws: WebSocket) -> None:
        self.clients.pop(ws, None)

    def subscribe(self, ws: WebSocket, airport: str | None) -> None:
        if ws in self.clients:
            self.clients[ws] = airport.upper() if airport else None

    def kpis(self, aircraft: list[dict[str, Any]]) -> dict[str, Any]:
        prog = self.state.progress.get("j1_clean")
        rate = prog[-1].get("inputRowsPerSecond") if prog else None
        return {
            "tracked": len(aircraft),
            "holding": sum(1 for a in aircraft if a.get("holding")),
            "msgs_per_s": rate,
            "e2e_latency_ms_p95": self.state.dq.get("latency_ms_p95"),
        }

    async def _send_all(self, build) -> None:
        dead = []
        for ws, flt in list(self.clients.items()):
            try:
                await ws.send_bytes(orjson.dumps(build(flt)))
            except Exception:  # noqa: BLE001
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)

    async def push(self, message: dict[str, Any]) -> None:
        """Immediate delivery of an alert or prediction."""
        await self._send_all(lambda _flt: message)

    async def run(self) -> None:
        while True:
            try:
                if self.clients:
                    aircraft = await run_in_threadpool(self.state.repo.live_aircraft)
                    ts = int(time.time() * 1000)
                    kpis = self.kpis(aircraft)
                    slim = [{k: a.get(k) for k in AIRCRAFT_FIELDS} for a in aircraft]
                    await self._send_all(snapshot_builder(ts, slim, kpis))
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - keep the loop alive across HBase hiccups
                pass
            await asyncio.sleep(TICK_S)
