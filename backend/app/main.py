"""FastAPI application: REST endpoints and the /ws/live WebSocket."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from app.deps import AppState
from app.routers import aircraft, airports, analytics, system
from app.services.hbase_repo import HBaseRepo
from app.services.kafka_consumers import run_consumer
from app.services.live_hub import LiveHub


@asynccontextmanager
async def lifespan(app: FastAPI):
    state = AppState(repo=HBaseRepo(8))
    state.hub = LiveHub(state)
    try:
        from ingest.kafka_util import make_producer

        state.producer = make_producer()
    except Exception:  # noqa: BLE001 - control endpoint reports 503 instead
        state.producer = None
    app.state.vimana = state
    tasks = [asyncio.create_task(state.hub.run()), asyncio.create_task(run_consumer(state))]
    try:
        yield
    finally:
        for t in tasks:
            t.cancel()


def create_app(with_lifespan: bool = True) -> FastAPI:
    app = FastAPI(title="Vimana API", version="0.1.0", lifespan=lifespan if with_lifespan else None)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:5173", "http://localhost:8080"],
        allow_methods=["*"],
        allow_headers=["*"],
    )
    for module in (aircraft, airports, analytics, system):
        app.include_router(module.router)

    @app.websocket("/ws/live")
    async def ws_live(ws: WebSocket) -> None:
        hub: LiveHub = ws.app.state.vimana.hub
        await hub.connect(ws)
        try:
            while True:
                msg = await ws.receive_json()
                sub = msg.get("subscribe") if isinstance(msg, dict) else None
                if isinstance(sub, dict):
                    hub.subscribe(ws, sub.get("airport"))
        except WebSocketDisconnect:
            pass
        finally:
            hub.disconnect(ws)

    return app


app = create_app()
