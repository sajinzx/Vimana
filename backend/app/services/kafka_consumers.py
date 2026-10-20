"""aiokafka consumer feeding the live hub and in-memory pipeline buffers."""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any

import orjson
from fastapi.concurrency import run_in_threadpool

from app.deps import AppState
from vimana_common.geo import sector
from vimana_common.settings import get_settings

TOPICS = ["alerts", "pipeline.metrics", "predictions", "flight.events", "dlq"]


def load_unimpeded() -> dict[str, float]:
    path = Path(get_settings().data_dir) / "models" / "delay" / "unimpeded.json"
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return {}


def actual_additional(landed: dict[str, Any], unimpeded: dict[str, float]) -> float:
    """Observed additional terminal time for a LANDED event.

    transit minus the unimpeded time for (airport, entry sector, category) when
    known; otherwise the time spent holding is used as the lower bound.
    """
    d = landed.get("details") or {}
    cat = d.get("category") or "narrowbody"
    sec = sector(d.get("entry_bearing") or 0.0, 8)
    base = unimpeded.get(f"{landed['airport']}|{sec}|{cat}")
    if base is None:
        return round(float(d.get("holding_min", 0.0)), 2)
    return round(max(float(d.get("transit_min", 0.0)) - base, 0.0), 2)


async def handle(state: AppState, topic: str, key: str | None, value: dict[str, Any]) -> None:
    if topic == "alerts":
        state.alerts.append(value)
        await state.hub.push({"type": "alert", "alert": value})
    elif topic == "predictions":
        await state.hub.push({"type": "prediction", "prediction": value})
    elif topic == "pipeline.metrics":
        state.last_progress_ts = time.time()
        if key == "j1_dq":
            state.dq = value
        else:
            state.record_progress(value.get("name") or key or "unknown", value)
    elif topic == "dlq":
        state.dlq_count += 1
        state.dlq_samples.append(value)
    elif topic == "flight.events" and value.get("type") == "LANDED":
        entry_ts = (value.get("details") or {}).get("entry_ts")
        if entry_ts is not None:
            actual = actual_additional(value, load_unimpeded())
            await run_in_threadpool(state.repo.set_actual, value["icao24"], int(entry_ts), actual)


async def run_consumer(state: AppState) -> None:
    from aiokafka import AIOKafkaConsumer

    consumer = AIOKafkaConsumer(
        *TOPICS,
        bootstrap_servers=get_settings().kafka_bootstrap,
        group_id="vimana-backend",
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )
    while True:
        try:
            await consumer.start()
            break
        except Exception:  # noqa: BLE001 - broker may still be starting
            await asyncio.sleep(3)
    try:
        async for msg in consumer:
            try:
                value = orjson.loads(msg.value)
                key = msg.key.decode() if msg.key else None
                await handle(state, msg.topic, key, value)
            except Exception:  # noqa: BLE001 - one bad message must not stop the stream
                continue
    finally:
        await consumer.stop()
