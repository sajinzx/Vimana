"""Health, pipeline telemetry and simulator control."""

from __future__ import annotations

import time
from typing import Any

import orjson
from fastapi import APIRouter, Depends, HTTPException

from app.deps import AppState, get_state, model_version
from app.models import SimCommand
from vimana_common.settings import get_settings

router = APIRouter(prefix="/api", tags=["system"])


def _kafka_ok() -> bool:
    try:
        from confluent_kafka.admin import AdminClient

        AdminClient({"bootstrap.servers": get_settings().kafka_bootstrap}).list_topics(timeout=2)
        return True
    except Exception:  # noqa: BLE001
        return False


@router.get("/health")
def health(state: AppState = Depends(get_state)) -> dict[str, Any]:
    age = time.time() - state.last_progress_ts if state.last_progress_ts else None
    return {
        "kafka": _kafka_ok(),
        "hbase": state.repo.ping(),
        "delta": state.gold.available(),
        "streaming_last_progress_age_s": None if age is None else round(age, 1),
        "model_version": model_version(),
    }


@router.get("/pipeline")
def pipeline(state: AppState = Depends(get_state)) -> dict[str, Any]:
    return {
        "queries": {name: list(buf) for name, buf in state.progress.items()},
        "dq": state.dq,
        "dlq_count": state.dlq_count,
        "dlq_samples": list(state.dlq_samples),
    }


@router.post("/sim/control")
def sim_control(cmd: SimCommand, state: AppState = Depends(get_state)) -> dict[str, Any]:
    if get_settings().adsb_provider != "synthetic":
        raise HTTPException(409, "simulator control is only available with ADSB_PROVIDER=synthetic")
    if cmd.action not in {"set_fog", "set_load", "reset"}:
        raise HTTPException(422, "action must be set_fog, set_load or reset")
    if cmd.action != "reset" and not cmd.airport:
        raise HTTPException(422, "airport is required")
    payload = cmd.model_dump(exclude_none=True)
    if state.producer is None:
        raise HTTPException(503, "Kafka producer unavailable")
    state.producer.produce("sim.control", value=orjson.dumps(payload))
    state.producer.flush(2)
    return {"ok": True, "sent": payload}
