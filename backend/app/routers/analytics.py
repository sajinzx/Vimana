from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from app.deps import AppState, get_state, model_metrics, model_version
from app.services.gold_repo import TABLES

router = APIRouter(prefix="/api", tags=["analytics"])


@router.get("/analytics/{name}")
def analytics(name: str, state: AppState = Depends(get_state)) -> list[dict[str, Any]]:
    if name not in TABLES:
        raise HTTPException(404, f"unknown analytics table; choose one of {sorted(TABLES)}")
    rows = state.gold.read(name)
    if rows is None:
        raise HTTPException(503, f"{TABLES[name]} not built yet; run `make batch`")
    return rows


@router.get("/model")
def model(state: AppState = Depends(get_state)) -> dict[str, Any]:
    recent = [p for p in state.repo.predictions(None, 400) if p["actual_min"] is not None][:100]
    errors = [abs(p["error_min"]) for p in recent if p["error_min"] is not None]
    return {
        "version": model_version(),
        "metrics": model_metrics(),
        "recent_errors": recent,
        "rolling_mae": round(sum(errors) / len(errors), 3) if errors else None,
    }
