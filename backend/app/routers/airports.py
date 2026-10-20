from __future__ import annotations

import datetime as dt
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from app.deps import AppState, get_repo, get_state
from app.models import AirportSummary
from app.services.congestion import band, congestion_score
from app.services.hbase_repo import Repo
from vimana_common.airports import load_airports
from vimana_common.fuel import co2_kg, extra_fuel_kg

router = APIRouter(prefix="/api/airports", tags=["airports"])

METRIC_KEYS = [
    "traffic_40nm",
    "low_alt_count",
    "avg_gs_kt",
    "arrivals_15m",
    "holds_15m",
    "go_arounds_15m",
]


def _num(v: str | None) -> float | None:
    try:
        return None if v is None else float(v)
    except ValueError:
        return None


def summarise(icao: str, repo: Repo) -> AirportSummary:
    apt = load_airports()[icao]
    row = repo.airport_latest(icao)
    metrics = {k: _num(row.get(k)) for k in METRIC_KEYS}
    score = congestion_score(
        metrics["traffic_40nm"],
        apt.capacity_per_10min,
        metrics["holds_15m"],
        row.get("category") in ("LIFR", "IFR") or (_num(row.get("vis_m")) or 9999) < 1500,
    )
    metar = {
        "raw": row.get("raw"),
        "vis_m": _num(row.get("vis_m")),
        "wind_dir": _num(row.get("wind_dir")),
        "wind_kt": _num(row.get("wind_kt")),
        "ceiling_ft": _num(row.get("ceiling_ft")),
        "fog": row.get("fog") == "1",
        "category": row.get("category"),
    }
    return AirportSummary(
        icao=icao,
        name=apt.name,
        lat=apt.lat,
        lon=apt.lon,
        capacity_per_10min=apt.capacity_per_10min,
        metrics=metrics,
        congestion=score,
        band=band(score),
        metar=metar,
    )


@router.get("", response_model=list[AirportSummary])
def list_airports(repo: Repo = Depends(get_repo)) -> list[AirportSummary]:
    return [summarise(icao, repo) for icao in load_airports()]


@router.get("/{icao}")
def airport_detail(
    icao: str, repo: Repo = Depends(get_repo), state: AppState = Depends(get_state)
) -> dict[str, Any]:
    icao = icao.upper()
    if icao not in load_airports():
        raise HTTPException(404, f"unknown airport {icao}")
    summary = summarise(icao, repo).model_dump()
    today = dt.datetime.now(dt.UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    start = int(today.timestamp() * 1000)
    events = [e for e in repo.events(icao, None, 500) if e["ts"] >= start]
    kinds = [e["type"] for e in events]
    fuel = sum(
        extra_fuel_kg(e["details"].get("holding_min", 0.0), (e["details"] or {}).get("category"))
        for e in events
        if e["type"] == "LANDED"
    )
    summary["today"] = {
        "holds": kinds.count("HOLDING_START"),
        "go_arounds": kinds.count("GO_AROUND"),
        "diversions": kinds.count("DIVERSION_SUSPECTED"),
        "landings": kinds.count("LANDED"),
        "extra_fuel_kg": round(fuel, 1),
        "extra_co2_kg": round(
            sum(e["details"].get("extra_co2_kg", 0.0) for e in events if e["type"] == "LANDED")
            or co2_kg(fuel),
            1,
        ),
    }
    return summary


@router.get("/{icao}/history")
def airport_history(
    icao: str, hours: int = Query(24, ge=1, le=48), repo: Repo = Depends(get_repo)
) -> dict[str, list[list[float]]]:
    if icao.upper() not in load_airports():
        raise HTTPException(404, f"unknown airport {icao}")
    return repo.airport_history(icao, hours)
