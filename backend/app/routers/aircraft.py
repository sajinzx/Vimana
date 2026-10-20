from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from app.deps import get_repo
from app.models import Aircraft, AircraftDetail, DensityCell, Event, Prediction, TrackPoint
from app.services.hbase_repo import Repo

router = APIRouter(prefix="/api", tags=["aircraft"])


@router.get("/aircraft/live", response_model=list[Aircraft])
def live(
    airport: str | None = None,
    bbox: str | None = Query(None, description="lon_min,lat_min,lon_max,lat_max"),
    repo: Repo = Depends(get_repo),
) -> list[dict]:
    rows = repo.live_aircraft()
    if airport:
        rows = [r for r in rows if r["airport"] == airport.upper()]
    if bbox:
        try:
            x0, y0, x1, y1 = (float(v) for v in bbox.split(","))
        except ValueError:
            raise HTTPException(422, "bbox must be lon_min,lat_min,lon_max,lat_max") from None
        rows = [
            r
            for r in rows
            if r["lat"] is not None
            and r["lon"] is not None
            and x0 <= r["lon"] <= x1
            and y0 <= r["lat"] <= y1
        ]
    return rows


@router.get("/aircraft/{icao24}", response_model=AircraftDetail)
def aircraft(icao24: str, repo: Repo = Depends(get_repo)) -> dict:
    state = repo.aircraft(icao24.lower())
    if state is None:
        raise HTTPException(404, "aircraft not found")
    return {**state, "prediction": repo.prediction_for(icao24.lower())}


@router.get("/aircraft/{icao24}/track", response_model=list[TrackPoint])
def track(
    icao24: str, minutes: int = Query(60, ge=1, le=360), repo: Repo = Depends(get_repo)
) -> list[dict]:
    return repo.track(icao24.lower(), minutes)


@router.get("/events", response_model=list[Event])
def events(
    airport: str | None = None,
    type: str | None = None,  # noqa: A002
    limit: int = Query(50, ge=1, le=500),
    repo: Repo = Depends(get_repo),
) -> list[dict]:
    return repo.events(airport, type.upper() if type else None, limit)


@router.get("/predictions", response_model=list[Prediction])
def predictions(
    airport: str | None = None,
    limit: int = Query(100, ge=1, le=1000),
    repo: Repo = Depends(get_repo),
) -> list[dict]:
    return repo.predictions(airport, limit)


@router.get("/airspace/density", response_model=list[DensityCell])
def density(minutes: int = Query(5, ge=1, le=60), repo: Repo = Depends(get_repo)) -> list[dict]:
    return repo.density(minutes)
