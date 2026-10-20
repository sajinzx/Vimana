"""Pydantic response models."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class Aircraft(BaseModel):
    icao24: str
    callsign: str | None = None
    lat: float | None = None
    lon: float | None = None
    alt_ft: float | None = None
    gs_kt: float | None = None
    track: float | None = None
    vr_fpm: float | None = None
    phase: str | None = None
    airport: str | None = None
    category: str | None = None
    holding: bool = False
    last_seen: int = 0


class AircraftDetail(Aircraft):
    prediction: Prediction | None = None


class TrackPoint(BaseModel):
    ts: int
    lat: float | None
    lon: float | None
    alt_ft: float | None
    gs_kt: float | None


class Event(BaseModel):
    airport: str
    type: str
    icao24: str | None = None
    callsign: str | None = None
    ts: int
    details: dict[str, Any] = {}


class Prediction(BaseModel):
    icao24: str
    entry_ts: int
    airport: str | None = None
    pred_min: float | None = None
    model_version: str | None = None
    actual_min: float | None = None
    error_min: float | None = None


class DensityCell(BaseModel):
    h3: str
    count: int


class AirportSummary(BaseModel):
    icao: str
    name: str
    lat: float
    lon: float
    capacity_per_10min: int
    metrics: dict[str, float | None]
    congestion: float
    band: str
    metar: dict[str, Any]


class SimCommand(BaseModel):
    action: str
    airport: str | None = None
    visibility_m: float | None = None
    duration_min: float | None = None
    load_factor: float | None = None


AircraftDetail.model_rebuild()
