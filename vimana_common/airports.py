"""Airport reference data.

Seed values come from config/airports.yaml. After batch/b00_bootstrap_static.py
has run, OurAirports coordinates and runway headings (written under
DATA_DIR/ref) override the seeds. When offline, the seeds and a default
09/27 runway are used so the system still runs.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

from vimana_common.settings import airports_config, get_settings

DEFAULT_RUNWAY_HEADINGS = (90.0, 270.0)


@dataclass(frozen=True)
class Airport:
    icao: str
    name: str
    lat: float
    lon: float
    elev_ft: float
    capacity_per_10min: int
    runway_headings: tuple[float, ...] = field(default=DEFAULT_RUNWAY_HEADINGS)

    @property
    def main_runway_heading(self) -> float:
        return self.runway_headings[0]


def _ref_overrides() -> dict[str, dict]:
    """Optional overrides written by the bootstrap job as JSON keyed by ICAO."""
    path = Path(os.environ.get("DATA_DIR", get_settings().data_dir)) / "ref" / "airports.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


@lru_cache(maxsize=1)
def load_airports() -> dict[str, Airport]:
    cfg = airports_config()
    overrides = _ref_overrides()
    wanted = set(get_settings().airport_list)
    out: dict[str, Airport] = {}
    for a in cfg["airports"]:
        icao = a["icao"].upper()
        if icao not in wanted:
            continue
        o = overrides.get(icao, {})
        headings = tuple(float(h) for h in o.get("runway_headings", [])) or DEFAULT_RUNWAY_HEADINGS
        out[icao] = Airport(
            icao=icao,
            name=a["name"],
            lat=float(o.get("lat", a["lat"])),
            lon=float(o.get("lon", a["lon"])),
            elev_ft=float(o.get("elev_ft", a["elev_ft"])),
            capacity_per_10min=int(a["capacity_per_10min"]),
            runway_headings=headings,
        )
    return out


def get_airport(icao: str) -> Airport:
    return load_airports()[icao.upper()]


def terminal_radius_nm() -> float:
    return float(airports_config()["terminal_radius_nm"])


def outer_radius_nm() -> float:
    return float(airports_config()["outer_radius_nm"])


def nearest_airport(lat: float, lon: float) -> tuple[Airport, float]:
    """Nearest configured airport and its distance in NM."""
    from vimana_common.geo import haversine_nm

    best = min(load_airports().values(), key=lambda a: haversine_nm(lat, lon, a.lat, a.lon))
    return best, haversine_nm(lat, lon, best.lat, best.lon)
