"""Canonical record schemas.

Field lists are plain data so the module imports without pyspark; the
Spark StructTypes are built lazily by the *_schema() functions.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

import orjson

CATEGORIES = ("regional", "narrowbody", "widebody")

EVENT_TYPES = (
    "ENTERED_TERMINAL",
    "HOLDING_START",
    "HOLDING_END",
    "GO_AROUND",
    "LANDED",
    "DIVERSION_SUSPECTED",
    "DEPARTED",
)


@dataclass
class Position:
    """Canonical, unit-standardised aircraft position (adsb.clean)."""

    icao24: str
    callsign: str | None
    ts: int
    fetched_at: int
    lat: float
    lon: float
    alt_ft: float | None
    alt_agl_ft: float | None
    gs_kt: float | None
    track_deg: float | None
    vr_fpm: float | None
    on_ground: bool
    category: str
    source: str
    airport: str | None
    dist_nm: float | None
    bearing_from_apt: float | None
    h3_r5: str | None = None
    h3_r7: str | None = None

    def to_json(self) -> bytes:
        return orjson.dumps(asdict(self))


@dataclass
class FlightEvent:
    type: str
    icao24: str
    callsign: str | None
    airport: str
    ts: int
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def event_id(self) -> str:
        return f"{self.airport}|{self.icao24}|{self.ts}|{self.type}"

    def to_json(self) -> bytes:
        d = asdict(self)
        d["event_id"] = self.event_id
        return orjson.dumps(d)


# (name, spark type name) pairs, in column order.
POSITION_COLUMNS: list[tuple[str, str]] = [
    ("icao24", "string"),
    ("callsign", "string"),
    ("ts", "long"),
    ("fetched_at", "long"),
    ("lat", "double"),
    ("lon", "double"),
    ("alt_ft", "double"),
    ("alt_agl_ft", "double"),
    ("gs_kt", "double"),
    ("track_deg", "double"),
    ("vr_fpm", "double"),
    ("on_ground", "boolean"),
    ("category", "string"),
    ("source", "string"),
    ("airport", "string"),
    ("dist_nm", "double"),
    ("bearing_from_apt", "double"),
    ("h3_r5", "string"),
    ("h3_r7", "string"),
]

EVENT_COLUMNS: list[tuple[str, str]] = [
    ("event_id", "string"),
    ("type", "string"),
    ("icao24", "string"),
    ("callsign", "string"),
    ("airport", "string"),
    ("ts", "long"),
    ("details", "string"),  # JSON text
]

METAR_COLUMNS: list[tuple[str, str]] = [
    ("station", "string"),
    ("obs_ts", "long"),
    ("wind_dir_deg", "double"),
    ("wind_kt", "double"),
    ("gust_kt", "double"),
    ("visibility_m", "double"),
    ("ceiling_ft", "long"),
    ("temp_c", "double"),
    ("dew_c", "double"),
    ("qnh_hpa", "double"),
    ("wx_codes", "array<string>"),
    ("fog", "boolean"),
    ("low_vis", "boolean"),
    ("flight_category", "string"),
    ("raw", "string"),
]


def _struct(columns: list[tuple[str, str]]):
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    simple = {
        "string": StringType(),
        "long": LongType(),
        "double": DoubleType(),
        "boolean": BooleanType(),
        "array<string>": ArrayType(StringType()),
    }
    return StructType([StructField(n, simple[t], True) for n, t in columns])


def position_schema():
    return _struct(POSITION_COLUMNS)


def event_schema():
    return _struct(EVENT_COLUMNS)


def metar_schema():
    return _struct(METAR_COLUMNS)
