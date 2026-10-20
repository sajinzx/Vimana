"""Delay-model features, shared by streaming (J4) and batch training (b03/b04).

One function builds the feature row so both code paths produce identical
values for the same inputs.
"""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta
from typing import Any

from vimana_common.airports import get_airport
from vimana_common.geo import sector

IST_OFFSET = timedelta(hours=5, minutes=30)

CATEGORICAL = ["airport", "category"]
NUMERIC = [
    "entry_sector",
    "hour_sin",
    "hour_cos",
    "is_weekend",
    "traffic_40nm",
    "low_alt_count",
    "arrivals_15m",
    "holds_15m",
    "traffic_ratio",
    "vis_m_log",
    "ceiling_ft",
    "wind_kt",
    "crosswind_kt",
    "fog",
    "low_vis",
    "wx_missing",
    "entry_alt_ft",
    "entry_gs_kt",
]
FEATURE_COLUMNS = CATEGORICAL + NUMERIC

DEFAULT_VIS_M = 9999.0
DEFAULT_CEILING_FT = 5000.0


def crosswind_kt(wind_dir: float | None, wind_kt: float | None, runway_heading: float) -> float:
    if wind_dir is None or wind_kt is None:
        return 0.0
    return abs(wind_kt * math.sin(math.radians(wind_dir - runway_heading)))


def build_features(
    entry: dict[str, Any], wx: dict[str, Any] | None, live: dict[str, Any] | None
) -> dict[str, Any]:
    """Feature row as of the entry moment.

    entry: airport, ts (ms), entry_bearing, category, alt_ft, gs_kt
    wx:    parsed METAR (metar_parse keys) or None
    live:  airport_live values at entry time (traffic_40nm, ...) or None
    """
    apt = get_airport(entry["airport"])
    local = datetime.fromtimestamp(entry["ts"] / 1000, tz=UTC) + IST_OFFSET
    hour = local.hour + local.minute / 60
    live = live or {}
    traffic = float(live.get("traffic_40nm") or 0.0)
    missing = wx is None or wx.get("visibility_m") is None
    wx = wx or {}
    vis = wx.get("visibility_m")
    vis = DEFAULT_VIS_M if vis is None else float(vis)
    ceiling = wx.get("ceiling_ft")
    return {
        "airport": entry["airport"],
        "category": entry.get("category") or "narrowbody",
        "entry_sector": sector(entry.get("entry_bearing") or 0.0, 8),
        "hour_sin": math.sin(2 * math.pi * hour / 24),
        "hour_cos": math.cos(2 * math.pi * hour / 24),
        "is_weekend": 1 if local.weekday() >= 5 else 0,
        "traffic_40nm": traffic,
        "low_alt_count": float(live.get("low_alt_count") or 0.0),
        "arrivals_15m": float(live.get("arrivals_15m") or 0.0),
        "holds_15m": float(live.get("holds_15m") or 0.0),
        "traffic_ratio": traffic / max(apt.capacity_per_10min, 1),
        "vis_m_log": math.log10(max(vis, 1.0)),
        "ceiling_ft": float(DEFAULT_CEILING_FT if ceiling is None else ceiling),
        "wind_kt": float(wx.get("wind_kt") or 0.0),
        "crosswind_kt": crosswind_kt(
            wx.get("wind_dir_deg"), wx.get("wind_kt"), apt.main_runway_heading
        ),
        "fog": 1 if wx.get("fog") else 0,
        "low_vis": 1 if wx.get("low_vis") else 0,
        "wx_missing": 1 if missing else 0,
        "entry_alt_ft": float(entry.get("alt_ft") or 0.0),
        "entry_gs_kt": float(entry.get("gs_kt") or 0.0),
    }
