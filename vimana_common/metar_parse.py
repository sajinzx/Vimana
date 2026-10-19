"""METAR decoder.

A deterministic token-based parser covering what Vimana needs: wind, visibility,
present weather, cloud ceiling, temperature, dew point and QNH. Unparseable
reports raise MetarParseError so callers can route them to the DLQ.
"""

from __future__ import annotations

import re
import time
from datetime import UTC, datetime, timedelta
from typing import Any

MPS_TO_KT = 1.943844
INHG_TO_HPA = 33.8639

_STATION = re.compile(r"^[A-Z]{4}$")
_TIME = re.compile(r"^(\d{2})(\d{2})(\d{2})Z$")
_WIND = re.compile(r"^(\d{3}|VRB)(\d{2,3})(?:G(\d{2,3}))?(KT|MPS)$")
_WIND_VAR = re.compile(r"^\d{3}V\d{3}$")
_VIS = re.compile(r"^(\d{4})(?:[NSEW]{1,2})?$")
_WX = re.compile(
    r"^[-+]?(?:VC)?(?:MI|BC|PR|DR|BL|SH|TS|FZ)?"
    r"(?:DZ|RA|SN|SG|IC|PL|GR|GS|UP|BR|FG|FU|VA|DU|SA|HZ|PY|PO|SQ|FC|SS|DS)+$"
)
_CLOUD = re.compile(r"^(FEW|SCT|BKN|OVC)(\d{3}|///)(?:CB|TCU|///)?$")
_VV = re.compile(r"^VV(\d{3}|///)$")
_TEMP = re.compile(r"^(M?\d{2})/(M?\d{2})?$")
_QNH = re.compile(r"^Q(\d{4})$")
_ALTIM = re.compile(r"^A(\d{4})$")
_STOP = {"RMK", "NOSIG", "TEMPO", "BECMG"}


class MetarParseError(ValueError):
    """Raised when a report lacks a station or observation time."""


def _signed(token: str) -> float:
    return -float(token[1:]) if token.startswith("M") else float(token)


def _resolve_obs_ts(day: int, hour: int, minute: int, ref_ms: int) -> int:
    """Pick the most recent month in which (day, hour, minute) is not after ref."""
    ref = datetime.fromtimestamp(ref_ms / 1000, tz=UTC)
    year, month = ref.year, ref.month
    for _ in range(3):
        try:
            cand = datetime(year, month, day, hour, minute, tzinfo=UTC)
        except ValueError:
            cand = None
        # Allow a small clock skew into the future.
        if cand is not None and cand <= ref + timedelta(hours=1):
            return int(cand.timestamp() * 1000)
        month -= 1
        if month == 0:
            month, year = 12, year - 1
    raise MetarParseError(f"cannot resolve observation day {day}")


def _flight_category(vis_m: float | None, ceiling_ft: int | None) -> str:
    ceil = ceiling_ft if ceiling_ft is not None else 10**6
    vis = vis_m if vis_m is not None else 10**6
    if ceil < 500 or vis < 1600:
        return "LIFR"
    if ceil < 1000 or vis < 4800:
        return "IFR"
    if ceil <= 3000 or vis < 8000:
        return "MVFR"
    return "VFR"


def parse_metar(raw: str, ref_ts_ms: int | None = None) -> dict[str, Any]:
    """Decode a METAR/SPECI string. ref_ts_ms resolves the day-of-month (default now)."""
    if not raw or not raw.strip():
        raise MetarParseError("empty report")
    tokens = raw.replace("=", " ").split()
    if tokens and tokens[0] in ("METAR", "SPECI"):
        tokens = tokens[1:]
    if len(tokens) < 2 or not _STATION.match(tokens[0]):
        raise MetarParseError(f"missing station: {raw!r}")
    station = tokens[0]
    m = _TIME.match(tokens[1])
    if not m:
        raise MetarParseError(f"missing observation time: {raw!r}")
    ref = ref_ts_ms if ref_ts_ms is not None else int(time.time() * 1000)
    obs_ts = _resolve_obs_ts(int(m[1]), int(m[2]), int(m[3]), ref)

    wind_dir = wind_kt = gust_kt = None
    vis_m: float | None = None
    temp_c = dew_c = qnh = None
    wx: list[str] = []
    ceilings: list[int] = []

    for tok in tokens[2:]:
        if tok in _STOP:
            if tok == "RMK":
                break
            continue
        if tok in ("AUTO", "COR", "NSW", "NSC", "NCD", "SKC", "CLR", "/////"):
            continue
        if tok == "CAVOK":
            vis_m = 10000.0
            continue
        if wind_kt is None and (w := _WIND.match(tok)):
            scale = MPS_TO_KT if w[4] == "MPS" else 1.0
            wind_dir = None if w[1] == "VRB" else float(w[1])
            wind_kt = round(float(w[2]) * scale, 1)
            gust_kt = round(float(w[3]) * scale, 1) if w[3] else None
            continue
        if _WIND_VAR.match(tok):
            continue
        if vis_m is None and (v := _VIS.match(tok)):
            vis_m = float(v[1])
            continue
        if (c := _VV.match(tok)):
            if c[1] != "///":
                ceilings.append(int(c[1]) * 100)
            continue
        if (c := _CLOUD.match(tok)):
            if c[1] in ("BKN", "OVC") and c[2] != "///":
                ceilings.append(int(c[2]) * 100)
            continue
        if _WX.match(tok):
            wx.append(tok)
            continue
        if temp_c is None and (t := _TEMP.match(tok)):
            temp_c = _signed(t[1])
            dew_c = _signed(t[2]) if t[2] else None
            continue
        if (q := _QNH.match(tok)):
            qnh = float(q[1])
            continue
        if (a := _ALTIM.match(tok)):
            qnh = round(float(a[1]) / 100 * INHG_TO_HPA, 1)
            continue

    ceiling_ft = min(ceilings) if ceilings else None
    # Shallow (MIFG) and in-the-vicinity (VCFG) fog do not restrict the airport itself.
    fog = any("FG" in c and not c.endswith("MIFG") and not c.startswith("VC") for c in wx)
    return {
        "station": station,
        "obs_ts": obs_ts,
        "wind_dir_deg": wind_dir,
        "wind_kt": wind_kt,
        "gust_kt": gust_kt,
        "visibility_m": vis_m,
        "ceiling_ft": ceiling_ft,
        "temp_c": temp_c,
        "dew_c": dew_c,
        "qnh_hpa": qnh,
        "wx_codes": wx,
        "fog": fog,
        "low_vis": vis_m is not None and vis_m < 1500,
        "flight_category": _flight_category(vis_m, ceiling_ft),
        "raw": raw.strip(),
    }


def parse_metar_safe(raw: str, ref_ts_ms: int | None = None) -> dict[str, Any] | None:
    """Like parse_metar but returns None instead of raising (for UDFs)."""
    try:
        return parse_metar(raw, ref_ts_ms)
    except MetarParseError:
        return None
