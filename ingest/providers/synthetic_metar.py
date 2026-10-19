"""Synthetic METAR generator that honours the shared fog scenario."""

from __future__ import annotations

import random
from datetime import UTC, datetime

# Plausible fair-weather baselines per station: (wind_dir, wind_kt, temp_c, dew_c, qnh_hpa)
BASELINE: dict[str, tuple[int, int, int, int, int]] = {
    "VIDP": (270, 6, 24, 12, 1012),
    "VABB": (280, 8, 31, 22, 1009),
    "VOBL": (240, 9, 26, 17, 1013),
    "VOMM": (90, 10, 30, 24, 1008),
    "VOHS": (230, 7, 28, 18, 1011),
    "VECC": (180, 5, 30, 25, 1006),
}
DEFAULT_BASELINE = (270, 6, 27, 18, 1010)


def build_metar(station: str, obs_ms: int, visibility_m: float | None = None, seed: int = 0) -> str:
    """Return a METAR string for the station at obs_ms (UTC).

    visibility_m forces reduced visibility (fog scenario); None gives fair weather.
    """
    t = datetime.fromtimestamp(obs_ms / 1000, tz=UTC)
    rng = random.Random(f"{seed}|{station}|{obs_ms // 1_800_000}")
    wdir, wkt, temp, dew, qnh = BASELINE.get(station, DEFAULT_BASELINE)
    wdir = (wdir + rng.randrange(-20, 21)) % 360 or 360
    wkt = max(wkt + rng.randrange(-2, 4), 0)
    stamp = t.strftime("%d%H%MZ")

    if visibility_m is not None and visibility_m < 1000:
        vis = max(int(round(visibility_m / 100.0)) * 100, 50)
        sky = "FG VV001" if vis < 400 else "FG BKN002"
        fog_temp = max(temp - 12, 3)
        return (
            f"{station} {stamp} {wdir:03d}{min(wkt, 5):02d}KT {vis:04d} {sky} "
            f"{fog_temp}/{fog_temp - 1} Q{qnh + 6} NOSIG"
        )

    if visibility_m is not None and visibility_m < 5000:
        vis = int(round(visibility_m / 100.0)) * 100
        return (
            f"{station} {stamp} {wdir:03d}{wkt:02d}KT {vis:04d} BR FEW004 "
            f"{temp - 6}/{temp - 8} Q{qnh} NOSIG"
        )

    vis = rng.choice([6000, 8000, 9999, 9999])
    sky = rng.choice(["FEW020", "SCT025", "FEW018 SCT040", "NSC"])
    return f"{station} {stamp} {wdir:03d}{wkt:02d}KT {vis:04d} {sky} {temp}/{dew} Q{qnh} NOSIG"
