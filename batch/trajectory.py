"""Pure helpers for approach-path clustering (numpy only)."""

from __future__ import annotations

import numpy as np
import pandas as pd

from vimana_common.geo import from_enu_km, to_enu_km

N_POINTS = 20
MIN_RUN_POINTS = 10


def resample_path(xy: np.ndarray, n: int = N_POINTS) -> np.ndarray:
    """Resample an (m, 2) path to n points equally spaced by along-track distance."""
    seg = np.hypot(*np.diff(xy, axis=0).T)
    along = np.concatenate([[0.0], np.cumsum(seg)])
    if along[-1] == 0:
        return np.repeat(xy[:1], n, axis=0)
    targets = np.linspace(0.0, along[-1], n)
    return np.column_stack(
        [np.interp(targets, along, xy[:, 0]), np.interp(targets, along, xy[:, 1])]
    )


def extract_approaches(
    tracks: pd.DataFrame, airports: dict[str, tuple[float, float]], entry_nm: float = 40.0
) -> list[dict]:
    """One record per landed flight: the path from the terminal ring to touchdown.

    tracks needs icao24, airport, ts, lat, lon, dist_nm, on_ground. A flight
    counts as landed when its last in-ring point is within 3 NM or on the ground.
    """
    out = []
    df = tracks.dropna(subset=["airport", "dist_nm", "lat", "lon"])
    for (icao24, airport), g in df.groupby(["icao24", "airport"]):
        if airport not in airports:
            continue
        g = g.sort_values("ts")
        run = g[g["dist_nm"] <= entry_nm]
        if len(run) < MIN_RUN_POINTS:
            continue
        last = run.iloc[-1]
        if not (bool(last["on_ground"]) or last["dist_nm"] <= 3.0):
            continue
        # drop go-around loops: keep points up to the closest approach
        stop = int(np.argmin(run["dist_nm"].to_numpy()))
        run = run.iloc[: stop + 1]
        if len(run) < MIN_RUN_POINTS:
            continue
        lat0, lon0 = airports[airport]
        xy = np.array(
            [to_enu_km(a, b, lat0, lon0) for a, b in zip(run["lat"], run["lon"], strict=True)]
        )
        out.append(
            {
                "flight_id": f"{icao24}-{int(run.iloc[0]['ts'])}",
                "icao24": icao24,
                "airport": airport,
                "vector": resample_path(xy).reshape(-1),
            }
        )
    return out


def vector_to_latlon(vec: np.ndarray, airport_lat: float, airport_lon: float) -> list[list[float]]:
    pts = vec.reshape(-1, 2)
    return [list(from_enu_km(float(x), float(y), airport_lat, airport_lon)) for x, y in pts]


def anomaly_flags(
    scaled: np.ndarray, labels: np.ndarray, centroids: np.ndarray, pct: float = 95.0
) -> tuple[np.ndarray, np.ndarray]:
    """(distance to assigned centroid, flag when above that cluster's pct-th percentile)."""
    dist = np.linalg.norm(scaled - centroids[labels], axis=1)
    flags = np.zeros(len(dist), dtype=bool)
    for k in np.unique(labels):
        mask = labels == k
        flags[mask] = dist[mask] > np.percentile(dist[mask], pct)
    return dist, flags
