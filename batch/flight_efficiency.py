"""Pure helpers for the RDD flight-efficiency job (no Spark imports)."""

from __future__ import annotations

from collections.abc import Iterable, Sequence

from vimana_common.geo import haversine_nm

# point: (ts_ms, lat, lon, alt_ft, on_ground, callsign)
Point = tuple[int, float, float, float | None, bool, str | None]

MAX_GAP_MS = 20 * 60_000
MIN_POINTS = 5


def split_into_flights(kv: tuple[str, Iterable[Point]]) -> list[tuple[str, list[Point]]]:
    """Split one aircraft's sorted points into flights.

    A new flight starts after a gap above 20 minutes or after the aircraft was
    on the ground. Segments with fewer than MIN_POINTS points are dropped.
    """
    icao24, pts = kv
    out: list[tuple[str, list[Point]]] = []
    cur: list[Point] = []
    for p in sorted(pts):
        if cur and (p[0] - cur[-1][0] > MAX_GAP_MS or cur[-1][4]):
            if len(cur) >= MIN_POINTS:
                out.append((f"{icao24}-{cur[0][0]}", cur))
            cur = []
        cur.append(p)
    if len(cur) >= MIN_POINTS:
        out.append((f"{icao24}-{cur[0][0]}", cur))
    return out


def compute_efficiency(
    flight: tuple[str, Sequence[Point]], airports: dict[str, tuple[float, float]], ring_nm: float
) -> tuple[str, tuple[float, float, int]] | None:
    """(destination_airport, (flown_nm, direct_nm, 1)) within the ring, or None.

    The destination is the airport nearest the flight's last point. Distance
    flown sums segments whose endpoints are both inside the ring around it;
    direct is the great-circle distance between the first and last such points.
    """
    _, pts = flight
    last = pts[-1]
    dest, (alat, alon) = min(
        airports.items(), key=lambda kv: haversine_nm(last[1], last[2], kv[1][0], kv[1][1])
    )
    inside = [p for p in pts if haversine_nm(p[1], p[2], alat, alon) <= ring_nm]
    if len(inside) < 2:
        return None
    flown = sum(
        haversine_nm(a[1], a[2], b[1], b[2]) for a, b in zip(inside, inside[1:], strict=False)
    )
    direct = haversine_nm(inside[0][1], inside[0][2], inside[-1][1], inside[-1][2])
    return dest, (flown, direct, 1)


def inefficiency_pct(flown: float, direct: float) -> float:
    return (flown / direct - 1.0) * 100.0 if direct > 0 else 0.0
