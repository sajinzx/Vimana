"""Geodesy helpers: great-circle distance, bearings, local ENU frame, H3 cells."""

from __future__ import annotations

import math

EARTH_RADIUS_NM = 3440.065


def haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in nautical miles."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = p2 - p1
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * EARTH_RADIUS_NM * math.asin(min(1.0, math.sqrt(a)))


def bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Initial bearing from point 1 to point 2, degrees in [0, 360)."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dlmb = math.radians(lon2 - lon1)
    y = math.sin(dlmb) * math.cos(p2)
    x = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dlmb)
    return (math.degrees(math.atan2(y, x)) + 360.0) % 360.0


def heading_diff(h1: float, h2: float) -> float:
    """Signed smallest turn from h1 to h2 in (-180, 180]; positive is clockwise."""
    d = ((h2 - h1 + 540.0) % 360.0) - 180.0
    return 180.0 if d == -180.0 else d


def destination(lat: float, lon: float, bearing: float, dist_nm: float) -> tuple[float, float]:
    """Point reached travelling dist_nm along an initial bearing."""
    delta = dist_nm / EARTH_RADIUS_NM
    theta = math.radians(bearing)
    p1, l1 = math.radians(lat), math.radians(lon)
    p2 = math.asin(
        math.sin(p1) * math.cos(delta) + math.cos(p1) * math.sin(delta) * math.cos(theta)
    )
    l2 = l1 + math.atan2(
        math.sin(theta) * math.sin(delta) * math.cos(p1),
        math.cos(delta) - math.sin(p1) * math.sin(p2),
    )
    return math.degrees(p2), (math.degrees(l2) + 540.0) % 360.0 - 180.0


def to_enu_km(lat: float, lon: float, ref_lat: float, ref_lon: float) -> tuple[float, float]:
    """Local east/north offsets in km from a reference point (equirectangular)."""
    km_per_deg = math.pi * EARTH_RADIUS_NM * 1.852 / 180.0
    east = (lon - ref_lon) * math.cos(math.radians(ref_lat)) * km_per_deg
    north = (lat - ref_lat) * km_per_deg
    return east, north


def from_enu_km(east: float, north: float, ref_lat: float, ref_lon: float) -> tuple[float, float]:
    """Inverse of to_enu_km."""
    km_per_deg = math.pi * EARTH_RADIUS_NM * 1.852 / 180.0
    lat = ref_lat + north / km_per_deg
    lon = ref_lon + east / (km_per_deg * math.cos(math.radians(ref_lat)))
    return lat, lon


def sector(bearing: float, n: int = 8) -> int:
    """Index of the n equal sectors (centred on 0 deg) containing the bearing."""
    width = 360.0 / n
    return int(((bearing + width / 2) % 360.0) // width)


def h3_cell(lat: float, lon: float, res: int) -> str:
    import h3

    return h3.latlng_to_cell(lat, lon, res)


def h3_boundary(cell: str) -> list[tuple[float, float]]:
    import h3

    return list(h3.cell_to_boundary(cell))
