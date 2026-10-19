import math

import pytest

from vimana_common import geo, units


def test_unit_round_trips():
    assert units.m_to_ft(1000) == pytest.approx(3280.84)
    assert units.ft_to_m(units.m_to_ft(123.4)) == pytest.approx(123.4)
    assert units.ms_to_kt(100) == pytest.approx(194.3844)
    assert units.kt_to_ms(units.ms_to_kt(55)) == pytest.approx(55)
    assert units.ms_to_fpm(1) == pytest.approx(196.8504)
    assert units.fpm_to_ms(units.ms_to_fpm(7)) == pytest.approx(7)
    assert units.km_to_nm(units.nm_to_km(10)) == pytest.approx(10)


def test_haversine_known_pairs():
    # Delhi to Mumbai is about 1136 km, roughly 614 NM.
    d = geo.haversine_nm(28.5665, 77.1031, 19.0887, 72.8679)
    assert 610 < d < 620
    assert geo.haversine_nm(10, 20, 10, 20) == 0
    # One degree of latitude is 60 NM.
    assert geo.haversine_nm(0, 0, 1, 0) == pytest.approx(60.0, abs=0.2)


def test_bearing_cardinals():
    assert geo.bearing_deg(0, 0, 1, 0) == pytest.approx(0, abs=1e-6)
    assert geo.bearing_deg(0, 0, 0, 1) == pytest.approx(90, abs=1e-6)
    assert geo.bearing_deg(0, 0, -1, 0) == pytest.approx(180, abs=1e-6)
    assert geo.bearing_deg(0, 0, 0, -1) == pytest.approx(270, abs=1e-6)


@pytest.mark.parametrize(
    ("h1", "h2", "expected"),
    [(350, 10, 20), (10, 350, -20), (0, 180, 180), (90, 90, 0), (180, 0, 180), (270, 90, 180)],
)
def test_heading_diff_wraps(h1, h2, expected):
    assert geo.heading_diff(h1, h2) == pytest.approx(expected)


def test_destination_inverts_distance_and_bearing():
    lat, lon = geo.destination(28.5, 77.1, 45, 50)
    assert geo.haversine_nm(28.5, 77.1, lat, lon) == pytest.approx(50, abs=0.05)
    assert geo.bearing_deg(28.5, 77.1, lat, lon) == pytest.approx(45, abs=0.2)


def test_enu_round_trip():
    e, n = geo.to_enu_km(13.3, 77.8, 13.2, 77.7)
    assert e > 0 and n > 0
    lat, lon = geo.from_enu_km(e, n, 13.2, 77.7)
    assert (lat, lon) == pytest.approx((13.3, 77.8), abs=1e-9)


def test_sector_centred_on_north():
    assert geo.sector(0) == 0
    assert geo.sector(359) == 0
    assert geo.sector(22) == 0
    assert geo.sector(23) == 1
    assert geo.sector(180) == 4
    assert {geo.sector(b) for b in range(360)} == set(range(8))


def test_h3_cell_resolution():
    c5, c7 = geo.h3_cell(28.6, 77.2, 5), geo.h3_cell(28.6, 77.2, 7)
    assert c5 != c7
    assert len(geo.h3_boundary(c5)) >= 5
    assert math.isfinite(geo.h3_boundary(c5)[0][0])
