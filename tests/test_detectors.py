"""Detector tests on scripted trajectories around VIDP."""

from __future__ import annotations

from streaming.detectors import FlightState, classify_phase, cumulative_turn, detector_config
from vimana_common.airports import get_airport
from vimana_common.geo import bearing_deg, destination, haversine_nm

APT = get_airport("VIDP")
T0 = 1_760_000_000_000


def rec(ts, lat, lon, alt, track, gs, vr, ground=False, cat="narrowbody"):
    d = haversine_nm(APT.lat, APT.lon, lat, lon)
    return {
        "icao24": "abc123",
        "callsign": "AIC101",
        "ts": ts,
        "lat": lat,
        "lon": lon,
        "alt_ft": alt,
        "alt_agl_ft": alt - APT.elev_ft,
        "gs_kt": gs,
        "track_deg": track,
        "vr_fpm": vr,
        "on_ground": ground,
        "category": cat,
        "airport": "VIDP",
        "dist_nm": d,
        "bearing_from_apt": bearing_deg(APT.lat, APT.lon, lat, lon),
    }


def run(records):
    st = FlightState.new("abc123")
    out = []
    for r in records:
        out.extend(st.step(r))
    return st, out


def types(events):
    return [e.type for e in events]


def straight_in(start_nm=60.0, step_s=10, gs=200.0):
    """Inbound along the 270-degree radial, descending to touchdown."""
    recs, ts, dist = [], T0, start_nm
    while dist > 0.3:
        lat, lon = destination(APT.lat, APT.lon, 270.0, dist)
        frac = dist / start_nm
        alt = APT.elev_ft + 20 + (10000 - APT.elev_ft) * frac
        recs.append(rec(ts, lat, lon, alt, 90.0, gs, -800))
        ts += step_s * 1000
        dist -= gs / 3600 * step_s
    lat, lon = destination(APT.lat, APT.lon, 270.0, 0.2)
    recs.append(rec(ts, lat, lon, APT.elev_ft, 90.0, 60.0, 0, ground=True))
    return recs


def racetrack(laps=2, step_s=5, start_ts=T0, centre_nm=25.0, alt=9000.0):
    """Standard-rate racetrack: 60 s legs, 180-degree turns at 3 deg/s."""
    lat, lon = destination(APT.lat, APT.lon, 270.0, centre_nm)
    hdg, ts, recs = 90.0, start_ts, []
    gs = 220.0
    for _ in range(laps):
        for kind, secs in (("leg", 60), ("turn", 60), ("leg", 60), ("turn", 60)):
            for _ in range(secs // step_s):
                if kind == "turn":
                    hdg = (hdg + 3.0 * step_s) % 360.0
                lat, lon = destination(lat, lon, hdg, gs / 3600 * step_s)
                recs.append(rec(ts, lat, lon, alt, hdg, gs, 0))
                ts += step_s * 1000
    return recs


def test_phase_classification():
    cfg = detector_config()
    assert classify_phase({"on_ground": True}, cfg) == "GROUND"
    assert classify_phase({"vr_fpm": 1500, "alt_agl_ft": 9000, "dist_nm": 80}, cfg) == "CLIMB"
    assert classify_phase({"vr_fpm": -1500, "alt_agl_ft": 30000, "dist_nm": 80}, cfg) == "DESCENT"
    assert classify_phase({"vr_fpm": -800, "alt_agl_ft": 5000, "dist_nm": 20}, cfg) == "APPROACH"
    assert classify_phase({"vr_fpm": 0, "alt_agl_ft": 35000, "dist_nm": 200}, cfg) == "CRUISE"


def test_cumulative_turn_wraps_through_north():
    pts = [[0, 0, 0, 0, h, 0, 0] for h in (350, 10, 30)]
    assert cumulative_turn(pts) == 40


def test_straight_in_enters_terminal_and_lands_without_holding():
    st, ev = run(straight_in())
    assert types(ev) == ["ENTERED_TERMINAL", "LANDED"]
    landed = ev[-1].details
    assert landed["holding_min"] == 0
    assert landed["transit_min"] > 5
    assert st.entered_terminal_ts is None


def test_racetrack_triggers_holding_within_one_lap():
    recs = racetrack(laps=2)
    _, ev = run(recs)
    starts = [e for e in ev if e.type == "HOLDING_START"]
    assert len(starts) == 1
    assert starts[0].ts - recs[0]["ts"] <= 6 * 60_000
    assert starts[0].details["turn_deg"] >= 330


def test_holding_ends_when_aircraft_leaves_pattern():
    hold = racetrack(laps=2)
    last = hold[-1]
    ts, lat, lon, recs = last["ts"], last["lat"], last["lon"], list(hold)
    for _ in range(60):  # 5 minutes straight and level toward the airport
        ts += 5000
        lat, lon = destination(lat, lon, 90.0, 250 / 3600 * 5)
        recs.append(rec(ts, lat, lon, 9000, 90.0, 250.0, 0))
    _, ev = run(recs)
    assert types(ev).count("HOLDING_START") == 1
    assert types(ev).count("HOLDING_END") == 1


def test_scripted_go_around_triggers_exactly_once():
    recs, ts = [], T0
    for dist, agl in [(14, 4000), (10, 3000), (7, 2200), (4.5, 1400), (3, 900), (2, 600)]:
        lat, lon = destination(APT.lat, APT.lon, 270.0, dist)
        recs.append(rec(ts, lat, lon, APT.elev_ft + agl, 90.0, 140.0, -700))
        ts += 20_000
    # go-around: climb out while still within 8 NM
    for dist, agl in [(1.5, 700), (1.0, 1000), (0.8, 1500), (1.5, 2200)]:
        lat, lon = destination(APT.lat, APT.lon, 270.0, dist)
        recs.append(rec(ts, lat, lon, APT.elev_ft + agl, 90.0, 150.0, 1800))
        ts += 20_000
    # make sure the aircraft had entered the terminal area first
    pre = []
    for i, d in enumerate((60, 45, 38)):
        lat, lon = destination(APT.lat, APT.lon, 270.0, d)
        pre.append(rec(T0 - (3 - i) * 60_000, lat, lon, 12000 - i * 1000, 90.0, 250.0, -900))
    _, ev = run(pre + recs)
    assert types(ev).count("GO_AROUND") == 1
    assert "LANDED" not in types(ev)


def test_diversion_when_aircraft_climbs_away():
    recs = straight_in(start_nm=60)[:55]  # inside the ring, not landed
    last = recs[-1]
    ts, lat, lon = last["ts"], last["lat"], last["lon"]
    for _ in range(80):
        ts += 20_000
        lat, lon = destination(lat, lon, 270.0, 300 / 3600 * 20)
        recs.append(rec(ts, lat, lon, 17000, 270.0, 300.0, 1500))
    _, ev = run(recs)
    assert types(ev).count("DIVERSION_SUSPECTED") == 1


def test_duplicate_and_out_of_order_samples_ignored():
    recs = straight_in()
    st = FlightState.new("abc123")
    for r in recs[:5]:
        st.step(r)
    assert st.step(recs[2]) == []
    assert st.last_ts == recs[4]["ts"]


def test_state_roundtrip():
    st, _ = run(racetrack(laps=1))
    again = FlightState.from_tuple(st.to_tuple())
    assert again == st
