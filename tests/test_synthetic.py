import math
from datetime import datetime, timedelta, timezone

import pytest

from ingest.providers import synthetic as syn
from ingest.scenario import Scenario
from vimana_common.airports import load_airports
from vimana_common.geo import haversine_nm, heading_diff

IST = timezone(timedelta(hours=5, minutes=30))
T0 = int(datetime(2026, 10, 19, 8, 0, tzinfo=IST).timestamp() * 1000)  # morning peak
STEP_MS = int(syn.STEP_S * 1000)


def quiet_sim(**kw):
    """Simulator with spawning disabled so tests control the only aircraft."""
    sc = Scenario()
    for icao in load_airports():
        sc.apply({"action": "set_load", "airport": icao, "load_factor": 0.0})
    return syn.Simulator(scenario=sc, seed=7, start_sim_ms=T0, clean=True, **kw)


def run(sim, ac, max_steps=900, until=lambda a: False):
    """Step until the aircraft is gone or `until` is true; return its history."""
    hist = []
    for _ in range(max_steps):
        sim.step_to(sim.sim_ms + STEP_MS, emit=False)
        if ac.icao24 not in sim.aircraft:
            hist.append(None)
            break
        hist.append((ac.mode, ac.x, ac.y, ac.alt_ft, ac.gs_kt, ac.hdg, ac.vr_fpm, ac.on_ground))
        if until(ac):
            break
    return hist


def straight_in(sim, icao="VIDP"):
    ac = sim.spawn_arrival(sim.airports[icao])
    ac.hold_decided = True  # skip the random hold decision
    return ac


def test_same_seed_gives_identical_output():
    def collect():
        sim = syn.Simulator(seed=11, start_sim_ms=T0)
        return sim.step_to(T0 + 5 * 60_000)

    a, b = collect(), collect()
    assert a and a == b


def test_envelope_matches_opensky_positional_layout():
    sim = syn.Simulator(seed=3, start_sim_ms=T0, clean=True)
    sim.warm_start(10)
    env = sim.step_to(sim.sim_ms + 30_000)[0]
    st = env["state"]
    assert env["source"] == "synthetic" and env["airport_hint"] in sim.airports
    assert len(st) == 18
    assert len(st[0]) == 6 and len(st[1]) == 8  # icao24, padded callsign
    assert isinstance(st[8], bool) and st[3] == st[4]
    assert 60 < st[5] < 100 and 0 < st[6] < 40  # lon, lat inside the India box
    assert env["fetched_at"] == st[3] * 1000


def test_warm_start_populates_airspace_immediately():
    sim = syn.Simulator(seed=5, start_sim_ms=T0)
    sim.warm_start()
    assert len(sim.aircraft) >= 20


def test_noise_drops_duplicates_and_corruption_are_injected():
    sim = syn.Simulator(seed=9, start_sim_ms=T0)
    sim.warm_start(15)
    msgs = sim.step_to(sim.sim_ms + 20 * 60_000)
    bad = [m for m in msgs if m["state"][0] is None or abs(m["state"][6]) > 90]
    keys = [(m["state"][0], m["state"][3]) for m in msgs if m["state"][0]]
    assert len(bad) > 0
    assert len(bad) / len(msgs) < 0.01
    assert len(keys) > len(set(keys))  # duplicates exist


def test_straight_in_arrival_lands_and_despawns():
    sim = quiet_sim()
    ac = straight_in(sim)
    hist = run(sim, ac)
    modes = [h[0] for h in hist if h]
    assert hist[-1] is None, "aircraft should despawn after rolling out"
    assert "HOLD" not in modes and "GA_CLIMB" not in modes
    assert "APPROACH" in modes and "ROLL" in modes
    roll = [h for h in hist if h and h[0] == "ROLL"]
    assert all(h[7] for h in roll)  # on_ground
    assert math.hypot(roll[0][1], roll[0][2]) < 1.0  # touched down at the airport


def test_arrival_crosses_40nm_descending_and_stays_on_glide_path():
    sim = quiet_sim()
    ac = straight_in(sim)
    apt = sim.airports["VIDP"]
    hist = run(sim, ac)
    crossing = next(h for h in hist if h and math.hypot(h[1], h[2]) <= 40)
    assert crossing[6] < -500  # descending at entry (ENTERED_TERMINAL rule)
    assert crossing[3] < 15000
    final = [h for h in hist if h and h[0] == "APPROACH" and math.hypot(h[1], h[2]) < 10]
    for _, x, y, alt, *_ in final:
        path = apt.elev_ft + math.hypot(x, y) * syn.GLIDE_FT_PER_NM
        assert alt <= path + 400


def test_holding_racetrack_has_the_signature_the_detector_needs():
    sim = quiet_sim()
    ac = sim.spawn_arrival(sim.airports["VOMM"])
    rad = math.radians(ac.corridor)
    ac.hold_decided = ac.hold_planned = True
    ac.hold_fix = (25 * math.sin(rad), 25 * math.cos(rad))
    ac.hold_alt, ac.laps_total = 8000.0, 2
    hist = run(sim, ac)
    hold = [h for h in hist if h and h[0] == "HOLD"]
    assert len(hold) >= 2 * 48  # two laps of ~4 minutes at 5 s steps
    window = 72  # 6 minutes
    turns = [heading_diff(a[5], b[5]) for a, b in zip(hold, hold[1:], strict=False)]
    best = max(sum(turns[i : i + window]) for i in range(len(turns) - window + 1))
    assert best >= 330
    assert max(h[3] for h in hold) - min(h[3] for h in hold) <= 600
    assert max(h[4] for h in hold) <= 280
    radius = max(math.hypot(a[1] - b[1], a[2] - b[2]) for a in hold for b in hold)
    assert radius <= 8
    assert hist[-1] is None  # leaves the hold and lands


def test_go_around_climbs_back_up_then_lands():
    sim = quiet_sim()
    ac = straight_in(sim)
    apt = sim.airports["VIDP"]
    run(sim, ac, until=lambda a: a.mode == "APPROACH")
    ac.ga_planned, ac.ga_agl = True, 500.0  # force it once the approach has started
    hist = run(sim, ac)
    modes = [h[0] for h in hist if h]
    assert "GA_CLIMB" in modes and "GA_RETURN" in modes
    assert modes.index("GA_CLIMB") < modes.index("APPROACH", modes.index("GA_RETURN"))
    ga = [h for h in hist if h and h[0] == "GA_CLIMB"]
    assert max(h[6] for h in ga) > 500  # climbing fast enough to trigger detection
    assert ga[-1][3] - ga[0][3] >= 800
    assert min(h[3] for h in ga) - apt.elev_ft < 1500
    assert hist[-1] is None


def test_departure_takes_off_climbs_and_leaves():
    sim = quiet_sim()
    ac = sim.spawn_departure(sim.airports["VABB"])
    hist = run(sim, ac, max_steps=1500)
    assert hist[0][7] is True and hist[0][4] == 0  # starts on the ground
    airborne = [h for h in hist if h and not h[7]]
    assert airborne and airborne[-1][3] > 20000
    assert hist[-1] is None


def test_fog_raises_hold_probability():
    clear = syn.hold_probability(traffic=4, capacity=12, fog=0.0)
    fog = syn.hold_probability(traffic=4, capacity=12, fog=syn.fog_factor(200))
    assert clear == pytest.approx(0.05)
    assert fog > 0.3
    assert syn.hold_probability(40, 12, 1.0) == 0.9  # clipped
    assert syn.fog_factor(None) == 0 and syn.fog_factor(1500) == 0
    assert syn.fog_factor(0) == 1


def test_fog_scenario_causes_holds_in_simulation():
    def holds(fog: bool) -> int:
        sc = Scenario()
        if fog:
            fog_cmd = {"action": "set_fog", "airport": "VIDP", "visibility_m": 200}
            sc.apply({**fog_cmd, "duration_min": 600})
        sim = syn.Simulator(scenario=sc, seed=21, start_sim_ms=T0, clean=True)
        sim.wall_ms_for_scenario = 0
        sim.warm_start(20)
        seen = set()
        for _ in range(240):  # 20 simulated minutes
            sim.step_to(sim.sim_ms + STEP_MS, emit=False)
            seen |= {
                a.icao24 for a in sim.aircraft.values() if a.mode == "HOLD" and a.airport == "VIDP"
            }
        return len(seen)

    assert holds(fog=True) > holds(fog=False)


def test_heavy_fog_can_divert_after_two_holds():
    sc = Scenario()
    sc.apply({"action": "set_fog", "airport": "VIDP", "visibility_m": 150, "duration_min": 10_000})
    sim = syn.Simulator(scenario=sc, seed=4, start_sim_ms=T0, clean=True)
    sim.wall_ms_for_scenario = 0
    diverted = 0
    for _ in range(40):
        ac = sim.spawn_arrival(sim.airports["VIDP"])
        rad = math.radians(ac.corridor)
        ac.hold_decided = ac.hold_planned = True
        ac.hold_fix = (25 * math.sin(rad), 25 * math.cos(rad))
        ac.hold_alt, ac.laps_total = 8000.0, 4
    for _ in range(400):
        sim.step_to(sim.sim_ms + STEP_MS, emit=False)
        diverted += sum(1 for a in sim.aircraft.values() if a.mode == "DIVERT")
    assert diverted > 0
    for a in sim.aircraft.values():
        if a.mode == "DIVERT":
            assert a.divert_to != "VIDP"


def test_load_curve_peaks_in_the_morning_and_evening():
    assert syn.load_curve(8) == 1.0 and syn.load_curve(19) == 1.0
    assert syn.load_curve(3) < syn.load_curve(12) < syn.load_curve(8)


def test_provider_adapter_tracks_the_clock():
    from ingest.scenario import SimClock

    clock = SimClock(speed=30, t0_wall_ms=T0)
    prov = syn.SyntheticProvider(clock, Scenario(), seed=1, now_ms=T0)
    msgs = prov.poll(T0 + 10_000)  # 10 s wall = 300 s sim
    assert msgs
    assert max(m["fetched_at"] for m in msgs) <= clock.sim_ms(T0 + 10_000)
    assert haversine_nm(0, 0, 0, 0) == 0
