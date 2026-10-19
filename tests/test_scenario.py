from ingest.scenario import Scenario, SimClock


def test_sim_clock_scales_elapsed_time():
    clock = SimClock(speed=30, t0_wall_ms=1_000_000)
    assert clock.sim_ms(1_000_000) == 1_000_000
    assert clock.sim_ms(1_010_000) == 1_000_000 + 300_000


def test_fog_applies_and_expires_in_sim_minutes():
    sc = Scenario(speed=30)
    fog = {"action": "set_fog", "airport": "vidp", "visibility_m": 200, "duration_min": 60}
    assert sc.apply(fog, 0)
    assert sc.visibility_m("VIDP", 1) == 200
    # 60 sim minutes at 30x speed is 120 wall seconds.
    assert sc.visibility_m("VIDP", 119_000) == 200
    assert sc.visibility_m("VIDP", 121_000) is None
    assert "VIDP" not in sc.fog


def test_load_and_reset():
    sc = Scenario()
    assert sc.load_factor("VABB") == 1.0
    sc.apply({"action": "set_load", "airport": "VABB", "load_factor": 1.6})
    assert sc.load_factor("VABB") == 1.6
    sc.apply({"action": "set_fog", "airport": "VIDP", "visibility_m": 300})
    assert sc.apply({"action": "reset"})
    assert sc.load_factor("VABB") == 1.0 and sc.visibility_m("VIDP") is None


def test_unknown_command_is_ignored_and_version_tracks_changes():
    sc = Scenario()
    v = sc.version
    assert not sc.apply({"action": "explode"})
    assert sc.version == v
    sc.apply({"action": "set_load", "airport": "VIDP", "load_factor": 2})
    assert sc.version == v + 1
