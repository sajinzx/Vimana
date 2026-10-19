from datetime import UTC, datetime

import pytest

from ingest.adsb_producer import message_key
from ingest.providers.synthetic_metar import build_metar
from ingest.recorder import Recorder, read_envelopes
from vimana_common.metar_parse import parse_metar

OBS = int(datetime(2026, 10, 19, 5, 30, tzinfo=UTC).timestamp() * 1000)


def test_recorder_writes_hourly_files_and_reads_back(tmp_path):
    rec = Recorder(tmp_path, "synthetic")
    h5 = OBS
    h6 = OBS + 3_600_000
    rec.write({"n": 1}, h5)
    rec.write({"n": 2}, h5 + 1000)
    rec.flush()
    rec.write({"n": 3}, h6)  # new hour switches file
    rec.flush()
    first = tmp_path / "synthetic" / "2026-10-19" / "05.jsonl.gz"
    second = tmp_path / "synthetic" / "2026-10-19" / "06.jsonl.gz"
    assert [e["n"] for e in read_envelopes(first)] == [1, 2]
    assert [e["n"] for e in read_envelopes(second)] == [3]


def test_recorder_appends_and_flushes_on_hour_change(tmp_path):
    rec = Recorder(tmp_path, "s")
    rec.write({"n": 1}, OBS)
    rec.write({"n": 2}, OBS + 3_600_000)  # implicit flush of the first hour
    rec.close()
    rec2 = Recorder(tmp_path, "s")
    rec2.write({"n": 9}, OBS)
    rec2.close()
    assert [e["n"] for e in read_envelopes(rec.path_for(OBS))] == [1, 9]


def test_message_key_prefers_icao24_then_hex_then_airport():
    assert message_key({"state": ["800c4a", "X"]}) == b"800c4a"
    assert message_key({"ac": {"hex": "abc123"}}) == b"abc123"
    assert message_key({"state": [None], "airport_hint": "VIDP"}) == b"VIDP"
    assert message_key({}) == b"unknown"


@pytest.mark.parametrize("station", ["VIDP", "VABB", "VOBL", "VOMM", "VOHS", "VECC"])
def test_fair_weather_metar_parses(station):
    raw = build_metar(station, OBS)
    m = parse_metar(raw, OBS)
    assert m["station"] == station
    assert m["fog"] is False
    assert m["visibility_m"] >= 6000
    assert m["obs_ts"] == OBS


def test_fog_metar_parses_as_fog_and_low_vis():
    raw = build_metar("VIDP", OBS, visibility_m=200)
    m = parse_metar(raw, OBS)
    assert "FG" in raw and "VV001" in raw
    assert m["fog"] and m["low_vis"] and m["visibility_m"] == 200
    assert m["ceiling_ft"] == 100
    assert m["flight_category"] == "LIFR"


def test_moderate_fog_and_mist_levels():
    assert parse_metar(build_metar("VIDP", OBS, 700), OBS)["fog"] is True
    mist = parse_metar(build_metar("VIDP", OBS, 3000), OBS)
    assert mist["fog"] is False and mist["wx_codes"] == ["BR"]


def test_metar_is_deterministic_per_half_hour():
    assert build_metar("VOMM", OBS, seed=1) == build_metar("VOMM", OBS, seed=1)
    assert build_metar("VOMM", OBS, seed=1) != build_metar("VOMM", OBS + 3_600_000, seed=1)
