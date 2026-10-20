from streaming.sinks.hbase_sink import cell_mutation, metar_mutation, track_mutation
from vimana_common.rowkeys import parse_track_rowkey

ROW = {
    "icao24": "800c4a",
    "callsign": "AIC101",
    "ts": 1760869195000,
    "lat": 28.61,
    "lon": 77.21,
    "alt_ft": 10000.4,
    "gs_kt": 250.0,
    "track_deg": 271.3,
    "vr_fpm": -1024.0,
    "on_ground": False,
    "airport": "VIDP",
    "dist_nm": 6.2,
    "h3_r5": "85189c2bfffffff",
    "h3_r7": "87189c2b0ffffff",
    "source": "synthetic",
}


def test_track_mutation_key_and_columns():
    key, data = track_mutation(ROW)
    assert parse_track_rowkey(key) == ("800c4a", 1760869195000)
    assert data[b"pos:lat"] == b"28.61"
    assert data[b"pos:gnd"] == b"0"
    assert data[b"ctx:airport"] == b"VIDP"
    assert data[b"ctx:src"] == b"synthetic"


def test_track_mutation_skips_none_values():
    _, data = track_mutation({**ROW, "callsign": None, "vr_fpm": None})
    assert b"ctx:callsign" not in data
    assert b"pos:vr_fpm" not in data


def test_track_mutation_is_deterministic():
    assert track_mutation(ROW) == track_mutation(dict(ROW))


def test_cell_mutation_uses_dynamic_column_per_aircraft():
    key, data = cell_mutation(ROW)
    assert key.startswith(b"85189c2bfffffff|")
    assert data == {b"a:800c4a": b"10000"}


def test_cell_mutation_skips_ground_and_missing_cell():
    assert cell_mutation({**ROW, "on_ground": True}) is None
    assert cell_mutation({**ROW, "h3_r5": None}) is None
    assert cell_mutation({**ROW, "alt_ft": None}) is None


def test_metar_mutation_uses_obs_time_as_cell_timestamp():
    key, data, ts = metar_mutation(
        {
            "station": "vidp",
            "obs_ts": 1760869200000,
            "raw": "VIDP 190000Z 00000KT 0200 FG VV001 12/12 Q1013",
            "visibility_m": 200.0,
            "fog": True,
            "ceiling_ft": 100,
            "flight_category": "LIFR",
        }
    )
    assert key == b"VIDP"
    assert ts == 1760869200000
    assert data[b"w:fog"] == b"1"
    assert data[b"w:vis_m"] == b"200.0"
    assert b"w:wind_kt" not in data
