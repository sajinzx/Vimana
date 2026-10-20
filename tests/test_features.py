import math

from vimana_common.features import FEATURE_COLUMNS, build_features, crosswind_kt

ENTRY = {
    "airport": "VIDP",
    "ts": 1_760_000_000_000,
    "entry_bearing": 270.0,
    "category": "widebody",
    "alt_ft": 11000,
    "gs_kt": 260,
}
WX = {
    "visibility_m": 200.0,
    "ceiling_ft": 100,
    "wind_kt": 8.0,
    "wind_dir_deg": 180.0,
    "fog": True,
    "low_vis": True,
}
LIVE = {"traffic_40nm": 12, "low_alt_count": 5, "arrivals_15m": 3, "holds_15m": 2}


def test_feature_row_has_every_model_column():
    f = build_features(ENTRY, WX, LIVE)
    assert set(FEATURE_COLUMNS) <= set(f)
    assert f["fog"] == 1 and f["low_vis"] == 1 and f["wx_missing"] == 0
    assert f["entry_sector"] == 6  # 270 degrees -> sector 6 of 8
    assert math.isclose(f["vis_m_log"], math.log10(200))
    assert f["traffic_ratio"] == 1.0  # VIDP capacity is 12 per 10 min


def test_missing_weather_is_imputed_and_flagged():
    f = build_features(ENTRY, None, None)
    assert f["wx_missing"] == 1 and f["fog"] == 0
    assert f["vis_m_log"] == math.log10(9999.0)
    assert f["traffic_40nm"] == 0.0


def test_crosswind_component():
    assert math.isclose(crosswind_kt(180, 10, 90), 10.0)
    assert math.isclose(crosswind_kt(90, 10, 90), 0.0, abs_tol=1e-9)
    assert crosswind_kt(None, 10, 90) == 0.0


def test_hour_uses_ist():
    # 2025-10-19 18:30 UTC is 00:00 IST the next day -> hour_cos 1, hour_sin 0
    ts = 1_760_898_600_000
    f = build_features({**ENTRY, "ts": ts}, None, None)
    assert math.isclose(f["hour_cos"], 1.0, abs_tol=1e-6)
