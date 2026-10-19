from datetime import UTC, datetime

import pytest

from vimana_common.metar_parse import MetarParseError, parse_metar, parse_metar_safe

REF = int(datetime(2026, 10, 19, 12, 0, tzinfo=UTC).timestamp() * 1000)


def p(raw):
    return parse_metar(raw, REF)


def test_dense_fog_with_vertical_visibility():
    m = p("VIDP 190530Z 27006KT 0200 FG VV001 12/11 Q1018 NOSIG")
    assert m["station"] == "VIDP"
    assert m["wind_dir_deg"] == 270 and m["wind_kt"] == 6
    assert m["visibility_m"] == 200
    assert m["wx_codes"] == ["FG"]
    assert m["ceiling_ft"] == 100
    assert m["fog"] is True and m["low_vis"] is True
    assert m["flight_category"] == "LIFR"
    assert m["temp_c"] == 12 and m["dew_c"] == 11 and m["qnh_hpa"] == 1018


def test_clear_day_few_clouds():
    m = p("VOMM 190530Z 09010KT 6000 FEW020 30/24 Q1008 NOSIG")
    assert m["visibility_m"] == 6000
    assert m["ceiling_ft"] is None  # FEW is not a ceiling
    assert m["fog"] is False and m["low_vis"] is False
    assert m["flight_category"] == "MVFR"  # vis below 8 km


def test_cavok():
    m = p("VABB 191200Z 28008KT CAVOK 33/22 Q1007")
    assert m["visibility_m"] == 10000
    assert m["ceiling_ft"] is None
    assert m["flight_category"] == "VFR"


def test_gusts_and_variable_wind_direction_group():
    m = p("VOBL 190900Z 24015G27KT 210V270 9999 SCT025 BKN040 26/18 Q1013")
    assert m["wind_kt"] == 15 and m["gust_kt"] == 27
    assert m["ceiling_ft"] == 4000
    assert m["flight_category"] == "VFR"


def test_vrb_wind_has_no_direction():
    m = p("VOHS 190600Z VRB03KT 8000 NSC 25/20 Q1011")
    assert m["wind_dir_deg"] is None and m["wind_kt"] == 3


def test_negative_temperatures_and_mps_wind():
    m = p("UUEE 190600Z 18004MPS 9999 BKN012 M02/M05 Q1021")
    assert m["temp_c"] == -2 and m["dew_c"] == -5
    assert m["wind_kt"] == pytest.approx(7.8, abs=0.1)
    assert m["ceiling_ft"] == 1200
    assert m["flight_category"] == "IFR" or m["flight_category"] == "MVFR"


def test_missing_cloud_group_slashes():
    m = p("VECC 190530Z 00000KT 3000 BR ///// 27/26 Q1005")
    assert m["wx_codes"] == ["BR"]
    assert m["visibility_m"] == 3000
    assert m["fog"] is False


def test_missing_cloud_height_cannot_be_a_ceiling():
    m = p("VIDP 190630Z 30004KT 0800 FG OVC/// 10/10 Q1016")
    assert m["ceiling_ft"] is None
    assert m["fog"] is True


def test_shallow_and_vicinity_fog_do_not_set_fog():
    assert p("VIDP 190600Z 00000KT 3000 MIFG SKC 10/09 Q1016")["fog"] is False
    assert p("VIDP 190600Z 00000KT 8000 VCFG SKC 10/09 Q1016")["fog"] is False
    assert p("VIDP 190600Z 00000KT 0900 BCFG SKC 10/09 Q1016")["fog"] is True


def test_thunderstorm_and_rain_codes():
    m = p("VABB 190800Z 24020G35KT 3000 +TSRA BKN008CB 26/25 Q1004")
    assert m["wx_codes"] == ["+TSRA"]
    assert m["ceiling_ft"] == 800
    assert m["flight_category"] == "IFR"


def test_altimeter_in_inches_converts_to_hpa():
    m = p("KJFK 191251Z 28016KT 9999 FEW250 22/12 A3002")
    assert m["qnh_hpa"] == pytest.approx(1016.7, abs=0.2)


def test_remarks_are_ignored():
    m = p("VIDP 190530Z 27006KT 5000 HZ SCT100 25/12 Q1010 RMK 9999 BKN001")
    assert m["ceiling_ft"] is None


def test_metar_prefix_and_trailing_equals():
    m = p("METAR VOMM 190530Z 09010KT 6000 FEW020 30/24 Q1008=")
    assert m["station"] == "VOMM"


def test_observation_day_resolves_to_most_recent_past_month():
    m = parse_metar("VIDP 301800Z 00000KT 9999 SKC 20/10 Q1015", REF)
    # The 30th has not happened yet in October 2026 at the reference time.
    assert datetime.fromtimestamp(m["obs_ts"] / 1000, tz=UTC).month == 9


@pytest.mark.parametrize(
    "bad", ["", "   ", "garbage", "VIDP", "VIDP notime 27006KT", "12345 190530Z"]
)
def test_unparseable_reports_raise(bad):
    with pytest.raises(MetarParseError):
        parse_metar(bad, REF)
    assert parse_metar_safe(bad, REF) is None
