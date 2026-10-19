import pytest

from vimana_common import airports, fuel, schemas, settings


def test_fuel_rates_and_co2():
    assert fuel.fuel_rate_kg_per_min("widebody") == 100
    assert fuel.fuel_rate_kg_per_min("regional") == 20
    assert fuel.fuel_rate_kg_per_min("unknown") == 40  # falls back to narrowbody
    assert fuel.fuel_rate_kg_per_min(None) == 40
    assert fuel.extra_fuel_kg(5, "narrowbody") == 200
    assert fuel.extra_fuel_kg(-3, "narrowbody") == 0
    assert fuel.co2_kg(100) == pytest.approx(316)


def test_airports_loaded_from_config():
    apts = airports.load_airports()
    assert set(apts) == {"VIDP", "VABB", "VOBL", "VOMM", "VOHS", "VECC"}
    delhi = airports.get_airport("vidp")
    assert delhi.capacity_per_10min == 12
    assert delhi.main_runway_heading == 90.0
    assert airports.terminal_radius_nm() == 40
    assert airports.outer_radius_nm() == 100


def test_nearest_airport():
    apt, dist = airports.nearest_airport(28.6, 77.2)
    assert apt.icao == "VIDP"
    assert dist < 10


def test_settings_defaults():
    s = settings.Settings(_env_file=None)
    assert s.adsb_provider == "synthetic"
    assert s.effective_weather_provider == "synthetic"
    assert s.airport_list[0] == "VIDP"
    assert settings.app_config()["detectors"]["holding"]["min_turn_deg"] == 330


def test_weather_provider_auto_follows_adsb():
    live = settings.Settings(adsb_provider="adsblol", _env_file=None)
    assert live.effective_weather_provider == "awc"
    explicit = settings.Settings(
        weather_provider="synthetic", adsb_provider="adsblol", _env_file=None
    )
    assert explicit.effective_weather_provider == "synthetic"


def test_event_id_and_serialisation():
    ev = schemas.FlightEvent("LANDED", "800c4a", "AIC101", "VIDP", 1000, {"transit_min": 9.5})
    assert ev.event_id == "VIDP|800c4a|1000|LANDED"
    assert b"event_id" in ev.to_json()
    assert {n for n, _ in schemas.POSITION_COLUMNS} >= {"icao24", "h3_r5", "dist_nm"}
