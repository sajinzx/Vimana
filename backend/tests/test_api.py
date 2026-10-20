from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.deps import AppState
from app.main import create_app
from app.services.congestion import band, congestion_score
from app.services.gold_repo import GoldRepo
from app.services.kafka_consumers import actual_additional
from app.services.live_hub import LiveHub, filter_for

AIRCRAFT = {
    "icao24": "800c4a",
    "callsign": "AIC101",
    "lat": 28.7,
    "lon": 77.3,
    "alt_ft": 9000.0,
    "gs_kt": 230.0,
    "track": 90.0,
    "vr_fpm": 0.0,
    "phase": "HOLDING",
    "airport": "VIDP",
    "category": "narrowbody",
    "holding": True,
    "last_seen": 1760869195000,
}
FAR = {
    **AIRCRAFT,
    "icao24": "800d11",
    "callsign": "IGO6E21",
    "lat": 12.9,
    "lon": 80.3,
    "airport": "VOMM",
    "holding": False,
    "phase": "CRUISE",
}
PRED = {
    "icao24": "800c4a",
    "entry_ts": 1760869000000,
    "airport": "VIDP",
    "pred_min": 6.5,
    "model_version": "baseline",
    "actual_min": 7.0,
    "error_min": -0.5,
}


class FakeRepo:
    def ping(self):
        return True

    def airport_latest(self, icao):
        if icao != "VIDP":
            return {}
        return {
            "traffic_40nm": "18",
            "holds_15m": "3",
            "arrivals_15m": "4",
            "vis_m": "200",
            "fog": "1",
            "category": "LIFR",
            "raw": "VIDP 190000Z 00000KT 0200 FG",
        }

    def airport_history(self, icao, hours):
        return {"traffic_40nm": [[1, 2.0], [2, 3.0]], "vis_m": [[1, 200.0]]}

    def live_aircraft(self):
        return [AIRCRAFT, FAR]

    def aircraft(self, icao24):
        return AIRCRAFT if icao24 == "800c4a" else None

    def track(self, icao24, minutes):
        return [{"ts": 1, "lat": 28.7, "lon": 77.3, "alt_ft": 9000.0, "gs_kt": 230.0}]

    def events(self, airport, etype, limit):
        ev = [
            {
                "airport": "VIDP",
                "type": "HOLDING_START",
                "icao24": "800c4a",
                "callsign": "AIC101",
                "ts": 2,
                "details": {"turn_deg": 360},
            },
            {
                "airport": "VIDP",
                "type": "LANDED",
                "icao24": "800c4a",
                "callsign": "AIC101",
                "ts": 1,
                "details": {"holding_min": 4.0, "extra_co2_kg": 500.0},
            },
        ]
        return [e for e in ev if etype in (None, e["type"])][:limit]

    def predictions(self, airport, limit):
        return [PRED]

    def prediction_for(self, icao24):
        return PRED

    def density(self, minutes):
        return [{"h3": "85189c2bfffffff", "count": 3}]

    def set_actual(self, *a):
        pass


@pytest.fixture
def client(tmp_path):
    app = create_app(with_lifespan=False)
    state = AppState(repo=FakeRepo(), gold=GoldRepo(str(tmp_path)))
    state.hub = LiveHub(state)
    app.state.vimana = state
    return TestClient(app)


def test_health(client):
    body = client.get("/api/health").json()
    assert body["hbase"] is True
    assert body["delta"] is True
    assert "model_version" in body


def test_airports_list_has_congestion_and_metar(client):
    rows = client.get("/api/airports").json()
    vidp = next(r for r in rows if r["icao"] == "VIDP")
    assert vidp["band"] == "red"
    assert vidp["metar"]["fog"] is True
    assert vidp["metrics"]["traffic_40nm"] == 18
    assert next(r for r in rows if r["icao"] == "VOMM")["band"] == "green"


def test_airport_detail_and_history(client):
    d = client.get("/api/airports/vidp").json()
    assert d["icao"] == "VIDP" and "today" in d
    assert client.get("/api/airports/XXXX").status_code == 404
    h = client.get("/api/airports/VIDP/history?hours=24").json()
    assert h["traffic_40nm"][0] == [1, 2.0]


def test_live_aircraft_filters(client):
    assert len(client.get("/api/aircraft/live").json()) == 2
    only = client.get("/api/aircraft/live?airport=vomm").json()
    assert [a["icao24"] for a in only] == ["800d11"]
    box = client.get("/api/aircraft/live?bbox=76,28,78,29").json()
    assert [a["icao24"] for a in box] == ["800c4a"]
    assert client.get("/api/aircraft/live?bbox=bad").status_code == 422


def test_aircraft_detail_track_and_404(client):
    d = client.get("/api/aircraft/800C4A").json()
    assert d["prediction"]["pred_min"] == 6.5
    assert client.get("/api/aircraft/ffffff").status_code == 404
    assert len(client.get("/api/aircraft/800c4a/track?minutes=30").json()) == 1


def test_events_predictions_density(client):
    assert len(client.get("/api/events?airport=VIDP").json()) == 2
    only = client.get("/api/events?type=landed").json()
    assert [e["type"] for e in only] == ["LANDED"]
    assert client.get("/api/predictions").json()[0]["error_min"] == -0.5
    assert client.get("/api/airspace/density").json()[0]["count"] == 3


def test_analytics_validates_names_and_missing_tables(client):
    assert client.get("/api/analytics/nope").status_code == 404
    assert client.get("/api/analytics/airport_hourly").status_code == 503


def test_model_and_pipeline_endpoints(client):
    m = client.get("/api/model").json()
    assert m["rolling_mae"] == 0.5
    p = client.get("/api/pipeline").json()
    assert p["dlq_count"] == 0


def test_sim_control_requires_producer_and_valid_action(client):
    assert client.post("/api/sim/control", json={"action": "boom"}).status_code == 422
    assert client.post("/api/sim/control", json={"action": "set_fog"}).status_code == 422
    ok = {"action": "set_fog", "airport": "VIDP", "visibility_m": 200}
    assert client.post("/api/sim/control", json=ok).status_code == 503  # no Kafka in tests


def test_websocket_registers_and_subscribes(client):
    hub = client.app.state.vimana.hub
    with client.websocket_connect("/ws/live") as ws:
        assert len(hub.clients) == 1
        ws.send_json({"subscribe": {"airport": "vidp"}})
        ws.send_json({"subscribe": {}})
    assert len(hub.clients) == 0


def test_congestion_score_and_bands():
    assert congestion_score(0, 12, 0, False) == 0
    assert congestion_score(18, 12, 4, True) == 100
    assert band(20) == "green" and band(50) == "amber" and band(80) == "red"


def test_ring_filter_for_websocket_subscription():
    assert len(filter_for([AIRCRAFT, FAR], None)) == 2
    assert [a["icao24"] for a in filter_for([AIRCRAFT, FAR], "VIDP")] == ["800c4a"]
    assert filter_for([AIRCRAFT], "ZZZZ") == []


def test_actual_additional_uses_unimpeded_or_holding():
    landed = {
        "airport": "VIDP",
        "details": {
            "entry_bearing": 270.0,
            "category": "narrowbody",
            "transit_min": 20.0,
            "holding_min": 4.0,
        },
    }
    assert actual_additional(landed, {}) == 4.0
    assert actual_additional(landed, {"VIDP|6|narrowbody": 14.0}) == 6.0
    assert actual_additional(landed, {"VIDP|6|narrowbody": 25.0}) == 0.0
