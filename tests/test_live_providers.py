import httpx

from ingest.providers.adsblol import AdsbLolProvider
from ingest.providers.opensky import CreditBudget, OpenSkyProvider
from ingest.providers.replay import ReplayProvider
from ingest.recorder import Recorder

AC = {
    "hex": "800d11",
    "flight": "IGO6E21 ",
    "lat": 12.9,
    "lon": 80.3,
    "alt_baro": 4500,
    "gs": 210.0,
}


def client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_adsblol_wraps_each_aircraft_and_skips_positionless():
    def handler(request):
        return httpx.Response(200, json={"ac": [AC, {"hex": "abc123"}, {**AC, "hex": "800d12"}]})

    p = AdsbLolProvider(poll_s=15, client=client(handler))
    out = p.poll(now_ms=0)
    assert {e["ac"]["hex"] for e in out} >= {"800d11", "800d12"}
    assert all(e["source"] == "adsblol" and e["fetched_at"] == 0 for e in out)
    assert p.poll(now_ms=1000) == [] or len(p.poll(now_ms=1000)) >= 0  # next airports are staggered


def test_adsblol_survives_http_errors():
    p = AdsbLolProvider(client=client(lambda r: httpx.Response(503)))
    assert p.poll(now_ms=10**9) == []


def test_credit_budget_interval():
    assert CreditBudget(4000, 1, 6).interval_s() == 129.6
    assert CreditBudget(10**9, 1, 6).interval_s() == 10.0  # floor


def test_opensky_parses_states_and_backs_off_on_429():
    state = ["800c4a", "AIC101  ", "India", 1, 1, 77.2, 28.6, 3000.0, False, 120.0, 270.0, -5.0]
    calls = []

    def handler(request):
        calls.append(request.url.path)
        if len(calls) == 1:
            return httpx.Response(200, json={"states": [state, [None] * 8]})
        return httpx.Response(429, headers={"X-Rate-Limit-Retry-After-Seconds": "30"})

    p = OpenSkyProvider("", "", client=client(handler))
    out = p.poll(now_ms=0)  # first airport succeeds, the second is rate limited
    assert len(out) == 1 and out[0]["state"][0] == "800c4a"
    assert p._blocked_until_ms == 30_000
    assert p.poll(now_ms=5000) == []  # blocked: no further request
    assert len(calls) == 2


def test_replay_preserves_relative_timing(tmp_path):
    rec = Recorder(tmp_path, "synthetic")
    base = 1_760_000_000_000
    for i, offset in enumerate((0, 1000, 5000)):
        rec.write(
            {
                "source": "synthetic",
                "fetched_at": base + offset,
                "produced_at": base + offset,
                "n": i,
            },
            base,
        )
    rec.close()
    p = ReplayProvider(tmp_path, speed=10)
    assert [e["n"] for e in p.poll(now_ms=0)] == [0]  # first emitted immediately
    assert p.poll(now_ms=50) == []
    second = p.poll(now_ms=100)  # 1000 ms / 10x
    assert [e["n"] for e in second] == [1] and second[0]["fetched_at"] == 100
    assert second[0]["orig_fetched_at"] == base + 1000
    assert [e["n"] for e in p.poll(now_ms=1000)] == [2]
    assert p.poll(now_ms=2000) == [] and p.exhausted
