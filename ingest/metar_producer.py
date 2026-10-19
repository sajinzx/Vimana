"""METAR producer: synthetic (default offline) or live from aviationweather.gov.

Synthetic reports are issued every 30 simulated minutes and immediately after
any scenario change, so injected fog shows up in the weather feed at once.
"""

from __future__ import annotations

import signal
import time

import httpx
import orjson

from ingest.kafka_util import drain_commands, make_control_consumer, make_producer, send_json
from ingest.providers.synthetic_metar import build_metar
from ingest.scenario import Scenario, SimClock
from vimana_common.airports import load_airports
from vimana_common.logging import configure_logging, get_logger
from vimana_common.settings import get_settings

TOPIC = "weather.metar"
SYNTH_INTERVAL_MIN = 30
AWC_URL = "https://aviationweather.gov/api/data/metar"
AWC_INTERVAL_S = 600


def synthetic_envelope(station: str, sim_ms: int, scenario: Scenario, wall_ms: int, seed: int):
    vis = scenario.visibility_m(station, wall_ms)
    return {
        "station": station,
        "obs_time": sim_ms,
        "raw": build_metar(station, sim_ms, vis, seed),
        "source": "synthetic",
        "fetched_at": sim_ms,
    }


def fetch_awc(stations: list[str]) -> list[dict]:
    """Latest METAR per station from aviationweather.gov."""
    now_ms = int(time.time() * 1000)
    resp = httpx.get(
        AWC_URL,
        params={"ids": ",".join(stations), "format": "json"},
        headers={"User-Agent": "vimana-student-project/1.0"},
        timeout=20,
    )
    resp.raise_for_status()
    out = []
    for item in resp.json():
        raw = item.get("rawOb")
        station = item.get("icaoId")
        if not raw or not station:
            continue
        out.append(
            {
                "station": station,
                "obs_time": int(item.get("obsTime", now_ms / 1000)) * 1000,
                "raw": raw,
                "source": "awc",
                "fetched_at": now_ms,
            }
        )
    return out


def main() -> None:
    configure_logging()
    log = get_logger("metar_producer")
    settings = get_settings()
    stations = sorted(load_airports())
    producer = make_producer()
    provider = settings.effective_weather_provider
    scenario = Scenario(speed=settings.sim_speed)
    clock = SimClock(settings.sim_speed)
    control = make_control_consumer() if provider == "synthetic" else None

    running = True

    def stop(*_: object) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    log.info("started", provider=provider, stations=stations)
    last_issue_sim: dict[str, int] = {}
    seen_version = -1
    last_awc = 0.0
    while running:
        wall_ms = int(time.time() * 1000)
        if provider == "synthetic":
            for cmd in drain_commands(control):
                scenario.apply(cmd, wall_ms)
            for station in stations:
                scenario.visibility_m(station, wall_ms)  # expire finished fog
            sim_ms = clock.sim_ms(wall_ms)
            changed = scenario.version != seen_version
            seen_version = scenario.version
            for station in stations:
                due = sim_ms - last_issue_sim.get(station, -(10**15)) >= SYNTH_INTERVAL_MIN * 60_000
                if due or changed:
                    env = synthetic_envelope(station, sim_ms, scenario, wall_ms, settings.sim_seed)
                    send_json(producer, TOPIC, station, env)
                    last_issue_sim[station] = sim_ms
                    log.info("metar", station=station, raw=env["raw"])
            time.sleep(1.0)
        else:
            if time.time() - last_awc >= AWC_INTERVAL_S:
                try:
                    for env in fetch_awc(stations):
                        producer.produce(
                            TOPIC, key=env["station"].encode(), value=orjson.dumps(env)
                        )
                    log.info("awc_fetched")
                except Exception as exc:  # noqa: BLE001
                    log.warning("awc_failed", error=str(exc))
                last_awc = time.time()
            producer.poll(0)
            time.sleep(5.0)

    producer.flush(5)
    log.info("stopped")


if __name__ == "__main__":
    main()
