"""ADS-B producer: polls the configured provider and publishes to adsb.raw.

Every envelope is also archived to gzip JSONL (cold storage / replay input).
Each message gets `produced_at` (wall clock) so end-to-end latency stays
meaningful even when the synthetic clock runs faster than real time.
"""

from __future__ import annotations

import signal
import time
from pathlib import Path

import orjson

from ingest.kafka_util import drain_commands, make_control_consumer, make_producer
from ingest.providers.base import Provider
from ingest.recorder import Recorder
from ingest.scenario import Scenario, SimClock
from vimana_common.logging import configure_logging, get_logger
from vimana_common.settings import Settings, get_settings

TOPIC = "adsb.raw"
TICK_S = 1.0


def build_provider(settings: Settings, clock: SimClock, scenario: Scenario) -> Provider:
    name = settings.adsb_provider
    if name == "synthetic":
        from ingest.providers.synthetic import SyntheticProvider

        return SyntheticProvider(clock, scenario, seed=settings.sim_seed)
    if name == "adsblol":
        from ingest.providers.adsblol import AdsbLolProvider

        return AdsbLolProvider(settings.adsblol_poll_s)
    if name == "opensky":
        from ingest.providers.opensky import OpenSkyProvider

        return OpenSkyProvider(
            settings.opensky_client_id,
            settings.opensky_client_secret,
            settings.opensky_daily_credits,
            settings.opensky_credits_per_call,
        )
    if name == "replay":
        from ingest.providers.replay import ReplayProvider

        return ReplayProvider(settings.replay_dir, settings.replay_speed)
    raise ValueError(f"unsupported ADSB_PROVIDER: {name!r}")


def message_key(envelope: dict) -> bytes:
    state = envelope.get("state")
    icao24 = state[0] if state else (envelope.get("ac") or {}).get("hex")
    return str(icao24 or envelope.get("airport_hint") or "unknown").encode()


def main() -> None:
    configure_logging()
    log = get_logger("adsb_producer")
    settings = get_settings()
    clock = SimClock(settings.sim_speed)
    scenario = Scenario(speed=settings.sim_speed)
    provider = build_provider(settings, clock, scenario)
    producer = make_producer()
    control = make_control_consumer() if settings.adsb_provider == "synthetic" else None
    recorder = (
        Recorder(Path(settings.data_dir) / "raw", provider.name) if settings.record_raw else None
    )

    running = True

    def stop(*_: object) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    log.info("started", provider=provider.name, speed=settings.sim_speed, seed=settings.sim_seed)
    sent = 0
    last_log = time.time()
    while running:
        tick_start = time.time()
        now_ms = int(tick_start * 1000)

        if control is not None:
            for cmd in drain_commands(control):
                if scenario.apply(cmd, now_ms):
                    log.info("scenario", command=cmd)

        for env in provider.poll(now_ms):
            env["produced_at"] = now_ms
            producer.produce(TOPIC, key=message_key(env), value=orjson.dumps(env))
            if recorder is not None:
                recorder.write(env, now_ms)
            sent += 1
        producer.poll(0)
        if recorder is not None:
            recorder.flush()

        if tick_start - last_log >= 30:
            log.info("progress", sent=sent)
            last_log = tick_start
        time.sleep(max(TICK_S - (time.time() - tick_start), 0.0))

    producer.flush(5)
    if recorder is not None:
        recorder.close()
    log.info("stopped", sent=sent)


if __name__ == "__main__":
    main()
