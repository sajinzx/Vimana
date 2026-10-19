"""Runtime settings (environment) and YAML configuration loaders."""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", case_sensitive=False)

    adsb_provider: str = "synthetic"
    weather_provider: str = "auto"
    airports: str = "VIDP,VABB,VOBL,VOMM,VOHS,VECC"
    sim_speed: float = 1.0
    sim_seed: int = 42
    replay_speed: float = 50.0
    replay_dir: str = "/data/raw"
    record_raw: bool = True
    opensky_client_id: str = ""
    opensky_client_secret: str = ""
    opensky_daily_credits: int = 4000
    opensky_credits_per_call: int = 1
    adsblol_poll_s: float = 15.0
    kafka_bootstrap: str = "kafka:9092"
    hbase_host: str = "hbase"
    hbase_thrift_port: int = 9090
    data_dir: str = "/data"
    log_level: str = "INFO"

    @property
    def airport_list(self) -> list[str]:
        return [a.strip().upper() for a in self.airports.split(",") if a.strip()]

    @property
    def effective_weather_provider(self) -> str:
        if self.weather_provider != "auto":
            return self.weather_provider
        return "synthetic" if self.adsb_provider == "synthetic" else "awc"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


def config_dir() -> Path:
    override = os.environ.get("CONFIG_DIR")
    if override:
        return Path(override)
    return Path(__file__).resolve().parent.parent / "config"


def _load_yaml(name: str) -> dict[str, Any]:
    with open(config_dir() / name, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


@lru_cache(maxsize=1)
def airports_config() -> dict[str, Any]:
    return _load_yaml("airports.yaml")


@lru_cache(maxsize=1)
def app_config() -> dict[str, Any]:
    return _load_yaml("app.yaml")


@lru_cache(maxsize=1)
def fuel_config() -> dict[str, Any]:
    return _load_yaml("fuel_burn.yaml")
