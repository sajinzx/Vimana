"""Shared application state and FastAPI dependencies."""

from __future__ import annotations

import json
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from fastapi import Request

from app.services.gold_repo import GoldRepo
from app.services.hbase_repo import Repo
from vimana_common.settings import get_settings


@dataclass
class AppState:
    repo: Repo
    gold: GoldRepo = field(default_factory=GoldRepo)
    hub: Any = None
    producer: Any = None
    progress: dict[str, deque] = field(default_factory=dict)
    dq: dict[str, Any] = field(default_factory=dict)
    dlq_count: int = 0
    dlq_samples: deque = field(default_factory=lambda: deque(maxlen=20))
    alerts: deque = field(default_factory=lambda: deque(maxlen=100))
    last_progress_ts: float = 0.0

    def record_progress(self, name: str, payload: dict[str, Any]) -> None:
        self.progress.setdefault(name, deque(maxlen=300)).append(payload)


def get_state(request: Request) -> AppState:
    return request.app.state.vimana


def get_repo(request: Request) -> Repo:
    return request.app.state.vimana.repo


def model_dir() -> Path:
    return Path(get_settings().data_dir) / "models" / "delay" / "current"


def model_version() -> str:
    f = model_dir() / "version.txt"
    return f.read_text().strip() if f.exists() else "baseline"


def model_metrics() -> dict[str, Any] | None:
    f = model_dir() / "metrics.json"
    try:
        return json.loads(f.read_text())
    except (OSError, ValueError):
        return None
