"""Replay provider: republish archived envelopes, preserving relative timing."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

from ingest.providers.base import Envelope
from ingest.recorder import read_envelopes


def archive_files(root: str | Path) -> list[Path]:
    """All hourly archives under root in chronological order (YYYY-MM-DD/HH sort naturally)."""
    return sorted(Path(root).glob("*/*/*.jsonl.gz"), key=lambda p: (p.parent.name, p.name))


class ReplayProvider:
    name = "replay"

    def __init__(self, root: str | Path, speed: float = 50.0) -> None:
        self.speed = max(speed, 0.01)
        self._stream: Iterator[Envelope] = self._envelopes(root)
        self._pending: Envelope | None = None
        self._origin_recorded: int | None = None
        self._origin_wall: int | None = None
        self.exhausted = False

    @staticmethod
    def _envelopes(root: str | Path) -> Iterator[Envelope]:
        for path in archive_files(root):
            yield from read_envelopes(path)

    def _recorded_ts(self, env: Envelope) -> int:
        return int(env.get("produced_at") or env["fetched_at"])

    def poll(self, now_ms: int) -> list[Envelope]:
        out: list[Envelope] = []
        while True:
            if self._pending is None:
                self._pending = next(self._stream, None)
                if self._pending is None:
                    self.exhausted = True
                    return out
            env = self._pending
            ts = self._recorded_ts(env)
            if self._origin_recorded is None:
                self._origin_recorded, self._origin_wall = ts, now_ms
            due_wall = self._origin_wall + (ts - self._origin_recorded) / self.speed
            if due_wall > now_ms:
                return out
            env = {**env, "orig_fetched_at": env["fetched_at"], "fetched_at": now_ms}
            out.append(env)
            self._pending = None
