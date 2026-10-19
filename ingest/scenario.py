"""Shared scenario state and simulation clock for the synthetic providers.

Both the ADS-B simulator and the synthetic METAR generator consume the
`sim.control` topic and apply the same commands here, so fog injected through
the API shows up in both the aircraft behaviour and the weather reports.
Durations are expressed in simulated minutes and converted to wall time using
SIM_SPEED so a 60-minute fog lasts 60 simulated minutes at any speed.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field


class SimClock:
    """Maps wall time to simulated time: sim = t0 + (wall - t0) * speed."""

    def __init__(self, speed: float = 1.0, t0_wall_ms: int | None = None) -> None:
        self.speed = max(speed, 0.01)
        self.t0_wall_ms = t0_wall_ms if t0_wall_ms is not None else int(time.time() * 1000)

    def sim_ms(self, wall_ms: int | None = None) -> int:
        wall = wall_ms if wall_ms is not None else int(time.time() * 1000)
        return int(self.t0_wall_ms + (wall - self.t0_wall_ms) * self.speed)


@dataclass
class Fog:
    visibility_m: float
    expires_wall_ms: int


@dataclass
class Scenario:
    speed: float = 1.0
    fog: dict[str, Fog] = field(default_factory=dict)
    load: dict[str, float] = field(default_factory=dict)
    version: int = 0  # bumped on every change so METAR can re-issue immediately

    def apply(self, cmd: dict, wall_ms: int | None = None) -> bool:
        """Apply a sim.control command. Returns True if the state changed."""
        now = wall_ms if wall_ms is not None else int(time.time() * 1000)
        action = cmd.get("action")
        airport = str(cmd.get("airport", "")).upper()
        if action == "set_fog" and airport:
            duration_min = float(cmd.get("duration_min", 60))
            expires = now + int(duration_min * 60_000 / max(self.speed, 0.01))
            self.fog[airport] = Fog(float(cmd.get("visibility_m", 200)), expires)
        elif action == "set_load" and airport:
            self.load[airport] = max(float(cmd.get("load_factor", 1.0)), 0.0)
        elif action == "reset":
            self.fog.clear()
            self.load.clear()
        else:
            return False
        self.version += 1
        return True

    def visibility_m(self, airport: str, wall_ms: int | None = None) -> float | None:
        """Injected visibility for an airport, or None when no fog is active."""
        now = wall_ms if wall_ms is not None else int(time.time() * 1000)
        f = self.fog.get(airport)
        if f is None:
            return None
        if f.expires_wall_ms <= now:
            del self.fog[airport]
            self.version += 1
            return None
        return f.visibility_m

    def load_factor(self, airport: str) -> float:
        return self.load.get(airport, 1.0)
