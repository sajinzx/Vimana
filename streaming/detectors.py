"""Pure-Python per-flight detectors: phase, terminal entry, holding, go-around,
landing, diversion and departure.

FlightState.step() consumes one canonical position at a time (in timestamp
order) and returns the events it triggers. No Spark imports, so the logic is
unit-tested directly with scripted trajectories.
"""

from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass, field
from typing import Any

from vimana_common.airports import load_airports
from vimana_common.fuel import co2_kg, extra_fuel_kg
from vimana_common.geo import haversine_nm, heading_diff, to_enu_km
from vimana_common.schemas import FlightEvent
from vimana_common.settings import app_config
from vimana_common.units import km_to_nm

# buffer point: [ts, lat, lon, alt_ft, track_deg, gs_kt, dist_nm]
Point = list[float]


def detector_config() -> dict[str, Any]:
    return app_config()["detectors"]


def classify_phase(r: dict[str, Any], cfg: dict[str, Any]) -> str:
    if r.get("on_ground"):
        return "GROUND"
    vr = r.get("vr_fpm") or 0.0
    ph = cfg["phase"]
    dist = r.get("dist_nm")
    agl = r.get("alt_agl_ft")
    if (
        dist is not None
        and dist <= cfg["terminal_radius_nm"]
        and vr < ph["approach_vr_fpm"]
        and agl is not None
        and agl < ph["approach_max_agl_ft"]
    ):
        return "APPROACH"
    if vr > ph["climb_vr_fpm"]:
        return "CLIMB"
    if vr < ph["descent_vr_fpm"]:
        return "DESCENT"
    return "CRUISE"


def cumulative_turn(points: list[Point]) -> float:
    """Signed sum of heading changes along the points (clockwise positive)."""
    total = 0.0
    prev: float | None = None
    for p in points:
        h = p[4]
        if h is None:
            continue
        if prev is not None:
            total += heading_diff(prev, h)
        prev = h
    return total


def bounding_radius_nm(points: list[Point]) -> float:
    """Largest distance from the points' centroid, in NM."""
    if not points:
        return 0.0
    lat0, lon0 = points[0][1], points[0][2]
    xy = [to_enu_km(p[1], p[2], lat0, lon0) for p in points]
    cx = sum(x for x, _ in xy) / len(xy)
    cy = sum(y for _, y in xy) / len(xy)
    return km_to_nm(max(math.hypot(x - cx, y - cy) for x, y in xy))


def _touched_down(on_ground: bool, agl: float | None, gs: float | None, ld: dict) -> bool:
    if on_ground:
        return True
    return agl is not None and gs is not None and agl < ld["agl_ft"] and gs < ld["gs_kt"]


def _centroid(points: list[Point]) -> tuple[float, float]:
    return (
        sum(p[1] for p in points) / len(points),
        sum(p[2] for p in points) / len(points),
    )


@dataclass
class FlightState:
    icao24: str
    callsign: str | None = None
    category: str = "narrowbody"
    phase: str = "CRUISE"
    airport: str | None = None
    last_ts: int = 0
    last_dist: float | None = None
    was_ground: bool = False
    entered_airport: str | None = None
    entered_terminal_ts: int | None = None
    entry_bearing: float | None = None
    holding_active: bool = False
    holding_start_ts: int | None = None
    hold_alt_ft: float | None = None
    hold_ignore_before: int = 0
    holding_total_ms: int = 0
    approach_armed: bool = False
    min_agl_near_rwy_ft: float | None = None
    buffer: list[Point] = field(default_factory=list)

    @classmethod
    def new(cls, icao24: str) -> FlightState:
        return cls(icao24=icao24)

    # -- (de)serialisation for Spark's GroupState -------------------------
    def to_tuple(self) -> tuple[str]:
        return (json.dumps(asdict(self), separators=(",", ":")),)

    @classmethod
    def from_tuple(cls, t: tuple[str]) -> FlightState:
        return cls(**json.loads(t[0]))

    # -- helpers ----------------------------------------------------------
    def _event(self, etype: str, airport: str, ts: int, **details: Any) -> FlightEvent:
        return FlightEvent(etype, self.icao24, self.callsign, airport, ts, details)

    def _reset_terminal(self) -> None:
        self.entered_airport = None
        self.entered_terminal_ts = None
        self.entry_bearing = None
        self.holding_total_ms = 0
        self.approach_armed = False
        self.min_agl_near_rwy_ft = None

    def _end_hold(self, ts: int, airport: str, reason: str) -> FlightEvent:
        dur = ts - (self.holding_start_ts or ts)
        self.holding_total_ms += dur
        self.holding_active = False
        self.holding_start_ts = None
        self.hold_ignore_before = ts
        return self._event(
            "HOLDING_END", airport, ts, duration_min=round(dur / 60000, 2), reason=reason
        )

    # -- main entry -------------------------------------------------------
    def step(self, r: dict[str, Any], cfg: dict[str, Any] | None = None) -> list[FlightEvent]:
        cfg = cfg or detector_config()
        events: list[FlightEvent] = []
        ts = int(r["ts"])
        if ts <= self.last_ts:
            return events  # out-of-order or duplicate sample
        if r.get("callsign"):
            self.callsign = r["callsign"]
        if r.get("category"):
            self.category = r["category"]

        airport = r.get("airport")
        dist = r.get("dist_nm")
        on_ground = bool(r.get("on_ground"))
        alt = r.get("alt_ft")
        agl = r.get("alt_agl_ft")
        gs = r.get("gs_kt")
        vr = r.get("vr_fpm") or 0.0

        self.buffer.append(
            [ts, r["lat"], r["lon"], alt, r.get("track_deg"), gs, dist if dist is not None else -1]
        )
        horizon = ts - cfg["state_timeout_minutes"] * 60000
        self.buffer = [p for p in self.buffer if p[0] >= horizon][-cfg["buffer_max_points"] :]

        if airport is None or dist is None:
            self.phase = classify_phase(r, cfg)
            self.last_ts, self.last_dist, self.was_ground = ts, None, on_ground
            self.airport = None
            return self._check_departure_far(ts, cfg, events)

        if self.airport != airport and not self.holding_active and self.entered_terminal_ts is None:
            self.approach_armed = False
            self.min_agl_near_rwy_ft = None
        self.airport = airport
        terminal_nm = cfg["terminal_radius_nm"]

        # DEPARTED: was on the ground, now climbing near the field.
        dep = cfg["departure"]
        if self.was_ground and not on_ground and vr > 0 and dist <= dep["max_dist_nm"]:
            events.append(self._event("DEPARTED", airport, ts, category=self.category))

        # ENTERED_TERMINAL: inbound crossing of the terminal ring.
        if (
            self.entered_terminal_ts is None
            and not on_ground
            and self.last_dist is not None
            and self.last_dist > terminal_nm >= dist
            and (vr < 0 or (alt is not None and alt < cfg["entry_max_alt_ft"]))
        ):
            self.entered_airport = airport
            self.entered_terminal_ts = ts
            self.entry_bearing = r.get("bearing_from_apt")
            events.append(
                self._event(
                    "ENTERED_TERMINAL",
                    airport,
                    ts,
                    entry_bearing=self.entry_bearing,
                    alt_ft=alt,
                    gs_kt=gs,
                    category=self.category,
                )
            )

        events.extend(self._holding(ts, airport, alt, gs, dist, on_ground, cfg))
        events.extend(self._approach(ts, airport, agl, gs, dist, vr, on_ground, cfg))
        events.extend(self._diversion(ts, airport, dist, alt, on_ground, agl, gs, cfg))

        self.phase = "HOLDING" if self.holding_active else classify_phase(r, cfg)
        self.last_ts, self.last_dist, self.was_ground = ts, dist, on_ground
        return events

    # -- individual detectors --------------------------------------------
    def _holding(self, ts, airport, alt, gs, dist, on_ground, cfg) -> list[FlightEvent]:
        h = cfg["holding"]
        out: list[FlightEvent] = []
        if on_ground:
            if self.holding_active:
                out.append(self._end_hold(ts, airport, "landed"))
            return out

        if not self.holding_active:
            start = max(ts - h["buffer_minutes"] * 60000, self.hold_ignore_before)
            win = [
                p
                for p in self.buffer
                if p[0] >= start and h["min_dist_nm"] <= p[6] <= h["max_dist_nm"]
            ]
            if len(win) < 3 or win[-1] is not self.buffer[-1]:
                return out
            alts = [p[3] for p in win if p[3] is not None]
            if not alts or max(alts) - min(alts) > h["max_alt_range_ft"]:
                return out
            if gs is None or gs > h["max_gs_kt"]:
                return out
            turn = cumulative_turn(win)
            if abs(turn) >= h["min_turn_deg"] and bounding_radius_nm(win) <= h["max_radius_nm"]:
                lat, lon = _centroid(win)
                self.holding_active = True
                self.holding_start_ts = win[0][0]
                self.hold_alt_ft = sum(alts) / len(alts)
                out.append(
                    self._event(
                        "HOLDING_START",
                        airport,
                        ts,
                        fix_lat=round(lat, 4),
                        fix_lon=round(lon, 4),
                        alt_ft=round(self.hold_alt_ft),
                        turn_deg=round(abs(turn)),
                    )
                )
            return out

        # holding already active: look for the exit
        if alt is not None and self.hold_alt_ft is not None:
            if self.hold_alt_ft - alt > h["end_descent_ft"]:
                out.append(self._end_hold(ts, airport, "descended"))
                return out
        end_start = ts - h["end_window_minutes"] * 60000
        if ts - (self.holding_start_ts or ts) >= h["end_window_minutes"] * 60000:
            recent = [p for p in self.buffer if p[0] >= end_start]
            if abs(cumulative_turn(recent)) < h["end_min_turn_deg"]:
                out.append(self._end_hold(ts, airport, "left_pattern"))
        return out

    def _approach(self, ts, airport, agl, gs, dist, vr, on_ground, cfg) -> list[FlightEvent]:
        ga, ld = cfg["go_around"], cfg["landing"]
        out: list[FlightEvent] = []
        landed_now = dist <= ld["max_dist_nm"] and (
            on_ground
            or (agl is not None and agl < ld["agl_ft"] and gs is not None and gs < ld["gs_kt"])
        )
        if landed_now:
            if self.entered_terminal_ts is not None and self.entered_airport == airport:
                if self.holding_active:
                    out.append(self._end_hold(ts, airport, "landed"))
                transit_min = (ts - self.entered_terminal_ts) / 60000
                hold_min = self.holding_total_ms / 60000
                fuel = extra_fuel_kg(hold_min, self.category)
                out.append(
                    self._event(
                        "LANDED",
                        airport,
                        ts,
                        entry_ts=self.entered_terminal_ts,
                        entry_bearing=self.entry_bearing,
                        transit_min=round(transit_min, 2),
                        holding_min=round(hold_min, 2),
                        extra_fuel_kg=round(fuel, 1),
                        extra_co2_kg=round(co2_kg(fuel), 1),
                    )
                )
                self._reset_terminal()
            return out

        if on_ground or agl is None:
            return out
        if not self.approach_armed:
            if agl < ga["armed_agl_ft"] and dist < ga["armed_dist_nm"]:
                self.approach_armed = True
                self.min_agl_near_rwy_ft = agl
            return out
        self.min_agl_near_rwy_ft = min(self.min_agl_near_rwy_ft, agl)
        if (
            agl - self.min_agl_near_rwy_ft >= ga["climb_ft"]
            and vr > ga["climb_vr_fpm"]
            and dist <= ga["max_dist_nm"]
        ):
            out.append(
                self._event(
                    "GO_AROUND",
                    airport,
                    ts,
                    min_agl_ft=round(self.min_agl_near_rwy_ft),
                    alt_ft=round(agl),
                )
            )
            self.approach_armed = False
            self.min_agl_near_rwy_ft = None
        return out

    def _diversion(self, ts, airport, dist, alt, on_ground, agl, gs, cfg) -> list[FlightEvent]:
        if self.entered_terminal_ts is None or self.entered_airport is None:
            return []
        dv, ld = cfg["diversion"], cfg["landing"]
        origin = self.entered_airport
        if airport != origin:
            down = dist <= dv["other_airport_nm"] and (
                on_ground
                or (agl is not None and agl < ld["agl_ft"] and gs is not None and gs < ld["gs_kt"])
            )
            if down:
                return [self._divert(ts, origin, to=airport, reason="landed_elsewhere")]
        apt = load_airports().get(origin)
        if apt is not None and alt is not None and alt > dv["leave_alt_ft"]:
            # use the latest point for the distance to the airport we entered
            p = self.buffer[-1]
            if haversine_nm(p[1], p[2], apt.lat, apt.lon) > dv["leave_dist_nm"]:
                return [self._divert(ts, origin, to=None, reason="left_area")]
        return []

    def _check_departure_far(self, ts, cfg, events: list[FlightEvent]) -> list[FlightEvent]:
        """Aircraft outside every airport's range after entering a terminal area."""
        if self.entered_terminal_ts is not None and self.entered_airport:
            events.append(self._divert(ts, self.entered_airport, to=None, reason="left_area"))
        return events

    def _divert(self, ts: int, origin: str, to: str | None, reason: str) -> FlightEvent:
        ev = self._event("DIVERSION_SUSPECTED", origin, ts, to=to, reason=reason)
        if self.holding_active:
            self.holding_active = False
            self.holding_start_ts = None
        self._reset_terminal()
        return ev

    def finalise_lost(self, now_ts: int) -> list[FlightEvent]:
        """State timed out: a tracked arrival that vanished is a suspected diversion."""
        if self.entered_terminal_ts is not None and self.entered_airport:
            return [self._divert(now_ts, self.entered_airport, to=None, reason="signal_lost")]
        return []
