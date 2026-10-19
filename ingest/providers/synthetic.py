"""Deterministic, seedable flight simulator emitting OpenSky-shaped payloads.

Geometry is simulated in a flat local frame (x east, y north, nautical miles)
centred on each airport and converted to lat/lon at emission time. Arrivals
fly corridor -> optional holding racetrack -> initial approach fix -> 3 degree
glide path, with go-arounds and fog diversions; departures climb out on the
runway heading. See implementation.md section 6.4.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from ingest.scenario import Scenario, SimClock
from vimana_common.airports import Airport, load_airports
from vimana_common.geo import from_enu_km, heading_diff
from vimana_common.units import NM_TO_KM

STEP_S = 5.0
SPAWN_NM = 120.0
IAF_NM = 15.0
GLIDE_FT_PER_NM = 318.0  # 3 degree path: 6076 ft/NM * tan(3 deg)
TURN_RATE = 3.0  # deg/s, standard rate
IST = timezone(timedelta(hours=5, minutes=30))

AIRLINES = ["AIC", "IGO", "VTI", "AKJ", "SEJ", "AXB", "IAD", "QTR", "UAE", "SIA"]
CATEGORY_WEIGHTS = [("narrowbody", 0.80), ("widebody", 0.15), ("regional", 0.05)]
OPENSKY_CATEGORY = {"regional": 3, "narrowbody": 4, "widebody": 6}

# Bearings (from the airport) that arrival traffic comes from.
CORRIDORS: dict[str, list[float]] = {
    "VIDP": [350, 40, 130, 210, 270, 310],
    "VABB": [10, 50, 90, 140, 190],
    "VOBL": [0, 45, 90, 160, 230, 300],
    "VOMM": [340, 10, 50, 200, 250, 300],
    "VOHS": [0, 60, 120, 180, 240, 300],
    "VECC": [300, 340, 20, 60, 200, 250],
}
DEFAULT_CORRIDORS = [0, 90, 180, 270]


def load_curve(hour_ist: float) -> float:
    """Daily traffic multiplier: quiet at night, peaks 07-10 and 18-22 IST."""
    h = hour_ist % 24
    if 1 <= h < 5:
        return 0.25
    if 5 <= h < 7:
        return 0.6
    if 7 <= h < 10:
        return 1.0
    if 10 <= h < 18:
        return 0.7
    if 18 <= h < 22:
        return 1.0
    return 0.5


def _interp(x: float, x0: float, x1: float, y0: float, y1: float) -> float:
    t = min(max((x - x0) / (x1 - x0), 0.0), 1.0)
    return y0 + t * (y1 - y0)


def alt_profile_ft(dist_nm: float, cruise_ft: float, elev_ft: float) -> float:
    """Target MSL altitude along the inbound path."""
    if dist_nm >= 40:
        return _interp(dist_nm, SPAWN_NM, 40, cruise_ft, 10000.0)
    iaf_alt = elev_ft + IAF_NM * GLIDE_FT_PER_NM
    return _interp(dist_nm, 40, IAF_NM, 10000.0, iaf_alt)


def speed_profile_kt(dist_nm: float, cruise_gs: float) -> float:
    if dist_nm >= 60:
        return cruise_gs
    if dist_nm >= 40:
        return _interp(dist_nm, 60, 40, cruise_gs, 280.0)
    if dist_nm >= 25:
        return _interp(dist_nm, 40, 25, 280.0, 250.0)
    if dist_nm >= IAF_NM:
        return _interp(dist_nm, 25, IAF_NM, 250.0, 200.0)
    return _interp(dist_nm, IAF_NM, 0, 200.0, 140.0)


def fog_factor(visibility_m: float | None) -> float:
    """0 (no fog) to 1 (zero visibility)."""
    if visibility_m is None:
        return 0.0
    return min(max((1500.0 - visibility_m) / 1500.0, 0.0), 1.0)


def hold_probability(traffic: int, capacity: int, fog: float) -> float:
    ratio = traffic / max(capacity, 1)
    return min(max(0.05 + 0.6 * max(0.0, ratio - 0.8) + 0.4 * fog, 0.0), 0.9)


@dataclass
class Aircraft:
    icao24: str
    callsign: str
    category: str
    airport: str
    kind: str  # "arrival" | "departure"
    x: float
    y: float
    alt_ft: float
    gs_kt: float
    hdg: float
    cruise_ft: float = 35000.0
    cruise_gs: float = 460.0
    mode: str = "ENROUTE"
    vr_fpm: float = 0.0
    on_ground: bool = False
    # arrival planning
    hold_decided: bool = False
    hold_planned: bool = False
    hold_done: bool = False
    hold_fix: tuple[float, float] = (0.0, 0.0)
    hold_alt: float = 8000.0
    laps_total: int = 0
    laps_done: int = 0
    hold_leg: str = "turn1"
    leg_t: float = 0.0
    ref_hdg: float = 0.0
    divert_checked: bool = False
    ga_planned: bool = False
    ga_agl: float = 500.0
    ga_used: bool = False
    divert_to: str | None = None
    ground_t: float = 0.0
    corridor: float = 0.0
    extra: dict = field(default_factory=dict)


class Simulator:
    """Advances all aircraft in sim time and emits OpenSky-shaped envelopes."""

    def __init__(
        self,
        scenario: Scenario | None = None,
        seed: int = 42,
        start_sim_ms: int | None = None,
        airports: dict[str, Airport] | None = None,
        clean: bool = False,
    ) -> None:
        self.rng = random.Random(seed)
        self.scenario = scenario or Scenario()
        self.airports = airports or load_airports()
        now_ms = int(datetime.now().timestamp() * 1000)
        self.sim_ms = start_sim_ms if start_sim_ms is not None else now_ms
        self.aircraft: dict[str, Aircraft] = {}
        self.clean = clean  # no noise/drops/corruption (used by tests)
        self.wall_ms_for_scenario: int | None = None  # tests may pin the wall clock

    # ------------------------------------------------------------------ helpers
    def _vis(self, icao: str) -> float | None:
        return self.scenario.visibility_m(icao, self.wall_ms_for_scenario)

    def _hour_ist(self) -> float:
        dt = datetime.fromtimestamp(self.sim_ms / 1000, tz=IST)
        return dt.hour + dt.minute / 60

    def _poisson(self, lam: float) -> int:
        if lam <= 0:
            return 0
        limit, k, p = math.exp(-lam), 0, 1.0
        while True:
            p *= self.rng.random()
            if p <= limit:
                return k
            k += 1

    def _new_icao24(self) -> str:
        while True:
            h = f"{self.rng.randrange(0x100000, 0xFFFFFF):06x}"
            if h not in self.aircraft:
                return h

    def _category(self) -> str:
        r, acc = self.rng.random(), 0.0
        for name, w in CATEGORY_WEIGHTS:
            acc += w
            if r < acc:
                return name
        return "narrowbody"

    def _callsign(self) -> str:
        n = self.rng.choice([3, 4])
        return self.rng.choice(AIRLINES) + "".join(str(self.rng.randrange(10)) for _ in range(n))

    def _traffic_in_terminal(self, icao: str) -> int:
        return sum(
            1
            for a in self.aircraft.values()
            if a.airport == icao
            and a.kind == "arrival"
            and not a.on_ground
            and math.hypot(a.x, a.y) <= 40
        )

    @staticmethod
    def _iaf(apt: Airport) -> tuple[float, float]:
        back = math.radians(apt.main_runway_heading + 180)
        return IAF_NM * math.sin(back), IAF_NM * math.cos(back)

    # -------------------------------------------------------------------- spawn
    def spawn_arrival(self, apt: Airport) -> Aircraft:
        corridors = CORRIDORS.get(apt.icao, DEFAULT_CORRIDORS)
        b = self.rng.choice(corridors) + self.rng.uniform(-8, 8)
        rad = math.radians(b)
        x, y = SPAWN_NM * math.sin(rad), SPAWN_NM * math.cos(rad)
        cruise = float(self.rng.choice(range(30000, 37001, 1000)))
        gs = self.rng.uniform(440, 480)
        ac = Aircraft(
            icao24=self._new_icao24(),
            callsign=self._callsign(),
            category=self._category(),
            airport=apt.icao,
            kind="arrival",
            x=x,
            y=y,
            alt_ft=cruise,
            gs_kt=gs,
            hdg=(b + 180) % 360,
            cruise_ft=cruise,
            cruise_gs=gs,
            corridor=b,
        )
        self.aircraft[ac.icao24] = ac
        return ac

    def spawn_departure(self, apt: Airport) -> Aircraft:
        ac = Aircraft(
            icao24=self._new_icao24(),
            callsign=self._callsign(),
            category=self._category(),
            airport=apt.icao,
            kind="departure",
            x=0.0,
            y=0.0,
            alt_ft=apt.elev_ft,
            gs_kt=0.0,
            hdg=apt.main_runway_heading,
            mode="DEP_GROUND",
            on_ground=True,
        )
        self.aircraft[ac.icao24] = ac
        return ac

    def _spawn_step(self, dt: float) -> None:
        mult = load_curve(self._hour_ist())
        for apt in self.airports.values():
            lam = apt.capacity_per_10min / 10.0 * mult * self.scenario.load_factor(apt.icao)
            for _ in range(self._poisson(lam * dt / 60.0)):
                self.spawn_arrival(apt)
            for _ in range(self._poisson(0.6 * lam * dt / 60.0)):
                self.spawn_departure(apt)

    # --------------------------------------------------------------- kinematics
    @staticmethod
    def _turn(hdg: float, target: float, dt: float, force_right: bool = False) -> float:
        diff = heading_diff(hdg, target)
        if force_right and diff < 0:
            diff += 360.0
        step = TURN_RATE * dt
        if abs(diff) <= step:
            return target % 360.0
        return (hdg + math.copysign(step, diff)) % 360.0

    @staticmethod
    def _approach_value(cur: float, target: float, max_delta: float) -> float:
        if abs(target - cur) <= max_delta:
            return target
        return cur + math.copysign(max_delta, target - cur)

    def _set_alt(self, ac: Aircraft, target_ft: float, dt: float, max_desc=3000, max_climb=2500):
        old = ac.alt_ft
        up, down = max_climb * dt / 60.0, max_desc * dt / 60.0
        if target_ft >= old:
            ac.alt_ft = old + min(target_ft - old, up)
        else:
            ac.alt_ft = old - min(old - target_ft, down)
        ac.vr_fpm = (ac.alt_ft - old) / dt * 60.0

    def _set_speed(self, ac: Aircraft, target_kt: float, dt: float, accel=2.0) -> None:
        ac.gs_kt = self._approach_value(ac.gs_kt, target_kt, accel * dt)

    @staticmethod
    def _advance(ac: Aircraft, dt: float) -> None:
        d = ac.gs_kt * dt / 3600.0
        ac.x += d * math.sin(math.radians(ac.hdg))
        ac.y += d * math.cos(math.radians(ac.hdg))

    # ------------------------------------------------------------ arrival logic
    def _decide_hold(self, ac: Aircraft, apt: Airport) -> None:
        ac.hold_decided = True
        vis = self._vis(apt.icao)
        fog = fog_factor(vis)
        traffic = self._traffic_in_terminal(apt.icao)
        p = hold_probability(traffic, apt.capacity_per_10min, fog)
        if self.rng.random() < p:
            ac.hold_planned = True
            hd = self.rng.uniform(20, 30)
            rad = math.radians(ac.corridor)
            ac.hold_fix = (hd * math.sin(rad), hd * math.cos(rad))
            ac.hold_alt = min(max(alt_profile_ft(hd, ac.cruise_ft, apt.elev_ft), 7000.0), 11000.0)
            stress = max(fog, min(traffic / max(apt.capacity_per_10min, 1), 1.0))
            ac.laps_total = self.rng.randint(1, 3 + int(3 * stress))

    def _step_arrival(self, ac: Aircraft, apt: Airport, dt: float) -> bool:
        """Advance one arrival; return False when it should be removed."""
        dist = math.hypot(ac.x, ac.y)
        rh = apt.main_runway_heading
        iaf = self._iaf(apt)

        if ac.mode == "ENROUTE":
            if not ac.hold_decided and dist < 75:
                self._decide_hold(ac, apt)
            holding_pending = ac.hold_planned and not ac.hold_done
            target = ac.hold_fix if holding_pending else iaf
            dx, dy = target[0] - ac.x, target[1] - ac.y
            ac.hdg = self._turn(ac.hdg, _bearing_xy(dx, dy), dt)
            alt_t = alt_profile_ft(dist, ac.cruise_ft, apt.elev_ft)
            if holding_pending:
                alt_t = max(alt_t, ac.hold_alt)
            self._set_alt(ac, alt_t, dt)
            slowing_for_hold = holding_pending and math.hypot(dx, dy) < 6
            speed_t = 220.0 if slowing_for_hold else speed_profile_kt(dist, ac.cruise_gs)
            self._set_speed(ac, speed_t, dt, accel=3.0)
            self._advance(ac, dt)
            reached = max(1.0, ac.gs_kt * dt / 3600.0 * 1.2)
            if math.hypot(target[0] - ac.x, target[1] - ac.y) < reached:
                if holding_pending:
                    ac.mode, ac.hold_leg, ac.leg_t, ac.ref_hdg = "HOLD", "turn1", 0.0, ac.hdg
                else:
                    self._start_approach(ac, apt)
            return True

        if ac.mode == "HOLD":
            self._step_hold(ac, apt, dt)
            return True

        if ac.mode == "APPROACH":
            ac.hdg = self._turn(ac.hdg, _bearing_xy(-ac.x, -ac.y), dt)
            agl = ac.alt_ft - apt.elev_ft
            path = apt.elev_ft + dist * GLIDE_FT_PER_NM
            self._set_alt(ac, min(ac.alt_ft, path), dt, max_desc=1500)
            self._set_speed(ac, speed_profile_kt(dist, ac.cruise_gs), dt, accel=2.0)
            self._advance(ac, dt)
            if ac.ga_planned and not ac.ga_used and agl <= ac.ga_agl and dist < 5:
                ac.ga_used = True
                ac.mode = "GA_CLIMB"
                return True
            if math.hypot(ac.x, ac.y) < 0.35 or ac.alt_ft - apt.elev_ft <= 10:
                ac.x, ac.y, ac.alt_ft = 0.0, 0.0, apt.elev_ft
                ac.on_ground, ac.mode, ac.vr_fpm, ac.hdg, ac.ground_t = True, "ROLL", 0.0, rh, 0.0
            return True

        if ac.mode == "ROLL":
            self._set_speed(ac, 30.0, dt, accel=4.0)
            self._advance(ac, dt)
            ac.ground_t += dt
            return ac.ground_t < 120.0

        if ac.mode == "GA_CLIMB":
            ac.hdg = self._turn(ac.hdg, rh, dt)
            self._set_alt(ac, apt.elev_ft + 3000.0, dt, max_climb=1500)
            ac.vr_fpm = max(ac.vr_fpm, 0.0)
            self._set_speed(ac, 160.0, dt)
            self._advance(ac, dt)
            if ac.alt_ft - apt.elev_ft >= 2900:
                ac.mode = "GA_RETURN"
            return True

        if ac.mode == "GA_RETURN":
            dx, dy = iaf[0] - ac.x, iaf[1] - ac.y
            ac.hdg = self._turn(ac.hdg, _bearing_xy(dx, dy), dt)
            self._set_alt(ac, apt.elev_ft + IAF_NM * GLIDE_FT_PER_NM, dt, max_climb=1500)
            self._set_speed(ac, 200.0, dt)
            self._advance(ac, dt)
            if math.hypot(dx, dy) < 1.0:
                self._start_approach(ac, apt, allow_ga=False)
            return True

        if ac.mode == "DIVERT":
            other = self.airports[ac.divert_to] if ac.divert_to in self.airports else apt
            tx, ty = _airport_xy(apt, other)
            ac.hdg = self._turn(ac.hdg, _bearing_xy(tx - ac.x, ty - ac.y), dt)
            self._set_alt(ac, 20000.0, dt, max_climb=1800)
            self._set_speed(ac, 330.0, dt, accel=3.0)
            self._advance(ac, dt)
            return dist < SPAWN_NM

        return True

    def _start_approach(self, ac: Aircraft, apt: Airport, allow_ga: bool = True) -> None:
        ac.mode = "APPROACH"
        if allow_ga and not ac.ga_used:
            fog = fog_factor(self._vis(apt.icao))
            p = 0.01 * (8 if fog > 0.3 else 1)
            ac.ga_planned = self.rng.random() < p
            ac.ga_agl = self.rng.uniform(300, 800)

    def _step_hold(self, ac: Aircraft, apt: Airport, dt: float) -> None:
        self._set_alt(ac, ac.hold_alt, dt)
        self._set_speed(ac, 220.0, dt, accel=3.0)
        if ac.hold_leg == "turn1":
            ac.hdg = self._turn(ac.hdg, (ac.ref_hdg + 180) % 360, dt, force_right=True)
            if abs(heading_diff(ac.hdg, ac.ref_hdg + 180)) < 0.5:
                ac.hold_leg, ac.leg_t = "out", 0.0
        elif ac.hold_leg == "out":
            ac.leg_t += dt
            if ac.leg_t >= 60:
                ac.hold_leg = "turn2"
        elif ac.hold_leg == "turn2":
            ac.hdg = self._turn(ac.hdg, ac.ref_hdg, dt, force_right=True)
            if abs(heading_diff(ac.hdg, ac.ref_hdg)) < 0.5:
                ac.hold_leg, ac.leg_t = "in", 0.0
        elif ac.hold_leg == "in":
            ac.leg_t += dt
            if ac.leg_t >= 60:
                ac.laps_done += 1
                if self._should_divert(ac, apt):
                    self._start_divert(ac, apt)
                elif ac.laps_done >= ac.laps_total:
                    ac.hold_done, ac.mode = True, "ENROUTE"
                else:
                    ac.hold_leg = "turn1"
        self._advance(ac, dt)

    def _should_divert(self, ac: Aircraft, apt: Airport) -> bool:
        if ac.divert_checked or ac.laps_done < 2 or len(self.airports) < 2:
            return False
        ac.divert_checked = True
        vis = self._vis(apt.icao)
        return vis is not None and vis < 300 and self.rng.random() < 0.3

    def _start_divert(self, ac: Aircraft, apt: Airport) -> None:
        others = [a for a in self.airports.values() if a.icao != apt.icao]
        nearest = min(others, key=lambda o: math.hypot(*_airport_xy(apt, o)))
        ac.divert_to, ac.mode = nearest.icao, "DIVERT"

    # ----------------------------------------------------------- departure logic
    def _step_departure(self, ac: Aircraft, apt: Airport, dt: float) -> bool:
        rh = apt.main_runway_heading
        dist = math.hypot(ac.x, ac.y)
        if ac.mode == "DEP_GROUND":
            ac.ground_t += dt
            if ac.ground_t >= 60:
                ac.mode = "DEP_ROLL"
            return True
        if ac.mode == "DEP_ROLL":
            self._set_speed(ac, 160.0, dt, accel=6.0)
            self._advance(ac, dt)
            if ac.gs_kt >= 150:
                ac.on_ground, ac.mode = False, "DEP_CLIMB"
            return True
        ac.hdg = self._turn(ac.hdg, rh, dt)
        self._set_alt(ac, 24000.0, dt, max_climb=2500)
        self._set_speed(ac, 250.0 if ac.alt_ft < 10000 + apt.elev_ft else 400.0, dt, accel=3.0)
        self._advance(ac, dt)
        return dist < SPAWN_NM

    # ----------------------------------------------------------------- stepping
    def _step_all(self, dt: float) -> None:
        for icao24 in list(self.aircraft):
            ac = self.aircraft[icao24]
            apt = self.airports[ac.airport]
            keep = (
                self._step_arrival(ac, apt, dt)
                if ac.kind == "arrival"
                else self._step_departure(ac, apt, dt)
            )
            if not keep:
                del self.aircraft[icao24]
        self._spawn_step(dt)

    def step_to(self, target_sim_ms: int, emit: bool = True) -> list[dict]:
        """Advance the simulation to target_sim_ms in 5 s steps; return envelopes."""
        out: list[dict] = []
        while self.sim_ms + STEP_S * 1000 <= target_sim_ms:
            self._step_all(STEP_S)
            self.sim_ms += int(STEP_S * 1000)
            if emit:
                out.extend(self._emit_all())
        return out

    def warm_start(self, minutes: float = 20.0) -> None:
        """Pre-populate the airspace so a fresh run shows traffic immediately."""
        end = self.sim_ms
        self.sim_ms = end - int(minutes * 60_000)
        self.step_to(end, emit=False)

    # ----------------------------------------------------------------- emission
    def _emit_all(self) -> list[dict]:
        out: list[dict] = []
        for ac in self.aircraft.values():
            env = self._envelope(ac)
            if env is None:
                continue
            out.append(env)
            if not self.clean and self.rng.random() < 0.005:
                out.append(env)  # duplicate
        return out

    def _envelope(self, ac: Aircraft) -> dict | None:
        r = self.rng
        if not self.clean and r.random() < 0.02:
            return None  # dropped message
        apt = self.airports[ac.airport]
        noise = not self.clean
        x_km = ac.x * NM_TO_KM + (r.gauss(0, 0.03) if noise else 0.0)
        y_km = ac.y * NM_TO_KM + (r.gauss(0, 0.03) if noise else 0.0)
        lat, lon = from_enu_km(x_km, y_km, apt.lat, apt.lon)
        alt_ft = ac.alt_ft + (r.gauss(0, 25) if noise and not ac.on_ground else 0.0)
        gs = max(ac.gs_kt + (r.gauss(0, 2) if noise else 0.0), 0.0)
        track = (ac.hdg + (r.gauss(0, 1) if noise else 0.0)) % 360.0
        icao24: str | None = ac.icao24
        if noise and r.random() < 0.002:
            if r.random() < 0.5:
                lat = 95.0 + r.random() * 10
            else:
                icao24 = None
        t_s = self.sim_ms // 1000
        state = [
            icao24,
            f"{ac.callsign:<8}",
            "India",
            t_s,
            t_s,
            round(lon, 6),
            round(lat, 6),
            round(alt_ft / 3.28084, 1),
            ac.on_ground,
            round(gs / 1.943844, 2),
            round(track, 1),
            round(ac.vr_fpm / 196.8504, 2),
            None,
            round(alt_ft / 3.28084, 1),
            "2000",
            False,
            0,
            OPENSKY_CATEGORY.get(ac.category, 4),
        ]
        return {
            "source": "synthetic",
            "fetched_at": self.sim_ms,
            "airport_hint": ac.airport,
            "state": state,
        }


def _bearing_xy(dx: float, dy: float) -> float:
    return (math.degrees(math.atan2(dx, dy)) + 360.0) % 360.0


def _airport_xy(origin: Airport, other: Airport) -> tuple[float, float]:
    """Position of another airport in the origin airport's local frame (NM)."""
    from vimana_common.geo import to_enu_km

    e, n = to_enu_km(other.lat, other.lon, origin.lat, origin.lon)
    return e / NM_TO_KM, n / NM_TO_KM


class SyntheticProvider:
    """Provider adapter around Simulator driven by a SimClock."""

    name = "synthetic"

    def __init__(
        self, clock: SimClock, scenario: Scenario, seed: int = 42, now_ms: int | None = None
    ) -> None:
        self.clock = clock
        self.sim = Simulator(scenario=scenario, seed=seed, start_sim_ms=clock.sim_ms(now_ms))
        self.sim.warm_start()

    def poll(self, now_ms: int) -> list[dict]:
        return self.sim.step_to(self.clock.sim_ms(now_ms))
