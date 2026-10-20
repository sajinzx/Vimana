"""adsb.lol provider: free, no key. One request per airport every ADSBLOL_POLL_S seconds."""

from __future__ import annotations

import httpx

from ingest.providers.base import Envelope
from vimana_common.airports import load_airports
from vimana_common.logging import get_logger

URL = "https://api.adsb.lol/v2/point/{lat}/{lon}/{radius}"
RADIUS_NM = 100
HEADERS = {"User-Agent": "vimana-student-project/1.0"}


class AdsbLolProvider:
    name = "adsblol"

    def __init__(self, poll_s: float = 15.0, client: httpx.Client | None = None) -> None:
        self.poll_s = max(poll_s, 5.0)
        self.client = client or httpx.Client(timeout=15, headers=HEADERS)
        self.log = get_logger("adsblol")
        airports = list(load_airports().values())
        stagger = self.poll_s / max(len(airports), 1) * 1000
        # first poll of each airport is spread over one interval to avoid a burst
        self._next = {a.icao: int(i * stagger) for i, a in enumerate(airports)}
        self._airports = {a.icao: a for a in airports}

    def parse(self, icao: str, payload: dict, fetched_at: int) -> list[Envelope]:
        out = []
        for ac in payload.get("ac") or []:
            if ac.get("lat") is None or ac.get("lon") is None or not ac.get("hex"):
                continue
            out.append(
                {"source": self.name, "fetched_at": fetched_at, "airport_hint": icao, "ac": ac}
            )
        return out

    def poll(self, now_ms: int) -> list[Envelope]:
        out: list[Envelope] = []
        for icao, due in self._next.items():
            if now_ms < due:
                continue
            self._next[icao] = now_ms + int(self.poll_s * 1000)
            apt = self._airports[icao]
            try:
                resp = self.client.get(URL.format(lat=apt.lat, lon=apt.lon, radius=RADIUS_NM))
                resp.raise_for_status()
                out.extend(self.parse(icao, resp.json(), now_ms))
            except (httpx.HTTPError, ValueError) as exc:
                self.log.warning("poll failed", airport=icao, error=str(exc))
        return out
