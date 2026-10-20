"""OpenSky Network provider: OAuth2 client credentials and a daily credit budget."""

from __future__ import annotations

import httpx

from ingest.providers.base import Envelope
from vimana_common.airports import load_airports
from vimana_common.logging import get_logger

STATES_URL = "https://opensky-network.org/api/states/all"
TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
)
BOX_DEG = 1.5
MIN_INTERVAL_S = 10.0


class CreditBudget:
    """Spreads a daily credit allowance evenly over the polled airports."""

    def __init__(self, daily_credits: int, credits_per_call: int, n_airports: int) -> None:
        self.daily_credits = daily_credits
        self.credits_per_call = credits_per_call
        self.n_airports = max(n_airports, 1)

    def interval_s(self) -> float:
        calls_per_day = max(self.daily_credits / max(self.credits_per_call, 1), 1)
        return max(86400 * self.n_airports / calls_per_day, MIN_INTERVAL_S)


class OpenSkyProvider:
    name = "opensky"

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        daily_credits: int = 4000,
        credits_per_call: int = 1,
        client: httpx.Client | None = None,
    ) -> None:
        self.client_id, self.client_secret = client_id, client_secret
        self.client = client or httpx.Client(timeout=20)
        self.log = get_logger("opensky")
        airports = list(load_airports().values())
        self._airports = {a.icao: a for a in airports}
        self.budget = CreditBudget(daily_credits, credits_per_call, len(airports))
        self._next = {a.icao: 0 for a in airports}
        self._token: str | None = None
        self._token_exp_ms = 0
        self._blocked_until_ms = 0

    def _auth_headers(self, now_ms: int) -> dict[str, str]:
        if not self.client_id:
            return {}  # anonymous access has a much smaller allowance
        if self._token is None or now_ms >= self._token_exp_ms:
            resp = self.client.post(
                TOKEN_URL,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
            )
            resp.raise_for_status()
            body = resp.json()
            self._token = body["access_token"]
            self._token_exp_ms = now_ms + (int(body.get("expires_in", 300)) - 60) * 1000
        return {"Authorization": f"Bearer {self._token}"}

    def parse(self, icao: str, payload: dict, fetched_at: int) -> list[Envelope]:
        return [
            {"source": self.name, "fetched_at": fetched_at, "airport_hint": icao, "state": s}
            for s in payload.get("states") or []
            if len(s) > 7 and s[5] is not None and s[6] is not None
        ]

    def poll(self, now_ms: int) -> list[Envelope]:
        out: list[Envelope] = []
        if now_ms < self._blocked_until_ms:
            return out
        interval_ms = int(self.budget.interval_s() * 1000)
        for icao, due in self._next.items():
            if now_ms < due:
                continue
            self._next[icao] = now_ms + interval_ms
            apt = self._airports[icao]
            params = {
                "lamin": apt.lat - BOX_DEG,
                "lamax": apt.lat + BOX_DEG,
                "lomin": apt.lon - BOX_DEG,
                "lomax": apt.lon + BOX_DEG,
                "extended": 1,
            }
            try:
                resp = self.client.get(
                    STATES_URL, params=params, headers=self._auth_headers(now_ms)
                )
                if resp.status_code == 429:
                    wait = float(resp.headers.get("X-Rate-Limit-Retry-After-Seconds", 60))
                    self._blocked_until_ms = now_ms + int(wait * 1000)
                    self.log.warning("rate limited", retry_after_s=wait)
                    break
                resp.raise_for_status()
                out.extend(self.parse(icao, resp.json(), now_ms))
            except (httpx.HTTPError, ValueError, KeyError) as exc:
                self.log.warning("poll failed", airport=icao, error=str(exc))
        return out
