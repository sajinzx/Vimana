"""Provider protocol: anything that yields adsb.raw envelopes."""

from __future__ import annotations

from typing import Protocol

Envelope = dict


class Provider(Protocol):
    """A source of provider-native ADS-B envelopes (see spec section 7.1)."""

    name: str

    def poll(self, now_ms: int) -> list[Envelope]:
        """Return envelopes that became available up to now (may be empty)."""
        ...
