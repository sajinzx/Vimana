"""Gold-layer Delta tables read through deltalake (no Spark in the API)."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from vimana_common.settings import get_settings

TABLES = {
    "airport_hourly": "gold_airport_hourly",
    "airline_stats": "gold_airline_stats",
    "weather_impact": "gold_weather_impact",
    "daily_kpis": "gold_daily_kpis",
    "holding_events": "gold_holding_events",
    "airport_efficiency": "gold_airport_efficiency",
    "approach_clusters": "gold_approach_clusters",
    "data_quality": "gold_data_quality",
}
CACHE_TTL_S = 60


class GoldRepo:
    def __init__(self, root: str | None = None) -> None:
        self.root = Path(root or f"{get_settings().data_dir}/delta/gold")
        self._cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}

    def available(self) -> bool:
        return self.root.exists()

    def read(self, name: str) -> list[dict[str, Any]] | None:
        """Rows for a gold table, or None when the table has not been built yet."""
        if name not in TABLES:
            raise KeyError(name)
        hit = self._cache.get(name)
        if hit and time.time() - hit[0] < CACHE_TTL_S:
            return hit[1]
        path = self.root / TABLES[name]
        if not path.exists():
            return None
        from deltalake import DeltaTable

        df = DeltaTable(str(path)).to_pandas()
        rows = json.loads(df.to_json(orient="records", date_format="iso"))
        self._cache[name] = (time.time(), rows)
        return rows
