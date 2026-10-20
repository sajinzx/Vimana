"""Fetch OurAirports reference data for the configured airports.

Writes DATA_DIR/ref/airports.json (coordinates, elevation, runway headings),
which vimana_common.airports reads as an override of config/airports.yaml.
Offline or on any failure the seeds remain in effect and the job still
succeeds, so the stack starts without network access.
"""

from __future__ import annotations

import csv
import io
import json
from pathlib import Path

import httpx

from vimana_common.logging import get_logger
from vimana_common.settings import airports_config, get_settings

BASE = "https://davidmegginson.github.io/ourairports-data"


def parse_reference(airports_csv: str, runways_csv: str, wanted: set[str]) -> dict[str, dict]:
    """Extract lat/lon/elevation and runway headings (deg true) per wanted ICAO."""
    out: dict[str, dict] = {}
    for row in csv.DictReader(io.StringIO(airports_csv)):
        icao = row.get("ident", "").upper()
        if icao in wanted:
            try:
                out[icao] = {
                    "lat": float(row["latitude_deg"]),
                    "lon": float(row["longitude_deg"]),
                    "elev_ft": float(row["elevation_ft"]) if row.get("elevation_ft") else None,
                    "runway_headings": [],
                }
            except (KeyError, ValueError):
                continue
    for row in csv.DictReader(io.StringIO(runways_csv)):
        icao = row.get("airport_ident", "").upper()
        if icao not in out or row.get("closed") == "1":
            continue
        for col in ("le_heading_degT", "he_heading_degT"):
            try:
                out[icao]["runway_headings"].append(float(row[col]))
            except (KeyError, ValueError):
                pass
    for v in out.values():
        if v["elev_ft"] is None:
            del v["elev_ft"]
        if not v["runway_headings"]:
            del v["runway_headings"]
    return out


def main() -> None:
    log = get_logger("bootstrap")
    wanted = {a["icao"].upper() for a in airports_config()["airports"]}
    ref_dir = Path(get_settings().data_dir) / "ref"
    ref_dir.mkdir(parents=True, exist_ok=True)
    target = ref_dir / "airports.json"
    try:
        with httpx.Client(timeout=20, follow_redirects=True) as client:
            airports = client.get(f"{BASE}/airports.csv").text
            runways = client.get(f"{BASE}/runways.csv").text
        data = parse_reference(airports, runways, wanted)
        target.write_text(json.dumps(data, indent=2), encoding="utf-8")
        log.info("reference data written", airports=sorted(data))
    except Exception as exc:  # noqa: BLE001 - offline start is supported
        log.warning("reference download failed; using config seeds", error=str(exc))


if __name__ == "__main__":
    main()
