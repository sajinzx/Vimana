import json

import numpy as np
import pandas as pd

from batch.b00_bootstrap_static import parse_reference
from batch.flight_efficiency import (
    compute_efficiency,
    inefficiency_pct,
    split_into_flights,
)
from batch.training_data import build_training_frame
from batch.trajectory import anomaly_flags, extract_approaches, resample_path
from vimana_common.airports import get_airport
from vimana_common.geo import destination

APT = get_airport("VIDP")


def pt(ts, lat, lon, ground=False):
    return (ts, lat, lon, 5000.0, ground, "AIC1")


def test_split_into_flights_on_gap_and_ground():
    a = [pt(i * 10_000, 28.0, 77.0 + i * 0.01) for i in range(6)]
    b = [pt(40 * 60_000 + i * 10_000, 28.0, 77.2 + i * 0.01) for i in range(6)]
    flights = split_into_flights(("abc", a + b))
    assert len(flights) == 2
    short = split_into_flights(("abc", a[:3]))
    assert short == []
    grounded = a[:5] + [pt(60_000, 28.0, 77.1, ground=True)] + b
    assert len(split_into_flights(("abc", grounded))) == 2


def test_efficiency_straight_vs_detour():
    airports = {"VIDP": (APT.lat, APT.lon)}
    straight = [pt(i, *destination(APT.lat, APT.lon, 270, 90 - i * 10)) for i in range(9)]
    flown, direct, n = compute_efficiency(("f", straight), airports, 100.0)[1]
    assert n == 1 and abs(inefficiency_pct(flown, direct)) < 0.5
    detour = list(straight)
    detour.insert(4, pt(3.5, *destination(APT.lat, APT.lon, 330, 40)))
    flown, direct, _ = compute_efficiency(("f", detour), airports, 100.0)[1]
    assert inefficiency_pct(flown, direct) > 5


def test_resample_path_is_evenly_spaced():
    xy = np.array([[0.0, 0.0], [10.0, 0.0], [10.0, 10.0]])
    r = resample_path(xy, 5)
    assert r.shape == (5, 2) and np.allclose(r[0], [0, 0]) and np.allclose(r[-1], [10, 10])
    assert np.allclose(r[2], [10, 0])  # halfway along the 20 km path


def _approach_track(icao24, bearing, n=40):
    rows = []
    for i in range(n):
        dist = 40 - i
        lat, lon = destination(APT.lat, APT.lon, bearing, max(dist, 0.5))
        rows.append(
            {
                "icao24": icao24,
                "airport": "VIDP",
                "ts": i * 10_000,
                "lat": lat,
                "lon": lon,
                "dist_nm": max(dist, 0.5),
                "on_ground": i == n - 1,
            }
        )
    return rows


def test_extract_approaches_and_anomaly_flags():
    rows = []
    for k in range(12):
        rows += _approach_track(f"a{k:05d}", 270)
    rows += _approach_track("b00001", 90)  # lands from the other side
    out = extract_approaches(pd.DataFrame(rows), {"VIDP": (APT.lat, APT.lon)})
    assert len(out) == 13 and out[0]["vector"].shape == (40,)
    X = np.array([o["vector"] for o in out])
    centroids = np.array([X[:12].mean(axis=0)])
    labels = np.zeros(len(X), dtype=int)
    dist, flags = anomaly_flags(X, labels, centroids, pct=90)
    assert flags[-1] and dist[-1] > dist[0]


def test_parse_reference_csv():
    airports = (
        "ident,latitude_deg,longitude_deg,elevation_ft\nVIDP,28.5665,77.1031,777\nXXXX,1,2,3\n"
    )
    runways = (
        "airport_ident,closed,le_heading_degT,he_heading_degT\nVIDP,0,103.5,283.5\nVIDP,1,10,190\n"
    )
    ref = parse_reference(airports, runways, {"VIDP"})
    assert ref == {
        "VIDP": {
            "lat": 28.5665,
            "lon": 77.1031,
            "elev_ft": 777.0,
            "runway_headings": [103.5, 283.5],
        }
    }


def test_build_training_frame_labels_and_features():
    ev = []
    t0 = 1_760_000_000_000
    for i in range(12):
        entry = t0 + i * 600_000
        extra = 8.0 if i >= 6 else 0.0
        ev.append(
            dict(
                type="ENTERED_TERMINAL",
                icao24=f"a{i:05d}",
                callsign="AIC1",
                airport="VIDP",
                ts=entry,
                details=json.dumps(
                    {
                        "entry_bearing": 270.0,
                        "alt_ft": 11000,
                        "gs_kt": 250,
                        "category": "narrowbody",
                    }
                ),
            )
        )
        ev.append(
            dict(
                type="LANDED",
                icao24=f"a{i:05d}",
                callsign="AIC1",
                airport="VIDP",
                ts=entry + int((14 + extra) * 60000),
                details=json.dumps(
                    {"entry_ts": entry, "transit_min": 14 + extra, "holding_min": extra}
                ),
            )
        )
    metar = pd.DataFrame(
        [
            {
                "station": "VIDP",
                "obs_ts": t0,
                "visibility_m": 300.0,
                "ceiling_ft": 100,
                "wind_kt": 3.0,
                "wind_dir_deg": 90.0,
                "fog": True,
                "low_vis": True,
            }
        ]
    )
    metrics = pd.DataFrame(
        [
            {
                "airport": "VIDP",
                "window_end": t0 + 60_000,
                "metrics_json": json.dumps({"traffic_40nm": 9, "holds_15m": 1}),
            }
        ]
    )
    frame, unimpeded, baseline = build_training_frame(pd.DataFrame(ev), metar, metrics)
    assert len(frame) == 12
    assert unimpeded["VIDP|6|narrowbody"] == 14.0
    assert frame["additional_min"].iloc[-1] == 8.0 and frame["additional_min"].iloc[0] == 0.0
    assert frame["wx_missing"].iloc[0] == 0 and frame["fog"].iloc[0] == 1
    assert frame["traffic_40nm"].iloc[0] == 0.0  # window closes after the first entry
    assert frame["traffic_40nm"].iloc[1] == 9.0
    assert baseline["VIDP"] == 4.0
