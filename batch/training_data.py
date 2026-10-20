"""Build the delay-model training frame from silver events, METAR and airport metrics.

Pure pandas so it can be unit tested; the Spark job only loads and saves.
"""

from __future__ import annotations

import json

import pandas as pd

from vimana_common.features import build_features
from vimana_common.geo import sector

MIN_GROUP_SAMPLES = 5
WX_WINDOW_MS = 90 * 60_000
LIVE_KEYS = ["traffic_40nm", "low_alt_count", "arrivals_15m", "holds_15m"]


def _details(df: pd.DataFrame) -> pd.DataFrame:
    parsed = pd.json_normalize(df["details"].map(lambda s: json.loads(s) if s else {}))
    return pd.concat([df.reset_index(drop=True), parsed.reset_index(drop=True)], axis=1)


def arrivals(events: pd.DataFrame) -> pd.DataFrame:
    """LANDED events matched to their ENTERED_TERMINAL entry."""
    landed = _details(events[events["type"] == "LANDED"])
    entries = _details(events[events["type"] == "ENTERED_TERMINAL"])
    entries = entries.rename(columns={"ts": "entry_ts"})[
        ["icao24", "airport", "entry_ts", "entry_bearing", "alt_ft", "gs_kt", "category"]
    ]
    landed = landed.rename(columns={"ts": "landed_ts"})[
        ["icao24", "airport", "landed_ts", "entry_ts", "transit_min", "holding_min"]
    ]
    merged = landed.merge(entries, on=["icao24", "airport", "entry_ts"], how="inner")
    merged["category"] = merged["category"].fillna("narrowbody")
    merged["entry_sector"] = merged["entry_bearing"].fillna(0.0).map(lambda b: sector(b, 8))
    return merged.dropna(subset=["transit_min"])


def unimpeded_minutes(arr: pd.DataFrame) -> dict[str, float]:
    """20th percentile of transit time per (airport, sector, category).

    Groups with too few samples fall back to the airport-level percentile.
    """
    out: dict[str, float] = {}
    by_airport = arr.groupby("airport")["transit_min"].quantile(0.2)
    for (apt, sec, cat), g in arr.groupby(["airport", "entry_sector", "category"]):
        base = g["transit_min"].quantile(0.2) if len(g) >= MIN_GROUP_SAMPLES else by_airport[apt]
        out[f"{apt}|{sec}|{cat}"] = round(float(base), 3)
    return out


def _latest_live(metrics: pd.DataFrame) -> pd.DataFrame:
    """Forward-filled airport_live-like values keyed by window end."""
    if metrics.empty:
        return pd.DataFrame(columns=["airport", "window_end", *LIVE_KEYS])
    flat = pd.json_normalize(metrics["metrics_json"].map(json.loads))
    if "airport" in flat.columns:
        flat = flat.drop(columns=["airport"])
    df = pd.concat([metrics[["airport", "window_end"]].reset_index(drop=True), flat], axis=1)
    df = df.loc[:, ~df.columns.duplicated()]
    for k in LIVE_KEYS:
        if k not in df:
            df[k] = None
    df = df.sort_values(["airport", "window_end"])
    df[LIVE_KEYS] = df.groupby("airport")[LIVE_KEYS].ffill()
    return df[["airport", "window_end", *LIVE_KEYS]]


def build_training_frame(
    events: pd.DataFrame, metar: pd.DataFrame, metrics: pd.DataFrame
) -> tuple[pd.DataFrame, dict[str, float], dict[str, float]]:
    """(training rows, unimpeded minutes table, per-airport median baseline)."""
    arr = arrivals(events)
    if arr.empty:
        return pd.DataFrame(), {}, {}
    unimpeded = unimpeded_minutes(arr)
    arr["unimpeded_min"] = [
        unimpeded[f"{a}|{s}|{c}"]
        for a, s, c in zip(arr["airport"], arr["entry_sector"], arr["category"], strict=True)
    ]
    arr["additional_min"] = (arr["transit_min"] - arr["unimpeded_min"]).clip(lower=0.0)
    arr = arr.sort_values("entry_ts")

    live = _latest_live(metrics).sort_values("window_end")
    wx = metar.sort_values("obs_ts")
    rows = []
    for r in arr.to_dict("records"):
        w = wx[(wx["station"] == r["airport"]) & (wx["obs_ts"] <= r["entry_ts"])]
        w = w[w["obs_ts"] >= r["entry_ts"] - WX_WINDOW_MS]
        wx_row = None if w.empty else w.iloc[-1].to_dict()
        lv = live[(live["airport"] == r["airport"]) & (live["window_end"] <= r["entry_ts"])]
        live_row = None if lv.empty else {k: lv.iloc[-1][k] for k in LIVE_KEYS}
        live_row = {k: (None if pd.isna(v) else v) for k, v in (live_row or {}).items()}
        entry = {
            "airport": r["airport"],
            "ts": r["entry_ts"],
            "entry_bearing": r["entry_bearing"],
            "category": r["category"],
            "alt_ft": r["alt_ft"],
            "gs_kt": r["gs_kt"],
        }
        feats = build_features(entry, wx_row, live_row)
        rows.append(
            {
                **feats,
                "icao24": r["icao24"],
                "entry_ts": r["entry_ts"],
                "additional_min": r["additional_min"],
            }
        )
    frame = pd.DataFrame(rows)
    baseline = frame.groupby("airport")["additional_min"].median().round(3).to_dict()
    return frame, unimpeded, baseline
