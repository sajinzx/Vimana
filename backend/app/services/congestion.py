"""Airport congestion score (0-100) and colour band."""

from __future__ import annotations


def congestion_score(
    traffic_40nm: float | None,
    capacity_per_10min: float,
    holds_15m: float | None,
    low_vis: bool | int | None,
) -> float:
    traffic = (traffic_40nm or 0.0) / max(capacity_per_10min, 1)
    score = 100 * (
        0.5 * min(traffic, 1.5) / 1.5 + 0.3 * min((holds_15m or 0.0) / 4, 1.0) + 0.2 * bool(low_vis)
    )
    return round(score, 1)


def band(score: float) -> str:
    if score < 35:
        return "green"
    if score <= 65:
        return "amber"
    return "red"
