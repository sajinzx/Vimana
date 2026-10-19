"""Approximate fuel burn and CO2 caused by airborne holding."""

from __future__ import annotations

from vimana_common.settings import fuel_config

DEFAULT_CATEGORY = "narrowbody"


def fuel_rate_kg_per_min(category: str | None) -> float:
    rates = fuel_config()["holding_fuel_kg_per_min"]
    return float(rates.get(category or DEFAULT_CATEGORY, rates[DEFAULT_CATEGORY]))


def extra_fuel_kg(holding_min: float, category: str | None) -> float:
    return max(holding_min, 0.0) * fuel_rate_kg_per_min(category)


def co2_kg(fuel_kg: float) -> float:
    return fuel_kg * float(fuel_config()["co2_per_kg_fuel"])
