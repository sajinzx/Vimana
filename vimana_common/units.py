"""Unit conversions used when standardising ADS-B data."""

from __future__ import annotations

M_TO_FT = 3.28084
MS_TO_KT = 1.943844
MS_TO_FPM = 196.8504
NM_TO_M = 1852.0
NM_TO_KM = 1.852
FT_TO_M = 1 / M_TO_FT


def m_to_ft(m: float) -> float:
    return m * M_TO_FT


def ft_to_m(ft: float) -> float:
    return ft / M_TO_FT


def ms_to_kt(ms: float) -> float:
    return ms * MS_TO_KT


def kt_to_ms(kt: float) -> float:
    return kt / MS_TO_KT


def ms_to_fpm(ms: float) -> float:
    return ms * MS_TO_FPM


def fpm_to_ms(fpm: float) -> float:
    return fpm / MS_TO_FPM


def nm_to_km(nm: float) -> float:
    return nm * NM_TO_KM


def km_to_nm(km: float) -> float:
    return km / NM_TO_KM
