from __future__ import annotations

from typing import Any


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def short_ca(address: str, left: int = 6, right: int = 6) -> str:
    if not address:
        return ""
    if len(address) <= left + right:
        return address
    return f"{address[:left]}...{address[-right:]}"


def calc_mcap(price: Any, supply: Any) -> float:
    return safe_float(price) * safe_float(supply)

