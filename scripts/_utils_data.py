"""Shared data loading, field normalization, and statistics utilities."""
import csv
import math
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Type conversion
# ---------------------------------------------------------------------------

def to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return default


def to_int(value: Any, default: int = 0) -> int:
    return int(to_float(value, float(default)))


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------

def fmt_money(value: Any) -> str:
    amount = to_float(value)
    if abs(amount) >= 1_000_000:
        return f"${amount / 1_000_000:.2f}M"
    if abs(amount) >= 1_000:
        return f"${amount / 1_000:.1f}K"
    return f"${amount:.0f}"


def fmt_pct(value: Any, signed: bool = True) -> str:
    number = to_float(value)
    prefix = "+" if signed and number > 0 else ""
    return f"{prefix}{number:.1f}%"


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

def median(values: list[float]) -> float:
    clean = [v for v in values if not math.isnan(v)]
    return statistics.median(clean) if clean else 0.0


def average(values: list[float]) -> float:
    clean = [v for v in values if not math.isnan(v)]
    return sum(clean) / len(clean) if clean else 0.0


def percentile(values: list[float], pct: float) -> float:
    clean = sorted(v for v in values if not math.isnan(v))
    if not clean:
        return 0.0
    pos = (len(clean) - 1) * pct / 100
    low = math.floor(pos)
    high = math.ceil(pos)
    if low == high:
        return clean[low]
    return clean[low] * (high - pos) + clean[high] * (pos - low)


# ---------------------------------------------------------------------------
# Bucketing
# ---------------------------------------------------------------------------

def mcap_bucket(mcap: float) -> str:
    if mcap < 20_000:
        return "<20K"
    if mcap < 50_000:
        return "20-50K"
    if mcap < 100_000:
        return "50-100K"
    return ">=100K"


def gain_bucket(value: float) -> str:
    if value >= 200:
        return ">=200%"
    if value >= 100:
        return "100-200%"
    if value >= 50:
        return "50-100%"
    if value >= 30:
        return "30-50%"
    if value >= 10:
        return "10-30%"
    return "<10%"


def drawdown_bucket(value: float) -> str:
    if value >= -20:
        return ">=-20%"
    if value >= -50:
        return "-50--20%"
    if value >= -80:
        return "-80--50%"
    return "<-80%"


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def load_csv(path: Path, encoding: str = "utf-8-sig") -> list[dict[str, Any]]:
    """Load a CSV file into a list of dicts."""
    rows = []
    with path.open("r", encoding=encoding, newline="") as handle:
        for raw in csv.DictReader(handle):
            rows.append(dict(raw))
    return rows


def normalize_rows(rows: list[dict], mappings: dict[str, tuple[str, callable]]) -> None:
    """Normalize fields in-place.

    mappings format: {'output_key': ('input_key', converter_fn)}
    Example: normalize_rows(rows, {'max_gain': ('max_gain_pct', to_float)})
    """
    for row in rows:
        for out_key, (in_key, converter) in mappings.items():
            row[out_key] = converter(row.get(in_key, ""))


# ---------------------------------------------------------------------------
# Group statistics
# ---------------------------------------------------------------------------

def group_stat(rows: list[dict], gain_key: str = "max_gain",
               current_key: str = "current_return",
               drawdown_key: str = "max_drawdown") -> dict[str, Any]:
    """Compute summary statistics for a group of rows."""
    if not rows:
        return {"count": 0, "hit30": 0.0, "hit100": 0.0, "alive": 0.0,
                "median_gain": 0.0, "avg_gain": 0.0, "median_current": 0.0,
                "median_drawdown": 0.0}
    gains = [to_float(r.get(gain_key)) for r in rows]
    currents = [to_float(r.get(current_key)) for r in rows]
    drawdowns = [to_float(r.get(drawdown_key)) for r in rows]
    return {
        "count": len(rows),
        "hit30": sum(1 for g in gains if g >= 30) / len(rows) * 100,
        "hit100": sum(1 for g in gains if g >= 100) / len(rows) * 100,
        "alive": sum(1 for c in currents if c > 0) / len(rows) * 100,
        "median_gain": median(gains),
        "avg_gain": average(gains),
        "median_current": median(currents),
        "median_drawdown": median(drawdowns),
    }


def bucket_counts(rows: list[dict], key: str,
                  order: list[str] | None = None) -> list[tuple[str, int]]:
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        counts[str(row.get(key, ""))] += 1
    if order:
        return [(label, counts.get(label, 0)) for label in order if counts.get(label, 0)]
    return sorted(counts.items())
