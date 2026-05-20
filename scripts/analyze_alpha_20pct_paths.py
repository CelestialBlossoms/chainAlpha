#!/usr/bin/env python3
"""Analyze Alpha 1m push paths using +20% as the baseline threshold."""

from __future__ import annotations

import argparse
import csv
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from db_client import db_op
from scripts._utils_data import to_float
from scripts._utils_kline import fetch_range, pct


DEFAULT_OUTPUT = ROOT / "gmgn_outputs" / "alpha_1m_push_20pct_paths_20260520.csv"


def to_ts(value: Any) -> int:
    if isinstance(value, datetime):
        return int(value.timestamp())
    return int(to_float(value))


def fmt_ts(value: Any) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return ""


def fetch_events() -> list[dict[str, Any]]:
    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            SELECT e.id, e.address, e.chain, e.symbol, e.entry_mcap, e.entry_price,
                   e.holder_count, e.fee_sol, e.buy_score, e.pushed_at,
                   e.raw_stats->>'name',
                   COALESCE(e.raw_stats->>'narrative_desc', e.raw_stats->>'narrative', ''),
                   COALESCE(e.raw_stats->>'narrative_type', ''),
                   c.pool_liquidity, c.sm_count, c.kol_count, c.top10_rate,
                   c.snipers, c.rug_ratio
            FROM alpha_push_events e
            LEFT JOIN alpha_token_candidates c ON c.address = e.address
            WHERE e.trend_interval = '1m'
            ORDER BY e.pushed_at ASC
            """
        )
        rows = []
        for row in cur.fetchall():
            rows.append(
                {
                    "id": row[0],
                    "address": row[1],
                    "chain": row[2] or "sol",
                    "symbol": row[3] or "",
                    "entry_mcap": to_float(row[4]),
                    "entry_price_db": to_float(row[5]),
                    "holder_count": int(row[6] or 0),
                    "fee_sol": to_float(row[7]),
                    "buy_score": int(row[8] or 0),
                    "pushed_at": row[9],
                    "pushed_ts": to_ts(row[9]),
                    "name": row[10] or "",
                    "narrative_desc": row[11] or "",
                    "narrative_type": row[12] or "",
                    "pool_liquidity": to_float(row[13]),
                    "sm_count": int(row[14] or 0),
                    "kol_count": int(row[15] or 0),
                    "top10_rate": to_float(row[16]),
                    "snipers": int(row[17] or 0),
                    "rug_ratio": to_float(row[18]),
                }
            )
        return rows

    return db_op(_op)


def classify_path(max_gain: float, current_return: float, min_before_20: float, reached_20: bool) -> str:
    if reached_20 and min_before_20 <= -5:
        return "回撤后上涨"
    if reached_20:
        return "直接上涨"
    if current_return <= -80 or min_before_20 <= -80:
        return "直接下跌归零"
    return "未达20%观察"


def analyze_event(event: dict[str, Any], now_ts: int) -> dict[str, Any]:
    candles = fetch_range(event["address"], max(0, event["pushed_ts"] - 10 * 60), now_ts)
    post = [candle for candle in candles if candle["ts"] >= event["pushed_ts"]]
    if not post:
        return {**event, "valid": False, "path_class": "无K线"}

    entry = event["entry_price_db"] or post[0]["open"] or post[0]["close"]
    max_high = max(candle["high"] for candle in post)
    min_low = min(candle["low"] for candle in post)
    current_close = post[-1]["close"]
    max_gain = pct(max_high, entry)
    current_return = pct(current_close, entry)
    max_drawdown = pct(min_low, entry)

    reached_20 = False
    first_20_ts = 0
    first_20_min = 0.0
    min_before_20 = 0.0
    min_seen = 0.0
    for candle in post:
        min_seen = min(min_seen, pct(candle["low"], entry))
        if candle["high"] >= entry * 1.2:
            reached_20 = True
            first_20_ts = int(candle["ts"])
            first_20_min = (first_20_ts - event["pushed_ts"]) / 60
            min_before_20 = min_seen
            break
    if not reached_20:
        min_before_20 = max_drawdown

    path_class = classify_path(max_gain, current_return, min_before_20, reached_20)
    max_mcap = event["entry_mcap"] * (1 + max_gain / 100)
    current_mcap_est = event["entry_mcap"] * (1 + current_return / 100)
    min_mcap_est = event["entry_mcap"] * (1 + max_drawdown / 100)

    return {
        **event,
        "valid": True,
        "candles": len(post),
        "entry_price": entry,
        "max_gain_pct": max_gain,
        "current_return_pct": current_return,
        "max_drawdown_pct": max_drawdown,
        "reached_20": reached_20,
        "time_to_20_min": first_20_min,
        "min_before_20_pct": min_before_20,
        "path_class": path_class,
        "max_mcap_est": max_mcap,
        "current_mcap_est": current_mcap_est,
        "min_mcap_est": min_mcap_est,
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "id", "symbol", "address", "pushed_at", "entry_mcap", "max_mcap_est",
        "current_mcap_est", "min_mcap_est", "path_class", "reached_20",
        "time_to_20_min", "min_before_20_pct", "max_gain_pct", "current_return_pct",
        "max_drawdown_pct", "holder_count", "fee_sol", "buy_score", "pool_liquidity",
        "sm_count", "kol_count", "top10_rate", "snipers", "rug_ratio",
        "name", "narrative_type", "narrative_desc", "valid", "candles",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            row = {**row, "pushed_at": fmt_ts(row.get("pushed_at"))}
            writer.writerow({field: row.get(field, "") for field in fields})


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze Alpha 1m +20% push paths.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--sleep", type=float, default=0.08)
    args = parser.parse_args()

    events = fetch_events()
    now_ts = int(time.time())
    rows = []
    for index, event in enumerate(events, start=1):
        row = analyze_event(event, now_ts)
        rows.append(row)
        print(
            f"[{index}/{len(events)}] {event['symbol']} {event['address'][:8]} "
            f"{row.get('path_class')} max={to_float(row.get('max_gain_pct')):.1f}% "
            f"cur={to_float(row.get('current_return_pct')):.1f}%"
        )
        time.sleep(args.sleep)
    write_csv(args.output, rows)
    print(f"CSV: {args.output}")


if __name__ == "__main__":
    main()
