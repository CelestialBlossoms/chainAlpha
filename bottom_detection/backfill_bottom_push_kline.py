#!/usr/bin/env python3
"""Backfill 5m kline cache around historical bottom abnormal push times."""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from db_client import db_op


BINANCE_KLINE_URL = "https://dquery.sintral.io/u-kline/v1/k-line/candles"
BINANCE_HEADERS = {
    "Accept-Encoding": "identity",
    "User-Agent": "binance-web3/1.1 (BottomKlineBackfill)",
}


def to_int(value: Any) -> int:
    try:
        if value is None or value == "":
            return 0
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def parse_binance_kline(raw: Any) -> list[dict[str, Any]]:
    candles: list[dict[str, Any]] = []
    for item in raw or []:
        if not isinstance(item, list) or len(item) < 6:
            continue
        try:
            raw_ts = to_int(item[5])
            ts = raw_ts // 1000 if raw_ts > 10_000_000_000 else raw_ts
            if ts <= 0:
                continue
            candles.append(
                {
                    "ts": ts,
                    "open": float(item[0]),
                    "high": float(item[1]),
                    "low": float(item[2]),
                    "close": float(item[3]),
                    "volume": float(item[4]),
                    "amount": None,
                }
            )
        except (TypeError, ValueError):
            continue
    candles.sort(key=lambda candle: int(candle["ts"]))
    return candles


def ensure_kline_cache_table() -> None:
    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS bottom_kline_cache (
                chain TEXT NOT NULL DEFAULT 'sol',
                address TEXT NOT NULL,
                resolution TEXT NOT NULL,
                ts BIGINT NOT NULL,
                open NUMERIC,
                high NUMERIC,
                low NUMERIC,
                close NUMERIC,
                volume NUMERIC,
                amount NUMERIC,
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (chain, address, resolution, ts)
            );
            CREATE INDEX IF NOT EXISTS idx_bottom_kline_cache_addr_res_ts
                ON bottom_kline_cache(address, resolution, ts);
            """
        )

    db_op(_op)


def fetch_push_records(limit: int, min_event_ts: int, max_event_ts: int) -> list[dict[str, Any]]:
    def _op(conn):
        cur = conn.cursor()
        where = [
            "chain = 'sol'",
            "source = 'bottom_abnormal'",
            "COALESCE(signal_type, '') <> ''",
            "COALESCE(signal_type, '') <> 'watch'",
            "address IS NOT NULL",
            "address <> ''",
            "COALESCE(NULLIF(event_ts, 0), extract(epoch from pushed_at)::bigint) > 0",
        ]
        params: list[Any] = []
        if min_event_ts > 0:
            where.append("COALESCE(NULLIF(event_ts, 0), extract(epoch from pushed_at)::bigint) >= %s")
            params.append(min_event_ts)
        if max_event_ts > 0:
            where.append("COALESCE(NULLIF(event_ts, 0), extract(epoch from pushed_at)::bigint) <= %s")
            params.append(max_event_ts)
        limit_sql = ""
        if limit > 0:
            limit_sql = "LIMIT %s"
            params.append(limit)
        cur.execute(
            f"""
            SELECT
                id,
                address,
                COALESCE(symbol, ''),
                COALESCE(signal_type, ''),
                COALESCE(NULLIF(event_ts, 0), extract(epoch from pushed_at)::bigint) AS event_ts
            FROM bottom_top100_push_records
            WHERE {" AND ".join(where)}
            ORDER BY event_ts ASC, id ASC
            {limit_sql}
            """,
            params,
        )
        return [
            {
                "id": int(row[0]),
                "address": str(row[1] or "").strip(),
                "symbol": str(row[2] or ""),
                "signal_type": str(row[3] or ""),
                "event_ts": int(row[4] or 0),
            }
            for row in cur.fetchall()
        ]

    return db_op(_op) or []


def fetch_binance_kline_range(address: str, from_ts: int, to_ts: int, interval: str) -> list[dict[str, Any]]:
    if not address or from_ts <= 0 or to_ts <= from_ts:
        return []
    params = {
        "address": address,
        "platform": "solana",
        "interval": interval,
        "pm": "p",
        "from": int(from_ts) * 1000,
        "to": int(to_ts) * 1000,
    }
    resp = requests.get(BINANCE_KLINE_URL, params=params, headers=BINANCE_HEADERS, timeout=30)
    if not resp.ok:
        print(f"{address[:8]} kline fetch failed: status={resp.status_code} body={resp.text[:160]}")
        return []
    return parse_binance_kline(resp.json().get("data"))


def insert_missing_kline(address: str, resolution: str, candles: list[dict[str, Any]]) -> int:
    if not address or not candles:
        return 0

    def _op(conn):
        cur = conn.cursor()
        rows = [
            (
                "sol",
                address,
                resolution,
                to_int(candle.get("ts")),
                candle.get("open"),
                candle.get("high"),
                candle.get("low"),
                candle.get("close"),
                candle.get("volume"),
                candle.get("amount"),
            )
            for candle in candles
            if to_int(candle.get("ts")) > 0
        ]
        if not rows:
            return 0
        cur.executemany(
            """
            INSERT INTO bottom_kline_cache (
                chain, address, resolution, ts, open, high, low, close, volume, amount
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (chain, address, resolution, ts) DO NOTHING
            """,
            rows,
        )
        return max(cur.rowcount, 0)

    return int(db_op(_op) or 0)


def fmt_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill 5m kline cache for historical bottom push CAs.")
    parser.add_argument("--limit", type=int, default=0, help="Max push records to process. 0 means all.")
    parser.add_argument("--window-hours", type=float, default=12, help="Hours before and after push time to fetch.")
    parser.add_argument("--resolution", default="5m", help="Cache resolution label.")
    parser.add_argument("--interval", default="5min", help="Binance kline interval.")
    parser.add_argument("--delay", type=float, default=0.25, help="Delay between API requests.")
    parser.add_argument("--min-event-ts", type=int, default=0, help="Only process pushes at or after this unix timestamp.")
    parser.add_argument("--max-event-ts", type=int, default=0, help="Only process pushes at or before this unix timestamp.")
    parser.add_argument("--dry-run", action="store_true", help="Fetch records but do not call Binance or write DB.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    window_sec = int(max(0, args.window_hours) * 3600)
    if window_sec <= 0:
        raise SystemExit("--window-hours must be positive")

    ensure_kline_cache_table()
    records = fetch_push_records(args.limit, args.min_event_ts, args.max_event_ts)
    print(f"backfill records={len(records)} window=+/-{args.window_hours:g}h resolution={args.resolution}")
    if args.dry_run:
        for row in records[:20]:
            event_ts = to_int(row.get("event_ts"))
            print(f"dry id={row['id']} {row['address']} {row['signal_type']} {fmt_ts(event_ts)}")
        return

    total_fetched = 0
    total_inserted = 0
    failures = 0
    for index, row in enumerate(records, start=1):
        address = row["address"]
        event_ts = to_int(row.get("event_ts"))
        from_ts = max(0, event_ts - window_sec)
        to_ts = event_ts + window_sec
        try:
            candles = fetch_binance_kline_range(address, from_ts, to_ts, args.interval)
            inserted = insert_missing_kline(address, args.resolution, candles)
            total_fetched += len(candles)
            total_inserted += inserted
            print(
                f"[{index}/{len(records)}] id={row['id']} {address[:8]} {row['signal_type']} "
                f"{fmt_ts(event_ts)} fetched={len(candles)} inserted={inserted}"
            )
        except Exception as exc:
            failures += 1
            print(f"[{index}/{len(records)}] id={row['id']} {address[:8]} failed: {exc}")
        if args.delay > 0 and index < len(records):
            time.sleep(args.delay)

    print(f"done records={len(records)} fetched={total_fetched} inserted={total_inserted} failures={failures}")


if __name__ == "__main__":
    main()
