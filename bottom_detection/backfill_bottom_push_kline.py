#!/usr/bin/env python3
"""Backfill kline cache around historical bottom abnormal push times."""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
import psycopg2.extras

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


def resolution_seconds(resolution: str) -> int:
    normalized = resolution.strip().lower()
    if normalized.endswith("min"):
        return max(1, to_int(normalized[:-3])) * 60
    if normalized.endswith("m"):
        return max(1, to_int(normalized[:-1])) * 60
    if normalized.endswith("h"):
        return max(1, to_int(normalized[:-1])) * 3600
    return 300


def interval_from_resolution(resolution: str) -> str:
    normalized = str(resolution or "").strip().lower()
    if normalized in {"1", "1m", "1min"}:
        return "1min"
    if normalized in {"5", "5m", "5min"}:
        return "5min"
    if normalized in {"15", "15m", "15min"}:
        return "15min"
    if normalized in {"30", "30m", "30min"}:
        return "30min"
    if normalized in {"1h", "60m", "60min"}:
        return "1h"
    return normalized or "5min"


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
            CREATE TABLE IF NOT EXISTS bottom_kline_cache_1m (
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
            CREATE INDEX IF NOT EXISTS idx_bottom_kline_cache_1m_addr_res_ts
                ON bottom_kline_cache_1m(address, resolution, ts);
            """
        )

    db_op(_op)


def kline_cache_table_for_resolution(resolution: str) -> str:
    return "bottom_kline_cache_1m" if str(resolution or "").lower() in {"1m", "1min", "1"} else "bottom_kline_cache"


def ensure_backfill_progress_table() -> None:
    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS bottom_kline_backfill_progress (
                record_id BIGINT NOT NULL,
                chain TEXT NOT NULL DEFAULT 'sol',
                address TEXT NOT NULL,
                resolution TEXT NOT NULL,
                from_ts BIGINT NOT NULL,
                to_ts BIGINT NOT NULL,
                status TEXT NOT NULL,
                fetched_count INTEGER NOT NULL DEFAULT 0,
                inserted_count INTEGER NOT NULL DEFAULT 0,
                error TEXT,
                processed_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (record_id, resolution, from_ts, to_ts)
            );
            CREATE INDEX IF NOT EXISTS idx_bottom_kline_backfill_progress_status
                ON bottom_kline_backfill_progress(status, processed_at DESC);
            CREATE INDEX IF NOT EXISTS idx_bottom_kline_backfill_progress_addr
                ON bottom_kline_backfill_progress(address, resolution, processed_at DESC);
            """
        )

    db_op(_op)


def completed_backfill_keys(resolution: str) -> set[tuple[int, int, int]]:
    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            SELECT record_id, from_ts, to_ts
            FROM bottom_kline_backfill_progress
            WHERE resolution = %s
              AND status = 'success'
            """,
            (resolution,),
        )
        return {(int(row[0]), int(row[1]), int(row[2])) for row in cur.fetchall()}

    return db_op(_op) or set()


def cached_kline_window(address: str, resolution: str, from_ts: int, to_ts: int) -> dict[str, int]:
    table = kline_cache_table_for_resolution(resolution)

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT COUNT(*), COALESCE(MIN(ts), 0), COALESCE(MAX(ts), 0)
            FROM {table}
            WHERE chain = 'sol'
              AND address = %s
              AND resolution = %s
              AND ts BETWEEN %s AND %s
            """,
            (address, resolution, from_ts, to_ts),
        )
        count, first_ts, last_ts = cur.fetchone()
        return {"count": int(count or 0), "first_ts": int(first_ts or 0), "last_ts": int(last_ts or 0)}

    return db_op(_op) or {"count": 0, "first_ts": 0, "last_ts": 0}


def is_cached_window_usable(address: str, resolution: str, from_ts: int, event_ts: int, to_ts: int) -> tuple[bool, dict[str, int]]:
    cached = cached_kline_window(address, resolution, from_ts, to_ts)
    step = resolution_seconds(resolution)
    usable = (
        cached["count"] >= 24
        and cached["first_ts"] > 0
        and cached["first_ts"] <= event_ts
        and cached["last_ts"] >= to_ts - (2 * step)
    )
    return usable, cached


def mark_backfill_progress(
    record_id: int,
    address: str,
    resolution: str,
    from_ts: int,
    to_ts: int,
    status: str,
    fetched_count: int,
    inserted_count: int,
    error: str = "",
) -> None:
    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO bottom_kline_backfill_progress (
                record_id, chain, address, resolution, from_ts, to_ts,
                status, fetched_count, inserted_count, error, processed_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (record_id, resolution, from_ts, to_ts) DO UPDATE SET
                status = EXCLUDED.status,
                fetched_count = EXCLUDED.fetched_count,
                inserted_count = EXCLUDED.inserted_count,
                error = EXCLUDED.error,
                processed_at = NOW()
            """,
            (
                record_id,
                "sol",
                address,
                resolution,
                from_ts,
                to_ts,
                status,
                fetched_count,
                inserted_count,
                error[:500] if error else None,
            ),
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
    table = kline_cache_table_for_resolution(resolution)

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
        psycopg2.extras.execute_values(
            cur,
            f"""
            INSERT INTO {table} (
                chain, address, resolution, ts, open, high, low, close, volume, amount
            ) VALUES %s
            ON CONFLICT (chain, address, resolution, ts) DO NOTHING
            """,
            rows,
            page_size=500,
        )
        return max(cur.rowcount, 0)

    return int(db_op(_op) or 0)


def fmt_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill kline cache for historical bottom push CAs.")
    parser.add_argument("--limit", type=int, default=0, help="Max push records to process. 0 means all.")
    parser.add_argument("--window-hours", type=float, default=12, help="Hours before and after push time to fetch.")
    parser.add_argument("--resolution", default="5m", help="Cache resolution label. 1m writes bottom_kline_cache_1m; others write bottom_kline_cache.")
    parser.add_argument("--interval", default="", help="Binance kline interval. Empty means infer from --resolution.")
    parser.add_argument("--delay", type=float, default=0.25, help="Delay between API requests.")
    parser.add_argument("--min-event-ts", type=int, default=0, help="Only process pushes at or after this unix timestamp.")
    parser.add_argument("--max-event-ts", type=int, default=0, help="Only process pushes at or before this unix timestamp.")
    parser.add_argument("--skip-completed", action=argparse.BooleanOptionalAction, default=True, help="Skip records already marked successful in progress table.")
    parser.add_argument("--skip-cached-window", action=argparse.BooleanOptionalAction, default=True, help="Skip records whose kline cache already covers the event window.")
    parser.add_argument("--dry-run", action="store_true", help="Fetch records but do not call Binance or write DB.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    args.resolution = str(args.resolution or "5m").strip().lower()
    args.interval = str(args.interval or interval_from_resolution(args.resolution)).strip()
    target_table = kline_cache_table_for_resolution(args.resolution)
    window_sec = int(max(0, args.window_hours) * 3600)
    if window_sec <= 0:
        raise SystemExit("--window-hours must be positive")

    ensure_kline_cache_table()
    ensure_backfill_progress_table()
    records = fetch_push_records(args.limit, args.min_event_ts, args.max_event_ts)
    print(
        f"backfill records={len(records)} window=+/-{args.window_hours:g}h "
        f"resolution={args.resolution} interval={args.interval} table={target_table}"
    )
    if args.dry_run:
        for row in records[:20]:
            event_ts = to_int(row.get("event_ts"))
            print(f"dry id={row['id']} {row['address']} {row['signal_type']} {fmt_ts(event_ts)}")
        return

    total_fetched = 0
    total_inserted = 0
    skipped = 0
    failures = 0
    completed_keys = completed_backfill_keys(args.resolution) if args.skip_completed else set()
    for index, row in enumerate(records, start=1):
        address = row["address"]
        event_ts = to_int(row.get("event_ts"))
        from_ts = max(0, event_ts - window_sec)
        to_ts = event_ts + window_sec
        progress_key = (int(row["id"]), from_ts, to_ts)
        if progress_key in completed_keys:
            skipped += 1
            print(
                f"[{index}/{len(records)}] id={row['id']} {address[:8]} {row['signal_type']} "
                f"{fmt_ts(event_ts)} skipped=completed"
            )
            continue
        if args.skip_cached_window:
            cached_ok, cached = is_cached_window_usable(address, args.resolution, from_ts, event_ts, to_ts)
            if cached_ok:
                skipped += 1
                mark_backfill_progress(
                    int(row["id"]),
                    address,
                    args.resolution,
                    from_ts,
                    to_ts,
                    "success",
                    0,
                    0,
                )
                print(
                    f"[{index}/{len(records)}] id={row['id']} {address[:8]} {row['signal_type']} "
                    f"{fmt_ts(event_ts)} skipped=cached count={cached['count']} "
                    f"range={fmt_ts(cached['first_ts'])}->{fmt_ts(cached['last_ts'])}"
                )
                continue
        try:
            candles = fetch_binance_kline_range(address, from_ts, to_ts, args.interval)
            inserted = insert_missing_kline(address, args.resolution, candles)
            mark_backfill_progress(
                int(row["id"]),
                address,
                args.resolution,
                from_ts,
                to_ts,
                "success",
                len(candles),
                inserted,
            )
            total_fetched += len(candles)
            total_inserted += inserted
            print(
                f"[{index}/{len(records)}] id={row['id']} {address[:8]} {row['signal_type']} "
                f"{fmt_ts(event_ts)} fetched={len(candles)} inserted={inserted}"
            )
        except Exception as exc:
            failures += 1
            mark_backfill_progress(
                int(row["id"]),
                address,
                args.resolution,
                from_ts,
                to_ts,
                "failed",
                0,
                0,
                str(exc),
            )
            print(f"[{index}/{len(records)}] id={row['id']} {address[:8]} failed: {exc}")
        if args.delay > 0 and index < len(records):
            time.sleep(args.delay)

    print(f"done records={len(records)} skipped={skipped} fetched={total_fetched} inserted={total_inserted} failures={failures}")


if __name__ == "__main__":
    main()
