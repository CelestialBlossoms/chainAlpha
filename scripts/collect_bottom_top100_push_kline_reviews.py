#!/usr/bin/env python3
"""Collect hourly K-line review rows for today's bottom Top100 push records."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests
from psycopg2.extras import Json

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from db_client import db_op


BINANCE_KLINE_URL = "https://dquery.sintral.io/u-kline/v1/k-line/candles"
BINANCE_HEADERS = {"Accept-Encoding": "identity", "User-Agent": "binance-web3/1.1 (chainAlpha)"}
CHAIN_PLATFORM_MAP = {"sol": "solana", "bsc": "bsc", "base": "base", "eth": "ethereum"}


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return default


def normalize_ts(value: Any) -> int:
    try:
        ts = int(float(value or 0))
    except (TypeError, ValueError):
        return 0
    return int(ts / 1000) if ts > 10_000_000_000 else ts


def resolution_seconds(resolution: str) -> int:
    mapping = {
        "1m": 60,
        "5m": 300,
        "15m": 900,
        "30m": 1800,
        "1h": 3600,
        "4h": 14400,
        "1d": 86400,
    }
    return mapping.get(resolution, 300)


def resolution_to_interval(resolution: str) -> str:
    mapping = {
        "1m": "1min",
        "5m": "5min",
        "15m": "15min",
        "30m": "30min",
        "1h": "1h",
        "4h": "4h",
        "1d": "1d",
    }
    return mapping.get(resolution, "5min")


def day_bounds(day: str, tz: ZoneInfo) -> tuple[int, int]:
    if day:
        start_dt = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=tz)
    else:
        now = datetime.now(tz)
        start_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end_dt = start_dt + timedelta(days=1)
    return int(start_dt.timestamp()), int(end_dt.timestamp())


def review_hour_ts(ts: int) -> int:
    return ts - (ts % 3600)


def ensure_review_table() -> None:
    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS bottom_top100_push_kline_reviews (
                id BIGSERIAL PRIMARY KEY,
                collected_at TIMESTAMPTZ DEFAULT now(),
                review_ts BIGINT NOT NULL,
                review_hour_ts BIGINT NOT NULL,
                push_record_id BIGINT NOT NULL,
                chain TEXT NOT NULL DEFAULT 'sol',
                source TEXT NOT NULL DEFAULT 'bottom_abnormal',
                address TEXT NOT NULL,
                symbol TEXT,
                signal_type TEXT,
                abnormal_rule TEXT,
                event_ts BIGINT NOT NULL,
                current_mcap NUMERIC DEFAULT 0,
                first_signal_mcap NUMERIC DEFAULT 0,
                price_change_pct NUMERIC DEFAULT 0,
                liquidity NUMERIC DEFAULT 0,
                resolution TEXT NOT NULL DEFAULT '5m',
                kline_from_ts BIGINT NOT NULL,
                kline_to_ts BIGINT NOT NULL,
                candle_count INTEGER DEFAULT 0,
                valid BOOLEAN DEFAULT FALSE,
                invalid_reason TEXT DEFAULT '',
                entry_ts BIGINT DEFAULT 0,
                entry_price NUMERIC DEFAULT 0,
                peak_ts BIGINT DEFAULT 0,
                peak_price NUMERIC DEFAULT 0,
                low_ts BIGINT DEFAULT 0,
                low_price NUMERIC DEFAULT 0,
                current_ts BIGINT DEFAULT 0,
                current_price NUMERIC DEFAULT 0,
                max_gain_pct NUMERIC DEFAULT 0,
                current_return_pct NUMERIC DEFAULT 0,
                entry_drawdown_pct NUMERIC DEFAULT 0,
                high_to_current_drawdown_pct NUMERIC DEFAULT 0,
                volume_usd NUMERIC DEFAULT 0,
                candles JSONB DEFAULT '[]'::jsonb,
                push_extra JSONB DEFAULT '{}'::jsonb
            );
            CREATE UNIQUE INDEX IF NOT EXISTS uq_bottom_push_kline_review_hour
                ON bottom_top100_push_kline_reviews(push_record_id, resolution, review_hour_ts);
            CREATE INDEX IF NOT EXISTS idx_bottom_push_kline_review_addr_ts
                ON bottom_top100_push_kline_reviews(address, review_ts DESC);
            CREATE INDEX IF NOT EXISTS idx_bottom_push_kline_review_event
                ON bottom_top100_push_kline_reviews(event_ts DESC);
            COMMENT ON TABLE bottom_top100_push_kline_reviews IS '底部异动推送CA的小时级K线回顾数据，用于后续胜率和回测计算';
            COMMENT ON COLUMN bottom_top100_push_kline_reviews.push_record_id IS '关联bottom_top100_push_records.id';
            COMMENT ON COLUMN bottom_top100_push_kline_reviews.review_hour_ts IS '回顾采集小时桶，同一push_record_id/resolution每小时只保留一条';
            COMMENT ON COLUMN bottom_top100_push_kline_reviews.candles IS 'Balance/Binance K线原始蜡烛，已裁剪为信号后到本次回顾时间窗口';
            """
        )

    db_op(_op)


def fetch_today_push_records(start_ts: int, end_ts: int, chain: str, limit: int) -> list[dict[str, Any]]:
    limit_sql = "LIMIT %s" if limit > 0 else ""
    params: list[Any] = [chain, start_ts, end_ts]
    if limit > 0:
        params.append(limit)

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT
                id, event_ts, chain, source, address, symbol, signal_type, abnormal_rule,
                current_mcap, first_signal_mcap, price_change_pct, liquidity, extra
            FROM bottom_top100_push_records
            WHERE chain = %s
              AND event_ts >= %s
              AND event_ts < %s
              AND COALESCE(signal_type, '') <> ''
            ORDER BY event_ts ASC, id ASC
            {limit_sql}
            """,
            params,
        )
        rows = []
        for row in cur.fetchall():
            extra = row[12] if isinstance(row[12], dict) else {}
            rows.append(
                {
                    "id": int(row[0]),
                    "event_ts": int(row[1] or 0),
                    "chain": row[2] or chain,
                    "source": row[3] or "bottom_abnormal",
                    "address": row[4] or "",
                    "symbol": row[5] or extra.get("symbol") or "",
                    "signal_type": row[6] or "",
                    "abnormal_rule": row[7] or "",
                    "current_mcap": to_float(row[8]),
                    "first_signal_mcap": to_float(row[9]),
                    "price_change_pct": to_float(row[10]),
                    "liquidity": to_float(row[11]),
                    "extra": extra,
                }
            )
        return rows

    return db_op(_op)


def fetch_kline(chain: str, address: str, resolution: str, from_ts: int, to_ts: int, limit: int) -> list[dict[str, Any]]:
    platform = CHAIN_PLATFORM_MAP.get(chain, "solana")
    params = {
        "address": address,
        "platform": platform,
        "interval": resolution_to_interval(resolution),
        "limit": limit,
        "to": to_ts * 1000,
        "pm": "p",
    }
    try:
        response = requests.get(BINANCE_KLINE_URL, params=params, headers=BINANCE_HEADERS, timeout=30)
        if response.status_code != 200:
            print(f"{address[:8]} kline http {response.status_code}")
            return []
        payload = response.json()
    except Exception as exc:
        print(f"{address[:8]} kline fetch failed: {exc}")
        return []

    raw = payload.get("data") if isinstance(payload, dict) else None
    candles: list[dict[str, Any]] = []
    for item in raw if isinstance(raw, list) else []:
        if not isinstance(item, list) or len(item) < 6:
            continue
        ts = normalize_ts(item[5])
        if ts < from_ts or ts > to_ts:
            continue
        candles.append(
            {
                "ts": ts,
                "open": to_float(item[0]),
                "high": to_float(item[1]),
                "low": to_float(item[2]),
                "close": to_float(item[3]),
                "volume": to_float(item[4]),
                "amount": to_float(item[4]),
            }
        )
    candles.sort(key=lambda candle: int(candle["ts"]))
    return candles


def analyze_review(push: dict[str, Any], candles: list[dict[str, Any]], resolution: str) -> dict[str, Any]:
    event_ts = int(push.get("event_ts") or 0)
    step = resolution_seconds(resolution)
    signal_candle = None
    for candle in candles:
        candle_ts = int(candle.get("ts") or 0)
        if candle_ts <= event_ts < candle_ts + step:
            signal_candle = candle
            break
    if signal_candle is None:
        before = [candle for candle in candles if int(candle.get("ts") or 0) <= event_ts]
        signal_candle = before[-1] if before else None

    post_candles = [candle for candle in candles if int(candle.get("ts") or 0) + step > event_ts]
    if not post_candles:
        return {"valid": False, "invalid_reason": "no_post_signal_kline", "candle_count": len(candles)}
    if signal_candle is None:
        signal_candle = post_candles[0]

    entry_price = to_float(signal_candle.get("close")) or to_float(signal_candle.get("open"))
    if entry_price <= 0:
        return {"valid": False, "invalid_reason": "invalid_entry_price", "candle_count": len(post_candles)}

    peak = max(post_candles, key=lambda candle: to_float(candle.get("high")))
    low = min(post_candles, key=lambda candle: to_float(candle.get("low")))
    current = post_candles[-1]
    peak_price = to_float(peak.get("high"))
    low_price = to_float(low.get("low"))
    current_price = to_float(current.get("close"))

    max_gain_pct = (peak_price / entry_price - 1) * 100 if peak_price > 0 else 0.0
    current_return_pct = (current_price / entry_price - 1) * 100 if current_price > 0 else 0.0
    entry_drawdown_pct = min(0.0, (low_price / entry_price - 1) * 100) if low_price > 0 else 0.0
    high_to_current_drawdown_pct = (
        max(0.0, (1 - current_price / peak_price) * 100)
        if peak_price > 0 and current_price > 0
        else 0.0
    )
    return {
        "valid": True,
        "invalid_reason": "",
        "candle_count": len(post_candles),
        "entry_ts": int(signal_candle.get("ts") or 0),
        "entry_price": entry_price,
        "peak_ts": int(peak.get("ts") or 0),
        "peak_price": peak_price,
        "low_ts": int(low.get("ts") or 0),
        "low_price": low_price,
        "current_ts": int(current.get("ts") or 0),
        "current_price": current_price,
        "max_gain_pct": max_gain_pct,
        "current_return_pct": current_return_pct,
        "entry_drawdown_pct": entry_drawdown_pct,
        "high_to_current_drawdown_pct": high_to_current_drawdown_pct,
        "volume_usd": sum(to_float(candle.get("volume")) for candle in post_candles),
    }


def save_review(
    push: dict[str, Any],
    resolution: str,
    review_ts_value: int,
    kline_from_ts: int,
    kline_to_ts: int,
    candles: list[dict[str, Any]],
    metrics: dict[str, Any],
) -> None:
    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO bottom_top100_push_kline_reviews (
                review_ts, review_hour_ts, push_record_id, chain, source, address, symbol,
                signal_type, abnormal_rule, event_ts, current_mcap, first_signal_mcap,
                price_change_pct, liquidity, resolution, kline_from_ts, kline_to_ts,
                candle_count, valid, invalid_reason, entry_ts, entry_price, peak_ts,
                peak_price, low_ts, low_price, current_ts, current_price, max_gain_pct,
                current_return_pct, entry_drawdown_pct, high_to_current_drawdown_pct,
                volume_usd, candles, push_extra
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s
            )
            ON CONFLICT (push_record_id, resolution, review_hour_ts) DO UPDATE SET
                collected_at = now(),
                review_ts = EXCLUDED.review_ts,
                kline_to_ts = EXCLUDED.kline_to_ts,
                candle_count = EXCLUDED.candle_count,
                valid = EXCLUDED.valid,
                invalid_reason = EXCLUDED.invalid_reason,
                entry_ts = EXCLUDED.entry_ts,
                entry_price = EXCLUDED.entry_price,
                peak_ts = EXCLUDED.peak_ts,
                peak_price = EXCLUDED.peak_price,
                low_ts = EXCLUDED.low_ts,
                low_price = EXCLUDED.low_price,
                current_ts = EXCLUDED.current_ts,
                current_price = EXCLUDED.current_price,
                max_gain_pct = EXCLUDED.max_gain_pct,
                current_return_pct = EXCLUDED.current_return_pct,
                entry_drawdown_pct = EXCLUDED.entry_drawdown_pct,
                high_to_current_drawdown_pct = EXCLUDED.high_to_current_drawdown_pct,
                volume_usd = EXCLUDED.volume_usd,
                candles = EXCLUDED.candles,
                push_extra = EXCLUDED.push_extra
            """,
            (
                review_ts_value,
                review_hour_ts(review_ts_value),
                push["id"],
                push["chain"],
                push["source"],
                push["address"],
                push["symbol"],
                push["signal_type"],
                push["abnormal_rule"],
                push["event_ts"],
                push["current_mcap"],
                push["first_signal_mcap"],
                push["price_change_pct"],
                push["liquidity"],
                resolution,
                kline_from_ts,
                kline_to_ts,
                int(metrics.get("candle_count") or 0),
                bool(metrics.get("valid")),
                metrics.get("invalid_reason") or "",
                int(metrics.get("entry_ts") or 0),
                to_float(metrics.get("entry_price")),
                int(metrics.get("peak_ts") or 0),
                to_float(metrics.get("peak_price")),
                int(metrics.get("low_ts") or 0),
                to_float(metrics.get("low_price")),
                int(metrics.get("current_ts") or 0),
                to_float(metrics.get("current_price")),
                to_float(metrics.get("max_gain_pct")),
                to_float(metrics.get("current_return_pct")),
                to_float(metrics.get("entry_drawdown_pct")),
                to_float(metrics.get("high_to_current_drawdown_pct")),
                to_float(metrics.get("volume_usd")),
                Json(candles),
                Json(push.get("extra") or {}),
            ),
        )

    db_op(_op)


def collect_once(args: argparse.Namespace) -> None:
    tz = ZoneInfo(args.timezone)
    start_ts, end_ts = day_bounds(args.day, tz)
    review_ts_value = int(time.time())
    kline_to_ts = min(review_ts_value, end_ts)
    ensure_review_table()
    pushes = fetch_today_push_records(start_ts, end_ts, args.chain, args.limit)
    print(
        f"review day={datetime.fromtimestamp(start_ts, tz).date()} "
        f"pushes={len(pushes)} resolution={args.resolution}"
    )
    saved = 0
    invalid = 0
    for push in pushes:
        address = push["address"]
        from_ts = max(0, int(push["event_ts"]) - resolution_seconds(args.resolution) * 2)
        candles = fetch_kline(args.chain, address, args.resolution, from_ts, kline_to_ts, args.kline_limit)
        metrics = analyze_review(push, candles, args.resolution)
        save_review(push, args.resolution, review_ts_value, from_ts, kline_to_ts, candles, metrics)
        saved += 1
        if not metrics.get("valid"):
            invalid += 1
        print(
            f"{address[:8]} {push.get('symbol') or ''} "
            f"candles={metrics.get('candle_count', 0)} "
            f"max={to_float(metrics.get('max_gain_pct')):.1f}% "
            f"cur={to_float(metrics.get('current_return_pct')):.1f}% "
            f"{metrics.get('invalid_reason') or 'ok'}"
        )
        if args.request_sleep > 0:
            time.sleep(args.request_sleep)
    print(f"saved={saved} invalid={invalid}")


def run_watch(args: argparse.Namespace) -> None:
    while True:
        started = time.time()
        try:
            collect_once(args)
        except Exception as exc:
            print(f"collect failed: {exc}")
        elapsed = time.time() - started
        sleep_sec = max(1.0, float(args.interval_sec) - elapsed)
        print(f"sleep {sleep_sec:.0f}s")
        time.sleep(sleep_sec)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Hourly collect Balance/Binance K-line reviews for today's bottom_top100_push_records."
    )
    parser.add_argument("--chain", default=os.getenv("BOTTOM_PUSH_REVIEW_CHAIN", "sol"))
    parser.add_argument("--day", default=os.getenv("BOTTOM_PUSH_REVIEW_DAY", ""), help="YYYY-MM-DD, default today in timezone.")
    parser.add_argument("--timezone", default=os.getenv("BOTTOM_PUSH_REVIEW_TZ", "Asia/Shanghai"))
    parser.add_argument("--resolution", default=os.getenv("BOTTOM_PUSH_REVIEW_RESOLUTION", "5m"))
    parser.add_argument("--interval-sec", type=float, default=float(os.getenv("BOTTOM_PUSH_REVIEW_INTERVAL_SEC", "3600")))
    parser.add_argument("--kline-limit", type=int, default=int(os.getenv("BOTTOM_PUSH_REVIEW_KLINE_LIMIT", "500")))
    parser.add_argument("--limit", type=int, default=int(os.getenv("BOTTOM_PUSH_REVIEW_LIMIT", "0")))
    parser.add_argument("--request-sleep", type=float, default=float(os.getenv("BOTTOM_PUSH_REVIEW_REQUEST_SLEEP", "0.25")))
    parser.add_argument("--watch", action="store_true", help="Run forever every interval-sec.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.watch:
        run_watch(args)
    else:
        collect_once(args)


if __name__ == "__main__":
    main()
