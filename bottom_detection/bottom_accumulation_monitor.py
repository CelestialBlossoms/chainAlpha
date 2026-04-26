#!/usr/bin/env python3
"""
Monitor watched token contracts for bottom accumulation followed by price expansion.

Default source table: tokens.ca

Pipeline:
1. Read token CAs from a source table.
2. Cache 5m GMGN klines in Postgres. First run backfills history; later runs only
   fetch the missing tail.
3. Detect "flat base then spike":
   - 1h close/open change >= 30% after a quiet base.
   - 4h close/open change >= 100% after a quiet base.
4. Snapshot Top100 holders and compare with the previous snapshot to detect
   accumulation, distribution, and rotation.
5. Store signals once per token/window/type to avoid duplicate alerts.

Run bottom_detection/init_bottom_accumulation_db.py once before starting this monitor.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from psycopg2 import sql
from psycopg2.extras import Json, execute_values

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from config import TG_BOT_TOKEN, TG_CHAT_ID
from db_client import db_op


CHAIN = "sol"
RESOLUTION = "5m"
DEFAULT_SOURCE_TABLE = os.getenv("BOTTOM_SOURCE_TABLE", "tokens")
DEFAULT_SOURCE_CA_COLUMN = os.getenv("BOTTOM_SOURCE_CA_COLUMN", "ca")
DEFAULT_SOURCE_CHAIN_COLUMN = os.getenv("BOTTOM_SOURCE_CHAIN_COLUMN", "")
DEFAULT_INTERVAL_SEC = int(os.getenv("BOTTOM_SCAN_INTERVAL", "60"))
DEFAULT_BACKFILL_HOURS = int(os.getenv("BOTTOM_BACKFILL_HOURS", "48"))
DEFAULT_HOLDER_REFRESH_SEC = int(os.getenv("BOTTOM_HOLDER_REFRESH_SEC", "900"))
TOP_HOLDER_LIMIT = int(os.getenv("BOTTOM_TOP_HOLDER_LIMIT", "100"))

ONE_HOUR_SPIKE = float(os.getenv("BOTTOM_ONE_HOUR_SPIKE", "0.30"))
FOUR_HOUR_SPIKE = float(os.getenv("BOTTOM_FOUR_HOUR_SPIKE", "1.00"))
BASE_MAX_RANGE = float(os.getenv("BOTTOM_BASE_MAX_RANGE", "0.22"))
BASE_MAX_DRIFT = float(os.getenv("BOTTOM_BASE_MAX_DRIFT", "0.12"))
MIN_ACCUMULATED_PCT_DELTA = float(os.getenv("BOTTOM_MIN_ACCUM_PCT_DELTA", "0.015"))
MIN_NETFLOW_USD = float(os.getenv("BOTTOM_MIN_NETFLOW_USD", "5000"))

SOL_CA_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")


@dataclass
class Candle:
    ts: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    amount: float
    raw: dict[str, Any]


def now_ts() -> int:
    return int(time.time())


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def to_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def valid_sol_ca(address: str) -> bool:
    return bool(SOL_CA_RE.match(address or ""))


def gmgn_command_prefix() -> list[str]:
    executable = shutil.which("gmgn-cli") or shutil.which("gmgn-cli.cmd") or shutil.which("gmgn-cli.ps1")
    if not executable:
        return ["gmgn-cli"]
    if executable.lower().endswith(".ps1"):
        return ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", executable]
    return [executable]


def run_gmgn(args: list[str], timeout: int = 60) -> dict[str, Any] | list[Any] | None:
    cmd = [*gmgn_command_prefix(), *args, "--raw"]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=timeout,
        )
    except Exception as exc:
        print(f"gmgn exception: {' '.join(cmd)} -> {exc}")
        return None
    if result.returncode != 0:
        err = (result.stderr or result.stdout or "").strip()
        print(f"gmgn failed rc={result.returncode}: {' '.join(cmd)}")
        if err:
            print(err[:500])
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        print(f"gmgn json decode failed: {exc}")
        return None


def fetch_source_tokens(table: str, ca_column: str, chain_column: str = "") -> list[str]:
    def _op(conn):
        cur = conn.cursor()
        if chain_column:
            query = sql.SQL("SELECT DISTINCT {ca} FROM {tbl} WHERE {ca} IS NOT NULL AND {chain} = %s").format(
                ca=sql.Identifier(ca_column),
                tbl=sql.Identifier(table),
                chain=sql.Identifier(chain_column),
            )
            cur.execute(query, (CHAIN,))
        else:
            query = sql.SQL("SELECT DISTINCT {ca} FROM {tbl} WHERE {ca} IS NOT NULL").format(
                ca=sql.Identifier(ca_column),
                tbl=sql.Identifier(table),
            )
            cur.execute(query)
        return [str(row[0]).strip() for row in cur.fetchall()]

    tokens = db_op(_op)
    return [addr for addr in tokens if valid_sol_ca(addr)]


def last_cached_ts(address: str) -> int | None:
    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            SELECT MAX(bucket_ts)
            FROM bottom_kline_cache
            WHERE chain=%s AND address=%s AND resolution=%s
            """,
            (CHAIN, address, RESOLUTION),
        )
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else None

    return db_op(_op)


def extract_candles(data: dict[str, Any] | list[Any] | None) -> list[Candle]:
    if not data:
        return []
    rows: Any
    if isinstance(data, list):
        rows = data
    else:
        rows = data.get("list") or data.get("data", {}).get("list") or data.get("data") or []
    candles: list[Candle] = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        raw_ts = to_int(row.get("time") or row.get("timestamp") or row.get("t"))
        ts = raw_ts // 1000 if raw_ts > 10_000_000_000 else raw_ts
        close = to_float(row.get("close") or row.get("c"))
        if ts <= 0 or close <= 0:
            continue
        candles.append(
            Candle(
                ts=ts,
                open=to_float(row.get("open") or row.get("o"), close),
                high=to_float(row.get("high") or row.get("h"), close),
                low=to_float(row.get("low") or row.get("l"), close),
                close=close,
                volume=to_float(row.get("volume") or row.get("v")),
                amount=to_float(row.get("amount") or row.get("a")),
                raw=row,
            )
        )
    candles.sort(key=lambda c: c.ts)
    return candles


def fetch_and_cache_klines(address: str, backfill_hours: int) -> int:
    end_ts = now_ts()
    last_ts = last_cached_ts(address)
    start_ts = (last_ts + 300) if last_ts else end_ts - backfill_hours * 3600
    if start_ts >= end_ts - 60:
        return 0
    data = run_gmgn(
        [
            "market",
            "kline",
            "--chain",
            CHAIN,
            "--address",
            address,
            "--resolution",
            RESOLUTION,
            "--from",
            str(start_ts),
            "--to",
            str(end_ts),
        ],
        timeout=75,
    )
    candles = extract_candles(data)
    if not candles:
        return 0

    def _op(conn):
        cur = conn.cursor()
        execute_values(
            cur,
            """
            INSERT INTO bottom_kline_cache (
                chain, address, resolution, bucket_ts, open, high, low, close,
                volume, amount, raw
            ) VALUES %s
            ON CONFLICT (chain, address, resolution, bucket_ts) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                amount = EXCLUDED.amount,
                raw = EXCLUDED.raw
            """,
            [
                (
                    CHAIN,
                    address,
                    RESOLUTION,
                    c.ts,
                    c.open,
                    c.high,
                    c.low,
                    c.close,
                    c.volume,
                    c.amount,
                    Json(c.raw),
                )
                for c in candles
            ],
        )

    db_op(_op)
    return len(candles)


def load_recent_candles(address: str, hours: int = 12) -> list[Candle]:
    min_ts = now_ts() - hours * 3600

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            SELECT bucket_ts, open, high, low, close, volume, amount, raw
            FROM bottom_kline_cache
            WHERE chain=%s AND address=%s AND resolution=%s AND bucket_ts >= %s
            ORDER BY bucket_ts ASC
            """,
            (CHAIN, address, RESOLUTION, min_ts),
        )
        return cur.fetchall()

    rows = db_op(_op)
    return [
        Candle(
            ts=int(row[0]),
            open=to_float(row[1]),
            high=to_float(row[2]),
            low=to_float(row[3]),
            close=to_float(row[4]),
            volume=to_float(row[5]),
            amount=to_float(row[6]),
            raw=row[7] or {},
        )
        for row in rows
    ]


def pct_change(start: float, end: float) -> float:
    return (end - start) / start if start > 0 else 0.0


def slice_since(candles: list[Candle], seconds: int) -> list[Candle]:
    if not candles:
        return []
    cutoff = candles[-1].ts - seconds
    return [c for c in candles if c.ts >= cutoff]


def base_stats(base: list[Candle]) -> tuple[float, float]:
    if len(base) < 6:
        return 0.0, 0.0
    lows = [c.low for c in base if c.low > 0]
    highs = [c.high for c in base if c.high > 0]
    if not lows or not highs:
        return 0.0, 0.0
    low = min(lows)
    high = max(highs)
    range_pct = (high - low) / low if low > 0 else 0.0
    drift_pct = abs(pct_change(base[0].close, base[-1].close))
    return range_pct, drift_pct


def detect_price_setup(candles: list[Candle]) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    if len(candles) < 24:
        return signals

    windows = [
        ("spike_1h", 60, ONE_HOUR_SPIKE, 3 * 3600),
        ("spike_4h", 240, FOUR_HOUR_SPIKE, 8 * 3600),
    ]
    for signal_type, minutes, min_spike, base_seconds in windows:
        spike = slice_since(candles, minutes * 60)
        if len(spike) < max(6, minutes // 5):
            continue
        base_end_ts = spike[0].ts - 1
        base_start_ts = base_end_ts - base_seconds
        base = [c for c in candles if base_start_ts <= c.ts <= base_end_ts]
        range_pct, drift_pct = base_stats(base)
        spike_pct = pct_change(spike[0].open, spike[-1].close)
        quiet_base = range_pct > 0 and range_pct <= BASE_MAX_RANGE and drift_pct <= BASE_MAX_DRIFT
        if spike_pct >= min_spike and quiet_base:
            signals.append(
                {
                    "signal_type": signal_type,
                    "window_minutes": minutes,
                    "signal_bucket_ts": spike[-1].ts,
                    "spike_pct": spike_pct,
                    "base_range_pct": range_pct,
                    "base_drift_pct": drift_pct,
                    "base_start_ts": base_start_ts,
                    "base_end_ts": base_end_ts,
                }
            )
    return signals


def is_pool_holder(holder: dict[str, Any]) -> bool:
    return to_int(holder.get("addr_type")) == 2 or "pool" in str(holder.get("exchange") or "").lower()


def holder_key(holder: dict[str, Any]) -> str:
    return str(holder.get("address") or holder.get("wallet_address") or "").strip()


def summarize_holders(holders: list[dict[str, Any]]) -> dict[str, Any]:
    non_pool = [h for h in holders if holder_key(h) and not is_pool_holder(h)]
    buy_volume = sum(to_float(h.get("buy_volume_cur")) for h in non_pool)
    sell_volume = sum(to_float(h.get("sell_volume_cur")) for h in non_pool)
    netflow = sum(to_float(h.get("netflow_usd")) for h in non_pool)
    return {
        "holder_count": len(holders),
        "non_pool_count": len(non_pool),
        "top10_pct": sum(to_float(h.get("amount_percentage")) for h in holders[:10]),
        "top100_pct": sum(to_float(h.get("amount_percentage")) for h in holders),
        "non_pool_pct": sum(to_float(h.get("amount_percentage")) for h in non_pool),
        "buy_volume": buy_volume,
        "sell_volume": sell_volume,
        "netflow": netflow,
        "top_buyers": sorted(
            [
                {
                    "wallet": holder_key(h),
                    "pct": to_float(h.get("amount_percentage")),
                    "usd": to_float(h.get("usd_value")),
                    "netflow": to_float(h.get("netflow_usd")),
                    "buy": to_float(h.get("buy_volume_cur")),
                    "sell": to_float(h.get("sell_volume_cur")),
                    "tags": h.get("maker_token_tags") or h.get("tags") or [],
                }
                for h in non_pool
            ],
            key=lambda x: to_float(x["netflow"]),
            reverse=True,
        )[:5],
    }


def fetch_holder_snapshot(address: str) -> list[dict[str, Any]]:
    data = run_gmgn(
        [
            "token",
            "holders",
            "--chain",
            CHAIN,
            "--address",
            address,
            "--limit",
            str(TOP_HOLDER_LIMIT),
            "--order-by",
            "amount_percentage",
            "--direction",
            "desc",
        ],
        timeout=75,
    )
    if not isinstance(data, dict):
        return []
    holders = data.get("list") or data.get("data", {}).get("list") or []
    return holders if isinstance(holders, list) else []


def latest_holder_snapshot_ts(address: str) -> int | None:
    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            SELECT MAX(snapshot_ts)
            FROM bottom_holder_snapshots
            WHERE chain=%s AND address=%s
            """,
            (CHAIN, address),
        )
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else None

    return db_op(_op)


def save_holder_snapshot(address: str, holders: list[dict[str, Any]]) -> int | None:
    if not holders:
        return None
    snapshot_ts = now_ts()
    summary = summarize_holders(holders)

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO bottom_holder_snapshots (
                chain, address, snapshot_ts, holder_count, non_pool_count,
                top10_pct, top100_pct, non_pool_pct, buy_volume, sell_volume,
                netflow, raw_summary
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
            """,
            (
                CHAIN,
                address,
                snapshot_ts,
                summary["holder_count"],
                summary["non_pool_count"],
                summary["top10_pct"],
                summary["top100_pct"],
                summary["non_pool_pct"],
                summary["buy_volume"],
                summary["sell_volume"],
                summary["netflow"],
                Json(summary),
            ),
        )
        snapshot_id = int(cur.fetchone()[0])
        wallet_rows = [
            (
                snapshot_id,
                address,
                holder_key(h),
                to_float(h.get("amount_percentage")),
                to_float(h.get("usd_value")),
                to_float(h.get("buy_volume_cur")),
                to_float(h.get("sell_volume_cur")),
                to_float(h.get("netflow_usd")),
                to_int(h.get("start_holding_at")) or None,
                Json(h.get("maker_token_tags") or h.get("tags") or []),
                Json(h),
            )
            for h in holders
            if holder_key(h)
        ]
        if wallet_rows:
            execute_values(
                cur,
                """
                INSERT INTO bottom_holder_wallets (
                    snapshot_id, address, wallet, amount_percentage, usd_value,
                    buy_volume_cur, sell_volume_cur, netflow_usd, start_holding_at,
                    tags, raw
                ) VALUES %s
                ON CONFLICT (snapshot_id, wallet) DO NOTHING
                """,
                wallet_rows,
            )
        return snapshot_id

    return db_op(_op)


def maybe_refresh_holders(address: str, refresh_sec: int) -> int | None:
    last_ts = latest_holder_snapshot_ts(address)
    if last_ts and now_ts() - last_ts < refresh_sec:
        return None
    holders = fetch_holder_snapshot(address)
    return save_holder_snapshot(address, holders)


def load_last_two_holder_snapshots(address: str) -> tuple[int | None, int | None]:
    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id
            FROM bottom_holder_snapshots
            WHERE chain=%s AND address=%s
            ORDER BY snapshot_ts DESC
            LIMIT 2
            """,
            (CHAIN, address),
        )
        rows = cur.fetchall()
        ids = [int(row[0]) for row in rows]
        return (ids[0] if len(ids) > 0 else None, ids[1] if len(ids) > 1 else None)

    return db_op(_op)


def load_snapshot_wallets(snapshot_id: int | None) -> dict[str, dict[str, Any]]:
    if not snapshot_id:
        return {}

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            SELECT wallet, amount_percentage, usd_value, buy_volume_cur, sell_volume_cur,
                   netflow_usd, start_holding_at, tags
            FROM bottom_holder_wallets
            WHERE snapshot_id=%s
            """,
            (snapshot_id,),
        )
        return cur.fetchall()

    rows = db_op(_op)
    return {
        row[0]: {
            "amount_percentage": to_float(row[1]),
            "usd_value": to_float(row[2]),
            "buy_volume_cur": to_float(row[3]),
            "sell_volume_cur": to_float(row[4]),
            "netflow_usd": to_float(row[5]),
            "start_holding_at": to_int(row[6]),
            "tags": row[7] or [],
        }
        for row in rows
    }


def analyze_accumulation(address: str) -> dict[str, Any]:
    latest_id, previous_id = load_last_two_holder_snapshots(address)
    latest = load_snapshot_wallets(latest_id)
    previous = load_snapshot_wallets(previous_id)
    accumulated_delta = 0.0
    distributed_delta = 0.0
    new_holder_pct = 0.0
    netflow = 0.0
    smart_or_tagged = 0
    top_buyers = sorted(
        [
            {
                "wallet": wallet,
                "pct": to_float(cur.get("amount_percentage")),
                "usd": to_float(cur.get("usd_value")),
                "netflow": to_float(cur.get("netflow_usd")),
                "buy": to_float(cur.get("buy_volume_cur")),
                "sell": to_float(cur.get("sell_volume_cur")),
                "tags": cur.get("tags") if isinstance(cur.get("tags"), list) else [],
            }
            for wallet, cur in latest.items()
        ],
        key=lambda x: to_float(x["netflow"]),
        reverse=True,
    )[:5]

    for wallet, cur in latest.items():
        cur_pct = to_float(cur.get("amount_percentage"))
        old_pct = to_float(previous.get(wallet, {}).get("amount_percentage"))
        delta = cur_pct - old_pct
        if delta > 0:
            accumulated_delta += delta
        elif delta < 0:
            distributed_delta += abs(delta)
        if wallet not in previous:
            new_holder_pct += cur_pct
        netflow += to_float(cur.get("netflow_usd"))
        tags = cur.get("tags") if isinstance(cur.get("tags"), list) else []
        if any(str(tag) in {"smart_degen", "renowned", "bundler", "rat_trader"} for tag in tags):
            smart_or_tagged += 1

    rotation_score = accumulated_delta / max(distributed_delta, 0.000001)
    score = 0
    reasons: list[str] = []
    if accumulated_delta >= MIN_ACCUMULATED_PCT_DELTA:
        score += 30
        reasons.append(f"top holders +{accumulated_delta:.2%}")
    if netflow >= MIN_NETFLOW_USD:
        score += 25
        reasons.append(f"netflow ${netflow:,.0f}")
    if rotation_score >= 1.5 and accumulated_delta > distributed_delta:
        score += 20
        reasons.append(f"rotation {rotation_score:.1f}x")
    if new_holder_pct >= MIN_ACCUMULATED_PCT_DELTA:
        score += 15
        reasons.append(f"new holders +{new_holder_pct:.2%}")
    if smart_or_tagged > 0:
        score += 10
        reasons.append(f"tagged wallets {smart_or_tagged}")

    return {
        "score": min(score, 100),
        "reasons": reasons,
        "accumulation_pct_delta": accumulated_delta,
        "distribution_pct_delta": distributed_delta,
        "new_holder_pct": new_holder_pct,
        "rotation_score": rotation_score,
        "netflow_usd": netflow,
        "top_buyers": top_buyers,
    }


def save_signal(address: str, price_signal: dict[str, Any], accumulation: dict[str, Any]) -> bool:
    raw = {"price": price_signal, "accumulation": accumulation}

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO bottom_accumulation_signals (
                chain, address, signal_type, window_minutes, signal_bucket_ts,
                spike_pct, base_range_pct, base_drift_pct, accumulation_score,
                accumulation_pct_delta, distribution_pct_delta, rotation_score,
                netflow_usd, top_buyers, raw_analysis
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (chain, address, signal_type, window_minutes, signal_bucket_ts)
            DO NOTHING
            RETURNING id
            """,
            (
                CHAIN,
                address,
                price_signal["signal_type"],
                price_signal["window_minutes"],
                price_signal["signal_bucket_ts"],
                price_signal["spike_pct"],
                price_signal["base_range_pct"],
                price_signal["base_drift_pct"],
                accumulation["score"],
                accumulation["accumulation_pct_delta"],
                accumulation["distribution_pct_delta"],
                accumulation["rotation_score"],
                accumulation["netflow_usd"],
                Json(accumulation.get("top_buyers", [])),
                Json(raw),
            ),
        )
        return cur.fetchone() is not None

    return db_op(_op)


def send_tg(text: str) -> None:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": text},
            timeout=15,
        )
        if not resp.ok:
            print(f"tg failed: {resp.status_code} {resp.text[:200]}")
    except Exception as exc:
        print(f"tg exception: {exc}")


def handle_token(address: str, args: argparse.Namespace) -> None:
    cached = fetch_and_cache_klines(address, args.backfill_hours)
    maybe_refresh_holders(address, args.holder_refresh_sec)
    candles = load_recent_candles(address, hours=max(12, args.backfill_hours))
    price_signals = detect_price_setup(candles)
    if not price_signals:
        print(f"{address[:8]} cached={cached} no setup")
        return
    accumulation = analyze_accumulation(address)
    for price_signal in price_signals:
        inserted = save_signal(address, price_signal, accumulation)
        if not inserted:
            continue
        text = (
            "Bottom accumulation signal\n"
            f"CA: {address}\n"
            f"type: {price_signal['signal_type']} window={price_signal['window_minutes']}m\n"
            f"spike: {price_signal['spike_pct']:.1%} | "
            f"base_range: {price_signal['base_range_pct']:.1%} | "
            f"base_drift: {price_signal['base_drift_pct']:.1%}\n"
            f"accum_score: {accumulation['score']} | "
            f"accum_delta: {accumulation['accumulation_pct_delta']:.2%} | "
            f"distribution_delta: {accumulation['distribution_pct_delta']:.2%} | "
            f"netflow: ${accumulation['netflow_usd']:,.0f}\n"
            f"reasons: {', '.join(accumulation['reasons']) or 'price setup only'}\n"
            f"https://gmgn.ai/sol/token/{address}"
        )
        print(text)
        if args.notify:
            send_tg(text)


def scan_once(args: argparse.Namespace) -> None:
    try:
        tokens = fetch_source_tokens(args.source_table, args.source_ca_column, args.source_chain_column)
    except Exception as exc:
        print(
            "source token query failed. Set --source-table/--source-ca-column "
            f"to match your DB schema. error={exc}"
        )
        return
    print(f"[{datetime.now().strftime('%H:%M:%S')}] source_tokens={len(tokens)}")
    for address in tokens[: args.max_tokens]:
        handle_token(address, args)
        time.sleep(args.token_delay)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Monitor bottom accumulation tokens from DB CA list.")
    parser.add_argument("--once", action="store_true", help="Run once and exit.")
    parser.add_argument("--watch", action="store_true", help="Run forever.")
    parser.add_argument("--notify", action="store_true", help="Send Telegram messages for new signals.")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL_SEC, help="Watch interval seconds.")
    parser.add_argument("--source-table", default=DEFAULT_SOURCE_TABLE, help="Source table containing token CAs.")
    parser.add_argument("--source-ca-column", default=DEFAULT_SOURCE_CA_COLUMN, help="CA column in source table.")
    parser.add_argument("--source-chain-column", default=DEFAULT_SOURCE_CHAIN_COLUMN, help="Optional chain column.")
    parser.add_argument("--backfill-hours", type=int, default=DEFAULT_BACKFILL_HOURS, help="Initial kline backfill hours.")
    parser.add_argument("--holder-refresh-sec", type=int, default=DEFAULT_HOLDER_REFRESH_SEC)
    parser.add_argument("--max-tokens", type=int, default=500)
    parser.add_argument("--token-delay", type=float, default=0.25, help="Delay between tokens to avoid rate limits.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    while True:
        scan_once(args)
        if args.once or not args.watch:
            break
        print(f"sleep {args.interval}s")
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
