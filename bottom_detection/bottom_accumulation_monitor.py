#!/usr/bin/env python3
"""
Monitor 1h trending tokens by storing each processed Top100 holder snapshot as JSON.
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
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from psycopg2.extras import Json

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from config import TG_BOT_TOKEN, TG_CHAT_ID
from db_client import db_op


CHAIN = "sol"
TREND_INTERVAL = os.getenv("BOTTOM_TREND_INTERVAL", "1h")
TREND_LIMIT = int(os.getenv("BOTTOM_TREND_LIMIT", "100"))
DEFAULT_INTERVAL_SEC = int(os.getenv("BOTTOM_SCAN_INTERVAL", "300"))
TOP_HOLDER_LIMIT = int(os.getenv("BOTTOM_TOP_HOLDER_LIMIT", "100"))
RECENT_COMPARE_LIMIT = int(os.getenv("BOTTOM_RECENT_COMPARE_LIMIT", "100"))
NEW_TOKEN_AGE_CUTOFF_SEC = int(os.getenv("BOTTOM_NEW_TOKEN_AGE_CUTOFF_SEC", str(24 * 3600)))
NEW_TOKEN_SNAPSHOT_INTERVAL_SEC = int(os.getenv("BOTTOM_NEW_TOKEN_SNAPSHOT_INTERVAL_SEC", "300"))
OLD_TOKEN_SNAPSHOT_INTERVAL_SEC = int(os.getenv("BOTTOM_OLD_TOKEN_SNAPSHOT_INTERVAL_SEC", "900"))
NEW_TOKEN_KLINE_RESOLUTION = os.getenv("BOTTOM_NEW_TOKEN_KLINE_RESOLUTION", "5m")
OLD_TOKEN_KLINE_RESOLUTION = os.getenv("BOTTOM_OLD_TOKEN_KLINE_RESOLUTION", "15m")
KLINE_LOOKBACK_SEC = int(os.getenv("BOTTOM_KLINE_LOOKBACK_SEC", str(6 * 3600)))
MIN_MCAP_USD = float(os.getenv("BOTTOM_MIN_MCAP_USD", "40000"))
MIN_TOKEN_AGE_SEC = int(os.getenv("BOTTOM_MIN_TOKEN_AGE_SEC", str(5 * 3600)))
MIN_FEE_SOL = float(os.getenv("BOTTOM_MIN_FEE_SOL", "10"))

MIN_ACCUMULATED_PCT_DELTA = float(os.getenv("BOTTOM_MIN_ACCUM_PCT_DELTA", "0.015"))
MIN_WINDOW_ACCUMULATED_PCT_DELTA = float(os.getenv("BOTTOM_MIN_WINDOW_ACCUM_PCT_DELTA", "0.10"))
MIN_SIGNAL_HISTORY_COUNT = int(os.getenv("BOTTOM_MIN_SIGNAL_HISTORY_COUNT", str(RECENT_COMPARE_LIMIT)))
MIN_WALLET_BEHAVIOR_PCT_DELTA = float(os.getenv("BOTTOM_MIN_WALLET_BEHAVIOR_PCT_DELTA", "0.003"))
MIN_WALLET_BEHAVIOR_NETFLOW_USD = float(os.getenv("BOTTOM_MIN_WALLET_BEHAVIOR_NETFLOW_USD", "1000"))
EARLY_WALLET_RANK_LIMIT = int(os.getenv("BOTTOM_EARLY_WALLET_RANK_LIMIT", "30"))
MIN_EARLY_WALLET_SELL_STEPS = int(os.getenv("BOTTOM_MIN_EARLY_WALLET_SELL_STEPS", "3"))
MIN_EARLY_WALLET_DISTRIBUTION_PCT = float(os.getenv("BOTTOM_MIN_EARLY_WALLET_DISTRIBUTION_PCT", "0.005"))
MIN_DISTRIBUTED_PCT_DELTA = float(os.getenv("BOTTOM_MIN_DISTRIB_PCT_DELTA", "0.015"))
MIN_ROTATION_PCT = float(os.getenv("BOTTOM_MIN_ROTATION_PCT", "0.02"))
MIN_NETFLOW_USD = float(os.getenv("BOTTOM_MIN_NETFLOW_USD", "5000"))
MIN_SIGNAL_SCORE = int(os.getenv("BOTTOM_MIN_SIGNAL_SCORE", "60"))

SOL_CA_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,50}$")


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


def run_gmgn(args: list[str], timeout: int = 75) -> dict[str, Any] | list[Any] | None:
    cmd = [*gmgn_command_prefix(), *args, "--raw"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", timeout=timeout)
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


def token_address(row: dict[str, Any]) -> str:
    return str(row.get("address") or row.get("token_address") or row.get("ca") or "").strip()


def token_label(row: dict[str, Any]) -> str:
    symbol = str(row.get("symbol") or row.get("ticker") or row.get("name") or "UNKNOWN").strip()
    address = token_address(row)
    short_addr = address[:8] if address else "noaddr"
    return f"${symbol}({short_addr})"


def holder_key(holder: dict[str, Any]) -> str:
    return str(holder.get("address") or holder.get("wallet_address") or "").strip()


def is_pool_holder(holder: dict[str, Any]) -> bool:
    return to_int(holder.get("addr_type")) == 2 or "pool" in str(holder.get("exchange") or "").lower()


def calc_mcap(row: dict[str, Any]) -> float:
    price = to_float(row.get("price"))
    circulating_supply = to_float(row.get("circulating_supply"))
    if price > 0 and circulating_supply > 0:
        return price * circulating_supply
    for key in ("market_cap", "usd_market_cap", "mcap", "fdv", "fully_diluted_valuation"):
        value = to_float(row.get(key))
        if value > 0:
            return value
    return 0.0


def first_value(row: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def parse_timestamp(value: Any) -> int:
    if value in (None, ""):
        return 0
    if isinstance(value, datetime):
        return int(value.timestamp())
    ts = to_int(value)
    if ts > 0:
        return ts // 1000 if ts > 10_000_000_000 else ts
    if isinstance(value, str):
        try:
            return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
        except ValueError:
            return 0
    return 0


def token_created_ts(row: dict[str, Any]) -> int:
    value = first_value(
        row,
        (
            "watchlist_create_at",
            "created_at",
            "creation_timestamp",
            "created_timestamp",
            "create_timestamp",
            "open_timestamp",
            "launch_timestamp",
            "pool_creation_timestamp",
            "pair_created_at",
        ),
    )
    return parse_timestamp(value)


def token_age_sec(row: dict[str, Any]) -> int:
    created_ts = token_created_ts(row)
    return now_ts() - created_ts if created_ts > 0 else 0


def is_new_token(row: dict[str, Any]) -> bool:
    age = token_age_sec(row)
    return age > 0 and age <= NEW_TOKEN_AGE_CUTOFF_SEC


def token_snapshot_interval_sec(row: dict[str, Any]) -> int:
    return NEW_TOKEN_SNAPSHOT_INTERVAL_SEC if is_new_token(row) else OLD_TOKEN_SNAPSHOT_INTERVAL_SEC


def token_kline_resolution(row: dict[str, Any]) -> str:
    return NEW_TOKEN_KLINE_RESOLUTION if is_new_token(row) else OLD_TOKEN_KLINE_RESOLUTION


def fee_sol(row: dict[str, Any]) -> float | None:
    value = first_value(
        row,
        (
            "fee_sol",
            "total_fee_sol",
            "fees_sol",
            "swap_fee_sol",
            "trade_fee_sol",
            "fee",
            "fees",
            "total_fee",
            "tx_fee_sol",
        ),
    )
    if value in (None, ""):
        return None
    fee = to_float(value)
    return fee / 1_000_000_000 if fee > 1_000_000 else fee


def token_basic_filter_reason(row: dict[str, Any]) -> str | None:
    mcap = calc_mcap(row)
    if mcap < MIN_MCAP_USD:
        return f"市值${mcap:,.0f}<{MIN_MCAP_USD:,.0f}"
    age = token_age_sec(row)
    if age and age < MIN_TOKEN_AGE_SEC:
        return f"创建{age / 3600:.1f}h<{MIN_TOKEN_AGE_SEC / 3600:.1f}h"
    return None


def token_fee_filter_reason(row: dict[str, Any]) -> str | None:
    fee = fee_sol(row)
    if fee is not None and fee < MIN_FEE_SOL:
        return f"手续费{fee:.2f}SOL<{MIN_FEE_SOL:.2f}SOL"
    return None


def fetch_trending_tokens() -> list[dict[str, Any]]:
    data = run_gmgn(
        ["market", "trending", "--chain", CHAIN, "--interval", TREND_INTERVAL, "--limit", str(TREND_LIMIT)]
    )
    if not isinstance(data, dict):
        return []
    rows = data.get("data", {}).get("rank") or data.get("rank") or data.get("list") or []
    tokens = []
    seen = set()
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        address = token_address(row)
        if not valid_sol_ca(address) or address in seen:
            continue
        seen.add(address)
        tokens.append(row)
    return tokens


def fetch_watchlist_tokens() -> list[dict[str, Any]]:
    def _op(conn):
        cur = conn.cursor()
        cur.execute("SELECT ca, create_at FROM bottom_watchlist_tokens WHERE ca IS NOT NULL")
        return cur.fetchall()

    try:
        rows = db_op(_op)
    except Exception as exc:
        print(f"watchlist query failed: {exc}")
        return []
    tokens = []
    for ca, create_at in rows:
        address = str(ca).strip()
        if not valid_sol_ca(address):
            continue
        token = {"address": address, "source": "watchlist"}
        if create_at:
            created_ts = int(create_at.timestamp()) if isinstance(create_at, datetime) else parse_timestamp(create_at)
            token["watchlist_create_at"] = created_ts
            token["created_at"] = created_ts
        tokens.append(token)
    return tokens


def merge_token_sources(*token_lists: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged = []
    by_address = {}
    for tokens in token_lists:
        for token in tokens:
            address = token_address(token)
            if not valid_sol_ca(address):
                continue
            if address in by_address:
                existing = by_address[address]
                sources = set(existing.get("_sources", []))
                sources.add(str(token.get("source") or "trending"))
                existing["_sources"] = sorted(sources)
                for key, value in token.items():
                    if key not in existing or existing.get(key) in (None, "", 0):
                        existing[key] = value
                continue
            item = dict(token)
            item["_sources"] = [str(token.get("source") or "trending")]
            by_address[address] = item
            merged.append(item)
    return merged


def fetch_top100_holders(address: str) -> list[dict[str, Any]]:
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
        timeout=90,
    )
    if not isinstance(data, dict):
        return []
    holders = data.get("list") or data.get("data", {}).get("list") or []
    return holders if isinstance(holders, list) else []


def fetch_token_metadata(address: str) -> tuple[dict[str, Any], dict[str, Any]]:
    info = run_gmgn(["token", "info", "--chain", CHAIN, "--address", address], timeout=75)
    sec = run_gmgn(["token", "security", "--chain", CHAIN, "--address", address], timeout=75)
    return (info if isinstance(info, dict) else {}, sec if isinstance(sec, dict) else {})


def fetch_token_pool(address: str) -> dict[str, Any] | list[Any] | None:
    return run_gmgn(["token", "pool", "--chain", CHAIN, "--address", address], timeout=75)


def extract_pool_rows(data: dict[str, Any] | list[Any] | None) -> list[dict[str, Any]]:
    if not data:
        return []
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        rows = (
            data.get("list")
            or data.get("pools")
            or data.get("pairs")
            or data.get("data", {}).get("list")
            or data.get("data", {}).get("pools")
            or data.get("data", {}).get("pairs")
        )
        if not rows and any(key in data for key in ("pool_address", "address", "liquidity", "exchange")):
            rows = [data]
    else:
        rows = []
    return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def normalize_pool(row: dict[str, Any]) -> dict[str, Any]:
    liquidity = to_float(
        row.get("liquidity")
        or row.get("liquidity_usd")
        or row.get("usd_liquidity")
        or row.get("reserve_usd")
    )
    return {
        "address": str(row.get("pool_address") or row.get("address") or row.get("pair_address") or "").strip(),
        "exchange": str(row.get("exchange") or row.get("dex") or row.get("amm") or "").strip(),
        "quote_address": str(row.get("quote_address") or "").strip(),
        "quote_symbol": str(row.get("quote_symbol") or row.get("quote") or "").strip(),
        "liquidity": liquidity,
        "base_reserve": to_float(row.get("base_reserve")),
        "quote_reserve": to_float(row.get("quote_reserve")),
        "price": to_float(row.get("price")),
        "created_ts": parse_timestamp(row.get("creation_timestamp") or row.get("created_at")),
    }


def summarize_pools(token: dict[str, Any]) -> dict[str, Any]:
    rows = extract_pool_rows(token.get("_gmgn_pool"))
    info_pool = token.get("_gmgn_info", {}).get("pool")
    if isinstance(info_pool, dict):
        rows.append(info_pool)
    if not rows:
        rows.append(
            {
                "pool_address": token.get("biggest_pool_address") or token.get("pool_address"),
                "exchange": token.get("exchange") or token.get("launchpad_platform"),
                "liquidity": token.get("liquidity") or token.get("pool_liquidity"),
                "price": token.get("price"),
            }
        )

    pools = [normalize_pool(row) for row in rows]
    pools = [pool for pool in pools if pool["liquidity"] > 0 or pool["address"] or pool["exchange"]]
    pools.sort(key=lambda item: item["liquidity"], reverse=True)
    total_liquidity = sum(pool["liquidity"] for pool in pools)
    main_pool = pools[0] if pools else {}
    main_liquidity = to_float(main_pool.get("liquidity")) if main_pool else 0.0
    mcap = calc_mcap(token)
    return {
        "pool_count": len(pools),
        "total_liquidity": total_liquidity,
        "main_liquidity": main_liquidity,
        "main_pool_address": main_pool.get("address", "") if main_pool else "",
        "main_exchange": main_pool.get("exchange", "") if main_pool else "",
        "main_quote_symbol": main_pool.get("quote_symbol", "") if main_pool else "",
        "main_price": to_float(main_pool.get("price")) if main_pool else 0.0,
        "main_share": main_liquidity / total_liquidity if total_liquidity > 0 else 0.0,
        "liquidity_mcap_ratio": total_liquidity / mcap if mcap > 0 else 0.0,
        "main_liquidity_mcap_ratio": main_liquidity / mcap if mcap > 0 else 0.0,
        "pools": pools[:8],
    }


def extract_kline_rows(data: dict[str, Any] | list[Any] | None) -> list[dict[str, Any]]:
    if not data:
        return []
    if isinstance(data, list):
        rows = data
    else:
        rows = data.get("list") or data.get("data", {}).get("list") or data.get("data") or []
    candles = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        raw_ts = to_int(row.get("time") or row.get("timestamp") or row.get("t"))
        ts = raw_ts // 1000 if raw_ts > 10_000_000_000 else raw_ts
        close = to_float(row.get("close") or row.get("c"))
        if ts <= 0 or close <= 0:
            continue
        candles.append(
            {
                "ts": ts,
                "open": to_float(row.get("open") or row.get("o"), close),
                "high": to_float(row.get("high") or row.get("h"), close),
                "low": to_float(row.get("low") or row.get("l"), close),
                "close": close,
                "volume": to_float(row.get("volume") or row.get("v")),
                "amount": to_float(row.get("amount") or row.get("a")),
            }
        )
    candles.sort(key=lambda item: item["ts"])
    return candles


def fetch_kline(address: str, resolution: str) -> list[dict[str, Any]]:
    end_ts = now_ts()
    start_ts = end_ts - KLINE_LOOKBACK_SEC
    data = run_gmgn(
        [
            "market",
            "kline",
            "--chain",
            CHAIN,
            "--address",
            address,
            "--resolution",
            resolution,
            "--from",
            str(start_ts),
            "--to",
            str(end_ts),
        ],
        timeout=75,
    )
    return extract_kline_rows(data)


def summarize_kline(candles: list[dict[str, Any]], resolution: str) -> dict[str, Any]:
    if not candles:
        return {"resolution": resolution, "count": 0}
    first = candles[0]
    last = candles[-1]
    open_price = to_float(first.get("open"))
    close_price = to_float(last.get("close"))
    lows = [to_float(c.get("low")) for c in candles if to_float(c.get("low")) > 0]
    highs = [to_float(c.get("high")) for c in candles if to_float(c.get("high")) > 0]
    total_volume = sum(to_float(c.get("volume")) for c in candles)
    return {
        "resolution": resolution,
        "count": len(candles),
        "from_ts": first.get("ts"),
        "to_ts": last.get("ts"),
        "open": open_price,
        "close": close_price,
        "change_pct": ((close_price - open_price) / open_price * 100) if open_price > 0 else 0,
        "high": max(highs) if highs else 0,
        "low": min(lows) if lows else 0,
        "volume_usd": total_volume,
        "last_volume_usd": to_float(last.get("volume")),
    }


def merge_token_metadata(token: dict[str, Any], info: dict[str, Any], security: dict[str, Any]) -> dict[str, Any]:
    merged = dict(token)
    for source in (security, info):
        for key, value in source.items():
            if key not in merged or merged.get(key) in (None, "", 0):
                merged[key] = value
    merged["_gmgn_info"] = info
    merged["_gmgn_security"] = security
    return merged


def attach_token_pool(token: dict[str, Any], pool_data: dict[str, Any] | list[Any] | None) -> dict[str, Any]:
    merged = dict(token)
    merged["_gmgn_pool"] = pool_data if pool_data is not None else {}
    return merged


def fill_watchlist_create_at(token: dict[str, Any]) -> None:
    if "watchlist" not in set(token.get("_sources", [])):
        return
    if token.get("watchlist_create_at"):
        return
    created_ts = token_created_ts(token)
    if created_ts <= 0:
        return
    address = token_address(token)

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE bottom_watchlist_tokens
            SET create_at = to_timestamp(%s)
            WHERE ca = %s AND create_at IS NULL
            """,
            (created_ts, address),
        )

    try:
        db_op(_op)
        token["watchlist_create_at"] = created_ts
        print(f"{token_label(token)} watchlist create_at filled")
    except Exception as exc:
        print(f"{token_label(token)} watchlist create_at fill failed: {exc}")


def normalize_holder(holder: dict[str, Any], rank_no: int) -> dict[str, Any] | None:
    wallet = holder_key(holder)
    if not wallet or is_pool_holder(holder):
        return None
    return {
        "rank": rank_no,
        "wallet": wallet,
        "hold_pct": to_float(holder.get("amount_percentage")),
        "usd_value": to_float(holder.get("usd_value")),
        "buy_volume": to_float(holder.get("buy_volume_cur")),
        "sell_volume": to_float(holder.get("sell_volume_cur")),
        "netflow": to_float(holder.get("netflow_usd")),
        "avg_cost": to_float(holder.get("avg_cost")),
        "profit": to_float(holder.get("profit")),
        "buy_count": to_int(holder.get("buy_tx_count_cur")),
        "sell_count": to_int(holder.get("sell_tx_count_cur")),
        "start_holding_at": to_int(holder.get("start_holding_at")),
        "last_active_at": to_int(holder.get("last_active_timestamp")),
        "tags": holder.get("maker_token_tags") or holder.get("tags") or [],
    }


def build_snapshot_json(
    token: dict[str, Any],
    raw_holders: list[dict[str, Any]],
    candles: list[dict[str, Any]] | None = None,
    kline_resolution: str | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    holders = []
    for rank_no, holder in enumerate(raw_holders, start=1):
        normalized = normalize_holder(holder, rank_no)
        if normalized:
            holders.append(normalized)

    pool_summary = summarize_pools(token)
    liquidity = pool_summary["total_liquidity"] or to_float(token.get("liquidity") or token.get("pool_liquidity"))

    summary = {
        "holder_count": len(raw_holders),
        "non_pool_count": len(holders),
        "top10_pct": sum(h["hold_pct"] for h in holders[:10]),
        "top20_pct": sum(h["hold_pct"] for h in holders[:20]),
        "top50_pct": sum(h["hold_pct"] for h in holders[:50]),
        "top100_pct": sum(h["hold_pct"] for h in holders[:100]),
        "buy_volume": sum(h["buy_volume"] for h in holders),
        "sell_volume": sum(h["sell_volume"] for h in holders),
        "netflow": sum(h["netflow"] for h in holders),
        "mcap": calc_mcap(token),
        "price": to_float(token.get("price")),
        "liquidity": liquidity,
        "pool": pool_summary,
        "created_ts": token_created_ts(token),
        "age_sec": token_age_sec(token),
        "fee_sol": fee_sol(token),
        "kline": summarize_kline(candles or [], kline_resolution or token_kline_resolution(token)),
        "kline_candles": candles or [],
    }
    return summary, holders


def recent_snapshots(address: str, limit: int = RECENT_COMPARE_LIMIT) -> list[dict[str, Any]]:
    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, snapshot_ts, summary, holders, analysis
            FROM bottom_top100_snapshots
            WHERE chain=%s AND address=%s
            ORDER BY snapshot_ts DESC
            LIMIT %s
            """,
            (CHAIN, address, limit),
        )
        return [
            {"id": row[0], "snapshot_ts": int(row[1] or 0), "summary": row[2] or {}, "holders": row[3] or [], "analysis": row[4] or {}}
            for row in cur.fetchall()
        ]

    return db_op(_op)


def json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return int(value.timestamp())
    if isinstance(value, dict):
        return {key: json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [json_safe(item) for item in value]
    return value


def latest_snapshot_ts(address: str) -> int | None:
    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            SELECT snapshot_ts
            FROM bottom_top100_snapshots
            WHERE chain=%s AND address=%s
            ORDER BY snapshot_ts DESC
            LIMIT 1
            """,
            (CHAIN, address),
        )
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else None

    return db_op(_op)


def recent_snapshot_skip_reason(address: str, token: dict[str, Any]) -> str | None:
    latest_ts = latest_snapshot_ts(address)
    if not latest_ts:
        return None
    age = now_ts() - latest_ts
    required_interval = token_snapshot_interval_sec(token)
    if age < required_interval:
        return f"最近快照{age / 60:.1f}m<{required_interval / 60:.1f}m"
    return None


def compare_holder_sets(current_holders: list[dict[str, Any]], previous_holders: list[dict[str, Any]]) -> dict[str, Any]:
    current = {h["wallet"]: h for h in current_holders}
    previous = {h["wallet"]: h for h in previous_holders}

    accumulated_delta = 0.0
    distributed_delta = 0.0
    new_holder_pct = 0.0
    exited_holder_pct = 0.0
    netflow_delta = 0.0
    tagged_delta = 0.0
    top_buyers = []
    top_sellers = []

    for wallet, cur in current.items():
        old = previous.get(wallet)
        old_pct = to_float(old.get("hold_pct")) if old else 0.0
        delta = cur["hold_pct"] - old_pct
        buy_delta = cur["buy_volume"] - (to_float(old.get("buy_volume")) if old else 0.0)
        sell_delta = cur["sell_volume"] - (to_float(old.get("sell_volume")) if old else 0.0)
        net_delta = buy_delta - sell_delta
        netflow_delta += net_delta
        if delta > 0:
            accumulated_delta += delta
            top_buyers.append({"wallet": wallet, "pct_delta": delta, "netflow": net_delta, "tags": cur.get("tags", [])})
        elif delta < 0:
            distributed_delta += abs(delta)
            top_sellers.append({"wallet": wallet, "pct_delta": delta, "netflow": net_delta, "tags": cur.get("tags", [])})
        if not old:
            new_holder_pct += cur["hold_pct"]
        if delta > 0 and any(str(tag) in {"smart_degen", "renowned", "bundler", "rat_trader", "fresh_wallet"} for tag in cur.get("tags", [])):
            tagged_delta += delta

    for wallet, old in previous.items():
        if wallet not in current:
            exited_holder_pct += old["hold_pct"]

    rotation_score = accumulated_delta / max(distributed_delta, 0.000001)
    return {
        "accumulation_pct_delta": accumulated_delta,
        "distribution_pct_delta": distributed_delta,
        "rotation_score": rotation_score,
        "new_holder_pct": new_holder_pct,
        "exited_holder_pct": exited_holder_pct,
        "turnover_pct": new_holder_pct + exited_holder_pct,
        "netflow_usd": netflow_delta,
        "tagged_delta": tagged_delta,
        "top_buyers": sorted(top_buyers, key=lambda item: item["pct_delta"], reverse=True)[:8],
        "top_sellers": sorted(top_sellers, key=lambda item: item["pct_delta"])[:8],
    }


def wallet_behavior_item(wallet: str, points: list[dict[str, Any]]) -> dict[str, Any]:
    first = points[0]
    last = points[-1]
    hold_delta = to_float(last.get("hold_pct")) - to_float(first.get("hold_pct"))
    buy_delta = to_float(last.get("buy_volume")) - to_float(first.get("buy_volume"))
    sell_delta = to_float(last.get("sell_volume")) - to_float(first.get("sell_volume"))
    netflow_delta = buy_delta - sell_delta
    rank_delta = to_int(first.get("rank")) - to_int(last.get("rank"))
    active_points = [point for point in points if to_float(point.get("hold_pct")) > 0]
    first_active = active_points[0] if active_points else first
    sell_steps = 0
    buy_steps = 0
    hold_down_steps = 0
    hold_up_steps = 0
    max_hold_pct = max((to_float(point.get("hold_pct")) for point in points), default=0.0)
    min_after_peak_pct = to_float(last.get("hold_pct"))
    peak_seen = False
    for prev, cur in zip(points, points[1:]):
        prev_hold = to_float(prev.get("hold_pct"))
        cur_hold = to_float(cur.get("hold_pct"))
        if prev_hold >= max_hold_pct:
            peak_seen = True
        if peak_seen:
            min_after_peak_pct = min(min_after_peak_pct, cur_hold)
        sell_step = to_float(cur.get("sell_volume")) - to_float(prev.get("sell_volume"))
        buy_step = to_float(cur.get("buy_volume")) - to_float(prev.get("buy_volume"))
        if sell_step > max(buy_step, 0) and sell_step > 0:
            sell_steps += 1
        if buy_step > max(sell_step, 0) and buy_step > 0:
            buy_steps += 1
        if cur_hold < prev_hold:
            hold_down_steps += 1
        elif cur_hold > prev_hold:
            hold_up_steps += 1
    distributed_from_peak = max_hold_pct - min_after_peak_pct
    return {
        "wallet": wallet,
        "first_ts": to_int(first.get("_snapshot_ts")),
        "last_ts": to_int(last.get("_snapshot_ts")),
        "first_active_ts": to_int(first_active.get("_snapshot_ts")),
        "first_rank": to_int(first.get("rank")),
        "last_rank": to_int(last.get("rank")),
        "first_active_rank": to_int(first_active.get("rank")),
        "rank_delta": rank_delta,
        "first_hold_pct": to_float(first.get("hold_pct")),
        "last_hold_pct": to_float(last.get("hold_pct")),
        "first_active_hold_pct": to_float(first_active.get("hold_pct")),
        "max_hold_pct": max_hold_pct,
        "distributed_from_peak": distributed_from_peak,
        "hold_delta": hold_delta,
        "buy_delta": buy_delta,
        "sell_delta": sell_delta,
        "netflow_delta": netflow_delta,
        "sell_steps": sell_steps,
        "buy_steps": buy_steps,
        "hold_down_steps": hold_down_steps,
        "hold_up_steps": hold_up_steps,
        "avg_cost": to_float(last.get("avg_cost")) or to_float(first.get("avg_cost")),
        "profit": to_float(last.get("profit")),
        "tags": last.get("tags") or first.get("tags") or [],
        "point_count": len(points),
        "is_new_entry": to_float(first.get("hold_pct")) <= 0 and to_float(last.get("hold_pct")) > 0,
        "is_exited": to_float(first.get("hold_pct")) > 0 and to_float(last.get("hold_pct")) <= 0,
    }


def analyze_wallet_behaviors(
    current_holders: list[dict[str, Any]],
    recent_history: list[dict[str, Any]],
) -> dict[str, Any]:
    frames = []
    for snap in reversed(recent_history):
        frames.append({"snapshot_ts": to_int(snap.get("snapshot_ts")), "holders": snap.get("holders") or []})
    frames.append({"snapshot_ts": now_ts(), "holders": current_holders})

    wallets = set()
    for frame in frames:
        holder_map = {}
        for holder in frame["holders"]:
            wallet = str(holder.get("wallet") or "").strip()
            if wallet:
                wallets.add(wallet)
                holder_map[wallet] = holder
        frame["holder_map"] = holder_map

    trajectories = {}
    for wallet in wallets:
        points = []
        previous_holder = None
        for frame in frames:
            holder = dict(frame["holder_map"].get(wallet) or {})
            if not holder:
                holder = {
                    "wallet": wallet,
                    "rank": 0,
                    "hold_pct": 0,
                    "usd_value": 0,
                    "buy_volume": previous_holder.get("buy_volume", 0) if previous_holder else 0,
                    "sell_volume": previous_holder.get("sell_volume", 0) if previous_holder else 0,
                    "netflow": previous_holder.get("netflow", 0) if previous_holder else 0,
                    "avg_cost": previous_holder.get("avg_cost", 0) if previous_holder else 0,
                    "profit": previous_holder.get("profit", 0) if previous_holder else 0,
                    "tags": previous_holder.get("tags", []) if previous_holder else [],
                }
            holder["_snapshot_ts"] = frame["snapshot_ts"]
            points.append(holder)
            previous_holder = holder
        trajectories[wallet] = wallet_behavior_item(wallet, points)

    def strong_buy(item: dict[str, Any]) -> bool:
        return (
            item["hold_delta"] >= MIN_WALLET_BEHAVIOR_PCT_DELTA
            and (item["netflow_delta"] >= MIN_WALLET_BEHAVIOR_NETFLOW_USD or item["buy_delta"] > item["sell_delta"] * 1.5)
        )

    def strong_sell(item: dict[str, Any]) -> bool:
        return (
            item["hold_delta"] <= -MIN_WALLET_BEHAVIOR_PCT_DELTA
            and (item["netflow_delta"] <= -MIN_WALLET_BEHAVIOR_NETFLOW_USD or item["sell_delta"] > item["buy_delta"] * 1.5)
        )

    def early_distributor(item: dict[str, Any]) -> bool:
        is_early = (
            0 < item["first_active_rank"] <= EARLY_WALLET_RANK_LIMIT
            or item["first_active_hold_pct"] >= MIN_EARLY_WALLET_DISTRIBUTION_PCT
        )
        persistent_sell = (
            item["sell_steps"] >= MIN_EARLY_WALLET_SELL_STEPS
            or item["hold_down_steps"] >= MIN_EARLY_WALLET_SELL_STEPS
        )
        meaningful_distribution = (
            item["distributed_from_peak"] >= MIN_EARLY_WALLET_DISTRIBUTION_PCT
            or abs(min(item["hold_delta"], 0)) >= MIN_EARLY_WALLET_DISTRIBUTION_PCT
        )
        sell_dominant = item["sell_delta"] > item["buy_delta"] or item["netflow_delta"] < 0
        return is_early and persistent_sell and meaningful_distribution and sell_dominant

    items = list(trajectories.values())
    accumulators = sorted([item for item in items if strong_buy(item)], key=lambda x: x["hold_delta"], reverse=True)[:10]
    distributors = sorted([item for item in items if strong_sell(item)], key=lambda x: x["hold_delta"])[:10]
    early_distributors = sorted(
        [item for item in items if early_distributor(item)],
        key=lambda x: (x["distributed_from_peak"], x["sell_steps"], abs(min(x["hold_delta"], 0))),
        reverse=True,
    )[:10]
    rotators_in = sorted(
        [item for item in items if item["is_new_entry"] and item["last_hold_pct"] >= MIN_WALLET_BEHAVIOR_PCT_DELTA],
        key=lambda x: x["last_hold_pct"],
        reverse=True,
    )[:10]
    rotators_out = sorted(
        [item for item in items if item["is_exited"] and item["first_hold_pct"] >= MIN_WALLET_BEHAVIOR_PCT_DELTA],
        key=lambda x: x["first_hold_pct"],
        reverse=True,
    )[:10]

    tagged_accumulation = [
        item for item in accumulators
        if any(str(tag) in {"smart_degen", "renowned", "bundler", "rat_trader", "fresh_wallet", "sniper"} for tag in item.get("tags", []))
    ]
    return {
        "frames": len(frames),
        "wallet_count": len(items),
        "accumulator_count": len(accumulators),
        "accumulator_hold_delta": sum(item["hold_delta"] for item in accumulators),
        "accumulator_netflow": sum(item["netflow_delta"] for item in accumulators),
        "distributor_count": len(distributors),
        "distributor_hold_delta": sum(abs(item["hold_delta"]) for item in distributors),
        "distributor_netflow": sum(item["netflow_delta"] for item in distributors),
        "early_distributor_count": len(early_distributors),
        "early_distributor_hold_delta": sum(item["distributed_from_peak"] for item in early_distributors),
        "early_distributor_netflow": sum(item["netflow_delta"] for item in early_distributors),
        "rotator_in_count": len(rotators_in),
        "rotator_in_hold": sum(item["last_hold_pct"] for item in rotators_in),
        "rotator_out_count": len(rotators_out),
        "rotator_out_hold": sum(item["first_hold_pct"] for item in rotators_out),
        "tagged_accumulator_count": len(tagged_accumulation),
        "tagged_accumulator_hold_delta": sum(item["hold_delta"] for item in tagged_accumulation),
        "accumulators": accumulators,
        "distributors": distributors,
        "early_distributors": early_distributors,
        "rotators_in": rotators_in,
        "rotators_out": rotators_out,
    }



def pool_change(current_summary: dict[str, Any], previous_summary: dict[str, Any] | None) -> dict[str, Any]:
    current_pool = current_summary.get("pool") or {}
    previous_pool = (previous_summary or {}).get("pool") or {}
    current_liq = to_float(current_pool.get("total_liquidity") or current_summary.get("liquidity"))
    previous_liq = to_float(previous_pool.get("total_liquidity") or (previous_summary or {}).get("liquidity"))
    current_ratio = to_float(current_pool.get("liquidity_mcap_ratio"))
    previous_ratio = to_float(previous_pool.get("liquidity_mcap_ratio"))
    return {
        "pool_count": to_int(current_pool.get("pool_count")),
        "pool_total_liquidity": current_liq,
        "pool_main_liquidity": to_float(current_pool.get("main_liquidity")),
        "pool_main_exchange": current_pool.get("main_exchange") or "",
        "pool_main_share": to_float(current_pool.get("main_share")),
        "pool_mcap_ratio": current_ratio,
        "pool_mcap_ratio_text": f"1:{(1 / current_ratio):.1f}" if current_ratio > 0 else "N/A",
        "pool_liquidity_delta": current_liq - previous_liq if previous_liq > 0 else 0.0,
        "pool_liquidity_delta_pct": ((current_liq - previous_liq) / previous_liq) if previous_liq > 0 else 0.0,
        "pool_mcap_ratio_delta": current_ratio - previous_ratio if previous_ratio > 0 else 0.0,
    }


def analyze_snapshot_change(
    current_holders: list[dict[str, Any]],
    recent_history: list[dict[str, Any]],
    current_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    pool_stats = pool_change(current_summary or {}, (recent_history[0].get("summary") if recent_history else None) or {})
    window_pool_stats = pool_change(current_summary or {}, (recent_history[-1].get("summary") if recent_history else None) or {})

    if not current_holders or not recent_history:
        return {"score": 0, "signal_type": "baseline", "reasons": ["需要历史快照"], "history_count": len(recent_history), **pool_stats}

    previous_holders = recent_history[0].get("holders") or []
    earliest_holders = recent_history[-1].get("holders") or []
    if not previous_holders:
        return {"score": 0, "signal_type": "baseline", "reasons": ["上一轮快照为空"], "history_count": len(recent_history), **pool_stats}

    last_change = compare_holder_sets(current_holders, previous_holders)
    window_change = compare_holder_sets(current_holders, earliest_holders) if earliest_holders else last_change
    historical_analyses = [snap.get("analysis") or {} for snap in recent_history]
    accumulation_hits = sum(1 for item in historical_analyses if item.get("signal_type") in {"accumulation", "rotation"})
    distribution_hits = sum(1 for item in historical_analyses if item.get("signal_type") == "distribution")
    history_ready = len(recent_history) >= MIN_SIGNAL_HISTORY_COUNT
    enough_history_for_window = len(recent_history) >= min(MIN_SIGNAL_HISTORY_COUNT, RECENT_COMPARE_LIMIT)
    window_accumulation_pct = window_change["accumulation_pct_delta"]
    window_distribution_pct = window_change["distribution_pct_delta"]
    window_accumulation_ready = (
        enough_history_for_window
        and window_accumulation_pct >= MIN_WINDOW_ACCUMULATED_PCT_DELTA
    )
    wallet_behaviors = analyze_wallet_behaviors(current_holders, recent_history)
    early_distribution_pct = wallet_behaviors["early_distributor_hold_delta"]
    trajectory_accumulation_pct = wallet_behaviors["accumulator_hold_delta"]
    early_distribution_dominates = (
        early_distribution_pct >= MIN_EARLY_WALLET_DISTRIBUTION_PCT * 2
        and early_distribution_pct >= max(window_accumulation_pct, trajectory_accumulation_pct) * 0.35
    )

    accumulated_delta = last_change["accumulation_pct_delta"]
    distributed_delta = last_change["distribution_pct_delta"]
    turnover_pct = last_change["turnover_pct"]
    tagged_delta = last_change["tagged_delta"]
    netflow_delta = last_change["netflow_usd"]
    score = 0
    reasons = []
    signal_type = "watch"

    if accumulated_delta >= MIN_ACCUMULATED_PCT_DELTA:
        score += 30
        reasons.append(f"本轮Top100增持{accumulated_delta:.2%}")
    if netflow_delta >= MIN_NETFLOW_USD:
        score += 25
        reasons.append(f"本轮净买入${netflow_delta:,.0f}")
    if tagged_delta >= 0.005:
        score += 15
        reasons.append(f"标签钱包增持{tagged_delta:.2%}")
    if wallet_behaviors["accumulator_hold_delta"] >= 0.02:
        score += 10
        reasons.append(
            f"轨迹吸筹钱包{wallet_behaviors['accumulator_count']}个/"
            f"{wallet_behaviors['accumulator_hold_delta']:.2%}"
        )
    if wallet_behaviors["tagged_accumulator_hold_delta"] >= 0.005:
        score += 10
        reasons.append(f"标签钱包轨迹吸筹{wallet_behaviors['tagged_accumulator_hold_delta']:.2%}")
    if early_distribution_pct >= MIN_EARLY_WALLET_DISTRIBUTION_PCT:
        score = max(score - (25 if early_distribution_dominates else 10), 0)
        reasons.append(
            f"早期钱包持续出货{wallet_behaviors['early_distributor_count']}个/"
            f"{early_distribution_pct:.2%}"
        )
    if window_accumulation_ready:
        score += 40
        reasons.append(f"近{len(recent_history)}次累计增持{window_accumulation_pct:.2%}")
    else:
        if not enough_history_for_window:
            reasons.append(
                f"历史快照{len(recent_history)}/{MIN_SIGNAL_HISTORY_COUNT}，"
                f"累计增持{window_accumulation_pct:.2%}"
            )
        else:
            reasons.append(
                f"近{len(recent_history)}次累计增持{window_accumulation_pct:.2%}"
                f"<{MIN_WINDOW_ACCUMULATED_PCT_DELTA:.0%}"
            )
    if window_change["netflow_usd"] >= MIN_NETFLOW_USD * 2:
        score += 15
        reasons.append(f"近{len(recent_history)}次净买入${window_change['netflow_usd']:,.0f}")

    if pool_stats["pool_mcap_ratio"] >= 0.12:
        score += 15
        reasons.append(f"池/市值{pool_stats['pool_mcap_ratio']:.1%}({pool_stats['pool_mcap_ratio_text']})")
    elif pool_stats["pool_mcap_ratio"] >= 0.08:
        score += 8
        reasons.append(f"池/市值接近1:10({pool_stats['pool_mcap_ratio']:.1%})")
    elif 0 < pool_stats["pool_mcap_ratio"] < 0.03:
        score = max(score - 15, 0)
        reasons.append(f"池子偏薄{pool_stats['pool_mcap_ratio']:.1%}")
    if pool_stats["pool_liquidity_delta_pct"] >= 0.2 and pool_stats["pool_liquidity_delta"] >= 5000:
        score += 15
        reasons.append(f"本轮池子增厚${pool_stats['pool_liquidity_delta']:,.0f}/{pool_stats['pool_liquidity_delta_pct']:.1%}")
    if window_pool_stats["pool_liquidity_delta_pct"] >= 0.3 and window_pool_stats["pool_liquidity_delta"] >= 8000:
        score += 15
        reasons.append(f"近{len(recent_history)}次池子增厚${window_pool_stats['pool_liquidity_delta']:,.0f}/{window_pool_stats['pool_liquidity_delta_pct']:.1%}")
    if pool_stats["pool_liquidity_delta_pct"] <= -0.25 and abs(pool_stats["pool_liquidity_delta"]) >= 5000:
        score = max(score - 25, 0)
        reasons.append(f"池子抽离${abs(pool_stats['pool_liquidity_delta']):,.0f}/{pool_stats['pool_liquidity_delta_pct']:.1%}")

    if accumulation_hits >= 2:
        score += 10
        reasons.append(f"历史连续吸筹/换筹{accumulation_hits}次")
    if turnover_pct >= MIN_ROTATION_PCT and accumulated_delta >= distributed_delta * 0.8 and window_accumulation_ready:
        score += 20
        signal_type = "rotation"
        reasons.append(f"换筹{turnover_pct:.2%}")
    if distributed_delta >= MIN_DISTRIBUTED_PCT_DELTA and distributed_delta > accumulated_delta * 1.3:
        signal_type = "distribution"
        score = max(score - 30, 0)
        reasons.append(f"派发{distributed_delta:.2%}")
    if early_distribution_dominates and window_distribution_pct > window_accumulation_pct * 0.8:
        signal_type = "distribution"
        score = max(score - 20, 0)
        reasons.append("早期出货压过吸筹")
    if distribution_hits >= 2 and not window_accumulation_ready:
        signal_type = "distribution"
        score = max(score - 20, 0)
        reasons.append(f"历史派发{distribution_hits}次")
    elif score >= MIN_SIGNAL_SCORE and signal_type != "rotation" and window_accumulation_ready:
        signal_type = "accumulation"
    elif signal_type != "distribution" and not window_accumulation_ready:
        signal_type = "watch"
        score = min(score, MIN_SIGNAL_SCORE - 1)

    return {
        "score": min(score, 100),
        "signal_type": signal_type,
        "reasons": reasons,
        "history_count": len(recent_history),
        "history_ready": history_ready,
        "enough_history_for_window": enough_history_for_window,
        "min_signal_history_count": MIN_SIGNAL_HISTORY_COUNT,
        "window_accumulation_ready": window_accumulation_ready,
        "min_window_accumulation_pct_delta": MIN_WINDOW_ACCUMULATED_PCT_DELTA,
        "early_distribution_dominates": early_distribution_dominates,
        **pool_stats,
        "window_pool_liquidity_delta": window_pool_stats["pool_liquidity_delta"],
        "window_pool_liquidity_delta_pct": window_pool_stats["pool_liquidity_delta_pct"],
        "window_pool_mcap_ratio_delta": window_pool_stats["pool_mcap_ratio_delta"],
        "accumulation_pct_delta": last_change["accumulation_pct_delta"],
        "distribution_pct_delta": last_change["distribution_pct_delta"],
        "rotation_score": last_change["rotation_score"],
        "new_holder_pct": last_change["new_holder_pct"],
        "exited_holder_pct": last_change["exited_holder_pct"],
        "netflow_usd": last_change["netflow_usd"],
        "window_accumulation_pct_delta": window_change["accumulation_pct_delta"],
        "window_distribution_pct_delta": window_change["distribution_pct_delta"],
        "window_netflow_usd": window_change["netflow_usd"],
        "accumulation_hits": accumulation_hits,
        "distribution_hits": distribution_hits,
        "top_buyers": last_change["top_buyers"],
        "top_sellers": last_change["top_sellers"],
        "wallet_behaviors": wallet_behaviors,
    }

def save_snapshot(scan_id: str, token: dict[str, Any], summary: dict[str, Any], holders: list[dict[str, Any]], analysis: dict[str, Any]) -> int:
    address = token_address(token)

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO bottom_top100_snapshots (
                scan_id, chain, trend_interval, address, symbol, snapshot_ts,
                signal_type, signal_score, summary, holders, analysis, raw_token
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
            """,
            (
                scan_id,
                CHAIN,
                TREND_INTERVAL,
                address,
                token.get("symbol"),
                now_ts(),
                analysis.get("signal_type"),
                analysis.get("score", 0),
                Json(json_safe(summary)),
                Json(json_safe(holders)),
                Json(json_safe(analysis)),
                Json(json_safe(token)),
            ),
        )
        return int(cur.fetchone()[0])

    return db_op(_op)


def send_tg(text: str) -> None:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": text, "disable_web_page_preview": True},
            timeout=15,
        )
        if not resp.ok:
            print(f"tg failed: {resp.status_code} {resp.text[:200]}")
    except Exception as exc:
        print(f"tg exception: {exc}")




def signal_type_text(signal_type: str) -> str:
    mapping = {
        "baseline": "基线",
        "watch": "观察",
        "accumulation": "吸筹",
        "rotation": "换筹",
        "distribution": "派发",
    }
    return mapping.get(signal_type, signal_type or "未知")


def short_wallet(address: str) -> str:
    address = str(address or "")
    if len(address) <= 12:
        return address
    return f"{address[:6]}...{address[-4:]}"


def wallet_behavior_text(analysis: dict[str, Any]) -> str:
    behaviors = analysis.get("wallet_behaviors") or {}
    if not behaviors:
        return "钱包轨迹: 暂无"
    lines = [
        (
            f"钱包轨迹: 吸筹{behaviors.get('accumulator_count', 0)}个/"
            f"{behaviors.get('accumulator_hold_delta', 0):.2%}/净${behaviors.get('accumulator_netflow', 0):,.0f} | "
            f"出货{behaviors.get('distributor_count', 0)}个/"
            f"{behaviors.get('distributor_hold_delta', 0):.2%}/净${behaviors.get('distributor_netflow', 0):,.0f}"
        ),
        (
            f"早期出货: {behaviors.get('early_distributor_count', 0)}个/"
            f"{behaviors.get('early_distributor_hold_delta', 0):.2%}/净${behaviors.get('early_distributor_netflow', 0):,.0f}"
        ),
        (
            f"换筹: 新进{behaviors.get('rotator_in_count', 0)}个/"
            f"{behaviors.get('rotator_in_hold', 0):.2%} | "
            f"退出{behaviors.get('rotator_out_count', 0)}个/"
            f"{behaviors.get('rotator_out_hold', 0):.2%} | "
            f"标签吸筹{behaviors.get('tagged_accumulator_count', 0)}个/"
            f"{behaviors.get('tagged_accumulator_hold_delta', 0):.2%}"
        ),
    ]
    top_accumulators = behaviors.get("accumulators") or []
    if top_accumulators:
        parts = []
        for item in top_accumulators[:3]:
            raw_tags = item.get("tags") or []
            tags = raw_tags if isinstance(raw_tags, list) else [raw_tags]
            tags = ",".join(str(tag) for tag in tags[:2])
            tag_text = f" {tags}" if tags else ""
            parts.append(
                f"{short_wallet(item.get('wallet'))} +{item.get('hold_delta', 0):.2%} "
                f"净${item.get('netflow_delta', 0):,.0f}{tag_text}"
            )
        lines.append("重点吸筹: " + " | ".join(parts))
    top_distributors = behaviors.get("distributors") or []
    if top_distributors:
        parts = []
        for item in top_distributors[:3]:
            raw_tags = item.get("tags") or []
            tags = raw_tags if isinstance(raw_tags, list) else [raw_tags]
            tags = ",".join(str(tag) for tag in tags[:2])
            tag_text = f" {tags}" if tags else ""
            parts.append(
                f"{short_wallet(item.get('wallet'))} {item.get('hold_delta', 0):.2%} "
                f"净${item.get('netflow_delta', 0):,.0f}{tag_text}"
            )
        lines.append("重点出货: " + " | ".join(parts))
    early_distributors = behaviors.get("early_distributors") or []
    if early_distributors:
        parts = []
        for item in early_distributors[:3]:
            raw_tags = item.get("tags") or []
            tags = raw_tags if isinstance(raw_tags, list) else [raw_tags]
            tags = ",".join(str(tag) for tag in tags[:2])
            tag_text = f" {tags}" if tags else ""
            parts.append(
                f"{short_wallet(item.get('wallet'))} 峰值出{item.get('distributed_from_peak', 0):.2%} "
                f"卖{item.get('sell_steps', 0)}次 净${item.get('netflow_delta', 0):,.0f}{tag_text}"
            )
        lines.append("早期持续出货: " + " | ".join(parts))
    return "\n".join(lines)


def signal_text(token: dict[str, Any], analysis: dict[str, Any]) -> str:
    address = token_address(token)
    return (
        f"Top100 筹码异动 | ${token.get('symbol') or 'UNKNOWN'}\n"
        f"类型: {signal_type_text(analysis['signal_type'])} | 分数: {analysis['score']}\n"
        f"CA: {address}\n"
        f"市值: ${calc_mcap(token):,.0f} | 价格: {to_float(token.get('price')):.12f}\n"
        f"池子: ${analysis.get('pool_total_liquidity', 0):,.0f} | 池/市值: {analysis.get('pool_mcap_ratio', 0):.1%} ({analysis.get('pool_mcap_ratio_text', 'N/A')}) | "
        f"本轮池变动: ${analysis.get('pool_liquidity_delta', 0):,.0f}/{analysis.get('pool_liquidity_delta_pct', 0):.1%}\n"
        f"主池: {analysis.get('pool_main_exchange') or '未知'} | 主池占比: {analysis.get('pool_main_share', 0):.1%}\n"
        f"增持: {analysis['accumulation_pct_delta']:.2%} | 减持: {analysis['distribution_pct_delta']:.2%}\n"
        f"新进: {analysis['new_holder_pct']:.2%} | 退出: {analysis['exited_holder_pct']:.2%}\n"
        f"换筹比: {analysis['rotation_score']:.2f} | 净买入: ${analysis['netflow_usd']:,.0f}\n"
        f"近{analysis.get('history_count', 0)}次: 增持{analysis.get('window_accumulation_pct_delta', 0):.2%} | "
        f"减持{analysis.get('window_distribution_pct_delta', 0):.2%} | 净买入${analysis.get('window_netflow_usd', 0):,.0f}\n"
        f"{wallet_behavior_text(analysis)}\n"
        f"理由: {', '.join(analysis['reasons']) or '无'}\n"
        f"https://gmgn.ai/sol/token/{address}"
    )

def should_notify(analysis: dict[str, Any]) -> bool:
    return analysis.get("score", 0) >= MIN_SIGNAL_SCORE or analysis.get("signal_type") == "distribution"


def handle_token(scan_id: str, token: dict[str, Any], notify: bool) -> bool:
    address = token_address(token)
    raw_holders = fetch_top100_holders(address)
    if not raw_holders:
        print(f"{token_label(token)} no holders")
        return False
    kline_resolution = token_kline_resolution(token)
    candles = fetch_kline(address, kline_resolution)
    summary, holders = build_snapshot_json(token, raw_holders, candles, kline_resolution)
    history = recent_snapshots(address)
    analysis = analyze_snapshot_change(holders, history, summary)
    snapshot_id = save_snapshot(scan_id, token, summary, holders, analysis)
    print(
        f"{token_label(token)} snapshot={snapshot_id} history={len(history)} "
        f"type={analysis.get('signal_type')} score={analysis.get('score')} "
        f"acc={analysis.get('accumulation_pct_delta', 0):.2%} "
        f"dist={analysis.get('distribution_pct_delta', 0):.2%} "
        f"win_acc={analysis.get('window_accumulation_pct_delta', 0):.2%} "
        f"pool=${analysis.get('pool_total_liquidity', 0):,.0f} "
        f"pool/mcap={analysis.get('pool_mcap_ratio', 0):.1%}"
    )
    if notify and should_notify(analysis):
        send_tg(signal_text(token, analysis))
    return True


def scan_once(args: argparse.Namespace) -> None:
    scan_id = str(uuid.uuid4())
    trending_tokens = fetch_trending_tokens()
    watchlist_tokens = fetch_watchlist_tokens()
    tokens = merge_token_sources(watchlist_tokens, trending_tokens)
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] scan_id={scan_id} "
        f"trending={len(trending_tokens)} watchlist={len(watchlist_tokens)} merged={len(tokens)}"
    )
    processed = 0
    skipped = 0
    for token in tokens[: args.max_tokens]:
        try:
            address = token_address(token)
            is_watchlist = "watchlist" in set(token.get("_sources", []))
            info, security = fetch_token_metadata(address)
            token = merge_token_metadata(token, info, security)
            fill_watchlist_create_at(token)
            if not is_watchlist:
                skip_reason = token_basic_filter_reason(token)
                if skip_reason:
                    skipped += 1
                    print(f"{token_label(token)} skip {skip_reason}")
                    continue
                skip_reason = token_fee_filter_reason(token)
                if skip_reason:
                    skipped += 1
                    print(f"{token_label(token)} skip {skip_reason}")
                    continue
            skip_reason = recent_snapshot_skip_reason(token_address(token), token)
            if skip_reason:
                skipped += 1
                print(f"{token_label(token)} skip {skip_reason}")
                continue
            pool_data = fetch_token_pool(address)
            token = attach_token_pool(token, pool_data)
            if handle_token(scan_id, token, args.notify):
                processed += 1
        except Exception as exc:
            print(f"{token_label(token)} failed: {exc}")
        time.sleep(args.token_delay)
    print(f"scan_id={scan_id} processed={processed}/{len(tokens)} skipped={skipped}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Monitor 1h trending tokens with one JSON snapshot table.")
    parser.add_argument("--once", action="store_true", help="Run once and exit.")
    parser.add_argument("--watch", action="store_true", help="Run forever.")
    parser.add_argument("--notify", action="store_true", help="Send Telegram messages for new signals.")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL_SEC, help="Watch interval seconds.")
    parser.add_argument("--max-tokens", type=int, default=TREND_LIMIT)
    parser.add_argument("--token-delay", type=float, default=0.5, help="Delay between holder calls.")
    parser.add_argument("--min-mcap", type=float, default=MIN_MCAP_USD, help="Skip tokens below this market cap in USD.")
    parser.add_argument("--min-age-hours", type=float, default=MIN_TOKEN_AGE_SEC / 3600, help="Skip tokens younger than this many hours.")
    parser.add_argument("--min-fee-sol", type=float, default=MIN_FEE_SOL, help="Skip tokens below this SOL fee value.")
    return parser


def main() -> None:
    global MIN_MCAP_USD, MIN_TOKEN_AGE_SEC, MIN_FEE_SOL
    args = build_parser().parse_args()
    MIN_MCAP_USD = args.min_mcap
    MIN_TOKEN_AGE_SEC = int(args.min_age_hours * 3600)
    MIN_FEE_SOL = args.min_fee_sol
    while True:
        scan_once(args)
        if args.once or not args.watch:
            break
        print(f"sleep {args.interval}s")
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
