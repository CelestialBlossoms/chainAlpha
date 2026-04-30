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
from tg_alert_stream import publish_tg_alert
from bottom_detection.bottom_watchlist_store import (
    clean_redis_stream_for_ca,
    daily_mcap_watchlist_needs_notify,
    delete_watchlist_token,
    ensure_watchlist_daily_mcap_columns,
    fetch_watchlist_records,
    fill_watchlist_create_at as store_fill_watchlist_create_at,
    fill_watchlist_token_created_at as store_fill_token_created_at,
    mark_daily_mcap_watchlist_notified,
    set_watchlist_blacklisted,
    update_watchlist_seen,
    upsert_daily_mcap_watchlist_token,
)


CHAIN = "sol"
TREND_INTERVAL = os.getenv("BOTTOM_TREND_INTERVAL", "1h")
TREND_LIMIT = int(os.getenv("BOTTOM_TREND_LIMIT", "100"))
DEFAULT_INTERVAL_SEC = int(os.getenv("BOTTOM_SCAN_INTERVAL", "300"))
TOP_HOLDER_LIMIT = int(os.getenv("BOTTOM_TOP_HOLDER_LIMIT", "100"))
RECENT_COMPARE_LIMIT = int(os.getenv("BOTTOM_RECENT_COMPARE_LIMIT", "100"))
NEW_TOKEN_AGE_CUTOFF_SEC = int(os.getenv("BOTTOM_NEW_TOKEN_AGE_CUTOFF_SEC", str(24 * 3600)))
MID_TOKEN_AGE_CUTOFF_SEC = int(os.getenv("BOTTOM_MID_TOKEN_AGE_CUTOFF_SEC", str(5 * 24 * 3600)))
NEW_TOKEN_SNAPSHOT_INTERVAL_SEC = int(os.getenv("BOTTOM_NEW_TOKEN_SNAPSHOT_INTERVAL_SEC", "300"))
OLD_TOKEN_SNAPSHOT_INTERVAL_SEC = int(os.getenv("BOTTOM_OLD_TOKEN_SNAPSHOT_INTERVAL_SEC", "900"))
NEW_TOKEN_KLINE_RESOLUTION = os.getenv("BOTTOM_NEW_TOKEN_KLINE_RESOLUTION", "5m")
YOUNG_TOKEN_KLINE_RESOLUTION = os.getenv("BOTTOM_YOUNG_TOKEN_KLINE_RESOLUTION", "15m")
MID_TOKEN_KLINE_RESOLUTION = os.getenv("BOTTOM_MID_TOKEN_KLINE_RESOLUTION", "1h")
KLINE_LOOKBACK_SEC = int(os.getenv("BOTTOM_KLINE_LOOKBACK_SEC", str(24 * 3600)))
KLINE_INCREMENT_OVERLAP_BARS = int(os.getenv("BOTTOM_KLINE_INCREMENT_OVERLAP_BARS", "10"))
MIN_MCAP_USD = float(os.getenv("BOTTOM_MIN_MCAP_USD", "40000"))
BOTTOM_ABNORMAL_MIN_ATH_MCAP_USD = float(os.getenv("BOTTOM_ABNORMAL_MIN_ATH_MCAP_USD", "1000000"))
BOTTOM_ABNORMAL_MIN_MCAP_USD = float(os.getenv("BOTTOM_ABNORMAL_MIN_MCAP_USD", "40000"))
BOTTOM_ABNORMAL_MAX_MCAP_USD = float(os.getenv("BOTTOM_ABNORMAL_MAX_MCAP_USD", "200000"))
BOTTOM_OLD_ABNORMAL_MIN_MCAP_USD = float(os.getenv("BOTTOM_OLD_ABNORMAL_MIN_MCAP_USD", "400000"))
BOTTOM_NEW_DROP_ATH_MCAP_USD = float(os.getenv("BOTTOM_NEW_DROP_ATH_MCAP_USD", "1000000"))
BOTTOM_NEW_DROP_LEVELS = tuple(
    float(item.strip())
    for item in os.getenv("BOTTOM_NEW_DROP_LEVELS", "500000,400000").split(",")
    if item.strip()
)
BOTTOM_ABNORMAL_HIGH_ATH_MCAP_USD = float(os.getenv("BOTTOM_ABNORMAL_HIGH_ATH_MCAP_USD", "5000000"))
BOTTOM_ABNORMAL_HIGH_MIN_MCAP_USD = float(os.getenv("BOTTOM_ABNORMAL_HIGH_MIN_MCAP_USD", "50000"))
BOTTOM_ABNORMAL_HIGH_MAX_MCAP_USD = float(os.getenv("BOTTOM_ABNORMAL_HIGH_MAX_MCAP_USD", "500000"))
BOTTOM_ABNORMAL_MIN_PRICE_UP_PCT = float(os.getenv("BOTTOM_ABNORMAL_MIN_PRICE_UP_PCT", "30"))
WATCHLIST_DELETE_BELOW_MCAP_USD = float(os.getenv("BOTTOM_WATCHLIST_DELETE_BELOW_MCAP_USD", "40000"))
DAILY_MCAP_MILESTONE_USD = float(os.getenv("BOTTOM_DAILY_MCAP_MILESTONE_USD", "1000000"))
DAILY_MCAP_MIN_FEE_SOL = float(os.getenv("BOTTOM_DAILY_MCAP_MIN_FEE_SOL", "20"))
MIN_TOKEN_AGE_SEC = int(os.getenv("BOTTOM_MIN_TOKEN_AGE_SEC", "0"))
MIN_FEE_SOL = float(os.getenv("BOTTOM_MIN_FEE_SOL", "0"))
BOTTOM_ABNORMAL_MIN_POOL_LIQUIDITY_USD = float(os.getenv("BOTTOM_ABNORMAL_MIN_POOL_LIQUIDITY_USD", "4000"))
BOTTOM_ABNORMAL_MIN_POOL_MCAP_RATIO = float(os.getenv("BOTTOM_ABNORMAL_MIN_POOL_MCAP_RATIO", "0.10"))

BOTTOM_ABNORMAL_RULES = [
    {
        "name": "ATH5M_50K_500K",
        "min_ath_mcap": BOTTOM_ABNORMAL_HIGH_ATH_MCAP_USD,
        "min_mcap": BOTTOM_ABNORMAL_HIGH_MIN_MCAP_USD,
        "max_mcap": BOTTOM_ABNORMAL_HIGH_MAX_MCAP_USD,
    },
    {
        "name": "ATH1M_40K_200K",
        "min_ath_mcap": BOTTOM_ABNORMAL_MIN_ATH_MCAP_USD,
        "min_mcap": BOTTOM_ABNORMAL_MIN_MCAP_USD,
        "max_mcap": BOTTOM_ABNORMAL_MAX_MCAP_USD,
    },
]

SOL_CA_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,50}$")
_KLINE_CACHE_TABLE_READY = False


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


def calc_ath_mcap(row: dict[str, Any], candles: list[dict[str, Any]] | None = None) -> float:
    for source in (row, row.get("_gmgn_info") or {}, row.get("_gmgn_security") or {}):
        if not isinstance(source, dict):
            continue
        for key in (
            "ath_market_cap",
            "ath_mcap",
            "highest_market_cap",
            "highest_mcap",
            "max_market_cap",
            "max_mcap",
            "history_high_market_cap",
            "market_cap_high",
        ):
            value = to_float(source.get(key))
            if value > 0:
                return value
    supply = to_float(row.get("circulating_supply"))
    if supply <= 0:
        supply = to_float((row.get("_gmgn_info") or {}).get("circulating_supply"))
    if supply > 0 and candles:
        high_price = max((to_float(candle.get("high")) for candle in candles), default=0.0)
        if high_price > 0:
            return high_price * supply
    return calc_mcap(row)


def match_abnormal_rule(ath_mcap: float, current_mcap: float) -> dict[str, Any] | None:
    for rule in sorted(BOTTOM_ABNORMAL_RULES, key=lambda item: item["min_ath_mcap"], reverse=True):
        if ath_mcap >= rule["min_ath_mcap"] and rule["min_mcap"] <= current_mcap <= rule["max_mcap"]:
            return rule
    return None


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
            "_gmgn_created_ts",
            "token_created_at",
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
    age = token_age_sec(row)
    if age <= 0 or age <= NEW_TOKEN_AGE_CUTOFF_SEC:
        return NEW_TOKEN_KLINE_RESOLUTION
    if age <= MID_TOKEN_AGE_CUTOFF_SEC:
        return YOUNG_TOKEN_KLINE_RESOLUTION
    return MID_TOKEN_KLINE_RESOLUTION


def kline_resolution_seconds(resolution: str) -> int:
    mapping = {
        "1m": 60,
        "5m": 5 * 60,
        "15m": 15 * 60,
        "30m": 30 * 60,
        "1h": 60 * 60,
        "4h": 4 * 60 * 60,
        "1d": 24 * 60 * 60,
    }
    return mapping.get(str(resolution), 60)


def fee_sol(row: dict[str, Any]) -> float | None:
    value = first_value(
        row,
        (
            "fee_sol",
            "total_fee_sol",
            "fees_sol",
            "swap_fee_sol",
            "trade_fee_sol",
            "gas_fee_sol",
            "gas_fee",
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


def quick_trending_mcap(row: dict[str, Any]) -> float:
    """Estimate MCap from trending API fields only (no extra API call)."""
    mcap = to_float(row.get("market_cap") or row.get("usd_market_cap"))
    if mcap > 0:
        return mcap
    price = to_float(row.get("price"))
    supply = to_float(row.get("total_supply") or row.get("circulating_supply"))
    if price > 0 and supply > 0:
        return price * supply
    return 0


def quick_trending_ath_mcap(row: dict[str, Any]) -> float:
    """Estimate ATH MCap from trending API fields only."""
    ath = to_float(row.get("history_highest_market_cap") or row.get("ath_market_cap"))
    if ath > 0:
        return ath
    return quick_trending_mcap(row)


def prefilter_trending_token(row: dict[str, Any]) -> str | None:
    """Quick filter using only trending API data. Returns skip reason or None."""
    mcap = quick_trending_mcap(row)
    ath = quick_trending_ath_mcap(row)
    peak = max(mcap, ath)

    # Must have at least $40K current MCap, OR be a potential daily 1M candidate
    if mcap < MIN_MCAP_USD and peak < DAILY_MCAP_MILESTONE_USD:
        return f"市值${mcap:,.0f}<${MIN_MCAP_USD:,.0f}且ATH${peak:,.0f}<${DAILY_MCAP_MILESTONE_USD:,.0f}"

    # Daily 1M candidate: ATH >= $1M with decent MCap — always keep
    if peak >= DAILY_MCAP_MILESTONE_USD and mcap >= DAILY_MCAP_MILESTONE_USD * 0.3:
        return None

    # Must have at least $40K
    if mcap < MIN_MCAP_USD:
        return f"市值${mcap:,.0f}<${MIN_MCAP_USD:,.0f}"

    return None


def fetch_watchlist_tokens() -> list[dict[str, Any]]:
    try:
        rows = fetch_watchlist_records()
    except Exception as exc:
        print(f"watchlist query failed: {exc}")
        return []
    tokens = []
    for row in rows:
        ca = row.get("ca")
        create_at = row.get("create_at")
        added_at = row.get("added_at")
        address = str(ca).strip()
        if not valid_sol_ca(address):
            continue
        token = {
            "address": address,
            "source": "watchlist",
            "watchlist_source": row.get("source"),
            "watchlist_peak_mcap": to_float(row.get("peak_mcap")),
            "watchlist_last_mcap": to_float(row.get("last_mcap")),
        }
        if create_at:
            created_ts = int(create_at.timestamp()) if isinstance(create_at, datetime) else parse_timestamp(create_at)
            token["watchlist_create_at"] = created_ts
            token["created_at"] = created_ts
        if added_at:
            token["watchlist_added_at"] = int(added_at.timestamp()) if isinstance(added_at, datetime) else parse_timestamp(added_at)
        if row.get("daily_mcap_date"):
            token["watchlist_daily_mcap_date"] = str(row.get("daily_mcap_date"))
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


def ensure_kline_cache_table() -> None:
    global _KLINE_CACHE_TABLE_READY
    if _KLINE_CACHE_TABLE_READY:
        return

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
    _KLINE_CACHE_TABLE_READY = True


def latest_cached_kline_ts(address: str, resolution: str) -> int:
    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            SELECT MAX(ts)
            FROM bottom_kline_cache
            WHERE chain=%s AND address=%s AND resolution=%s
            """,
            (CHAIN, address, resolution),
        )
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else 0

    return int(db_op(_op) or 0)


def save_kline_cache(address: str, resolution: str, candles: list[dict[str, Any]]) -> int:
    if not candles:
        return 0

    def _op(conn):
        cur = conn.cursor()
        rows = [
            (
                CHAIN,
                address,
                resolution,
                int(candle["ts"]),
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
            ON CONFLICT (chain, address, resolution, ts) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                amount = EXCLUDED.amount,
                updated_at = NOW()
            """,
            rows,
        )
        return len(rows)

    return int(db_op(_op) or 0)


def load_kline_cache(address: str, resolution: str) -> list[dict[str, Any]]:
    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            SELECT ts, open, high, low, close, volume, amount
            FROM bottom_kline_cache
            WHERE chain=%s AND address=%s AND resolution=%s
            ORDER BY ts ASC
            """,
            (CHAIN, address, resolution),
        )
        return [
            {
                "ts": int(row[0]),
                "open": to_float(row[1]),
                "high": to_float(row[2]),
                "low": to_float(row[3]),
                "close": to_float(row[4]),
                "volume": to_float(row[5]),
                "amount": to_float(row[6]),
            }
            for row in cur.fetchall()
        ]

    return db_op(_op) or []


def initial_kline_start_ts(token: dict[str, Any], end_ts: int) -> int:
    created_ts = token_created_ts(token)
    if created_ts > 0:
        return min(created_ts, end_ts - kline_resolution_seconds(token_kline_resolution(token)))
    return end_ts - KLINE_LOOKBACK_SEC


def fetch_kline_range(address: str, resolution: str, start_ts: int, end_ts: int) -> list[dict[str, Any]]:
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


def fetch_kline(address: str, resolution: str, token: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    ensure_kline_cache_table()
    end_ts = now_ts()
    latest_ts = latest_cached_kline_ts(address, resolution)
    step = kline_resolution_seconds(resolution)
    if latest_ts > 0:
        start_ts = max(0, latest_ts - KLINE_INCREMENT_OVERLAP_BARS * step)
    else:
        start_ts = initial_kline_start_ts(token or {"address": address}, end_ts)
    fresh = fetch_kline_range(address, resolution, start_ts, end_ts)
    saved = save_kline_cache(address, resolution, fresh)
    cached = load_kline_cache(address, resolution)
    print(
        f"{address[:8]} kline {resolution}: fetch_from={datetime.fromtimestamp(start_ts).strftime('%Y-%m-%d %H:%M:%S')} "
        f"fresh={len(fresh)} saved={saved} cached={len(cached)}"
    )
    return cached


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
    try:
        store_fill_watchlist_create_at(address, created_ts)
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
        "ath_mcap": calc_ath_mcap(token, candles or []),
        "price": to_float(token.get("price")),
        "liquidity": liquidity,
        "pool": pool_summary,
        "created_ts": token_created_ts(token),
        "age_sec": token_age_sec(token),
        "fee_sol": fee_sol(token),
        "kline": summarize_kline(candles or [], kline_resolution or token_kline_resolution(token)),
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
        elif delta < 0:
            distributed_delta += abs(delta)
        if not old:
            new_holder_pct += cur["hold_pct"]

    for wallet, old in previous.items():
        if wallet not in current:
            exited_holder_pct += old["hold_pct"]

    return {
        "accumulation_pct_delta": accumulated_delta,
        "distribution_pct_delta": distributed_delta,
        "new_holder_pct": new_holder_pct,
        "exited_holder_pct": exited_holder_pct,
        "netflow_usd": netflow_delta,
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



def analyze_abnormal_snapshot(
    current_holders: list[dict[str, Any]],
    recent_history: list[dict[str, Any]],
    current_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    current_summary = current_summary or {}
    pool_stats = pool_change(current_summary, (recent_history[0].get("summary") if recent_history else None) or {})
    window_pool_stats = pool_change(current_summary, (recent_history[-1].get("summary") if recent_history else None) or {})
    kline_summary = current_summary.get("kline") or {}
    price_change_pct = to_float(kline_summary.get("change_pct"))
    current_mcap = to_float(current_summary.get("mcap"))
    ath_mcap = to_float(current_summary.get("ath_mcap"))
    token_age = to_int(current_summary.get("age_sec"))
    is_under_24h = token_age <= 0 or token_age <= NEW_TOKEN_AGE_CUTOFF_SEC
    price_ready = price_change_pct >= BOTTOM_ABNORMAL_MIN_PRICE_UP_PCT
    pool_ratio = to_float(pool_stats.get("pool_mcap_ratio"))
    pool_liquidity = to_float(pool_stats.get("pool_total_liquidity"))
    pool_liquidity_ready = pool_liquidity >= BOTTOM_ABNORMAL_MIN_POOL_LIQUIDITY_USD
    pool_ratio_ready = pool_ratio >= BOTTOM_ABNORMAL_MIN_POOL_MCAP_RATIO
    pool_ready = pool_liquidity_ready and pool_ratio_ready
    drop_level = 0.0
    if is_under_24h and ath_mcap >= BOTTOM_NEW_DROP_ATH_MCAP_USD:
        for level in sorted(BOTTOM_NEW_DROP_LEVELS):
            if current_mcap <= level:
                drop_level = level
                break
    old_abnormal_ready = (
        not is_under_24h
        and current_mcap >= BOTTOM_OLD_ABNORMAL_MIN_MCAP_USD
        and price_ready
        and pool_ready
    )
    if drop_level > 0 and pool_ready:
        signal_type = f"drop_{int(drop_level / 10000)}w"
    elif old_abnormal_ready:
        signal_type = "abnormal"
    else:
        signal_type = "watch"
    previous_holders = recent_history[0].get("holders") if recent_history else []
    holder_change = (
        compare_holder_sets(current_holders, previous_holders)
        if current_holders and previous_holders
        else {
            "accumulation_pct_delta": 0.0,
            "distribution_pct_delta": 0.0,
            "new_holder_pct": 0.0,
            "exited_holder_pct": 0.0,
            "netflow_usd": 0.0,
        }
    )
    if drop_level > 0:
        rule_name = f"NEW_ATH1M_DROP_{int(drop_level / 10000)}W"
        min_ath_mcap = BOTTOM_NEW_DROP_ATH_MCAP_USD
        min_mcap = 0
        max_mcap = drop_level
        rule_reason = (
            f"新币回落{rule_name}: 创建{token_age / 3600:.1f}h, "
            f"ATH${ath_mcap:,.0f}>={min_ath_mcap:,.0f}, 当前市值${current_mcap:,.0f}<=${drop_level:,.0f}"
        )
    elif old_abnormal_ready:
        rule_name = "OLD_MCAP_40W_UP30"
        min_ath_mcap = 0
        min_mcap = BOTTOM_OLD_ABNORMAL_MIN_MCAP_USD
        max_mcap = 0
        rule_reason = (
            f"老币异动: 创建{token_age / 3600:.1f}h, "
            f"当前市值${current_mcap:,.0f}>=${min_mcap:,.0f}, 价格上涨{price_change_pct:.1f}%"
        )
    else:
        rule_name = "未命中"
        min_ath_mcap = BOTTOM_NEW_DROP_ATH_MCAP_USD if is_under_24h else 0
        min_mcap = 0 if is_under_24h else BOTTOM_OLD_ABNORMAL_MIN_MCAP_USD
        max_mcap = 0
        if is_under_24h:
            rule_reason = (
                f"未命中新币回落: 创建{token_age / 3600:.1f}h, "
                f"ATH${ath_mcap:,.0f}, 当前市值${current_mcap:,.0f}"
            )
        else:
            rule_reason = (
                f"未命中老币异动: 创建{token_age / 3600:.1f}h, "
                f"当前市值${current_mcap:,.0f}<${BOTTOM_OLD_ABNORMAL_MIN_MCAP_USD:,.0f}或涨幅/池子不足"
            )
    reasons = [rule_reason]
    if not signal_type.startswith("drop_"):
        reasons.append(
            (
                f"价格上涨{price_change_pct:.1f}%>={BOTTOM_ABNORMAL_MIN_PRICE_UP_PCT:.1f}%"
                if price_ready
                else f"价格上涨{price_change_pct:.1f}%<{BOTTOM_ABNORMAL_MIN_PRICE_UP_PCT:.1f}%"
            )
        )
    reasons.extend(
        [
            (
                f"池子${pool_liquidity:,.0f}>=${BOTTOM_ABNORMAL_MIN_POOL_LIQUIDITY_USD:,.0f}"
                if pool_liquidity_ready
                else f"池子${pool_liquidity:,.0f}<${BOTTOM_ABNORMAL_MIN_POOL_LIQUIDITY_USD:,.0f}"
            ),
            (
                f"池/市值{pool_ratio:.1%}>={BOTTOM_ABNORMAL_MIN_POOL_MCAP_RATIO:.1%}"
                if pool_ratio_ready
                else f"池/市值{pool_ratio:.1%}<{BOTTOM_ABNORMAL_MIN_POOL_MCAP_RATIO:.1%}"
            ),
        ]
    )
    return {
        "score": 100 if signal_type != "watch" else 0,
        "signal_type": signal_type,
        "reasons": reasons,
        "history_count": len(recent_history),
        "is_under_24h": is_under_24h,
        "drop_level_mcap": drop_level,
        "price_confirmation_ready": price_ready,
        "pool_confirmation_ready": pool_ready,
        "pool_liquidity_confirmation_ready": pool_liquidity_ready,
        "pool_ratio_confirmation_ready": pool_ratio_ready,
        "price_change_pct": price_change_pct,
        "required_price_change_pct": 0 if signal_type.startswith("drop_") else BOTTOM_ABNORMAL_MIN_PRICE_UP_PCT,
        "required_pool_liquidity": BOTTOM_ABNORMAL_MIN_POOL_LIQUIDITY_USD,
        "required_pool_mcap_ratio": BOTTOM_ABNORMAL_MIN_POOL_MCAP_RATIO,
        "current_mcap": current_mcap,
        "ath_mcap": ath_mcap,
        "abnormal_rule": rule_name,
        "min_ath_mcap": min_ath_mcap,
        "min_abnormal_mcap": min_mcap,
        "max_abnormal_mcap": max_mcap,
        "token_age_sec": token_age,
        **pool_stats,
        "window_pool_liquidity_delta": window_pool_stats["pool_liquidity_delta"],
        "window_pool_liquidity_delta_pct": window_pool_stats["pool_liquidity_delta_pct"],
        "window_pool_mcap_ratio_delta": window_pool_stats["pool_mcap_ratio_delta"],
        "accumulation_pct_delta": holder_change["accumulation_pct_delta"],
        "distribution_pct_delta": holder_change["distribution_pct_delta"],
        "new_holder_pct": holder_change["new_holder_pct"],
        "exited_holder_pct": holder_change["exited_holder_pct"],
        "netflow_usd": holder_change["netflow_usd"],
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


def send_tg(text: str, extra: dict[str, Any] | None = None) -> None:
    extra = extra or {}
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        publish_tg_alert(text, "bottom_abnormal", status="dry_run", chat_id=TG_CHAT_ID, extra=extra)
        return
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": text, "disable_web_page_preview": True},
            timeout=15,
        )
        if not resp.ok:
            print(f"tg failed: {resp.status_code} {resp.text[:200]}")
            publish_tg_alert(text, "bottom_abnormal", status=f"failed_http_{resp.status_code}", chat_id=TG_CHAT_ID, extra=extra)
            return
        payload = resp.json()
        message_id = payload.get("result", {}).get("message_id") if isinstance(payload, dict) else None
        publish_tg_alert(text, "bottom_abnormal", status="sent", chat_id=TG_CHAT_ID, message_id=message_id, extra=extra)
    except Exception as exc:
        print(f"tg exception: {exc}")
        publish_tg_alert(text, "bottom_abnormal", status="exception", chat_id=TG_CHAT_ID, extra={"error": str(exc)})


def daily_mcap_signal_text(token: dict[str, Any], current_mcap: float, current_fee_sol: float) -> str:
    address = token_address(token)
    created_ts = token_created_ts(token)
    age_text = f"{token_age_sec(token) / 3600:.1f}h" if created_ts > 0 else "未知"
    pool_summary = summarize_pools(token)
    pool_liquidity = to_float(pool_summary.get("total_liquidity"))
    pool_ratio = to_float(pool_summary.get("liquidity_mcap_ratio"))
    return (
        f"每日过1M市值 | ${token.get('symbol') or 'UNKNOWN'}\n"
        f"CA: {address}\n"
        f"市值: ${current_mcap:,.0f} | 要求: >=${DAILY_MCAP_MILESTONE_USD:,.0f}\n"
        f"手续费: {current_fee_sol:.2f} SOL | 要求: >={DAILY_MCAP_MIN_FEE_SOL:.2f} SOL\n"
        f"池子: ${pool_liquidity:,.0f} | 池/市值: {pool_ratio:.1%} | 要求: >=${BOTTOM_ABNORMAL_MIN_POOL_LIQUIDITY_USD:,.0f} / >={BOTTOM_ABNORMAL_MIN_POOL_MCAP_RATIO:.1%}\n"
        f"创建年龄: {age_text}\n"
        f"https://gmgn.ai/sol/token/{address}"
    )


def daily_1m_zone(mcap: float):
    if mcap >= 1_000_000: return "green", ">=$1M"
    if mcap >= 500_000: return "yellow", f"${mcap/1000:.0f}K"
    return "red", f"${mcap/1000:.0f}K"


def publish_daily_1m_frontend_update(token, current_mcap, peak_mcap):
    zone, zone_label = daily_1m_zone(current_mcap)
    drop = round((1 - current_mcap / max(peak_mcap, 1)) * 100, 1)
    milestone_date = token.get("watchlist_daily_mcap_date") or ""
    extra = {
        "source_type": "daily_1m",
        "symbol": token.get("symbol"),
        "address": token_address(token),
        "current_mcap": current_mcap,
        "peak_mcap": peak_mcap,
        "ath_mcap": max(to_float(token.get("_gmgn_ath_mcap") or token.get("ath_mcap")), peak_mcap),
        "milestone_date": milestone_date,
        "zone": zone,
        "zone_label": zone_label,
        "drop_from_peak_pct": drop,
        "liquidity": to_float(token.get("liquidity") or token.get("pool_liquidity")),
        "holders": token.get("holder_count", 0),
    }
    text = f"每日1M | ${token.get('symbol', '?')}\n市值: ${current_mcap:,.0f} | 峰值: ${peak_mcap:,.0f} | {zone_label}"
    publish_tg_alert(text, "daily_1m", status="update", extra=extra)


def maybe_record_daily_mcap_milestone(token: dict[str, Any], current_mcap: float, notify: bool) -> None:
    ath_mcap = calc_ath_mcap(token)
    peak_mcap = max(current_mcap, ath_mcap, to_float(token.get("peak_mcap")), to_float(token.get("watchlist_peak_mcap")))
    if peak_mcap < DAILY_MCAP_MILESTONE_USD:
        return
    age_sec = token_age_sec(token)
    if age_sec <= 0 or age_sec > NEW_TOKEN_AGE_CUTOFF_SEC:
        print(
            f"{token_label(token)} daily 1M skip age "
            f"{age_sec / 3600:.1f}h>{NEW_TOKEN_AGE_CUTOFF_SEC / 3600:.1f}h"
        )
        return
    current_fee_sol = fee_sol(token) or 0.0
    address = token_address(token)
    if current_fee_sol < DAILY_MCAP_MIN_FEE_SOL:
        print(
            f"{token_label(token)} daily 1M skip fee "
            f"{current_fee_sol:.2f} SOL<{DAILY_MCAP_MIN_FEE_SOL:.2f} SOL"
        )
        return
    pool_summary = summarize_pools(token)
    pool_liquidity = to_float(pool_summary.get("total_liquidity"))
    pool_ratio = to_float(pool_summary.get("liquidity_mcap_ratio"))
    if pool_liquidity < BOTTOM_ABNORMAL_MIN_POOL_LIQUIDITY_USD or pool_ratio < BOTTOM_ABNORMAL_MIN_POOL_MCAP_RATIO:
        print(
            f"{token_label(token)} daily 1M skip pool "
            f"liq=${pool_liquidity:,.0f}/${BOTTOM_ABNORMAL_MIN_POOL_LIQUIDITY_USD:,.0f} "
            f"pool/mcap={pool_ratio:.1%}/{BOTTOM_ABNORMAL_MIN_POOL_MCAP_RATIO:.1%}"
        )
        return
    upsert_daily_mcap_watchlist_token(
        address,
        token_created_ts(token),
        peak_mcap,
        current_fee_sol,
        symbol=token.get("symbol"),
        threshold_mcap=DAILY_MCAP_MILESTONE_USD,
    )
    print(f"{token_label(token)} watchlist daily 1M recorded peak=${peak_mcap:,.0f} cur=${current_mcap:,.0f} fee={current_fee_sol:.2f} SOL")
    if notify and daily_mcap_watchlist_needs_notify(address):
        extra = {
            "signal_type": "daily_mcap_over_1m",
            "address": address,
            "symbol": token.get("symbol"),
            "current_mcap": current_mcap,
            "peak_mcap": peak_mcap,
            "ath_mcap": ath_mcap,
            "threshold_mcap": DAILY_MCAP_MILESTONE_USD,
            "fee_sol": current_fee_sol,
            "required_fee_sol": DAILY_MCAP_MIN_FEE_SOL,
            "pool_total_liquidity": pool_liquidity,
            "required_pool_liquidity": BOTTOM_ABNORMAL_MIN_POOL_LIQUIDITY_USD,
            "pool_mcap_ratio": pool_ratio,
            "required_pool_mcap_ratio": BOTTOM_ABNORMAL_MIN_POOL_MCAP_RATIO,
            "created_ts": token_created_ts(token),
            "age_sec": age_sec,
        }
        send_tg(daily_mcap_signal_text(token, current_mcap, current_fee_sol), extra=extra)
        mark_daily_mcap_watchlist_notified(address)




def signal_type_text(signal_type: str) -> str:
    mapping = {
        "watch": "观察",
        "abnormal": "异动检测",
        "drop_50w": "新币跌破50W",
        "drop_40w": "新币跌破40W",
    }
    return mapping.get(signal_type, signal_type or "未知")


def abnormal_signal_text(token: dict[str, Any], analysis: dict[str, Any]) -> str:
    address = token_address(token)
    max_mcap = to_float(analysis.get("max_abnormal_mcap"))
    is_drop_signal = analysis.get("signal_type", "").startswith("drop_")
    if is_drop_signal:
        mcap_line = (
            f"当前市值: ${analysis.get('current_mcap', calc_mcap(token)):,.0f} | "
            f"跌破: ${analysis.get('drop_level_mcap', 0):,.0f}\n"
        )
    elif max_mcap > 0:
        mcap_line = (
            f"当前市值: ${analysis.get('current_mcap', calc_mcap(token)):,.0f} | "
            f"区间: ${analysis.get('min_abnormal_mcap', 0):,.0f}-${max_mcap:,.0f}\n"
        )
    else:
        mcap_line = (
            f"当前市值: ${analysis.get('current_mcap', calc_mcap(token)):,.0f} | "
            f"要求: >=${analysis.get('min_abnormal_mcap', 0):,.0f}\n"
        )
    price_line = (
        ""
        if is_drop_signal
        else f"价格上涨: {analysis.get('price_change_pct', 0):.1f}% | 要求: >={analysis.get('required_price_change_pct', 0):.1f}%\n"
    )
    return (
        f"底部异动检测 | ${token.get('symbol') or 'UNKNOWN'}\n"
        f"类型: {signal_type_text(analysis.get('signal_type'))}\n"
        f"档位: {analysis.get('abnormal_rule') or '未命中'}\n"
        f"CA: {address}\n"
        f"历史最高市值: ${analysis.get('ath_mcap', 0):,.0f} | 要求: >${analysis.get('min_ath_mcap', 0):,.0f}\n"
        f"{mcap_line}"
        f"{price_line}"
        f"池子: ${analysis.get('pool_total_liquidity', 0):,.0f} | 要求: >=${analysis.get('required_pool_liquidity', 0):,.0f} | 池/市值: {analysis.get('pool_mcap_ratio', 0):.1%} ({analysis.get('pool_mcap_ratio_text', 'N/A')}) | 要求: >={analysis.get('required_pool_mcap_ratio', 0):.1%}\n"
        f"Top100变化: 增持{analysis.get('accumulation_pct_delta', 0):.2%} | 减持{analysis.get('distribution_pct_delta', 0):.2%} | 净买入${analysis.get('netflow_usd', 0):,.0f}\n"
        f"理由: {', '.join(analysis.get('reasons') or []) or '无'}\n"
        f"https://gmgn.ai/sol/token/{address}"
    )

def should_notify(analysis: dict[str, Any]) -> bool:
    return analysis.get("signal_type") != "watch"


def previous_signal_exists(address: str, signal_type: str) -> bool:
    if not signal_type or signal_type == "watch":
        return False

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1
            FROM bottom_top100_snapshots
            WHERE chain=%s AND address=%s AND signal_type=%s
            LIMIT 1
            """,
            (CHAIN, address, signal_type),
        )
        return cur.fetchone() is not None

    return bool(db_op(_op))


def notify_skip_reason(analysis: dict[str, Any]) -> str:
    return "; ".join(analysis.get("reasons") or ["未满足异动检测条件"])


def handle_token(scan_id: str, token: dict[str, Any], notify: bool) -> bool:
    address = token_address(token)
    raw_holders = fetch_top100_holders(address)
    if not raw_holders:
        print(f"{token_label(token)} no holders")
        return False
    kline_resolution = token_kline_resolution(token)
    candles = fetch_kline(address, kline_resolution, token)
    summary, holders = build_snapshot_json(token, raw_holders, candles, kline_resolution)
    history = recent_snapshots(address)
    analysis = analyze_abnormal_snapshot(holders, history, summary)
    already_notified = previous_signal_exists(address, analysis.get("signal_type", ""))
    snapshot_id = save_snapshot(scan_id, token, summary, holders, analysis)
    print(
        f"{token_label(token)} snapshot={snapshot_id} history={len(history)} "
        f"type={analysis.get('signal_type')} score={analysis.get('score')} "
        f"ath=${analysis.get('ath_mcap', 0):,.0f} "
        f"mcap=${analysis.get('current_mcap', 0):,.0f} "
        f"price={analysis.get('price_change_pct', 0):.1f}%/{analysis.get('required_price_change_pct', 0):.1f}% "
        f"pool=${analysis.get('pool_total_liquidity', 0):,.0f} "
        f"pool/mcap={analysis.get('pool_mcap_ratio', 0):.1%}"
    )
    if notify and should_notify(analysis) and not already_notified:
        pool_summary = summary.get("pool") or {}
        web_extra = {
            "signal_type": analysis.get("signal_type"),
            "abnormal_rule": analysis.get("abnormal_rule"),
            "ath_mcap": analysis.get("ath_mcap", 0),
            "min_ath_mcap": analysis.get("min_ath_mcap", 0),
            "current_mcap": analysis.get("current_mcap", calc_mcap(token)),
            "min_abnormal_mcap": analysis.get("min_abnormal_mcap", 0),
            "max_abnormal_mcap": analysis.get("max_abnormal_mcap", 0),
            "price_change_pct": analysis.get("price_change_pct", 0),
            "required_price_change_pct": analysis.get("required_price_change_pct", 0),
            "pool_total_liquidity": analysis.get("pool_total_liquidity", 0),
            "required_pool_liquidity": analysis.get("required_pool_liquidity", 0),
            "pool_main_exchange": pool_summary.get("main_exchange", ""),
            "pool_mcap_ratio": analysis.get("pool_mcap_ratio", 0),
            "pool_mcap_ratio_text": analysis.get("pool_mcap_ratio_text", "N/A"),
            "accumulation_pct_delta": analysis.get("accumulation_pct_delta", 0),
            "distribution_pct_delta": analysis.get("distribution_pct_delta", 0),
            "netflow_usd": analysis.get("netflow_usd", 0),
            "score": analysis.get("score", 0),
            "history_count": analysis.get("history_count", 0),
            "reasons": analysis.get("reasons", []),
            "symbol": token.get("symbol"),
            "address": token_address(token),
        }
        send_tg(abnormal_signal_text(token, analysis), extra=web_extra)
    elif notify and should_notify(analysis) and already_notified:
        print(f"{token_label(token)} signal {analysis.get('signal_type')} already notified")
    return True


def scan_once(args: argparse.Namespace) -> None:
    scan_id = str(uuid.uuid4())
    trending_raw = fetch_trending_tokens()
    watchlist_tokens = fetch_watchlist_tokens()

    # Phase 1: Quick filter trending using ONLY trending API data (no extra calls)
    filtered_trending = []
    prefilter_skipped = 0
    for row in trending_raw:
        reason = prefilter_trending_token(row)
        if reason:
            prefilter_skipped += 1
        else:
            filtered_trending.append(row)

    # Phase 2: Merge watchlist + pre-filtered trending
    tokens = merge_token_sources(watchlist_tokens, filtered_trending)
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] scan_id={scan_id} "
        f"trending={len(trending_raw)}→{len(filtered_trending)}(skipped {prefilter_skipped}) "
        f"watchlist={len(watchlist_tokens)} merged={len(tokens)}"
    )
    processed = 0
    skipped = 0
    for token in tokens[: args.max_tokens]:
        try:
            address = token_address(token)
            is_watchlist = "watchlist" in set(token.get("_sources", []))
            if token.get("blacklisted"):
                print(f"{token_label(token)} blacklisted, skipped")
                skipped += 1
                continue
            info, security = fetch_token_metadata(address)
            token = merge_token_metadata(token, info, security)
            fill_watchlist_create_at(token)
            gmgn_created_ts = int(to_float(info.get("creation_timestamp") or info.get("open_timestamp") or 0))
            gmgn_ath_mcap = to_float((info.get("dev") or {}).get("ath_token_info", {}).get("ath_mc"))
            if gmgn_created_ts > 0 or gmgn_ath_mcap > 0:
                store_fill_token_created_at(address, gmgn_created_ts, gmgn_ath_mcap)
                token["_gmgn_created_ts"] = gmgn_created_ts
                token["_gmgn_ath_mcap"] = gmgn_ath_mcap
            current_mcap = calc_mcap(token)
            pool_data = fetch_token_pool(address)
            token = attach_token_pool(token, pool_data)
            maybe_record_daily_mcap_milestone(token, current_mcap, args.notify)
            if is_watchlist:
                update_watchlist_seen(address, current_mcap)
                daily_mcap_date = str(token.get("watchlist_daily_mcap_date") or "")
                if daily_mcap_date == datetime.now().date().isoformat() and current_mcap >= DAILY_MCAP_MILESTONE_USD * 0.3:
                    gmgn_ts = int(to_float(token.get("_gmgn_created_ts") or info.get("creation_timestamp") or info.get("open_timestamp") or 0))
                    age_sec = (now_ts() - gmgn_ts) if gmgn_ts > 0 else 0
                    if 0 < age_sec <= NEW_TOKEN_AGE_CUTOFF_SEC:
                        pool_summary = summarize_pools(token)
                        pool_liq = to_float(pool_summary.get("total_liquidity"))
                        pool_ratio = to_float(pool_summary.get("liquidity_mcap_ratio"))
                        if pool_liq >= BOTTOM_ABNORMAL_MIN_POOL_LIQUIDITY_USD and pool_ratio >= BOTTOM_ABNORMAL_MIN_POOL_MCAP_RATIO:
                            peak = max(to_float(token.get("watchlist_peak_mcap")), to_float(token.get("peak_mcap")), current_mcap)
                            publish_daily_1m_frontend_update(token, current_mcap, peak)
                if current_mcap > 0 and current_mcap < WATCHLIST_DELETE_BELOW_MCAP_USD:
                    if token.get("watchlist_daily_mcap_date"):
                        skipped += 1
                        print(
                            f"{address[:8]} watchlist daily mcap record kept: "
                            f"mcap ${current_mcap:,.0f}<${WATCHLIST_DELETE_BELOW_MCAP_USD:,.0f}"
                        )
                        continue
                    deleted = delete_watchlist_token(address)
                    if deleted:
                        print(
                            f"{address[:8]} watchlist deleted: "
                            f"mcap ${current_mcap:,.0f}<${WATCHLIST_DELETE_BELOW_MCAP_USD:,.0f}"
                        )
                    skipped += 1
                    continue
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
    ensure_kline_cache_table()
    ensure_watchlist_daily_mcap_columns()
    while True:
        scan_once(args)
        if args.once or not args.watch:
            break
        print(f"sleep {args.interval}s")
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
