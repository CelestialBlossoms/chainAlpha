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
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
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
from binance_narrative import classify_narrative_category, compact_narrative, get_binance_narrative, resolve_cached_or_db_narrative
from plugin_signal_stream import publish_plugin_signal
from redis_client import get_redis_client, redis_key
from tg_alert_stream import publish_tg_alert
from bottom_detection.bottom_watchlist_store import (
    daily_mcap_watchlist_needs_notify,
    delete_watchlist_token,
    ensure_watchlist_daily_mcap_columns,
    fetch_watchlist_records,
    fill_watchlist_create_at as store_fill_watchlist_create_at,
    fill_watchlist_token_created_at as store_fill_token_created_at,
    is_watchlist_blacklisted,
    mark_daily_mcap_watchlist_notified,
    update_watchlist_seen,
    upsert_daily_mcap_watchlist_token,
)
from bottom_detection.top100_push_record_store import (
    record_top100_push,
    top100_signal_push_record_exists,
    update_top100_push_deepseek,
)


CHAIN = "sol"
_BOTTOM_TOP100_SNAPSHOT_COMMENTS_READY = False
_BOTTOM_TOP100_TRADER_COLUMNS_READY = False
TREND_INTERVALS = tuple(
    item.strip()
    for item in os.getenv("BOTTOM_TREND_INTERVALS", os.getenv("BOTTOM_TREND_INTERVAL", "1m,5m,1h,6h,24h")).split(",")
    if item.strip()
)
TREND_INTERVAL = TREND_INTERVALS[0] if TREND_INTERVALS else "5m"
TREND_INTERVAL_SCHEDULES_RAW = os.getenv("BOTTOM_TREND_INTERVAL_SCHEDULES", "1m:60,5m:120,1h:300,6h:600,24h:900")
TREND_PRIMARY_INTERVAL = os.getenv("BOTTOM_TREND_PRIMARY_INTERVAL", "1m")
TREND_CROSS_WINDOW_DEDUP_SEC = int(os.getenv("BOTTOM_TREND_CROSS_WINDOW_DEDUP_SEC", "180"))
TREND_SCHEDULER_IDLE_SLEEP_SEC = float(os.getenv("BOTTOM_TREND_SCHEDULER_IDLE_SLEEP_SEC", "2"))
TREND_ORDER_BYS = tuple(
    item.strip()
    for item in os.getenv("BOTTOM_TREND_ORDER_BYS", "default,change5m").split(",")
    if item.strip()
)
TREND_FILTERS = tuple(
    item.strip()
    for item in os.getenv("BOTTOM_TREND_FILTERS", "is_out_market,not_wash_trading").split(",")
    if item.strip()
)
TREND_LIMIT = int(os.getenv("BOTTOM_TREND_LIMIT", "100"))
MAX_TOKENS = int(os.getenv("BOTTOM_MAX_TOKENS", "0"))
DEFAULT_INTERVAL_SEC = int(os.getenv("BOTTOM_SCAN_INTERVAL", "300"))
TOP_HOLDER_LIMIT = int(os.getenv("BOTTOM_TOP_HOLDER_LIMIT", "100"))
TOP_TRADER_SNAPSHOT_LIMIT = int(os.getenv("BOTTOM_TOP_TRADER_SNAPSHOT_LIMIT", "100"))
RECENT_COMPARE_LIMIT = int(os.getenv("BOTTOM_RECENT_COMPARE_LIMIT", "100"))
NEW_TOKEN_AGE_CUTOFF_SEC = int(os.getenv("BOTTOM_NEW_TOKEN_AGE_CUTOFF_SEC", str(48 * 3600)))
MID_TOKEN_AGE_CUTOFF_SEC = int(os.getenv("BOTTOM_MID_TOKEN_AGE_CUTOFF_SEC", str(5 * 24 * 3600)))
NEW_TOKEN_SNAPSHOT_INTERVAL_SEC = int(os.getenv("BOTTOM_NEW_TOKEN_SNAPSHOT_INTERVAL_SEC", "300"))
OLD_TOKEN_SNAPSHOT_INTERVAL_SEC = int(os.getenv("BOTTOM_OLD_TOKEN_SNAPSHOT_INTERVAL_SEC", "900"))
# Fast scan: bottom-range watchlist tokens checked every 1 min.
FAST_SCAN_ENABLED = os.getenv("BOTTOM_FAST_SCAN_ENABLED", "1") != "0"
FAST_SCAN_INTERVAL_SEC = int(os.getenv("BOTTOM_FAST_SCAN_INTERVAL_SEC", "60"))
FAST_SCAN_MIN_MCAP = float(os.getenv("BOTTOM_FAST_SCAN_MIN_MCAP", "40000"))
FAST_SCAN_MAX_MCAP = float(os.getenv("BOTTOM_FAST_SCAN_MAX_MCAP", "300000"))
FAST_SCAN_SNAPSHOT_INTERVAL_SEC = int(os.getenv("BOTTOM_FAST_SCAN_SNAPSHOT_INTERVAL_SEC", "60"))
FAST_SCAN_TOKEN_DELAY = float(os.getenv("BOTTOM_FAST_SCAN_TOKEN_DELAY", "0.3"))
FAST_SCAN_MAX_TOKENS = int(os.getenv("BOTTOM_FAST_SCAN_MAX_TOKENS", "0"))
SIGNAL_DEDUP_MAX_AGE_SEC = int(os.getenv("BOTTOM_SIGNAL_DEDUP_MAX_AGE_SEC", str(24 * 3600)))
FIRST_SIGNAL_BASELINE_MAX_AGE_SEC = int(os.getenv("BOTTOM_FIRST_SIGNAL_BASELINE_MAX_AGE_SEC", str(24 * 3600)))
FRONTEND_REPEAT_MIN_KLINE_CHANGE_PCT = float(os.getenv("BOTTOM_FRONTEND_REPEAT_MIN_KLINE_CHANGE_PCT", "0"))
QUIET_BREAKOUT_ENABLED = os.getenv("BOTTOM_QUIET_BREAKOUT_ENABLED", "1") != "0"  # 22% success rate, filtered by risk tags
QUIET_BREAKOUT_MIN_QUIET_BARS = int(os.getenv("BOTTOM_QUIET_BREAKOUT_MIN_QUIET_BARS", "24"))
QUIET_BREAKOUT_RECENT_BARS = int(os.getenv("BOTTOM_QUIET_BREAKOUT_RECENT_BARS", "3"))
QUIET_BREAKOUT_MAX_RANGE_PCT = float(os.getenv("BOTTOM_QUIET_BREAKOUT_MAX_RANGE_PCT", "7"))
QUIET_BREAKOUT_MAX_AVG_VOLUME_USD = float(os.getenv("BOTTOM_QUIET_BREAKOUT_MAX_AVG_VOLUME_USD", "2000"))
QUIET_BREAKOUT_MIN_CHANGE_PCT = float(os.getenv("BOTTOM_QUIET_BREAKOUT_MIN_CHANGE_PCT", "10"))
QUIET_BREAKOUT_LOW_MCAP_MAX_USD = float(os.getenv("BOTTOM_QUIET_BREAKOUT_LOW_MCAP_MAX_USD", "300000"))
QUIET_BREAKOUT_HIGH_MCAP_MIN_USD = float(os.getenv("BOTTOM_QUIET_BREAKOUT_HIGH_MCAP_MIN_USD", "1000000"))
QUIET_BREAKOUT_MIN_VOLUME_RATIO = float(os.getenv("BOTTOM_QUIET_BREAKOUT_MIN_VOLUME_RATIO", "3"))
QUIET_BREAKOUT_MIN_BREAKOUT_VOLUME_USD = float(os.getenv("BOTTOM_QUIET_BREAKOUT_MIN_BREAKOUT_VOLUME_USD", "5000"))
QUIET_RUNUP_ENABLED = os.getenv("BOTTOM_QUIET_RUNUP_ENABLED", "1") != "0"
QUIET_RUNUP_LOOKBACK_BARS = int(os.getenv("BOTTOM_QUIET_RUNUP_LOOKBACK_BARS", "120"))
QUIET_RUNUP_MIN_QUIET_BARS = int(os.getenv("BOTTOM_QUIET_RUNUP_MIN_QUIET_BARS", "6"))
QUIET_RUNUP_MAX_RANGE_PCT = float(os.getenv("BOTTOM_QUIET_RUNUP_MAX_RANGE_PCT", "10"))
QUIET_RUNUP_MIN_GAIN_PCT = float(os.getenv("BOTTOM_QUIET_RUNUP_MIN_GAIN_PCT", "40"))
QUIET_RUNUP_MIN_BREAKOUT_VOLUME_RATIO = float(os.getenv("BOTTOM_QUIET_RUNUP_MIN_BREAKOUT_VOLUME_RATIO", "3"))
NEW_TOKEN_KLINE_RESOLUTION = os.getenv("BOTTOM_NEW_TOKEN_KLINE_RESOLUTION", "5m")
YOUNG_TOKEN_KLINE_RESOLUTION = os.getenv("BOTTOM_YOUNG_TOKEN_KLINE_RESOLUTION", "5m")
MID_TOKEN_KLINE_RESOLUTION = os.getenv("BOTTOM_MID_TOKEN_KLINE_RESOLUTION", "5m")
KLINE_LOOKBACK_SEC = int(os.getenv("BOTTOM_KLINE_LOOKBACK_SEC", str(24 * 3600)))
KLINE_INCREMENT_OVERLAP_BARS = int(os.getenv("BOTTOM_KLINE_INCREMENT_OVERLAP_BARS", "10"))
KLINE_SIGNAL_BARS = int(os.getenv("BOTTOM_KLINE_SIGNAL_BARS", "12"))
KLINE_REVIVAL_MIN_DRAWDOWN_PCT = float(os.getenv("BOTTOM_KLINE_REVIVAL_MIN_DRAWDOWN_PCT", "20"))
MIN_MCAP_USD = float(os.getenv("BOTTOM_MIN_MCAP_USD", "40000"))
BOTTOM_ABNORMAL_MIN_ATH_MCAP_USD = float(os.getenv("BOTTOM_ABNORMAL_MIN_ATH_MCAP_USD", "1000000"))
BOTTOM_ABNORMAL_MIN_MCAP_USD = float(os.getenv("BOTTOM_ABNORMAL_MIN_MCAP_USD", "40000"))
BOTTOM_ABNORMAL_MAX_MCAP_USD = float(os.getenv("BOTTOM_ABNORMAL_MAX_MCAP_USD", "200000"))
BOTTOM_OLD_ABNORMAL_MIN_MCAP_USD = float(os.getenv("BOTTOM_OLD_ABNORMAL_MIN_MCAP_USD", "40000"))
BOTTOM_NEW_DROP_ATH_MCAP_USD = float(os.getenv("BOTTOM_NEW_DROP_ATH_MCAP_USD", "1000000"))
BOTTOM_NEW_DROP_LEVELS = tuple(
    float(item.strip())
    for item in os.getenv("BOTTOM_NEW_DROP_LEVELS", "500000,400000").split(",")
    if item.strip()
)
BOTTOM_NEW_REVIVAL_MAX_LOW_MCAP_USD = float(os.getenv("BOTTOM_NEW_REVIVAL_MAX_LOW_MCAP_USD", "200000"))
BOTTOM_NEW_REVIVAL_MIN_PRICE_UP_PCT = float(os.getenv("BOTTOM_NEW_REVIVAL_MIN_PRICE_UP_PCT", "15"))
BOTTOM_ABNORMAL_HIGH_ATH_MCAP_USD = float(os.getenv("BOTTOM_ABNORMAL_HIGH_ATH_MCAP_USD", "5000000"))
BOTTOM_ABNORMAL_HIGH_MIN_MCAP_USD = float(os.getenv("BOTTOM_ABNORMAL_HIGH_MIN_MCAP_USD", "50000"))
BOTTOM_ABNORMAL_HIGH_MAX_MCAP_USD = float(os.getenv("BOTTOM_ABNORMAL_HIGH_MAX_MCAP_USD", "500000"))
BOTTOM_ABNORMAL_MIN_PRICE_UP_PCT = float(os.getenv("BOTTOM_ABNORMAL_MIN_PRICE_UP_PCT", "15"))
WATCHLIST_DELETE_BELOW_MCAP_USD = float(os.getenv("BOTTOM_WATCHLIST_DELETE_BELOW_MCAP_USD", "40000"))
DAILY_MCAP_MILESTONE_USD = float(os.getenv("BOTTOM_DAILY_MCAP_MILESTONE_USD", "1000000"))
DAILY_MCAP_MIN_FEE_SOL = float(os.getenv("BOTTOM_DAILY_MCAP_MIN_FEE_SOL", "20"))
DAILY_MCAP_MIN_POOL_MCAP_RATIO = float(os.getenv("BOTTOM_DAILY_MCAP_MIN_POOL_MCAP_RATIO", "0.07"))
MIN_TOKEN_AGE_SEC = int(os.getenv("BOTTOM_MIN_TOKEN_AGE_SEC", "0"))
MIN_FEE_SOL = float(os.getenv("BOTTOM_MIN_FEE_SOL", "2"))
BOTTOM_ABNORMAL_MIN_POOL_LIQUIDITY_USD = float(os.getenv("BOTTOM_ABNORMAL_MIN_POOL_LIQUIDITY_USD", "4000"))
BOTTOM_ABNORMAL_MIN_POOL_MCAP_RATIO = float(os.getenv("BOTTOM_ABNORMAL_MIN_POOL_MCAP_RATIO", "0.10"))
WATCHLIST_DELETE_BELOW_POOL_LIQUIDITY_USD = float(os.getenv("BOTTOM_WATCHLIST_DELETE_BELOW_POOL_LIQUIDITY_USD", "10000"))
MIN_POOL_LIQUIDITY_USD = float(os.getenv("BOTTOM_MIN_POOL_LIQUIDITY_USD", str(WATCHLIST_DELETE_BELOW_POOL_LIQUIDITY_USD)))
USE_AGENT_DECISION = os.getenv("BOTTOM_USE_AGENT_DECISION", "1") != "0"
EMA_GOLDEN_CROSS_ENABLED = os.getenv("BOTTOM_EMA_GOLDEN_CROSS_ENABLED", "0") == "1"
POST_PUSH_REDIS_TRACK_ENABLED = os.getenv("BOTTOM_POST_PUSH_REDIS_TRACK_ENABLED", "1") != "0"
POST_PUSH_REDIS_PREFIX = os.getenv("BOTTOM_POST_PUSH_REDIS_PREFIX", "bottom:post_push")
POST_PUSH_TRACK_TTL_SEC = int(os.getenv("BOTTOM_POST_PUSH_TRACK_TTL_SEC", str(4 * 3600)))
POST_PUSH_POLL_INTERVAL_SEC = int(os.getenv("BOTTOM_POST_PUSH_POLL_INTERVAL_SEC", "60"))
POST_PUSH_ENTRY_DD_MIN_PCT = float(os.getenv("BOTTOM_POST_PUSH_ENTRY_DD_MIN_PCT", "30"))
POST_PUSH_ENTRY_DD_MAX_PCT = float(os.getenv("BOTTOM_POST_PUSH_ENTRY_DD_MAX_PCT", "50"))
POST_PUSH_KLINE_INTERVAL = os.getenv("BOTTOM_POST_PUSH_KLINE_INTERVAL", "1min")
POST_PUSH_MIN_PEAK_GAIN_PCT = float(os.getenv("BOTTOM_POST_PUSH_MIN_PEAK_GAIN_PCT", "15"))
POST_PUSH_GAIN_REPLY_PCT = float(os.getenv("BOTTOM_POST_PUSH_GAIN_REPLY_PCT", str(POST_PUSH_MIN_PEAK_GAIN_PCT)))
POST_PUSH_LOSS_REPLY_PCT = float(os.getenv("BOTTOM_POST_PUSH_LOSS_REPLY_PCT", str(POST_PUSH_ENTRY_DD_MIN_PCT)))
POST_PUSH_DRAWDOWN_REPLY_PCT = float(os.getenv("BOTTOM_POST_PUSH_DRAWDOWN_REPLY_PCT", "20"))
POST_PUSH_REPLY_COOLDOWN_SEC = int(os.getenv("BOTTOM_POST_PUSH_REPLY_COOLDOWN_SEC", str(60 * 60)))
POST_PUSH_MAX_REPLIES = int(os.getenv("BOTTOM_POST_PUSH_MAX_REPLIES", "3"))
BINANCE_SOL_CHAIN_ID = os.getenv("BINANCE_SOL_CHAIN_ID", "CT_501")
BINANCE_WEB3_USER_AGENT = os.getenv("BINANCE_WEB3_USER_AGENT", "binance-web3/1.1 (Skill)")
BINANCE_DYNAMIC_URL = "https://web3.binance.com/bapi/defi/v4/public/wallet-direct/buw/wallet/market/token/dynamic/info/ai"
BINANCE_TOKEN_META_URL = "https://web3.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/dex/market/token/meta/info/ai"
BINANCE_KLINE_URL = "https://dquery.sintral.io/u-kline/v1/k-line/candles"
BINANCE_HEADERS = {"Accept-Encoding": "identity", "User-Agent": BINANCE_WEB3_USER_AGENT}
_POST_PUSH_MONITOR_STARTED = False

# ---------------------------------------------------------------------------
# Live tracking for frontend real-time dashboard (24h window for bottom signals)
# ---------------------------------------------------------------------------
BOTTOM_LIVE_TRACK_ENABLED = os.getenv("BOTTOM_LIVE_TRACK_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
BOTTOM_LIVE_TRACK_REDIS_PREFIX = os.getenv("BOTTOM_LIVE_TRACK_REDIS_PREFIX", "bottom:live_track")
BOTTOM_LIVE_TRACK_TTL_SEC = int(os.getenv("BOTTOM_LIVE_TRACK_TTL_SEC", str(24 * 3600)))  # 24h
BOTTOM_LIVE_TRACK_PUBSUB_CHANNEL = os.getenv("BOTTOM_LIVE_TRACK_PUBSUB", "bottom:live_track:updates")
BOTTOM_LIVE_TRACK_REMOVE_DEAD_MCAP_USD = float(os.getenv("BOTTOM_LIVE_TRACK_DEAD_MCAP", "6000"))  # < 6K = dead
BOTTOM_LIVE_TRACK_REMOVE_LOW_MCAP_USD = float(os.getenv("BOTTOM_LIVE_TRACK_LOW_MCAP", "10000"))  # < 10K within 30min
BOTTOM_LIVE_TRACK_LOW_MCAP_WINDOW_SEC = int(os.getenv("BOTTOM_LIVE_TRACK_LOW_WINDOW", "1800"))  # 30min
BOTTOM_DEEPSEEK_ASYNC_ENABLED = os.getenv("BOTTOM_DEEPSEEK_ASYNC_ENABLED", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
BOTTOM_DEEPSEEK_PUSH_LEFT_ENABLED = os.getenv("BOTTOM_DEEPSEEK_PUSH_LEFT_ENABLED", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
BOTTOM_DEEPSEEK_ASYNC_MAX_WORKERS = max(1, int(os.getenv("BOTTOM_DEEPSEEK_ASYNC_MAX_WORKERS", "2")))
_DEEPSEEK_ASYNC_EXECUTOR = ThreadPoolExecutor(max_workers=BOTTOM_DEEPSEEK_ASYNC_MAX_WORKERS)
_DEEPSEEK_ASYNC_INFLIGHT: set[str] = set()
_DEEPSEEK_ASYNC_LOCK = threading.Lock()

# Old token surge detection (老币异动拉升)
OLD_TOKEN_SURGE_ENABLED = os.getenv("BOTTOM_OLD_TOKEN_SURGE_ENABLED", "1") != "0"
OLD_TOKEN_SURGE_MIN_AGE_SEC = int(os.getenv("BOTTOM_OLD_TOKEN_SURGE_MIN_AGE_SEC", "0"))
OLD_TOKEN_SURGE_MIN_MCAP_USD = float(os.getenv("BOTTOM_OLD_TOKEN_SURGE_MIN_MCAP_USD", "40000"))
SURGE_NEW_TOKEN_AGE_SEC = int(os.getenv("BOTTOM_SURGE_NEW_TOKEN_AGE_SEC", str(48 * 3600)))
SURGE_MID_TOKEN_AGE_SEC = int(os.getenv("BOTTOM_SURGE_MID_TOKEN_AGE_SEC", str(7 * 24 * 3600)))
SURGE_NEW_TOKEN_PRICE_UP_PCT = float(os.getenv("BOTTOM_SURGE_NEW_TOKEN_PRICE_UP_PCT", "20"))
SURGE_MID_TOKEN_PRICE_UP_PCT = float(os.getenv("BOTTOM_SURGE_MID_TOKEN_PRICE_UP_PCT", "15"))
SURGE_OLD_TOKEN_PRICE_UP_PCT = float(os.getenv("BOTTOM_SURGE_OLD_TOKEN_PRICE_UP_PCT", "10"))
OLD_TOKEN_SURGE_RESOLUTIONS = tuple(
    item.strip()
    for item in os.getenv("BOTTOM_OLD_TOKEN_SURGE_RESOLUTIONS", "1h,5m,1m").split(",")
    if item.strip()
)
OLD_TOKEN_SURGE_COOLDOWN_SEC = int(os.getenv("BOTTOM_OLD_TOKEN_SURGE_COOLDOWN_SEC", "1800"))

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
    for key in ("market_cap", "usd_market_cap", "mcap", "fdv", "fully_diluted_valuation"):
        value = to_float(row.get(key))
        if value > 0:
            return value
    price = to_float(row.get("price"))
    if isinstance(price, dict):
        price = to_float(price.get("price") or price.get("price_1m"))
    supply = to_float(row.get("circulating_supply") or row.get("total_supply"))
    if price > 0 and supply > 0:
        return price * supply
    return 0.0


def current_token_ath_mcap(row: dict[str, Any]) -> float:
    """Read the current token ATH market cap from GMGN's token-level fields."""
    for source in (row, row.get("_gmgn_info") or {}):
        if not isinstance(source, dict):
            continue
        ath = to_float(source.get("history_highest_market_cap"))
        if ath > 0:
            return ath
        ath_price = to_float(source.get("ath_price"))
        supply = to_float(source.get("circulating_supply") or source.get("total_supply"))
        if ath_price > 0 and supply > 0:
            return ath_price * supply
    return 0.0


def calc_ath_mcap(row: dict[str, Any], candles: list[dict[str, Any]] | None = None) -> float:
    current_mcap = calc_mcap(row)
    ath_mcap = current_token_ath_mcap(row) or to_float(row.get("_gmgn_ath_mcap"))
    if ath_mcap > 0:
        return ath_mcap

    supply = to_float(row.get("circulating_supply"))
    if supply <= 0:
        supply = to_float((row.get("_gmgn_info") or {}).get("circulating_supply"))
    if supply > 0 and candles:
        high_price = max((to_float(candle.get("high")) for candle in candles), default=0.0)
        if high_price > 0:
            candle_ath = high_price * supply
            # Same sanity check for candle-derived ATH
            if current_mcap <= 0 or candle_ath <= current_mcap * 500:
                return candle_ath
    return current_mcap


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
            "token_created_at",
            "gmgn_created_at",
            "_gmgn_created_ts",
            "watchlist_create_at",
            "created_at",
            "creation_timestamp",
            "created_timestamp",
            "create_timestamp",
        ),
    )
    return parse_timestamp(value)


def token_launch_ts(row: dict[str, Any]) -> int:
    value = first_value(
        row,
        (
            "token_launch_at",
            "gmgn_open_at",
            "_gmgn_open_ts",
            "open_timestamp",
            "pool_creation_timestamp",
            "launch_timestamp",
            "pair_created_at",
        ),
    )
    return parse_timestamp(value)


def token_active_ts(row: dict[str, Any]) -> int:
    return token_launch_ts(row) or token_created_ts(row)


def token_age_sec(row: dict[str, Any]) -> int:
    active_ts = token_active_ts(row)
    return now_ts() - active_ts if active_ts > 0 else 0


def token_creation_age_sec(row: dict[str, Any]) -> int:
    created_ts = token_created_ts(row)
    return now_ts() - created_ts if created_ts > 0 else 0


def token_launch_age_sec(row: dict[str, Any]) -> int:
    launch_ts = token_launch_ts(row)
    return now_ts() - launch_ts if launch_ts > 0 else 0


def is_new_token(row: dict[str, Any]) -> bool:
    age = token_age_sec(row)
    return age > 0 and age <= NEW_TOKEN_AGE_CUTOFF_SEC


def is_watchlist_token(row: dict[str, Any]) -> bool:
    sources = set(row.get("_sources") or [])
    source = str(row.get("source") or "")
    return source == "watchlist" or "watchlist" in sources or bool(row.get("watchlist_source"))


def is_trending_token(row: dict[str, Any]) -> bool:
    sources = set(str(item) for item in (row.get("_sources") or []))
    source = str(row.get("source") or "")
    return source.startswith("trending") or any(item.startswith("trending") for item in sources)


def is_fast_scan_watchlist_token(row: dict[str, Any]) -> bool:
    if not is_watchlist_token(row):
        return False
    mcap = calc_mcap(row) or to_float(row.get("watchlist_last_mcap"))
    return FAST_SCAN_MIN_MCAP <= mcap <= FAST_SCAN_MAX_MCAP


def token_snapshot_interval_sec(row: dict[str, Any]) -> int:
    if is_fast_scan_watchlist_token(row):
        return FAST_SCAN_SNAPSHOT_INTERVAL_SEC
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
        return f"发射{age / 3600:.1f}h<{MIN_TOKEN_AGE_SEC / 3600:.1f}h"
    return None


def token_fee_filter_reason(row: dict[str, Any]) -> str | None:
    fee = fee_sol(row)
    if fee is not None and fee < MIN_FEE_SOL:
        return f"手续费{fee:.2f}SOL<{MIN_FEE_SOL:.2f}SOL"
    return None


def token_pool_filter_reason(pool_liquidity: float, pool_reliable: bool, reason: str = "") -> str | None:
    if not pool_reliable:
        return f"池子数据不可用:{reason or 'unknown'}"
    if pool_liquidity < MIN_POOL_LIQUIDITY_USD:
        return f"池子${pool_liquidity:,.0f}<${MIN_POOL_LIQUIDITY_USD:,.0f}"
    return None


def fetch_trending_tokens_for_interval(interval: str, order_by: str = "default") -> list[dict[str, Any]]:
    args = ["market", "trending", "--chain", CHAIN, "--interval", interval, "--limit", str(TREND_LIMIT)]
    for filter_tag in TREND_FILTERS:
        args.extend(["--filter", filter_tag])
    if order_by and order_by != "default":
        args.extend(["--order-by", order_by, "--direction", "desc"])
    data = run_gmgn(args)
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
        item = dict(row)
        item["_trend_interval"] = interval
        item["_trend_order_by"] = order_by or "default"
        item["_sources"] = [f"trending_{interval}_{order_by or 'default'}"]
        tokens.append(item)
    return tokens


def fetch_trending_tokens(
    intervals: tuple[str, ...] | list[str] | None = None,
    order_bys: tuple[str, ...] | list[str] | None = None,
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen = set()
    active_intervals = tuple(intervals or TREND_INTERVALS)
    active_order_bys = tuple(order_bys or TREND_ORDER_BYS)
    for interval in active_intervals:
        for order_by in active_order_bys:
            rows = fetch_trending_tokens_for_interval(interval, order_by)
            print(f"trending {interval}/{order_by}: {len(rows)}")
            for row in rows:
                address = token_address(row)
                if not address:
                    continue
                source = f"trending_{interval}_{order_by or 'default'}"
                if address in seen:
                    existing = next((item for item in merged if token_address(item) == address), None)
                    if existing is not None:
                        sources = set(existing.get("_sources") or [])
                        sources.add(source)
                        existing["_sources"] = sorted(sources)
                        intervals = set(str(existing.get("_trend_interval") or "").split(","))
                        intervals.add(interval)
                        existing["_trend_interval"] = ",".join(sorted(item for item in intervals if item))
                        order_bys = set(str(existing.get("_trend_order_by") or "").split(","))
                        order_bys.add(order_by or "default")
                        existing["_trend_order_by"] = ",".join(sorted(item for item in order_bys if item))
                    continue
                seen.add(address)
                merged.append(row)
    return merged


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
    ath = current_token_ath_mcap(row)
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
            "watchlist_highest_mcap": to_float(row.get("highest_mcap")),
            "watchlist_current_mcap": to_float(row.get("current_mcap")),
            "watchlist_ath_mcap": to_float(row.get("ath_mcap")),
            "watchlist_last_pool_liquidity": to_float(row.get("last_pool_liquidity")),
            "watchlist_last_pool_mcap_ratio": to_float(row.get("last_pool_mcap_ratio")),
            "watchlist_narrative_desc": row.get("narrative_desc") or "",
            "watchlist_narrative_type": row.get("narrative_type") or "",
            "watchlist_narrative_category": row.get("narrative_category") or "",
            "narrative_desc": row.get("narrative_desc") or "",
            "narrative_type": row.get("narrative_type") or "",
            "narrative_category": row.get("narrative_category") or "",
            "symbol": row.get("symbol") or "",
            "blacklisted": bool(row.get("blacklisted")),
        }
        current_mcap = to_float(row.get("current_mcap")) or to_float(row.get("last_mcap"))
        if current_mcap > 0:
            token["market_cap"] = current_mcap
            token["usd_market_cap"] = current_mcap
            token["mcap"] = current_mcap
        ath_mcap = to_float(row.get("ath_mcap")) or to_float(row.get("highest_mcap")) or to_float(row.get("peak_mcap"))
        if ath_mcap > 0:
            token["history_highest_market_cap"] = ath_mcap
        if row.get("token_created_at"):
            token["token_created_at"] = row.get("token_created_at")
        if row.get("gmgn_created_at"):
            token["gmgn_created_at"] = row.get("gmgn_created_at")
        if row.get("token_launch_at"):
            token["token_launch_at"] = row.get("token_launch_at")
        if row.get("gmgn_open_at"):
            token["gmgn_open_at"] = row.get("gmgn_open_at")
        if create_at:
            created_ts = int(create_at.timestamp()) if isinstance(create_at, datetime) else parse_timestamp(create_at)
            token["watchlist_create_at"] = created_ts
            token["created_at"] = created_ts
        if added_at:
            token["watchlist_added_at"] = int(added_at.timestamp()) if isinstance(added_at, datetime) else parse_timestamp(added_at)
        if row.get("daily_mcap_date"):
            token["watchlist_daily_mcap_date"] = str(row.get("daily_mcap_date"))
        if row.get("updated_at"):
            token["watchlist_updated_at"] = int(row["updated_at"].timestamp()) if isinstance(row["updated_at"], datetime) else parse_timestamp(row["updated_at"])
        tokens.append(token)
    return tokens


def fetch_alpha_abnormal_tokens() -> list[dict[str, Any]]:
    """Read CA list from alpha_abnormal_analysis table (same structure as bottom_watchlist_tokens)."""
    try:
        def _op(conn):
            cur = conn.cursor()
            cur.execute(
                """
                SELECT ca, create_at, added_at, last_seen_at, updated_at,
                       source, peak_mcap, last_mcap, highest_mcap, current_mcap,
                       gmgn_created_at, gmgn_open_at, note, remark, symbol,
                       fee_sol, token_created_at, token_launch_at,
                       daily_mcap_date, daily_mcap_threshold,
                       daily_mcap_notified_date, daily_mcap_notified_at,
                       ath_mcap, blacklisted, last_pool_liquidity,
                       last_pool_mcap_ratio, narrative_desc, narrative_type, narrative_category
                FROM alpha_abnormal_analysis
                ORDER BY added_at DESC
                """
            )
            return cur.fetchall()
        rows = db_op(_op) or []
    except Exception as exc:
        print(f"alpha_abnormal_analysis query failed: {exc}")
        return []
    tokens = []
    for row in rows:
        ca = row[0]
        address = str(ca).strip()
        if not valid_sol_ca(address):
            continue
        token = {
            "address": address,
            "source": "alpha_abnormal",
            "alpha_abnormal_source": row[5],
            "alpha_abnormal_peak_mcap": to_float(row[6]),
            "alpha_abnormal_last_mcap": to_float(row[7]),
            "alpha_abnormal_last_pool_liquidity": to_float(row[24]),
            "alpha_abnormal_last_pool_mcap_ratio": to_float(row[25]),
            "alpha_abnormal_narrative_desc": row[26] or "",
            "alpha_abnormal_narrative_type": row[27] or "",
            "alpha_abnormal_narrative_category": row[28] or "",
            "narrative_desc": row[26] or "",
            "narrative_type": row[27] or "",
            "narrative_category": row[28] or "",
            "symbol": row[14] or "",
            "blacklisted": bool(row[23]),
        }
        if row[15]:
            token["fee_sol"] = row[15]
        if row[16]:
            token["token_created_at"] = row[16]
        if row[17]:
            token["token_launch_at"] = row[17]
        if row[11]:
            token["gmgn_created_at"] = row[11]
        if row[12]:
            token["gmgn_open_at"] = row[12]
        if row[1]:
            created_ts = int(row[1].timestamp()) if isinstance(row[1], datetime) else parse_timestamp(row[1])
            token["alpha_abnormal_create_at"] = created_ts
            token["created_at"] = created_ts
        if row[2]:
            token["alpha_abnormal_added_at"] = int(row[2].timestamp()) if isinstance(row[2], datetime) else parse_timestamp(row[2])
        if row[18]:
            token["alpha_abnormal_daily_mcap_date"] = str(row[18])
        if row[4]:
            token["alpha_abnormal_updated_at"] = int(row[4].timestamp()) if isinstance(row[4], datetime) else parse_timestamp(row[4])
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


def fetch_token_traders_snapshot(address: str, order_by: str, limit: int | None = None) -> list[dict[str, Any]]:
    data = run_gmgn(
        [
            "token",
            "traders",
            "--chain",
            CHAIN,
            "--address",
            address,
            "--limit",
            str(limit or TOP_TRADER_SNAPSHOT_LIMIT),
            "--order-by",
            order_by,
            "--direction",
            "desc",
        ],
        timeout=90,
    )
    if not isinstance(data, dict):
        return []
    traders = data.get("list") or data.get("data", {}).get("list") or []
    return traders if isinstance(traders, list) else []


def trader_wallet_key(row: dict[str, Any]) -> str:
    return str(row.get("address") or row.get("account_address") or row.get("wallet_address") or "").strip()


def trader_loss_value(row: dict[str, Any]) -> float:
    profit = to_float(row.get("profit") or row.get("realized_profit"))
    unrealized = to_float(row.get("unrealized_profit"))
    losses = [value for value in (profit, unrealized) if value < 0]
    return min(losses) if losses else 0.0


def build_top_loss_traders(address: str) -> list[dict[str, Any]]:
    # GMGN currently rejects direction=asc for token_top_traders, so collect
    # high-activity trader candidates and sort their negative PnL locally.
    merged: dict[str, dict[str, Any]] = {}
    for order_by in ("buy_volume_cur", "sell_volume_cur", "amount_percentage"):
        for row in fetch_token_traders_snapshot(address, order_by):
            if not isinstance(row, dict):
                continue
            key = trader_wallet_key(row)
            if key and key not in merged:
                merged[key] = row
    loss_rows = [row for row in merged.values() if trader_loss_value(row) < 0]
    loss_rows.sort(key=trader_loss_value)
    return loss_rows[:TOP_TRADER_SNAPSHOT_LIMIT]


def fetch_profit_loss_trader_snapshots(address: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    top_profit_traders = fetch_token_traders_snapshot(address, "profit")
    top_loss_traders = build_top_loss_traders(address)
    return top_profit_traders, top_loss_traders


def fetch_token_metadata(address: str) -> tuple[dict[str, Any], dict[str, Any]]:
    info = fetch_binance_token_metadata(address)
    return (info if isinstance(info, dict) else {}, {})


def fetch_token_pool(address: str) -> dict[str, Any] | list[Any] | None:
    return run_gmgn(["token", "pool", "--chain", CHAIN, "--address", address], timeout=75)


POOL_LIQUIDITY_KEYS = (
    "liquidity",
    "liquidity_usd",
    "usd_liquidity",
    "reserve_usd",
    "pool_liquidity",
    "total_liquidity",
    "base_reserve_value",
    "quote_reserve_value",
)


def extract_pool_rows(data: dict[str, Any] | list[Any] | None) -> list[dict[str, Any]]:
    if not data:
        return []
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        nested_data = data.get("data") if isinstance(data.get("data"), dict) else {}
        rows = (
            data.get("list")
            or data.get("pools")
            or data.get("pairs")
            or nested_data.get("list")
            or nested_data.get("pools")
            or nested_data.get("pairs")
        )
        if not rows and nested_data and any(key in nested_data for key in ("pool_address", "address", "liquidity", "exchange")):
            rows = [nested_data]
        if not rows and any(key in data for key in ("pool_address", "address", "liquidity", "exchange")):
            rows = [data]
    else:
        rows = []
    return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def normalize_pool(row: dict[str, Any]) -> dict[str, Any]:
    liquidity = first_pool_liquidity(row)
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


def dedupe_pools(pools: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: dict[str, int] = {}
    for pool in pools:
        address = str(pool.get("address") or "").strip().lower()
        key = address
        if not key:
            key = "|".join(
                [
                    str(pool.get("exchange") or "").strip().lower(),
                    str(pool.get("quote_address") or "").strip().lower(),
                    str(pool.get("quote_symbol") or "").strip().lower(),
                    str(pool.get("created_ts") or ""),
                ]
            )
        if key and key in seen:
            existing = deduped[seen[key]]
            if to_float(pool.get("liquidity")) > to_float(existing.get("liquidity")):
                deduped[seen[key]] = pool
            continue
        if key:
            seen[key] = len(deduped)
        deduped.append(pool)
    return deduped


def first_pool_liquidity(row: dict[str, Any]) -> float:
    for key in ("liquidity", "liquidity_usd", "usd_liquidity", "reserve_usd", "pool_liquidity", "total_liquidity"):
        if row.get(key) not in (None, ""):
            return to_float(row.get(key))
    base_value = row.get("base_reserve_value")
    quote_value = row.get("quote_reserve_value")
    if base_value not in (None, "") or quote_value not in (None, ""):
        return to_float(base_value) + to_float(quote_value)
    return 0.0


def pool_rows_have_explicit_liquidity(rows: list[dict[str, Any]]) -> bool:
    for row in rows:
        if any(row.get(key) not in (None, "") for key in POOL_LIQUIDITY_KEYS):
            return True
    return False


def summarize_gmgn_pool_data(pool_data: dict[str, Any] | list[Any] | None, token: dict[str, Any]) -> tuple[dict[str, Any], bool, str]:
    """Summarize only gmgn-cli token pool data and report whether deletion can trust it."""
    if pool_data is None:
        return summarize_pools({"address": token_address(token), "_gmgn_pool": {}}), False, "pool_fetch_failed"
    rows = extract_pool_rows(pool_data)
    if not rows:
        return summarize_pools({"address": token_address(token), "_gmgn_pool": pool_data}), False, "pool_empty_or_unrecognized"
    if not pool_rows_have_explicit_liquidity(rows):
        return summarize_pools({"address": token_address(token), "_gmgn_pool": pool_data}), False, "pool_liquidity_field_missing"
    summary = summarize_pools({"address": token_address(token), "_gmgn_pool": pool_data, **token})
    return summary, True, ""


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
    pools = dedupe_pools(pools)
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
    _KLINE_CACHE_TABLE_READY = True


def kline_cache_table_for_resolution(resolution: str) -> str:
    return "bottom_kline_cache_1m" if str(resolution or "").lower() in {"1m", "1min", "1"} else "bottom_kline_cache"


def latest_cached_kline_ts(address: str, resolution: str) -> int:
    table = kline_cache_table_for_resolution(resolution)

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT MAX(ts)
            FROM {table}
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
    table = kline_cache_table_for_resolution(resolution)

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
            f"""
            INSERT INTO {table} (
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
    table = kline_cache_table_for_resolution(resolution)

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT ts, open, high, low, close, volume, amount
            FROM {table}
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
    start_ts = token_launch_ts(token) or token_created_ts(token)
    if start_ts > 0:
        return min(start_ts, end_ts - kline_resolution_seconds(token_kline_resolution(token)))
    return end_ts - KLINE_LOOKBACK_SEC


def binance_interval_from_resolution(resolution: str) -> str:
    return {
        "1m": "1min",
        "1min": "1min",
        "5m": "5min",
        "5min": "5min",
        "15m": "15min",
        "15min": "15min",
        "30m": "30min",
        "30min": "30min",
        "1h": "1h",
        "4h": "4h",
        "1d": "1d",
    }.get(str(resolution or "").lower(), resolution or "1min")


def fetch_kline_range(address: str, resolution: str, start_ts: int, end_ts: int) -> list[dict[str, Any]]:
    if not address or start_ts <= 0 or end_ts <= 0 or end_ts <= start_ts:
        return []
    try:
        resp = requests.get(
            BINANCE_KLINE_URL,
            params={
                "address": address,
                "platform": "solana",
                "interval": binance_interval_from_resolution(resolution),
                "pm": "p",
                "from": max(0, int(start_ts)) * 1000,
                "to": max(0, int(end_ts)) * 1000,
            },
            headers=BINANCE_HEADERS,
            timeout=25,
        )
        if not resp.ok:
            print(f"{address[:8]} binance kline range failed: status={resp.status_code}")
            return []
        return parse_binance_kline_rows(resp.json().get("data"))
    except Exception as exc:
        print(f"{address[:8]} binance kline range exception: {exc}")
        return []


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


def summarize_rebound_after_high(candles: list[dict[str, Any]]) -> dict[str, Any]:
    valid = []
    for candle in candles:
        high = to_float(candle.get("high"))
        low = to_float(candle.get("low"))
        close = to_float(candle.get("close"))
        if high > 0 and low > 0 and close > 0:
            valid.append(
                {
                    "ts": candle.get("ts"),
                    "high": high,
                    "low": low,
                    "close": close,
                }
            )
    if len(valid) < 3:
        return {"ready": False, "reason": "not_enough_kline"}

    current = valid[-1]
    current_close = current["close"]
    high_index, high_candle = max(enumerate(valid), key=lambda item: item[1]["high"])
    if high_index >= len(valid) - 1:
        return {
            "ready": False,
            "reason": "highest_point_is_current",
            "high": high_candle["high"],
            "high_ts": high_candle.get("ts"),
            "close": current_close,
            "close_ts": current.get("ts"),
        }

    candidates = valid[high_index + 1 : -1]
    if not candidates:
        return {
            "ready": False,
            "reason": "no_pullback_after_high",
            "high": high_candle["high"],
            "high_ts": high_candle.get("ts"),
            "close": current_close,
            "close_ts": current.get("ts"),
        }

    low_index = None
    for index in range(len(valid) - 2, high_index, -1):
        prev_low = valid[index - 1]["low"] if index - 1 > high_index else high_candle["high"]
        next_low = valid[index + 1]["low"]
        cur_low = valid[index]["low"]
        if cur_low <= prev_low and cur_low <= next_low:
            low_index = index
            break
    if low_index is None:
        low_index, _ = min(
            ((index, valid[index]) for index in range(high_index + 1, len(valid) - 1)),
            key=lambda item: item[1]["low"],
        )

    low_candle = valid[low_index]
    pullback_low = low_candle["low"]
    drawdown_pct = ((high_candle["high"] - pullback_low) / high_candle["high"] * 100) if high_candle["high"] > 0 else 0
    rebound_pct = ((current_close - pullback_low) / pullback_low * 100) if pullback_low > 0 else 0
    ready = (
        high_index < low_index < len(valid) - 1
        and drawdown_pct >= KLINE_REVIVAL_MIN_DRAWDOWN_PCT
        and rebound_pct > 0
    )
    return {
        "ready": ready,
        "reason": "ok" if ready else "drawdown_or_rebound_not_enough",
        "high": high_candle["high"],
        "high_ts": high_candle.get("ts"),
        "low": pullback_low,
        "low_ts": low_candle.get("ts"),
        "close": current_close,
        "close_ts": current.get("ts"),
        "drawdown_pct": drawdown_pct,
        "change_pct": rebound_pct,
        "high_index": high_index,
        "low_index": low_index,
        "close_index": len(valid) - 1,
    }


KLINE_JOURNEY_BASELINES: dict[str, dict[str, dict[str, Any]]] = {
    "new_revival": {
        "底部持续下跌": {
            "wr20": 84,
            "med_peak": "+86%",
            "boom_prob": 59,
            "downtrend_prob": 12,
            "score_delta": 10,
            "note": "08文档最优结构，推送后4h暴涨分支占比高，阴跌分支低",
        },
        "高点下跌回落中": {
            "wr20": 77,
            "med_peak": "+113%",
            "boom_prob": 31,
            "downtrend_prob": 23,
            "score_delta": 8,
            "note": "跌深后反弹空间大，MedPeak为new_revival优势结构",
        },
        "底部横盘": {
            "wr20": 74,
            "med_peak": "+102%",
            "boom_prob": 42,
            "downtrend_prob": 21,
            "score_delta": 6,
            "note": "底部窄幅后突破型结构，需观察后续方向确认",
        },
        "持续拉升中": {
            "wr20": 74,
            "med_peak": "+99%",
            "boom_prob": 33,
            "downtrend_prob": 30,
            "score_delta": -2,
            "note": "分化最大，0-1h转弱时后续阴跌概率升高",
        },
        "底部反弹启动": {
            "wr20": 67,
            "med_peak": "+27%",
            "boom_prob": 17,
            "downtrend_prob": 39,
            "score_delta": -4,
            "note": "反弹可能夭折，文档要求等待后4h确认",
        },
        "强势拉升后高位": {
            "wr20": 80,
            "med_peak": "+78%",
            "boom_prob": 30,
            "downtrend_prob": 40,
            "score_delta": -3,
            "note": "高位触发分化明显，到峰后回吐风险需要单独看",
        },
        "其他结构": {
            "wr20": 77,
            "med_peak": "-",
            "boom_prob": 27,
            "downtrend_prob": 22,
            "score_delta": 0,
            "note": "形态不够清晰，不作为独立强信号",
        },
    },
    "abnormal": {
        "底部反弹启动": {
            "wr20": 80,
            "med_peak": "+456%",
            "boom_prob": 20,
            "downtrend_prob": 40,
            "score_delta": 8,
            "note": "abnormal文档最优前置结构，但少数大赢家拉高均值",
        },
        "高位加速拉升": {
            "wr20": 83,
            "med_peak": "+89%",
            "boom_prob": 17,
            "downtrend_prob": 0,
            "score_delta": 6,
            "note": "样本小但WR20高，关键看后4h是否守住",
        },
        "持续拉升中": {
            "wr20": 65,
            "med_peak": "+123%",
            "boom_prob": 21,
            "downtrend_prob": 19,
            "score_delta": -5,
            "note": "abnormal主力结构，只有暴涨/强涨守住分支更优",
        },
        "底部持续下跌": {
            "wr20": 57,
            "med_peak": "-",
            "boom_prob": 14,
            "downtrend_prob": 0,
            "score_delta": -6,
            "note": "同名结构在abnormal中明显弱于new_revival",
        },
        "其他结构": {
            "wr20": 57,
            "med_peak": "-",
            "boom_prob": 0,
            "downtrend_prob": 0,
            "score_delta": -8,
            "note": "信号不清晰，08文档中没有稳定规律",
        },
    },
}


def _pct_change_from_prices(start: float, end: float) -> float:
    return ((end - start) / start * 100) if start > 0 else 0.0


def _segment_change_pct(segment: list[dict[str, Any]]) -> float:
    if not segment:
        return 0.0
    return _pct_change_from_prices(to_float(segment[0].get("open")), to_float(segment[-1].get("close")))


def _volume_journey_label(ratio: float) -> str:
    if ratio >= 1.5:
        return "放量"
    if ratio <= 0.65 and ratio > 0:
        return "缩量"
    if ratio > 0:
        return "平量"
    return "未知"


def classify_kline_journey(
    candles: list[dict[str, Any]],
    resolution: str,
    signal_type: str = "",
) -> dict[str, Any]:
    """
    Classify the 5m pre-signal 4h structure from
    onchain_trading_guides/08-5m-fingerprint-encyclopedia.md.
    """
    valid = [
        c
        for c in candles
        if to_float(c.get("open")) > 0 and to_float(c.get("close")) > 0 and to_float(c.get("high")) > 0 and to_float(c.get("low")) > 0
    ]
    if not valid:
        return {"ready": False, "reason": "no_valid_candles"}
    if str(resolution).lower() not in {"5m", "5min", "5"}:
        return {"ready": False, "reason": "requires_5m", "resolution": resolution, "count": len(valid)}
    pre = valid[-48:] if len(valid) >= 48 else valid
    if len(pre) < 24:
        return {"ready": False, "reason": "need_at_least_24_5m_candles", "resolution": resolution, "count": len(valid)}

    chunk_size = max(1, len(pre) // 4)
    segments = [
        pre[0:chunk_size],
        pre[chunk_size : chunk_size * 2],
        pre[chunk_size * 2 : chunk_size * 3],
        pre[chunk_size * 3 :],
    ]
    q = [_segment_change_pct(seg) for seg in segments]
    lows = [to_float(c.get("low")) for c in pre]
    highs = [to_float(c.get("high")) for c in pre]
    close = to_float(pre[-1].get("close"))
    first_open = to_float(pre[0].get("open"))
    low = min(lows) if lows else 0.0
    high = max(highs) if highs else 0.0
    total_change = _pct_change_from_prices(first_open, close)
    close_position = (close - low) / (high - low) if high > low and close > 0 else 0.0
    range_pct = ((high - low) / low * 100) if low > 0 else 0.0

    recent = pre[-12:] if len(pre) >= 24 else pre[len(pre) // 2 :]
    previous = pre[-24:-12] if len(pre) >= 24 else pre[: len(pre) // 2]
    recent_vol = sum(to_float(c.get("volume")) for c in recent)
    previous_vol = sum(to_float(c.get("volume")) for c in previous)
    volume_ratio = recent_vol / previous_vol if previous_vol > 0 else 0.0
    volume_label = _volume_journey_label(volume_ratio)

    structure = "其他结构"
    if close_position <= 0.35 and q[3] <= -18:
        structure = "底部持续下跌"
    elif close_position <= 0.40 and abs(q[3]) <= 6 and range_pct <= 45:
        structure = "底部横盘"
    elif close_position <= 0.55 and q[2] <= -12 and q[3] >= 12:
        structure = "底部反弹启动"
    elif q[0] <= -15 and q[1] <= -5 and q[2] <= 2 and q[3] >= 5:
        structure = "高点下跌回落中"
    elif close_position >= 0.70 and q[3] >= 20 and q[2] <= 10:
        structure = "高位加速拉升"
    elif close_position >= 0.70 and (q[0] >= 25 or total_change >= 40):
        structure = "强势拉升后高位"
    elif q[1] >= 0 and q[2] >= 0 and q[3] >= 10 and total_change >= 20:
        structure = "持续拉升中"
    elif sum(1 for item in q if item < -3) >= 3 and close_position > 0.35:
        structure = "持续下跌中"
    elif range_pct <= 15:
        structure = "长期横盘震荡"

    signal_key = signal_type if signal_type in KLINE_JOURNEY_BASELINES else ""
    doc = (KLINE_JOURNEY_BASELINES.get(signal_key) or {}).get(structure)
    if not doc:
        doc = (KLINE_JOURNEY_BASELINES.get(signal_key) or {}).get("其他结构") or {}
    return {
        "ready": True,
        "source_doc": "onchain_trading_guides/08-5m-fingerprint-encyclopedia.md",
        "resolution": resolution,
        "count": len(valid),
        "pre_bars": len(pre),
        "pre_structure": structure,
        "q_changes_pct": [round(item, 1) for item in q],
        "total_change_pct": round(total_change, 1),
        "range_pct": round(range_pct, 1),
        "close_position_pct": round(close_position * 100, 1),
        "volume_label": volume_label,
        "volume_ratio": round(volume_ratio, 2),
        "doc_wr20_pct": to_float(doc.get("wr20")),
        "doc_med_peak": doc.get("med_peak") or "-",
        "doc_boom_prob_pct": to_float(doc.get("boom_prob")),
        "doc_downtrend_prob_pct": to_float(doc.get("downtrend_prob")),
        "score_delta": to_float(doc.get("score_delta")),
        "doc_note": doc.get("note") or "",
    }


def enrich_kline_journey_for_signal(journey: dict[str, Any] | None, signal_type: str) -> dict[str, Any]:
    journey = dict(journey or {})
    if not journey.get("ready"):
        return journey
    signal_key = signal_type if signal_type in KLINE_JOURNEY_BASELINES else ""
    structure = str(journey.get("pre_structure") or "其他结构")
    doc = (KLINE_JOURNEY_BASELINES.get(signal_key) or {}).get(structure)
    if not doc:
        doc = (KLINE_JOURNEY_BASELINES.get(signal_key) or {}).get("其他结构") or {}
    journey.update(
        {
            "source_doc": "onchain_trading_guides/08-5m-fingerprint-encyclopedia.md",
            "doc_wr20_pct": to_float(doc.get("wr20")),
            "doc_med_peak": doc.get("med_peak") or "-",
            "doc_boom_prob_pct": to_float(doc.get("boom_prob")),
            "doc_downtrend_prob_pct": to_float(doc.get("downtrend_prob")),
            "score_delta": to_float(doc.get("score_delta")),
            "doc_note": doc.get("note") or "",
        }
    )
    return journey


def classify_1m_micro_strategy(
    candles: list[dict[str, Any]],
    signal_type: str,
    current_mcap: float,
) -> dict[str, Any]:
    """
    Classify the 1m micro setup using the 09 bar-level strategy:
    recent 5m average volume / prior 30m average volume.
    """
    valid = [
        c
        for c in candles
        if to_float(c.get("open")) > 0 and to_float(c.get("close")) > 0 and to_float(c.get("high")) > 0 and to_float(c.get("low")) > 0
    ]
    if len(valid) < 10:
        return {"ready": False, "reason": "need_more_1m_candles", "resolution": "1m", "count": len(valid)}
    post = valid[-5:] if len(valid) >= 5 else valid
    pre = valid[-35:-5] if len(valid) >= 35 else valid[: max(1, len(valid) - len(post))]
    if not pre or not post:
        return {"ready": False, "reason": "no_pre_or_post_window", "resolution": "1m", "count": len(valid)}

    pre_avg_vol = sum(to_float(c.get("volume")) for c in pre) / max(1, len(pre))
    post_avg_vol = sum(to_float(c.get("volume")) for c in post) / max(1, len(post))
    volume_ratio = post_avg_vol / pre_avg_vol if pre_avg_vol > 0 else 0.0
    if volume_ratio >= 4:
        volume_label = "天量"
    elif volume_ratio >= 2:
        volume_label = "放量"
    elif volume_ratio >= 0.5:
        volume_label = "平量"
    elif volume_ratio > 0:
        volume_label = "缩量"
    else:
        volume_label = "未知"

    open_price = to_float(post[0].get("open"))
    close_price = to_float(post[-1].get("close"))
    change_pct = _pct_change_from_prices(open_price, close_price)
    up_bars = sum(1 for c in post if to_float(c.get("close")) >= to_float(c.get("open")))
    down_bars = len(post) - up_bars
    if change_pct <= -3 or down_bars >= 4:
        direction = "跌为主"
    elif change_pct >= 3 or up_bars >= 4:
        direction = "涨为主"
    else:
        direction = "涨跌互现"

    label = f"{direction}+{volume_label}"
    score_delta = 0.0
    doc_wr20 = 0.0
    doc_med_peak = "-"
    decision = "观察"
    note = "09 bar策略：推送后5分钟量价用于短线确认。"
    if signal_type == "new_revival":
        if volume_label == "天量":
            score_delta = -18
            doc_wr20 = 40
            doc_med_peak = "+13.8%"
            decision = "回避/仅观察"
            note = "推送后5分钟天量>4x，09策略标记为接盘风险。"
        elif direction == "跌为主" and volume_label == "平量":
            score_delta = 12
            doc_wr20 = 83
            doc_med_peak = "+66.8%"
            decision = "高价值确认"
            note = "跌为主+平量是09策略最佳1m确认：恐慌下跌但量能正常。"
        elif direction == "跌为主" and volume_label == "缩量":
            score_delta = 8
            doc_wr20 = 78
            doc_med_peak = "+53.8%"
            decision = "高价值确认"
            note = "跌为主+缩量：缩量跌不是真砸盘，后续Hit20较高。"
        elif direction == "涨跌互现" and volume_label == "缩量":
            score_delta = 7
            doc_wr20 = 77
            doc_med_peak = "+59.1%"
            decision = "中高价值确认"
            note = "震荡缩量在09策略中后4h稳健上涨概率高。"
        elif direction == "涨跌互现" and volume_label == "平量":
            score_delta = 5
            doc_wr20 = 70
            doc_med_peak = "+89.9%"
            decision = "中等价值确认"
            note = "横盘平量可能突破，但需要配合5m前置结构。"
        elif direction == "涨为主" and volume_label == "平量":
            score_delta = 2
            doc_wr20 = 67
            doc_med_peak = "+90.6%"
            decision = "追高确认"
            note = "涨为主+平量空间仍大，但胜率低于回调确认。"
        elif volume_label == "放量":
            score_delta = -5
            decision = "放量观察"
            note = "09策略将2-4x放量列为异常量，需等5m/后4h确认。"
    elif signal_type == "abnormal":
        if direction == "跌为主" and volume_label in {"平量", "放量"}:
            score_delta = -14
            doc_wr20 = 17
            decision = "低价值/回避"
            note = "09策略红灯：abnormal 后5min跌+正常量，WR20约17%。"
        elif volume_label == "天量":
            score_delta = -16
            decision = "低价值/回避"
            note = "abnormal推送后天量容易形成顶部接盘。"
        elif direction == "涨为主" and volume_label in {"平量", "缩量"}:
            score_delta = 4
            decision = "短线确认"
            note = "abnormal需要后4h暴涨/强涨守住确认，1m只作为初筛。"

    return {
        "ready": True,
        "source_doc": "onchain_trading_guides/09-bar-level-strategy.md",
        "resolution": "1m",
        "count": len(valid),
        "pre_minutes": len(pre),
        "post_minutes": len(post),
        "pre_avg_volume_usd": round(pre_avg_vol, 2),
        "post_avg_volume_usd": round(post_avg_vol, 2),
        "volume_ratio": round(volume_ratio, 2),
        "volume_label": volume_label,
        "direction": direction,
        "label": label,
        "change_pct": round(change_pct, 1),
        "doc_wr20_pct": doc_wr20,
        "doc_med_peak": doc_med_peak,
        "score_delta": score_delta,
        "decision": decision,
        "note": note,
    }


def summarize_kline(candles: list[dict[str, Any]], resolution: str) -> dict[str, Any]:
    if not candles:
        return {"resolution": resolution, "count": 0}
    signal_candles = candles[-KLINE_SIGNAL_BARS:] if KLINE_SIGNAL_BARS > 0 else candles
    first = signal_candles[0]
    last = signal_candles[-1]
    open_price = to_float(first.get("open"))
    close_price = to_float(last.get("close"))
    lows = [to_float(c.get("low")) for c in signal_candles if to_float(c.get("low")) > 0]
    highs = [to_float(c.get("high")) for c in signal_candles if to_float(c.get("high")) > 0]
    total_volume = sum(to_float(c.get("volume")) for c in signal_candles)
    signal_low = min(lows) if lows else 0
    # Bottom-to-current: change from the LOWEST price in signal window to current close
    bottom_to_current_pct = ((close_price - signal_low) / signal_low * 100) if signal_low > 0 else 0
    return {
        "resolution": resolution,
        "count": len(candles),
        "signal_count": len(signal_candles),
        "signal_bars": KLINE_SIGNAL_BARS,
        "cache_from_ts": candles[0].get("ts"),
        "cache_to_ts": candles[-1].get("ts"),
        "from_ts": first.get("ts"),
        "to_ts": last.get("ts"),
        "open": open_price,
        "close": close_price,
        "change_pct": ((close_price - open_price) / open_price * 100) if open_price > 0 else 0,
        "bottom_to_current_pct": bottom_to_current_pct,
        "high": max(highs) if highs else 0,
        "low": signal_low,
        "volume_usd": total_volume,
        "last_volume_usd": to_float(last.get("volume")),
        "rebound_after_high": summarize_rebound_after_high(candles),
        "journey": classify_kline_journey(candles, resolution),
    }


def build_deepseek_kline_signal_context(
    token: dict[str, Any],
    summary: dict[str, Any],
    analysis: dict[str, Any],
) -> dict[str, Any]:
    """Compact signal context for DeepSeek K-line prediction."""
    pool = summary.get("pool") if isinstance(summary.get("pool"), dict) else {}
    kline = summary.get("kline") if isinstance(summary.get("kline"), dict) else {}
    micro_1m = summary.get("_1m_micro") if isinstance(summary.get("_1m_micro"), dict) else {}
    journey = summary.get("_5m_kline_journey") if isinstance(summary.get("_5m_kline_journey"), dict) else kline.get("journey")
    return {
        "chain": CHAIN,
        "symbol": token.get("symbol") or "UNKNOWN",
        "address": token_address(token),
        "signal_type": analysis.get("signal_type") or "",
        "abnormal_rule": analysis.get("abnormal_rule") or "",
        "current_mcap": to_float(analysis.get("current_mcap") or summary.get("mcap")),
        "ath_mcap": to_float(analysis.get("ath_mcap") or summary.get("ath_mcap")),
        "price_change_pct": to_float(analysis.get("price_change_pct")),
        "first_signal_change_pct": to_float(analysis.get("first_signal_change_pct")),
        "pool_total_liquidity": to_float(analysis.get("pool_total_liquidity") or pool.get("total_liquidity")),
        "pool_mcap_ratio": to_float(analysis.get("pool_mcap_ratio") or pool.get("liquidity_mcap_ratio")),
        "holder_count": to_int(summary.get("holder_count")),
        "age_sec": to_int(summary.get("age_sec")),
        "top10_current_pct": to_float(analysis.get("top10_current_pct")),
        "top20_current_pct": to_float(analysis.get("top20_current_pct")),
        "top50_current_pct": to_float(analysis.get("top50_current_pct")),
        "top100_current_pct": to_float(analysis.get("top100_current_pct")),
        "top10_pct_delta": to_float(analysis.get("top10_pct_delta")),
        "top20_pct_delta": to_float(analysis.get("top20_pct_delta")),
        "top50_pct_delta": to_float(analysis.get("top50_pct_delta")),
        "top100_pct_delta": to_float(analysis.get("top100_pct_delta")),
        "netflow_usd": to_float(analysis.get("netflow_usd")),
        "kline_summary": {
            "resolution": kline.get("resolution"),
            "count": kline.get("count"),
            "change_pct": kline.get("change_pct"),
            "bottom_to_current_pct": kline.get("bottom_to_current_pct"),
            "volume_usd": kline.get("volume_usd"),
        },
        "local_5m_journey": journey if isinstance(journey, dict) else {},
        "local_1m_micro": micro_1m,
        "source_docs_expected": [
            "onchain_trading_guides/11-ca-analysis-methodology.md",
            "onchain_trading_guides/08-5m-fingerprint-encyclopedia.md",
        ],
    }


def deepseek_signal_eligible(signal_type: str) -> bool:
    return str(signal_type or "") in {"new_revival", "abnormal", "watchlist_abnormal"}


def maybe_attach_deepseek_kline_prediction(
    *,
    token: dict[str, Any],
    summary: dict[str, Any],
    analysis: dict[str, Any],
    candles_5m: list[dict[str, Any]],
    candles_1m: list[dict[str, Any]],
) -> dict[str, Any]:
    """Attach DeepSeek 5m/1m K-line prediction synchronously."""
    signal_type = str((analysis or {}).get("signal_type") or "")
    if not signal_type or signal_type == "watch" or analysis.get("deepseek_kline_prediction"):
        return analysis
    if not deepseek_signal_eligible(signal_type):
        return analysis
    address = token_address(token)
    if not address:
        return analysis
    try:
        from bottom_detection.deepseek_kline_predictor import analyze_deepseek_kline_prediction, warmup_deepseek_cache
        warmup_deepseek_cache()  # non-blocking, pre-warms prompt cache

        prediction = analyze_deepseek_kline_prediction(
            address=address,
            signal=build_deepseek_kline_signal_context(token, summary, analysis),
            candles_5m=candles_5m,
            candles_1m=candles_1m,
        )
    except Exception as exc:
        print(f"{address[:8]} deepseek kline prediction exception: {exc}")
        return analysis
    if not prediction.get("ready"):
        status = prediction.get("status") or "not_ready"
        if status not in {"disabled", "missing_api_key"}:
            print(f"{address[:8]} deepseek kline prediction skipped: {status}")
        return analysis
    return {**analysis, "deepseek_kline_prediction": prediction}


def merge_token_metadata(token: dict[str, Any], info: dict[str, Any], security: dict[str, Any]) -> dict[str, Any]:
    merged = dict(token)
    for source in (security, info):
        for key, value in source.items():
            if key not in merged or merged.get(key) in (None, "", 0):
                merged[key] = value
    merged = apply_binance_dynamic_metrics(merged, info)
    # Flatten nested price object from legacy token-info payloads.
    price_val = merged.get("price")
    if isinstance(price_val, dict):
        merged["price"] = price_val.get("price") or price_val.get("price_1m") or 0
    merged["_binance_info"] = info
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

    holder_count = to_int(
        token.get("holder_count")
        or (token.get("stat") or {}).get("holder_count")
        or (token.get("_gmgn_info") or {}).get("holder_count")
        or ((token.get("_gmgn_info") or {}).get("stat") or {}).get("holder_count")
    )
    summary = {
        "holder_count": holder_count or len(raw_holders),
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
        "launch_ts": token_launch_ts(token),
        "created_age_sec": token_creation_age_sec(token),
        "launch_age_sec": token_launch_age_sec(token),
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


def ensure_bottom_top100_snapshot_comments() -> None:
    global _BOTTOM_TOP100_SNAPSHOT_COMMENTS_READY
    if _BOTTOM_TOP100_SNAPSHOT_COMMENTS_READY:
        return

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            COMMENT ON TABLE bottom_top100_snapshots IS 'Top100持仓快照表。每次进入异动检测流程都会记录当时GMGN Top100持仓、摘要和分析结果';
            COMMENT ON COLUMN bottom_top100_snapshots.id IS '快照自增ID，可被bottom_top100_push_records.snapshot_id引用';
            COMMENT ON COLUMN bottom_top100_snapshots.scan_id IS '一次扫描批次ID';
            COMMENT ON COLUMN bottom_top100_snapshots.chain IS '链名称，当前主要为sol';
            COMMENT ON COLUMN bottom_top100_snapshots.trend_interval IS '扫描来源时间窗口，例如1m、5m、1h，watchlist来源可能沿用当前窗口';
            COMMENT ON COLUMN bottom_top100_snapshots.address IS '代币CA';
            COMMENT ON COLUMN bottom_top100_snapshots.symbol IS '快照时识别到的代币符号';
            COMMENT ON COLUMN bottom_top100_snapshots.snapshot_ts IS '快照采集时间，Unix秒';
            COMMENT ON COLUMN bottom_top100_snapshots.signal_type IS '本次快照分析出的信号类型，watch表示仅观察未推送';
            COMMENT ON COLUMN bottom_top100_snapshots.signal_score IS '本次信号评分';
            COMMENT ON COLUMN bottom_top100_snapshots.notified IS '历史兼容字段，当前推送状态以bottom_top100_push_records为准';
            COMMENT ON COLUMN bottom_top100_snapshots.summary IS '本次快照的市值、池子、Top10/20/50/100占比、买卖额等摘要JSON';
            COMMENT ON COLUMN bottom_top100_snapshots.holders IS '本次快照归一化后的GMGN Top100持仓明细JSON';
            COMMENT ON COLUMN bottom_top100_snapshots.analysis IS '本次异动检测分析结果JSON';
            COMMENT ON COLUMN bottom_top100_snapshots.raw_token IS '合并trending、watchlist、metadata后的原始代币数据JSON';
            COMMENT ON COLUMN bottom_top100_snapshots.created_at IS '数据库写入时间';
            """
        )

    db_op(_op)
    _BOTTOM_TOP100_SNAPSHOT_COMMENTS_READY = True


def ensure_bottom_top100_trader_columns() -> None:
    global _BOTTOM_TOP100_TRADER_COLUMNS_READY
    if _BOTTOM_TOP100_TRADER_COLUMNS_READY:
        return

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            ALTER TABLE bottom_top100_snapshots
                ADD COLUMN IF NOT EXISTS top_profit_traders JSONB NOT NULL DEFAULT '[]'::jsonb,
                ADD COLUMN IF NOT EXISTS top_loss_traders JSONB NOT NULL DEFAULT '[]'::jsonb;
            COMMENT ON COLUMN bottom_top100_snapshots.top_profit_traders
                IS 'GMGN token traders snapshot ordered by realized profit desc';
            COMMENT ON COLUMN bottom_top100_snapshots.top_loss_traders
                IS 'GMGN token traders loss-candidate snapshot sorted locally by negative realized or unrealized PnL';
            """
        )

    db_op(_op)
    _BOTTOM_TOP100_TRADER_COLUMNS_READY = True


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



# ---------------------------------------------------------------------------
# EMA crossover detection (9/26 golden cross)
# ---------------------------------------------------------------------------

def compute_ema(prices, period):
    """Compute Exponential Moving Average for a price series."""
    if len(prices) < period:
        return [0] * len(prices)
    ema = [0] * len(prices)
    # SMA as first EMA value
    sma = sum(prices[:period]) / period
    ema[period - 1] = sma
    multiplier = 2 / (period + 1)
    for i in range(period, len(prices)):
        ema[i] = (prices[i] - ema[i - 1]) * multiplier + ema[i - 1]
    return ema


def detect_ema_crossover(prices):
    """
    Detect EMA9/EMA26 golden cross (金叉) and death cross (死叉).
    Returns dict with crossover info or None if no recent signal.
    """
    if len(prices) < 30:
        return None

    ema9 = compute_ema(prices, 9)
    ema26 = compute_ema(prices, 26)

    # Check recent bars for crossovers (wider window: ~1h for 5m candles)
    lookback = min(12, len(prices) - 27)
    for i in range(len(prices) - 1, len(prices) - 1 - lookback, -1):
        if ema9[i] <= 0 or ema26[i] <= 0:
            continue

        prev_diff = ema9[i - 1] - ema26[i - 1]
        curr_diff = ema9[i] - ema26[i]

        # Golden cross: EMA9 crosses ABOVE EMA26
        if prev_diff < 0 and curr_diff > 0:
            # Calculate how long EMA9 was below EMA26 before crossing
            bars_below = 0
            for j in range(i - 1, max(i - 50, 26), -1):
                if ema9[j] > 0 and ema26[j] > 0 and ema9[j] < ema26[j]:
                    bars_below += 1
                else:
                    break

            return {
                "type": "golden_cross",
                "bar_index": i,
                "ema9": round(ema9[i], 12),
                "ema26": round(ema26[i], 12),
                "prev_ema9": round(ema9[i - 1], 12),
                "prev_ema26": round(ema26[i - 1], 12),
                "bars_below_before_cross": bars_below,
                "strength": "strong" if bars_below >= 8 else "normal",
            }

        # Death cross: EMA9 crosses BELOW EMA26
        if prev_diff > 0 and curr_diff < 0:
            return {
                "type": "death_cross",
                "bar_index": i,
                "ema9": round(ema9[i], 12),
                "ema26": round(ema26[i], 12),
                "prev_ema9": round(ema9[i - 1], 12),
                "prev_ema26": round(ema26[i - 1], 12),
                "strength": "normal",
            }

    return None


def ema_crossover_signal_text(token, crossover, current_mcap, pool_liquidity, pool_ratio, crossover_ts=0):
    """Format EMA crossover TG alert message."""
    address = token_address(token)
    signal_label = "EMA 金叉" if crossover["type"] == "golden_cross" else "EMA 死叉"
    ema_info = (
        f"EMA9({crossover['ema9']:.10f}) 上穿 EMA26({crossover['ema26']:.10f})\n"
        if crossover["type"] == "golden_cross"
        else f"EMA9({crossover['ema9']:.10f}) 下穿 EMA26({crossover['ema26']:.10f})\n"
    )
    time_line = ""
    if crossover_ts > 0:
        time_line = f"金叉时间: {datetime.fromtimestamp(crossover_ts).strftime('%Y-%m-%d %H:%M:%S')}\n"
    return (
        f"{signal_label} | ${token.get('symbol') or 'UNKNOWN'}\n"
        f"{ema_info}"
        f"强度: {crossover['strength']} | EMA9在EMA26下方{crossover.get('bars_below_before_cross', 0)}根后金叉\n"
        f"{time_line}"
        f"CA: {address}\n"
        f"当前市值: ${current_mcap:,.0f} | 池子: ${pool_liquidity:,.0f} | 池/市值: {pool_ratio:.1%}\n"
        f"https://gmgn.ai/sol/token/{address}"
    )


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
    bottom_to_current_pct = to_float(kline_summary.get("bottom_to_current_pct"))
    rebound_after_high = kline_summary.get("rebound_after_high") or {}
    rebound_ready = bool(rebound_after_high.get("ready"))
    new_revival_price_change_pct = to_float(rebound_after_high.get("change_pct")) if rebound_ready else 0.0
    kline_low = to_float(rebound_after_high.get("low")) if rebound_ready else 0.0
    kline_close = to_float(rebound_after_high.get("close")) if rebound_ready else 0.0
    current_mcap = to_float(current_summary.get("mcap"))
    ath_mcap = to_float(current_summary.get("ath_mcap"))
    bottom_low_mcap = current_mcap * (kline_low / kline_close) if current_mcap > 0 and kline_low > 0 and kline_close > 0 else 0.0
    token_age = to_int(current_summary.get("age_sec"))
    is_under_24h = token_age <= 0 or token_age <= NEW_TOKEN_AGE_CUTOFF_SEC
    # Use bottom-to-current instead of open-to-close for detection
    # Catches V-reversals where price dipped then recovered
    price_ready = (bottom_to_current_pct if bottom_to_current_pct > 0 else price_change_pct) >= BOTTOM_ABNORMAL_MIN_PRICE_UP_PCT
    new_revival_price_ready = new_revival_price_change_pct >= BOTTOM_NEW_REVIVAL_MIN_PRICE_UP_PCT
    pool_ratio = to_float(pool_stats.get("pool_mcap_ratio"))
    pool_liquidity = to_float(pool_stats.get("pool_total_liquidity"))
    pool_liquidity_ready = pool_liquidity >= BOTTOM_ABNORMAL_MIN_POOL_LIQUIDITY_USD
    pool_ratio_ready = pool_ratio >= BOTTOM_ABNORMAL_MIN_POOL_MCAP_RATIO
    pool_ready = pool_liquidity_ready and pool_ratio_ready
    drop_level = 0.0
    # Commented out: new coin high level drop check (drop_50w/drop_40w) as per user request
    # if is_under_24h and ath_mcap >= BOTTOM_NEW_DROP_ATH_MCAP_USD:
    #     for level in sorted(BOTTOM_NEW_DROP_LEVELS):
    #         if current_mcap <= level:
    #             drop_level = level
    #             break
    old_abnormal_ready = (
        not is_under_24h
        and current_mcap >= BOTTOM_OLD_ABNORMAL_MIN_MCAP_USD
        and price_ready
        and pool_ready
    )
    new_revival_ready = (
        is_under_24h
        and rebound_ready
        and bottom_low_mcap > 0
        and bottom_low_mcap <= BOTTOM_NEW_REVIVAL_MAX_LOW_MCAP_USD
        and new_revival_price_ready
        and pool_ready
    )
    # Commented out: drop_* signal trigger as per user request
    # if drop_level > 0 and pool_ready:
    #     signal_type = f"drop_{int(drop_level / 10000)}w"
    # else:
    if new_revival_ready:
        signal_type = "new_revival"
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
    current_top10_pct = sum(to_float(holder.get("hold_pct")) for holder in current_holders[:10])
    current_top20_pct = sum(to_float(holder.get("hold_pct")) for holder in current_holders[:20])
    current_top50_pct = sum(to_float(holder.get("hold_pct")) for holder in current_holders[:50])
    current_top100_pct = sum(to_float(holder.get("hold_pct")) for holder in current_holders[:100])
    previous_top10_pct = sum(to_float(holder.get("hold_pct")) for holder in previous_holders[:10]) if previous_holders else 0.0
    previous_top20_pct = sum(to_float(holder.get("hold_pct")) for holder in previous_holders[:20]) if previous_holders else 0.0
    previous_top50_pct = sum(to_float(holder.get("hold_pct")) for holder in previous_holders[:50]) if previous_holders else 0.0
    previous_top100_pct = sum(to_float(holder.get("hold_pct")) for holder in previous_holders[:100]) if previous_holders else 0.0
    top10_pct_delta = current_top10_pct - previous_top10_pct if previous_holders else 0.0
    top20_pct_delta = current_top20_pct - previous_top20_pct if previous_holders else 0.0
    top50_pct_delta = current_top50_pct - previous_top50_pct if previous_holders else 0.0
    top100_pct_delta = current_top100_pct - previous_top100_pct if previous_holders else 0.0
    # Commented out: drop_level rule as per user request
    # if drop_level > 0:
    #     rule_name = f"NEW_ATH1M_DROP_{int(drop_level / 10000)}W"
    #     min_ath_mcap = BOTTOM_NEW_DROP_ATH_MCAP_USD
    #     min_mcap = 0
    #     max_mcap = drop_level
    #     rule_reason = (
    #         f"新币回落{rule_name}: 创建{token_age / 3600:.1f}h, "
    #         f"ATH${ath_mcap:,.0f}>={min_ath_mcap:,.0f}, 当前市值${current_mcap:,.0f}<=${drop_level:,.0f}"
    #     )
    if new_revival_ready:
        rule_name = "NEW_BOTTOM_REVIVAL"
        min_ath_mcap = 0
        min_mcap = 0
        max_mcap = 0
        rule_reason = (
            f"新币底部启动: 创建{token_age / 3600:.1f}h, "
            f"高点回落后最近反弹点市值约${bottom_low_mcap:,.0f}<=${BOTTOM_NEW_REVIVAL_MAX_LOW_MCAP_USD:,.0f}, "
            f"当前市值${current_mcap:,.0f}, "
            f"反弹涨幅{new_revival_price_change_pct:.1f}%"
        )
    elif old_abnormal_ready:
        rule_name = "OLD_MCAP_4W_UP15"
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
                f"ATH${ath_mcap:,.0f}, 当前市值${current_mcap:,.0f}, "
                f"高点回落后最近反弹点市值约${bottom_low_mcap:,.0f}"
            )
        else:
            rule_reason = (
                f"未命中老币异动: 创建{token_age / 3600:.1f}h, "
                f"当前市值${current_mcap:,.0f}<${BOTTOM_OLD_ABNORMAL_MIN_MCAP_USD:,.0f}或涨幅/池子不足"
            )
    # Use bottom_to_current for abnormal signals (catches V-reversals)
    if signal_type == "abnormal":
        display_price_change_pct = bottom_to_current_pct if bottom_to_current_pct > 0 else price_change_pct
    elif signal_type == "new_revival":
        display_price_change_pct = new_revival_price_change_pct
    else:
        display_price_change_pct = price_change_pct
    display_price_ready = new_revival_price_ready if signal_type == "new_revival" else price_ready
    reasons = [rule_reason]
    if not signal_type.startswith("drop_"):
        reasons.append(
            (
                f"底部反弹{display_price_change_pct:.1f}%>={BOTTOM_ABNORMAL_MIN_PRICE_UP_PCT:.1f}% (12根内低点→现价)"
                if display_price_ready
                else f"底部反弹{display_price_change_pct:.1f}%<{BOTTOM_ABNORMAL_MIN_PRICE_UP_PCT:.1f}% (12根内低点→现价)"
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
        "new_revival_price_confirmation_ready": new_revival_price_ready,
        "rebound_after_high_ready": rebound_ready,
        "rebound_after_high": rebound_after_high,
        "bottom_low_mcap": bottom_low_mcap,
        "required_bottom_low_mcap": BOTTOM_NEW_REVIVAL_MAX_LOW_MCAP_USD,
        "pool_confirmation_ready": pool_ready,
        "pool_liquidity_confirmation_ready": pool_liquidity_ready,
        "pool_ratio_confirmation_ready": pool_ratio_ready,
        "raw_kline_change_pct": price_change_pct,
        "bottom_to_current_pct": bottom_to_current_pct,
        "price_change_pct": display_price_change_pct,
        "required_price_change_pct": 0 if signal_type.startswith("drop_") else (BOTTOM_NEW_REVIVAL_MIN_PRICE_UP_PCT if signal_type == "new_revival" else BOTTOM_ABNORMAL_MIN_PRICE_UP_PCT),
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
        "top10_pct_delta": top10_pct_delta,
        "top10_current_pct": current_top10_pct,
        "top10_previous_pct": previous_top10_pct,
        "top20_pct_delta": top20_pct_delta,
        "top20_current_pct": current_top20_pct,
        "top20_previous_pct": previous_top20_pct,
        "top50_pct_delta": top50_pct_delta,
        "top50_current_pct": current_top50_pct,
        "top50_previous_pct": previous_top50_pct,
        "top100_pct_delta": top100_pct_delta,
        "top100_current_pct": current_top100_pct,
        "top100_previous_pct": previous_top100_pct,
        "new_holder_pct": holder_change["new_holder_pct"],
        "exited_holder_pct": holder_change["exited_holder_pct"],
        "netflow_usd": holder_change["netflow_usd"],
    }

def save_snapshot(
    scan_id: str,
    token: dict[str, Any],
    summary: dict[str, Any],
    holders: list[dict[str, Any]],
    analysis: dict[str, Any],
    top_profit_traders: list[dict[str, Any]] | None = None,
    top_loss_traders: list[dict[str, Any]] | None = None,
) -> int:
    address = token_address(token)
    ensure_bottom_top100_snapshot_comments()
    ensure_bottom_top100_trader_columns()

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO bottom_top100_snapshots (
                scan_id, chain, trend_interval, address, symbol, snapshot_ts,
                signal_type, signal_score, summary, holders, analysis, raw_token,
                top_profit_traders, top_loss_traders
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
            """,
            (
                scan_id,
                CHAIN,
                str(token.get("_trend_interval") or TREND_INTERVAL),
                address,
                token.get("symbol"),
                now_ts(),
                analysis.get("signal_type"),
                analysis.get("score", 0),
                Json(json_safe(summary)),
                Json(json_safe(holders)),
                Json(json_safe(analysis)),
                Json(json_safe(token)),
                Json(json_safe(top_profit_traders or [])),
                Json(json_safe(top_loss_traders or [])),
            ),
        )
        return int(cur.fetchone()[0])

    return db_op(_op)


def format_ts_text(ts: Any) -> str:
    ts_int = to_int(ts)
    if ts_int <= 0:
        return "未知"
    return datetime.fromtimestamp(ts_int).strftime("%Y-%m-%d %H:%M:%S")


def format_age_text(age_sec: Any) -> str:
    age = to_int(age_sec)
    if age <= 0:
        return "未知"
    if age >= 86400:
        return f"{age / 86400:.1f}天"
    if age >= 3600:
        return f"{age / 3600:.1f}小时"
    return f"{max(1, age // 60)}分钟"


def format_money_text(value: Any) -> str:
    amount = to_float(value)
    if amount <= 0:
        return "$0"
    return f"${amount:,.0f}"


def format_pct_text(value: Any, *, signed: bool = True) -> str:
    pct = to_float(value)
    prefix = "+" if signed and pct > 0 else ""
    return f"{prefix}{pct:.1f}%"


def short_text(value: Any, limit: int = 220) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."


def is_bottom_tg_extra(extra: dict[str, Any]) -> bool:
    signal_type = str((extra or {}).get("signal_type") or "")
    return signal_type in {
        "abnormal",
        "new_revival",
        "drop_50w",
        "drop_40w",
        "quiet_breakout",
        "quiet_runup",
        "ema_golden_cross",
    }


def format_bottom_tg_message(text: str, extra: dict[str, Any]) -> str:
    if not is_bottom_tg_extra(extra):
        return text

    address = str(extra.get("address") or "").strip() or extract_address_from_text(text)
    symbol = str(extra.get("symbol") or "UNKNOWN").strip() or "UNKNOWN"
    signal_type = str(extra.get("signal_type") or "")
    current_mcap = to_float(extra.get("current_mcap"))
    first_mcap = to_float(extra.get("first_signal_mcap")) or current_mcap
    first_ts = to_int(extra.get("first_signal_ts")) or to_int(extra.get("event_ts")) or now_ts()
    event_ts = to_int(extra.get("event_ts") or extra.get("signal_ts")) or now_ts()
    ath_mcap = to_float(extra.get("ath_mcap") or extra.get("peak_mcap"))
    post_peak_mcap = max(first_mcap, current_mcap, to_float(extra.get("post_signal_peak_mcap")))
    history_gain_pct = (
        to_float(extra.get("history_gain_pct"))
        if extra.get("history_gain_pct") is not None
        else ((post_peak_mcap - first_mcap) / first_mcap * 100) if first_mcap > 0 and post_peak_mcap > 0 else 0.0
    )
    liquidity = to_float(extra.get("pool_total_liquidity") or extra.get("liquidity") or extra.get("pool_liquidity"))
    pool_ratio = to_float(extra.get("pool_mcap_ratio"))
    pool_exchange = str(extra.get("pool_main_exchange") or "").strip()
    pool_count = to_int(extra.get("pool_count"))
    pool_label = pool_exchange or "未知"
    if pool_count > 1:
        pool_label = f"{pool_label}({pool_count}池)"
    narrative_type = short_text(extra.get("narrative_type"), 80) or "未分类"
    narrative_category = short_text(extra.get("narrative_category"), 20) or "其他"
    narrative_desc = short_text(extra.get("narrative_desc") or extra.get("narrative"), 120) or "暂无"
    signal_label = extra.get("signal_label") or f"{signal_type_text(signal_type)} ({signal_type})"
    risk_tags = extra.get("risk_tags") if isinstance(extra.get("risk_tags"), list) else []
    risk_text = " / ".join(str(item) for item in risk_tags) if risk_tags else "无明显风险标签"
    avoid_reasons = extra.get("avoid_reasons") if isinstance(extra.get("avoid_reasons"), list) else []
    avoid_text = "；".join(str(item) for item in avoid_reasons) if avoid_reasons else "无硬过滤项"
    ath_ratio = to_float(extra.get("ath_mcap_ratio"))

    return (
        f"底部异动 | ${symbol}\n"
        f"类型: {signal_label} | 档位: {extra.get('abnormal_rule') or '-'}\n"
        f"风险: {risk_text} | {avoid_text}\n"
        f"叙事: {narrative_category} | {narrative_type} | {narrative_desc}\n"
        f"当前市值: {format_money_text(current_mcap)} | 首次异动市值: {format_money_text(first_mcap)} | ATH/现值: {ath_ratio:.1f}x\n"
        f"首次异动时间: {format_ts_text(first_ts)}\n"
        f"相对首次异动涨幅: {format_pct_text(extra.get('first_signal_change_pct'))} | 涨幅: {format_pct_text(extra.get('price_change_pct') or extra.get('change_pct'))}\n"
        f"历史涨幅: {format_pct_text(history_gain_pct)} | 币龄: {format_age_text(extra.get('age_sec'))}\n"
        f"Top持仓变化: T10 {format_pct_text(to_float(extra.get('top10_pct_delta')) * 100)} | T20 {format_pct_text(to_float(extra.get('top20_pct_delta')) * 100)} | T50 {format_pct_text(to_float(extra.get('top50_pct_delta')) * 100)} | T100 {format_pct_text(to_float(extra.get('top100_pct_delta')) * 100)}\n"
        f"最高市值: {format_money_text(ath_mcap)} | 流动性: {format_money_text(liquidity)}\n"
        f"池子: {pool_label} | 池/市值: {pool_ratio:.1%}\n"
        f"异动时间: {format_ts_text(event_ts)}\n"
        f"CA: {address}\n"
        f"https://gmgn.ai/sol/token/{address}"
    )


def extract_address_from_text(text: str) -> str:
    match = SOL_CA_RE.search(text or "")
    return match.group(0) if match else ""


def post_push_track_key(address: str) -> str:
    return redis_key(POST_PUSH_REDIS_PREFIX, address)


def refresh_post_push_track_ttl(client: Any, key: str, state: dict[str, Any]) -> bool:
    if POST_PUSH_TRACK_TTL_SEC <= 0:
        return True
    created_ts = to_int((state or {}).get("created_ts"))
    if created_ts <= 0:
        client.expire(key, POST_PUSH_TRACK_TTL_SEC)
        return True
    remaining = created_ts + POST_PUSH_TRACK_TTL_SEC - now_ts()
    if remaining <= 0:
        client.delete(key)
        return False
    client.expire(key, int(remaining))
    return True


def post_push_track_age_sec(state: dict[str, Any], now_value: int | None = None) -> int:
    signal_ts = to_int((state or {}).get("signal_ts")) or to_int((state or {}).get("created_ts"))
    if signal_ts <= 0:
        return 0
    return max(0, int(now_value or now_ts()) - signal_ts)


def post_push_track_within_window(state: dict[str, Any], now_value: int | None = None) -> bool:
    if POST_PUSH_TRACK_TTL_SEC <= 0:
        return True
    age_sec = post_push_track_age_sec(state, now_value)
    return 0 < age_sec <= POST_PUSH_TRACK_TTL_SEC


def register_post_push_track(address: str, extra: dict[str, Any], message_id: int | str | None) -> None:
    if not POST_PUSH_REDIS_TRACK_ENABLED or not address or not message_id:
        return
    if extra.get("post_push_reply"):
        return
    signal_type = str(extra.get("signal_type") or "").strip()
    if not signal_type or signal_type == "watch":
        return
    current_mcap = to_float(extra.get("current_mcap"))
    if current_mcap <= 0:
        return
    client = get_redis_client()
    if client is None:
        return
    now_value = now_ts()
    signal_ts = to_int(extra.get("event_ts")) or now_value
    key = post_push_track_key(address)
    payload = {
        "address": address,
        "symbol": str(extra.get("symbol") or ""),
        "signal_type": signal_type,
        "chat_id": str(TG_CHAT_ID),
        "message_id": str(message_id),
        "entry_mcap": str(current_mcap),
        "peak_mcap": str(max(current_mcap, to_float(extra.get("post_signal_peak_mcap")))),
        "last_mcap": str(current_mcap),
        "signal_ts": str(signal_ts),
        "created_ts": str(now_value),
        "updated_ts": str(now_value),
        "last_reply_ts": "0",
        "reply_count": "0",
        "last_reply_bucket": "0",
        "last_gain_reply_ts": "0",
        "gain_reply_count": "0",
        "last_gain_bucket": "0",
        "last_loss_reply_ts": "0",
        "loss_reply_count": "0",
        "last_loss_bucket": "0",
        "dd_alert_sent": "0",
        "dd_alert_ts": "0",
        "dd_alert_message_id": "0",
    }
    try:
        client.hset(key, mapping=payload)
        refresh_post_push_track_ttl(client, key, payload)
    except Exception as exc:
        print(f"{address[:8]} post-push redis register failed: {exc}")


# ---------------------------------------------------------------------------
# Bottom live-track Redis helpers (24h frontend tracking)
# ---------------------------------------------------------------------------
def _bottom_live_track_key(address: str) -> str:
    return redis_key(BOTTOM_LIVE_TRACK_REDIS_PREFIX, address)


def _bottom_live_track_index_key() -> str:
    return redis_key(BOTTOM_LIVE_TRACK_REDIS_PREFIX, "__index__")


def start_bottom_live_tracking(
    address: str,
    symbol: str = "",
    entry_mcap: float = 0,
    entry_price: float = 0,
    signal_type: str = "",
    pool_liquidity: float = 0,
    created_ts: int = 0,
    launch_ts: int = 0,
    age_sec: int = 0,
    narrative_desc: str = "",
    narrative_type: str = "",
    narrative_category: str = "",
    winrate_prediction: dict[str, Any] | None = None,
) -> None:
    """Store a bottom-abnormal CA in Redis for the configured real-time tracking window."""
    if not BOTTOM_LIVE_TRACK_ENABLED or not address:
        return
    client = get_redis_client()
    if client is None:
        return
    payload = {
        "address": address,
        "chain": CHAIN,
        "symbol": symbol or "UNKNOWN",
        "signal_type": signal_type,
        "source": "bottom_abnormal",
        "entry_mcap": to_float(entry_mcap),
        "entry_price": to_float(entry_price),
        "created_ts": to_int(created_ts),
        "launch_ts": to_int(launch_ts),
        "age_sec": to_int(age_sec),
        "pushed_at": now_ts(),
        "current_mcap": to_float(entry_mcap),
        "current_price": to_float(entry_price),
        "peak_mcap": to_float(entry_mcap),
        "peak_mcap_at": now_ts(),
        "pool_liquidity": to_float(pool_liquidity),
        "narrative": short_text(narrative_desc, 180),
        "narrative_desc": short_text(narrative_desc, 180),
        "narrative_type": short_text(narrative_type, 80),
        "narrative_category": short_text(narrative_category, 40),
        "holders": 0,
        "volume_5m": 0,
        "volume_1h": 0,
        "pnl_pct": 0.0,
        "last_updated": now_ts(),
        "status": "tracking",
        "remove_reason": "",
    }
    if winrate_prediction:
        payload["winrate_prediction"] = winrate_prediction
    try:
        key = _bottom_live_track_key(address)
        client.setex(key, BOTTOM_LIVE_TRACK_TTL_SEC, json.dumps(payload, ensure_ascii=False))
        client.sadd(_bottom_live_track_index_key(), address)
        client.expire(_bottom_live_track_index_key(), BOTTOM_LIVE_TRACK_TTL_SEC)
        print(f"  [BottomLiveTrack] 开始实时追踪 ${symbol} {address[:8]}... signal={signal_type}")
    except Exception as exc:
        print(f"  [BottomLiveTrack] Redis写入失败 {address[:8]}: {exc}")


def _bottom_live_track_load_local(address: str) -> dict[str, Any]:
    client = get_redis_client()
    if client is None or not address:
        return {}
    try:
        raw = client.get(_bottom_live_track_key(address))
        if not raw:
            return {}
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _bottom_live_track_publish_update(item: dict[str, Any]) -> None:
    client = get_redis_client()
    if client is None or not item:
        return
    try:
        payload = json.dumps(
            {"ts": now_ts(), "items": [item], "track_ttl_sec": BOTTOM_LIVE_TRACK_TTL_SEC},
            ensure_ascii=False,
        )
        client.publish(BOTTOM_LIVE_TRACK_PUBSUB_CHANNEL, payload)
    except Exception as exc:
        print(f"  [BottomLiveTrack] DeepSeek update publish failed: {exc}")


def update_bottom_live_track_deepseek(address: str, extra: dict[str, Any]) -> None:
    if not BOTTOM_LIVE_TRACK_ENABLED or not address:
        return
    client = get_redis_client()
    if client is None:
        return
    current = _bottom_live_track_load_local(address)
    if not current:
        current = {
            "address": address,
            "chain": CHAIN,
            "symbol": str(extra.get("symbol") or "UNKNOWN"),
            "signal_type": str(extra.get("signal_type") or ""),
            "source": "bottom_abnormal",
            "entry_mcap": to_float(extra.get("current_mcap")),
            "entry_price": to_float(extra.get("price")),
            "created_ts": to_int(extra.get("created_ts")),
            "launch_ts": to_int(extra.get("launch_ts")),
            "age_sec": to_int(extra.get("created_age_sec") or extra.get("age_sec")),
            "pushed_at": to_int(extra.get("event_ts") or extra.get("signal_ts")) or now_ts(),
            "current_mcap": to_float(extra.get("current_mcap")),
            "current_price": to_float(extra.get("price")),
            "peak_mcap": to_float(extra.get("current_mcap")),
            "peak_mcap_at": now_ts(),
            "pool_liquidity": to_float(extra.get("liquidity") or extra.get("pool_total_liquidity")),
            "pnl_pct": 0.0,
            "status": "tracking",
            "remove_reason": "",
        }
    merged = {
        **current,
        "symbol": str(extra.get("symbol") or current.get("symbol") or "UNKNOWN"),
        "signal_type": str(extra.get("signal_type") or current.get("signal_type") or ""),
        "narrative": str(extra.get("narrative_desc") or extra.get("narrative") or current.get("narrative") or ""),
        "narrative_desc": str(extra.get("narrative_desc") or extra.get("narrative") or current.get("narrative_desc") or ""),
        "narrative_type": str(extra.get("narrative_type") or current.get("narrative_type") or ""),
        "narrative_category": str(extra.get("narrative_category") or current.get("narrative_category") or ""),
        "deepseek_kline_prediction": extra.get("deepseek_kline_prediction") or {},
        "deepseek_async_status": extra.get("deepseek_async_status") or "",
        "deepseek_async_elapsed_ms": to_int(extra.get("deepseek_async_elapsed_ms")),
        "winrate_prediction": extra.get("winrate_prediction") or {},
        "last_updated": now_ts(),
    }
    try:
        key = _bottom_live_track_key(address)
        ttl = client.ttl(key)
        if ttl is None or ttl <= 0:
            ttl = BOTTOM_LIVE_TRACK_TTL_SEC
        client.setex(key, int(ttl), json.dumps(json_safe(merged), ensure_ascii=False))
        client.sadd(_bottom_live_track_index_key(), address)
        client.expire(_bottom_live_track_index_key(), BOTTOM_LIVE_TRACK_TTL_SEC)
        _bottom_live_track_publish_update(merged)
    except Exception as exc:
        print(f"  [BottomLiveTrack] DeepSeek Redis update failed {address[:8]}: {exc}")


def _deepseek_async_key(address: str, signal_type: str, snapshot_id: int = 0) -> str:
    return f"{address}:{signal_type}:{snapshot_id or 0}"


def format_deepseek_async_reply(address: str, extra: dict[str, Any]) -> str:
    symbol = str(extra.get("symbol") or "UNKNOWN").strip() or "UNKNOWN"
    prediction = extra.get("deepseek_kline_prediction") if isinstance(extra.get("deepseek_kline_prediction"), dict) else {}
    purchase = prediction.get("purchase_value") if isinstance(prediction.get("purchase_value"), dict) else {}
    pattern_5m = prediction.get("pattern_5m") if isinstance(prediction.get("pattern_5m"), dict) else {}
    micro_1m = prediction.get("micro_1m") if isinstance(prediction.get("micro_1m"), dict) else {}
    forecast = prediction.get("forecast") if isinstance(prediction.get("forecast"), dict) else {}
    risks = prediction.get("risk_factors") if isinstance(prediction.get("risk_factors"), list) else []
    elapsed_sec = to_float(prediction.get("elapsed_ms") or extra.get("deepseek_async_elapsed_ms")) / 1000
    score = to_float(purchase.get("score_pct"))
    score_text = f"{score:.1f}%" if score > 0 else "-"
    return (
        f"DeepSeek K线补充 | ${symbol}\n"
        f"购买价值: {purchase.get('label') or '-'} | 评分: {score_text} | 置信: {prediction.get('confidence') or '-'}\n"
        f"AI偏向: {prediction.get('bias') or '-'} | 状态: {prediction.get('status') or '-'} | 耗时: {elapsed_sec:.1f}s\n"
        f"摘要: {short_text(prediction.get('summary'), 180) or '-'}\n"
        f"5m结构: {pattern_5m.get('label') or '-'} | 风险: {pattern_5m.get('risk_level') or '-'}\n"
        f"1m确认: {micro_1m.get('label') or '-'} | 决策: {micro_1m.get('decision') or '-'}\n"
        f"窗口: 5m {forecast.get('next_5m') or '-'} | 30m {forecast.get('next_30m') or '-'} | 4h {forecast.get('next_4h') or '-'}\n"
        f"风险: {short_text('；'.join(str(item) for item in risks if item), 180) or '-'}\n"
        f"CA: {address}"
    )


def _run_deepseek_post_push_analysis(
    *,
    key: str,
    token: dict[str, Any],
    summary: dict[str, Any],
    analysis: dict[str, Any],
    candles_5m: list[dict[str, Any]],
    candles_1m: list[dict[str, Any]],
    base_extra: dict[str, Any],
    signal_text: str,
    tg_message_id: int | None,
) -> None:
    address = token_address(token)
    signal_type = str((analysis or {}).get("signal_type") or base_extra.get("signal_type") or "")
    started = time.time()
    try:
        enriched_analysis = maybe_attach_deepseek_kline_prediction(
            token=token,
            summary=summary,
            analysis=analysis,
            candles_5m=candles_5m,
            candles_1m=candles_1m,
        )
        prediction = enriched_analysis.get("deepseek_kline_prediction") if isinstance(enriched_analysis, dict) else {}
        if not isinstance(prediction, dict) or not prediction:
            print(f"{address[:8]} deepseek async finished without prediction")
            return
        elapsed_ms = to_int(prediction.get("elapsed_ms")) or int((time.time() - started) * 1000)
        api_ready = is_deepseek_api_prediction(prediction)
        updated_extra = {
            **base_extra,
            "deepseek_kline_prediction": prediction,
            "kline_prediction_source_docs": prediction.get("source_docs") or [],
            "kline_prediction_summary": prediction.get("summary") or "",
            "deepseek_async_status": "ok" if api_ready else str(prediction.get("status") or "fallback"),
            "deepseek_async_elapsed_ms": elapsed_ms,
            "event_ts": base_extra.get("event_ts") or now_ts(),
        }
        updated_extra = enrich_signal_strategy_extra(updated_extra)
        try:
            update_top100_push_deepseek(
                address=address,
                signal_type=signal_type,
                extra=json_safe(updated_extra),
                text=signal_text,
                status="deepseek_update",
                source="bottom_abnormal",
                chain=CHAIN,
            )
        except Exception as exc:
            print(f"{address[:8]} deepseek async db update failed: {exc}")
        update_bottom_live_track_deepseek(address, updated_extra)
        update_text = format_deepseek_async_reply(address, updated_extra)
        publish_plugin_signal(update_text, "bottom_abnormal", status="deepseek_update", ca=address, extra=updated_extra)
        publish_tg_alert(update_text, "bottom_abnormal_followup", status="deepseek_update", ca=address, extra=updated_extra)
        if tg_message_id and api_ready:
            send_tg_reply(update_text, tg_message_id, updated_extra)
        print(
            f"{address[:8]} deepseek async update done: status={prediction.get('status')} "
            f"api={api_ready} elapsed={elapsed_ms}ms"
        )
    except Exception as exc:
        print(f"{address[:8]} deepseek async update exception: {exc}")
    finally:
        with _DEEPSEEK_ASYNC_LOCK:
            _DEEPSEEK_ASYNC_INFLIGHT.discard(key)


def schedule_deepseek_post_push_analysis(
    *,
    token: dict[str, Any],
    summary: dict[str, Any],
    analysis: dict[str, Any],
    candles_5m: list[dict[str, Any]],
    candles_1m: list[dict[str, Any]],
    base_extra: dict[str, Any],
    signal_text: str,
    tg_message_id: int | None = None,
) -> bool:
    if not BOTTOM_DEEPSEEK_ASYNC_ENABLED:
        return False
    signal_type = str((analysis or {}).get("signal_type") or base_extra.get("signal_type") or "")
    if not deepseek_signal_eligible(signal_type) or (analysis or {}).get("deepseek_kline_prediction"):
        return False
    address = token_address(token)
    if not address:
        return False
    key = _deepseek_async_key(address, signal_type, to_int((analysis or {}).get("snapshot_id") or base_extra.get("snapshot_id")))
    with _DEEPSEEK_ASYNC_LOCK:
        if key in _DEEPSEEK_ASYNC_INFLIGHT:
            return False
        _DEEPSEEK_ASYNC_INFLIGHT.add(key)
    _DEEPSEEK_ASYNC_EXECUTOR.submit(
        _run_deepseek_post_push_analysis,
        key=key,
        token=json_safe(token),
        summary=json_safe(summary),
        analysis=json_safe(analysis),
        candles_5m=json_safe(candles_5m or []),
        candles_1m=json_safe(candles_1m or []),
        base_extra=json_safe(base_extra or {}),
        signal_text=signal_text or "",
        tg_message_id=to_int(tg_message_id),
    )
    print(f"{address[:8]} deepseek async scheduled signal={signal_type} tg_reply={to_int(tg_message_id)}")
    return True


def send_tg_reply(text: str, reply_to_message_id: int, extra: dict[str, Any]) -> int | None:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        publish_tg_alert(text, "bottom_abnormal_followup", status="dry_run", chat_id=TG_CHAT_ID, extra=extra)
        return None
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
            json={
                "chat_id": TG_CHAT_ID,
                "text": text,
                "disable_web_page_preview": True,
                "reply_to_message_id": reply_to_message_id,
                "allow_sending_without_reply": True,
            },
            timeout=15,
        )
        if not resp.ok:
            print(f"tg reply failed: {resp.status_code} {resp.text[:200]}")
            publish_tg_alert(text, "bottom_abnormal_followup", status=f"failed_http_{resp.status_code}", chat_id=TG_CHAT_ID, extra=extra)
            return None
        payload = resp.json()
        message_id = payload.get("result", {}).get("message_id") if isinstance(payload, dict) else None
        publish_tg_alert(text, "bottom_abnormal_followup", status="sent", chat_id=TG_CHAT_ID, message_id=message_id, extra=extra)
        return int(message_id) if message_id else None
    except Exception as exc:
        print(f"tg reply exception: {exc}")
        publish_tg_alert(text, "bottom_abnormal_followup", status="exception", chat_id=TG_CHAT_ID, extra={**extra, "error": str(exc)})
        return None


def _first_number_by_keys(payload: Any, keys: tuple[str, ...]) -> float:
    if isinstance(payload, dict):
        for key in keys:
            if key in payload:
                value = to_float(payload.get(key))
                if value > 0:
                    return value
        for value in payload.values():
            found = _first_number_by_keys(value, keys)
            if found > 0:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = _first_number_by_keys(item, keys)
            if found > 0:
                return found
    return 0.0


def _first_value_by_keys(payload: Any, keys: tuple[str, ...]) -> Any:
    if isinstance(payload, dict):
        for key in keys:
            value = payload.get(key)
            if value not in (None, ""):
                return value
        for value in payload.values():
            found = _first_value_by_keys(value, keys)
            if found not in (None, ""):
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = _first_value_by_keys(item, keys)
            if found not in (None, ""):
                return found
    return None


def fetch_binance_token_meta(address: str) -> dict[str, Any]:
    if not address:
        return {}
    try:
        resp = requests.get(
            BINANCE_TOKEN_META_URL,
            params={"chainId": BINANCE_SOL_CHAIN_ID, "contractAddress": address},
            headers=BINANCE_HEADERS,
            timeout=12,
        )
        if not resp.ok:
            return {}
        data = resp.json().get("data") or {}
        if not isinstance(data, dict):
            return {}
        preview = data.get("previewLink") if isinstance(data.get("previewLink"), dict) else {}
        website = next(iter(preview.get("website") or []), "")
        twitter = next(iter(preview.get("x") or []), "")
        telegram = next(iter(preview.get("tg") or []), "")
        create_time = _first_value_by_keys(data, ("createTime", "createdTime", "created_at", "creationTimestamp"))
        return {
            "address": address,
            "token_address": address,
            "data_source": "binance_meta",
            "name": data.get("name") or "",
            "symbol": data.get("symbol") or "",
            "decimals": to_int(data.get("decimals")),
            "logo": data.get("icon") or data.get("logo") or "",
            "description": data.get("description") or "",
            "website": website,
            "twitter": twitter,
            "telegram": telegram,
            "creator": data.get("creatorAddress") or "",
            "created_at": parse_timestamp(create_time),
            "creation_timestamp": parse_timestamp(create_time),
            "token_created_at": parse_timestamp(create_time),
            "audit_info": data.get("auditInfo") if isinstance(data.get("auditInfo"), dict) else {},
        }
    except Exception as exc:
        print(f"{address[:8]} binance meta fetch failed: {exc}")
        return {}


def fetch_binance_token_metadata(address: str) -> dict[str, Any]:
    meta = fetch_binance_token_meta(address)
    dynamic = fetch_binance_dynamic_metrics(address)
    merged = dict(meta)
    for key, value in dynamic.items():
        if key not in merged or merged.get(key) in (None, "", 0, {}):
            merged[key] = value
    if meta or dynamic:
        merged["data_source"] = "binance_meta_dynamic"
    return merged


def fetch_binance_dynamic_metrics(address: str) -> dict[str, Any]:
    if not address:
        return {}
    try:
        resp = requests.get(
            BINANCE_DYNAMIC_URL,
            params={"chainId": BINANCE_SOL_CHAIN_ID, "contractAddress": address},
            headers=BINANCE_HEADERS,
            timeout=12,
        )
        if not resp.ok:
            return {}
        data = resp.json().get("data") or {}
        if not isinstance(data, dict):
            return {}
        market_cap = _first_number_by_keys(data, ("marketCap", "market_cap", "mcap", "fdv"))
        liquidity = _first_number_by_keys(
            data,
            ("liquidity", "liquidityUsd", "liquidity_usd", "poolLiquidity", "pool_liquidity", "totalLiquidity"),
        )
        price = _first_number_by_keys(data, ("price", "tokenPrice", "priceUsd", "price_usd"))
        fdv = _first_number_by_keys(data, ("fdv", "fullyDilutedValuation", "fully_diluted_valuation"))
        holders = to_int(_first_value_by_keys(data, ("holders", "holderCount", "holder_count")))
        launch_time = _first_value_by_keys(
            data,
            ("launchTime", "launch_time", "openTimestamp", "open_timestamp", "pairCreatedAt", "pair_created_at"),
        )
        created_time = _first_value_by_keys(
            data,
            ("createTime", "createdTime", "created_at", "creationTimestamp", "creation_timestamp"),
        )
        circulating_supply = _first_number_by_keys(data, ("circulatingSupply", "circulating_supply"))
        total_supply = _first_number_by_keys(data, ("totalSupply", "total_supply", "supply"))
        ath_mcap = _first_number_by_keys(
            data,
            ("historyHighestMarketCap", "history_highest_market_cap", "athMarketCap", "ath_market_cap"),
        )
        return {
            "address": address,
            "data_source": "binance_dynamic",
            "price": price,
            "market_cap": market_cap,
            "usd_market_cap": market_cap,
            "mcap": market_cap,
            "fdv": fdv or market_cap,
            "fully_diluted_valuation": fdv or market_cap,
            "liquidity": liquidity,
            "pool_liquidity": liquidity,
            "pool_mcap_ratio": liquidity / market_cap if liquidity > 0 and market_cap > 0 else 0.0,
            "holders": holders,
            "holder_count": holders,
            "stat": {"holder_count": holders},
            "volume_5m": _first_number_by_keys(data, ("volume5m", "volume_5m")),
            "volume_1h": _first_number_by_keys(data, ("volume1h", "volume_1h")),
            "volume_6h": _first_number_by_keys(data, ("volume6h", "volume_6h")),
            "volume_24h": _first_number_by_keys(data, ("volume24h", "volume_24h")),
            "symbol": _first_value_by_keys(data, ("symbol", "ticker")) or "",
            "name": _first_value_by_keys(data, ("name", "tokenName", "token_name")) or "",
            "open_timestamp": parse_timestamp(launch_time),
            "launch_timestamp": parse_timestamp(launch_time),
            "created_at": parse_timestamp(created_time) or parse_timestamp(launch_time),
            "creation_timestamp": parse_timestamp(created_time),
            "circulating_supply": circulating_supply,
            "total_supply": total_supply,
            "history_highest_market_cap": ath_mcap,
        }
    except Exception as exc:
        print(f"{address[:8]} binance dynamic fetch failed: {exc}")
        return {}


def apply_binance_dynamic_metrics(token: dict[str, Any], dynamic: dict[str, Any] | None = None) -> dict[str, Any]:
    """Overwrite analysis-critical token fields with Binance dynamic data."""
    if not isinstance(token, dict):
        return token
    if not isinstance(dynamic, dict) or not dynamic:
        dynamic = token.get("_binance_dynamic") or token.get("_binance_info") or {}
    if not isinstance(dynamic, dict) or not dynamic:
        return token

    market_cap = to_float(dynamic.get("market_cap") or dynamic.get("usd_market_cap") or dynamic.get("mcap"))
    if market_cap > 0:
        token["market_cap"] = market_cap
        token["usd_market_cap"] = market_cap
        token["mcap"] = market_cap

    fdv = to_float(dynamic.get("fdv") or dynamic.get("fully_diluted_valuation"))
    if fdv > 0:
        token["fdv"] = fdv
        token["fully_diluted_valuation"] = fdv
    elif market_cap > 0:
        token["fdv"] = market_cap
        token["fully_diluted_valuation"] = market_cap

    dynamic_numeric_keys = (
        "price",
        "liquidity",
        "pool_liquidity",
        "pool_mcap_ratio",
        "holder_count",
        "holders",
        "volume_5m",
        "volume_1h",
        "volume_6h",
        "volume_24h",
        "circulating_supply",
        "total_supply",
        "history_highest_market_cap",
        "open_timestamp",
        "launch_timestamp",
        "created_at",
        "creation_timestamp",
    )
    for key in dynamic_numeric_keys:
        value = to_float(dynamic.get(key))
        if value > 0:
            token[key] = value

    if to_int(token.get("holder_count")) > 0:
        stat = dict(token.get("stat") or {}) if isinstance(token.get("stat"), dict) else {}
        stat["holder_count"] = to_int(token.get("holder_count"))
        token["stat"] = stat

    for key in ("symbol", "name"):
        value = dynamic.get(key)
        if value and token.get(key) in (None, "", 0):
            token[key] = value

    token["_binance_dynamic"] = dynamic
    token["_binance_mcap_source"] = "binance_dynamic" if market_cap > 0 else token.get("_binance_mcap_source")
    return token


def parse_binance_kline_rows(raw: Any) -> list[dict[str, float]]:
    candles: list[dict[str, float]] = []
    for item in raw or []:
        try:
            if isinstance(item, list) and len(item) >= 6:
                ts = int(item[5] / 1000) if to_float(item[5]) > 10_000_000_000 else int(item[5])
                candles.append(
                    {
                        "ts": ts,
                        "open": float(item[0]),
                        "high": float(item[1]),
                        "low": float(item[2]),
                        "close": float(item[3]),
                        "volume": float(item[4]),
                    }
                )
            elif isinstance(item, dict):
                ts = to_int(item.get("time") or item.get("timestamp") or item.get("t"))
                ts = ts // 1000 if ts > 10_000_000_000 else ts
                close = to_float(item.get("close") or item.get("c"))
                if ts > 0 and close > 0:
                    candles.append(
                        {
                            "ts": ts,
                            "open": to_float(item.get("open") or item.get("o"), close),
                            "high": to_float(item.get("high") or item.get("h"), close),
                            "low": to_float(item.get("low") or item.get("l"), close),
                            "close": close,
                            "volume": to_float(item.get("volume") or item.get("v")),
                        }
                    )
        except (TypeError, ValueError):
            continue
    candles.sort(key=lambda candle: int(candle["ts"]))
    return candles


def fetch_binance_kline(address: str, interval: str = "5min", limit: int = 24) -> list[dict[str, float]]:
    if not address:
        return []
    try:
        resp = requests.get(
            BINANCE_KLINE_URL,
            params={"address": address, "platform": "solana", "interval": interval, "limit": limit, "pm": "p"},
            headers=BINANCE_HEADERS,
            timeout=15,
        )
        if not resp.ok:
            return []
        return parse_binance_kline_rows(resp.json().get("data"))
    except Exception as exc:
        print(f"{address[:8]} binance kline fetch failed: {exc}")
        return []


def _post_push_entry_dd_text(
    *,
    address: str,
    symbol: str,
    signal_type: str,
    entry_mcap: float,
    current_mcap: float,
    entry_loss_pct: float,
    elapsed_hours: float,
    pool_liquidity: float,
    pool_mcap_ratio: float,
    candles: list[dict[str, float]],
    kline_interval: str,
) -> str:
    recent = candles[-5:] if candles else []
    recent_volume = sum(to_float(candle.get("volume")) for candle in recent)
    last_close = to_float(recent[-1].get("close")) if recent else 0.0
    first_open = to_float(recent[0].get("open")) if recent else 0.0
    kline_change = (last_close / first_open - 1) * 100 if first_open > 0 and last_close > 0 else 0.0
    return (
        f"底部异动回撤区间触发 | ${symbol or 'UNKNOWN'}\n"
        f"信号类型: {signal_type or 'unknown'}\n"
        f"首推市值: {format_money_text(entry_mcap)}\n"
        f"当前市值: {format_money_text(current_mcap)}\n"
        f"相对首推回撤: -{entry_loss_pct:.1f}% "
        f"(目标 {POST_PUSH_ENTRY_DD_MIN_PCT:.0f}%-{POST_PUSH_ENTRY_DD_MAX_PCT:.0f}%)\n"
        f"池子流动性: {format_money_text(pool_liquidity)} | 池/市值: {pool_mcap_ratio:.1%}\n"
        f"近5根{kline_interval} K线: {kline_change:+.1f}% | 量: {recent_volume:,.0f}\n"
        f"异动至当前: {elapsed_hours:.2f}小时\n"
        f"CA: {address}\n"
        f"https://gmgn.ai/sol/token/{address}"
    )


def send_post_push_entry_drawdown_alert(
    address: str,
    state: dict[str, Any],
    current_mcap: float,
    pool_liquidity: float,
    pool_mcap_ratio: float,
    candles: list[dict[str, float]],
    *,
    source: str,
    kline_interval: str = POST_PUSH_KLINE_INTERVAL,
) -> int | None:
    entry_mcap = to_float(state.get("entry_mcap"))
    if entry_mcap <= 0 or current_mcap <= 0:
        return None
    entry_loss_pct = (1 - current_mcap / entry_mcap) * 100
    if entry_loss_pct < POST_PUSH_ENTRY_DD_MIN_PCT or entry_loss_pct > POST_PUSH_ENTRY_DD_MAX_PCT:
        return None
    elapsed_hours = post_push_track_age_sec(state) / 3600
    message_id = to_int(state.get("message_id"))
    if message_id <= 0:
        return None
    text = _post_push_entry_dd_text(
        address=address,
        symbol=str(state.get("symbol") or "UNKNOWN"),
        signal_type=str(state.get("signal_type") or ""),
        entry_mcap=entry_mcap,
        current_mcap=current_mcap,
        entry_loss_pct=entry_loss_pct,
        elapsed_hours=elapsed_hours,
        pool_liquidity=pool_liquidity,
        pool_mcap_ratio=pool_mcap_ratio,
        candles=candles,
        kline_interval=kline_interval,
    )
    extra = {
        "post_push_reply": True,
        "post_push_reply_kind": "entry_drawdown",
        "source": source,
        "address": address,
        "symbol": state.get("symbol") or "UNKNOWN",
        "signal_type": state.get("signal_type") or "",
        "entry_mcap": entry_mcap,
        "current_mcap": current_mcap,
        "entry_loss_pct": entry_loss_pct,
        "elapsed_hours": elapsed_hours,
        "signal_ts": to_int(state.get("signal_ts")) or to_int(state.get("created_ts")),
        "pool_liquidity": pool_liquidity,
        "pool_total_liquidity": pool_liquidity,
        "pool_mcap_ratio": pool_mcap_ratio,
        "kline_interval": kline_interval,
        "reply_to_message_id": message_id,
    }
    return send_tg_reply(text, message_id, extra)


def scan_post_push_entry_drawdowns_once() -> None:
    if not POST_PUSH_REDIS_TRACK_ENABLED:
        return
    client = get_redis_client()
    if client is None:
        return
    pattern = post_push_track_key("*")
    try:
        keys = list(client.scan_iter(match=pattern, count=100))
    except Exception as exc:
        print(f"post-push redis scan failed: {exc}")
        return
    for key in keys:
        try:
            state = client.hgetall(key)
            if not state or to_int(state.get("dd_alert_sent")):
                continue
            if not post_push_track_within_window(state):
                client.delete(key)
                continue
            address = str(state.get("address") or "").strip() or str(key).split(":")[-1]
            if not valid_sol_ca(address):
                continue
            dynamic = fetch_binance_dynamic_metrics(address)
            current_mcap = to_float(dynamic.get("market_cap"))
            if current_mcap <= 0:
                continue
            pool_liquidity = to_float(dynamic.get("pool_liquidity"))
            pool_mcap_ratio = to_float(dynamic.get("pool_mcap_ratio"))
            candles = fetch_binance_kline(address, interval=POST_PUSH_KLINE_INTERVAL, limit=24)
            sent_message_id = send_post_push_entry_drawdown_alert(
                address,
                state,
                current_mcap,
                pool_liquidity,
                pool_mcap_ratio,
                candles,
                source="redis_poll",
                kline_interval=POST_PUSH_KLINE_INTERVAL,
            )
            updates = {
                "last_mcap": str(current_mcap),
                "updated_ts": str(now_ts()),
                "last_binance_pool_liquidity": str(pool_liquidity),
                "last_binance_pool_mcap_ratio": str(pool_mcap_ratio),
            }
            if sent_message_id:
                updates.update(
                    {
                        "dd_alert_sent": "1",
                        "dd_alert_ts": str(now_ts()),
                        "dd_alert_message_id": str(sent_message_id),
                    }
                )
            client.hset(key, mapping=updates)
            refresh_post_push_track_ttl(client, key, state)
        except Exception as exc:
            print(f"post-push redis poll item failed: {exc}")


def post_push_entry_drawdown_monitor_loop() -> None:
    while True:
        scan_post_push_entry_drawdowns_once()
        time.sleep(max(10, POST_PUSH_POLL_INTERVAL_SEC))


def start_post_push_entry_drawdown_monitor() -> None:
    global _POST_PUSH_MONITOR_STARTED
    if _POST_PUSH_MONITOR_STARTED or not POST_PUSH_REDIS_TRACK_ENABLED:
        return
    _POST_PUSH_MONITOR_STARTED = True
    threading.Thread(target=post_push_entry_drawdown_monitor_loop, daemon=True).start()


def maybe_reply_post_push_drawdown(token: dict[str, Any], summary: dict[str, Any], analysis: dict[str, Any]) -> None:
    if not POST_PUSH_REDIS_TRACK_ENABLED:
        return
    address = token_address(token)
    if not address:
        return
    client = get_redis_client()
    if client is None:
        return
    key = post_push_track_key(address)
    try:
        state = client.hgetall(key)
    except Exception as exc:
        print(f"{address[:8]} post-push redis read failed: {exc}")
        return
    if not state:
        return
    if not post_push_track_within_window(state):
        try:
            client.delete(key)
        except Exception:
            pass
        return

    current_mcap = to_float(analysis.get("current_mcap")) or to_float(summary.get("mcap")) or calc_mcap(token)
    if current_mcap <= 0:
        return
    entry_mcap = to_float(state.get("entry_mcap"))
    previous_peak = to_float(state.get("peak_mcap"))
    peak_mcap = max(previous_peak, current_mcap, entry_mcap)
    now_value = now_ts()
    updates = {"last_mcap": str(current_mcap), "peak_mcap": str(peak_mcap), "updated_ts": str(now_value)}

    if entry_mcap <= 0 or peak_mcap <= 0:
        try:
            client.hset(key, mapping=updates)
        except Exception:
            pass
        return
    entry_loss_pct = (1 - current_mcap / entry_mcap) * 100 if current_mcap < entry_mcap else 0.0
    loss_reply_count = to_int(state.get("loss_reply_count"))
    last_loss_reply_ts = to_int(state.get("last_loss_reply_ts"))
    last_loss_bucket = to_int(state.get("last_loss_bucket"))
    dd_alert_sent = to_int(state.get("dd_alert_sent"))

    loss_bucket = int(entry_loss_pct // POST_PUSH_LOSS_REPLY_PCT) if POST_PUSH_LOSS_REPLY_PCT > 0 else 0
    reply_kind = ""
    bucket = 0
    if (
        POST_PUSH_LOSS_REPLY_PCT > 0
        and entry_loss_pct >= POST_PUSH_LOSS_REPLY_PCT
        and entry_loss_pct <= POST_PUSH_ENTRY_DD_MAX_PCT
        and not dd_alert_sent
        and loss_bucket > last_loss_bucket
        and loss_reply_count < POST_PUSH_MAX_REPLIES
        and (now_value - last_loss_reply_ts) >= POST_PUSH_REPLY_COOLDOWN_SEC
    ):
        reply_kind = "entry_drawdown"
        bucket = loss_bucket

    if not reply_kind:
        try:
            client.hset(key, mapping=updates)
            refresh_post_push_track_ttl(client, key, state)
        except Exception as exc:
            print(f"{address[:8]} post-push redis update failed: {exc}")
        return

    message_id = to_int(state.get("message_id"))
    if message_id <= 0:
        return
    if reply_kind == "entry_drawdown":
        dynamic = fetch_binance_dynamic_metrics(address)
        binance_mcap = to_float(dynamic.get("market_cap")) or current_mcap
        pool_liquidity = to_float(dynamic.get("pool_liquidity")) or to_float(summary.get("pool", {}).get("total_liquidity"))
        pool_mcap_ratio = to_float(dynamic.get("pool_mcap_ratio")) or to_float(summary.get("pool", {}).get("liquidity_mcap_ratio"))
        candles = fetch_binance_kline(address, interval=POST_PUSH_KLINE_INTERVAL, limit=24)
        sent_message_id = send_post_push_entry_drawdown_alert(
            address,
            state,
            binance_mcap,
            pool_liquidity,
            pool_mcap_ratio,
            candles,
            source="scan_followup",
            kline_interval=POST_PUSH_KLINE_INTERVAL,
        )
        if sent_message_id:
            updates.update(
                {
                    "last_loss_reply_ts": str(now_value),
                    "loss_reply_count": str(loss_reply_count + 1),
                    "last_loss_bucket": str(bucket),
                    "last_loss_reply_message_id": str(sent_message_id),
                    "dd_alert_sent": "1",
                    "dd_alert_ts": str(now_value),
                    "dd_alert_message_id": str(sent_message_id),
                }
            )
        try:
            client.hset(key, mapping=updates)
            refresh_post_push_track_ttl(client, key, state)
        except Exception as exc:
            print(f"{address[:8]} post-push redis entry-dd update failed: {exc}")
        return
    return
    symbol = token.get("symbol") or state.get("symbol") or "UNKNOWN"
    title_map = {
        "entry_drawdown": "底部异动回撤区间触发",
        "gain": "底部异动后盈利跟踪",
        "drawdown": "底部异动后高点回撤观察",
        "loss": "底部异动后未盈利回撤观察",
    }
    text = (
        f"{title_map.get(reply_kind, '底部异动后跟踪')} | ${symbol}\n"
        f"首推市值: {format_money_text(entry_mcap)}\n"
        f"推送后高点: {format_money_text(peak_mcap)} (+{peak_gain_pct:.1f}%)\n"
        f"当前市值: {format_money_text(current_mcap)}\n"
        f"相对首推: {format_pct_text((current_mcap / entry_mcap - 1) * 100)}\n"
        f"高点回撤: {drawdown_pct:.1f}% | 信号后下跌: {entry_loss_pct:.1f}%\n"
        f"CA: {address}\n"
        f"https://gmgn.ai/sol/token/{address}"
    )
    reply_extra = {
        "post_push_reply": True,
        "post_push_reply_kind": reply_kind,
        "address": address,
        "symbol": symbol,
        "signal_type": state.get("signal_type") or analysis.get("signal_type"),
        "entry_mcap": entry_mcap,
        "peak_mcap": peak_mcap,
        "current_mcap": current_mcap,
        "peak_gain_pct": peak_gain_pct,
        "drawdown_pct": drawdown_pct,
        "entry_loss_pct": entry_loss_pct,
        "reply_to_message_id": message_id,
        "reply_count": reply_count + 1 if reply_kind == "drawdown" else reply_count,
        "gain_reply_count": gain_reply_count + 1 if reply_kind == "gain" else gain_reply_count,
        "loss_reply_count": loss_reply_count + 1 if reply_kind in {"loss", "entry_drawdown"} else loss_reply_count,
    }
    sent_message_id = send_tg_reply(text, message_id, reply_extra)
    if sent_message_id:
        if reply_kind == "gain":
            updates.update(
                {
                    "last_gain_reply_ts": str(now_value),
                    "gain_reply_count": str(gain_reply_count + 1),
                    "last_gain_bucket": str(bucket),
                    "last_gain_reply_message_id": str(sent_message_id),
                }
            )
        elif reply_kind in {"loss", "entry_drawdown"}:
            updates.update(
                {
                    "last_loss_reply_ts": str(now_value),
                    "loss_reply_count": str(loss_reply_count + 1),
                    "last_loss_bucket": str(bucket),
                    "last_loss_reply_message_id": str(sent_message_id),
                    "dd_alert_sent": "1",
                    "dd_alert_ts": str(now_value),
                    "dd_alert_message_id": str(sent_message_id),
                }
            )
        else:
            updates.update(
                {
                    "last_reply_ts": str(now_value),
                    "reply_count": str(reply_count + 1),
                    "last_reply_bucket": str(bucket),
                    "last_reply_message_id": str(sent_message_id),
                }
            )
    try:
        client.hset(key, mapping=updates)
        refresh_post_push_track_ttl(client, key, state)
    except Exception as exc:
        print(f"{address[:8]} post-push redis reply update failed: {exc}")


def send_tg(text: str, extra: dict[str, Any] | None = None) -> int | None:
    extra = extra or {}
    if not extra.get("event_ts"):
        extra = {**extra, "event_ts": now_ts()}
    extra = enrich_signal_strategy_extra(extra)
    address = str(extra.get("address") or "").strip() or extract_address_from_text(text)
    try:
        tg_text = format_bottom_tg_message(text, extra)
    except Exception as exc:
        print(f"tg format exception: {exc}")
        extra = {**extra, "format_error": str(exc)}
        tg_text = text
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        publish_tg_alert(tg_text, "bottom_abnormal", status="dry_run", ca=address, chat_id=TG_CHAT_ID, extra=extra)
        return None
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": tg_text, "disable_web_page_preview": True},
            timeout=15,
        )
        if not resp.ok:
            print(f"tg failed: {resp.status_code} {resp.text[:200]}")
            publish_tg_alert(tg_text, "bottom_abnormal", status=f"failed_http_{resp.status_code}", ca=address, chat_id=TG_CHAT_ID, extra=extra)
            return None
        payload = resp.json()
        message_id = payload.get("result", {}).get("message_id") if isinstance(payload, dict) else None
        publish_tg_alert(tg_text, "bottom_abnormal", status="sent", ca=address, chat_id=TG_CHAT_ID, message_id=message_id, extra=extra)
        register_post_push_track(address, extra, message_id)
        # Live-track: push to Redis for frontend real-time dashboard
        start_bottom_live_tracking(
            address=address,
            symbol=str(extra.get("symbol") or ""),
            entry_mcap=to_float(extra.get("current_mcap")),
            entry_price=to_float(extra.get("price")),
            signal_type=str(extra.get("signal_type") or ""),
            pool_liquidity=to_float(extra.get("liquidity") or extra.get("pool_total_liquidity")),
            created_ts=to_int(extra.get("created_ts")),
            launch_ts=to_int(extra.get("launch_ts")),
            age_sec=to_int(extra.get("created_age_sec") or extra.get("age_sec")),
            narrative_desc=str(extra.get("narrative_desc") or extra.get("narrative") or ""),
            narrative_type=str(extra.get("narrative_type") or ""),
            narrative_category=str(extra.get("narrative_category") or ""),
            winrate_prediction=extra.get("winrate_prediction") or compute_historical_winrate_prediction(extra),
        )
        return int(message_id) if message_id else None
    except Exception as exc:
        print(f"tg exception: {exc}")
        publish_tg_alert(tg_text, "bottom_abnormal", status="exception", ca=address, chat_id=TG_CHAT_ID, extra={**extra, "error": str(exc)})
        return None


def compute_risk_tags(extra: dict[str, Any]) -> list[str]:
    """Classify a push signal with risk tags based on known failure patterns."""
    tags = []
    mcap = to_float(extra.get("current_mcap", 0))
    ath = to_float(extra.get("ath_mcap", 0))
    price_change = to_float(extra.get("price_change_pct", 0))
    volume = to_float(extra.get("breakout_volume_usd", 0) or extra.get("volume_usd", 0))

    # Transient: signal change_pct > 50% == likely already peaked
    if price_change > 50:
        tags.append("瞬爆")

    # Ceiling: ATH < 1.5x current mcap == no room to grow
    ath_ratio = ath / max(1, mcap) if ath and mcap else 0
    if 0 < ath_ratio < 1.5:
        tags.append("天花板")

    # Large mcap: > $500K = too expensive to pump
    if mcap > 500_000:
        tags.append("大市值")

    # Dead volume: no meaningful trading behind the signal
    if 0 < volume < 10_000:
        tags.append("无量")

    # Positive tags
    if 30_000 <= mcap < 120_000:
        tags.append("黄金区间")

    return tags


def compute_strategy_profile(extra: dict[str, Any], risk_tags: list[str] | None = None) -> dict[str, Any]:
    """Build strategy-facing fields from the data-driven guide."""
    risk_tags = risk_tags if risk_tags is not None else compute_risk_tags(extra or {})
    signal_type = str((extra or {}).get("signal_type") or "")
    mcap = to_float((extra or {}).get("current_mcap", 0))
    ath = to_float((extra or {}).get("ath_mcap", 0))
    ath_ratio = ath / max(1, mcap) if ath and mcap else 0.0

    if signal_type == "new_revival" and mcap < 120_000:
        profile = "优先观察"
    elif signal_type == "quiet_breakout":
        profile = "低优先级"
    elif "瞬爆" in risk_tags:
        profile = "快峰风险"
    elif mcap > 500_000:
        profile = "高市值谨慎"
    else:
        profile = "回调观察"

    avoid_reasons: list[str] = []
    if "瞬爆" in risk_tags:
        avoid_reasons.append("5m内冲顶，高位延续风险")
    if "大市值" in risk_tags:
        avoid_reasons.append(">$500K拉盘成本高")
    if "天花板" in risk_tags:
        avoid_reasons.append("ATH/现值<1.5x")
    if "无量" in risk_tags:
        avoid_reasons.append("量能<$10K")
    if signal_type == "quiet_breakout":
        avoid_reasons.append("quiet_breakout历史样本弱")

    if profile in {"低优先级", "快峰风险", "高市值谨慎"}:
        action_hint = "第一波高位延续风险，观察回踩和二次放量确认"
    elif profile == "优先观察":
        action_hint = "观察-5%~-15%回调区和止跌结构"
    else:
        action_hint = "观察-5%~-15%回调，未回调则高位延续风险仍在"

    return {
        "signal_label": f"{signal_type_text(signal_type)} ({signal_type})" if signal_type else "未知",
        "strategy_profile": profile,
        "strategy_action": action_hint,
        "entry_watch_zone": "-5%~-15%",
        "hard_risk_line": "-35%",
        "hold_watch_window": "至少1h，1-4h较优",
        "fast_peak_rule": "5m内到峰按瞬爆处理",
        "ath_mcap_ratio": ath_ratio,
        "avoid_reasons": avoid_reasons,
    }


def _pct_value(value: Any) -> float:
    """Normalize a ratio-or-percent value to percent units."""
    raw = to_float(value)
    return raw * 100 if 0 < abs(raw) <= 1 else raw


def _bounded_winrate(value: float) -> float:
    return max(10.0, min(85.0, value))


def _round_price(value: float) -> float:
    if value <= 0:
        return 0.0
    return round(value, 12)


def _mcap_bucket(mcap: float) -> str:
    if mcap < 50_000:
        return "<50K"
    if mcap < 100_000:
        return "50K-100K"
    if mcap < 300_000:
        return "100K-300K"
    return "300K+"


def _historical_bottom_bucket(signal_type: str, mcap: float) -> dict[str, Any]:
    if signal_type == "watchlist_abnormal":
        signal_type = "abnormal"
    bucket = _mcap_bucket(mcap)
    peak_window = {
        "new_revival": "P25 10min / Median 90min / P75 410min",
        "abnormal": "P25 20min / Median 150min / P75 620min",
    }.get(signal_type, "-")
    table: dict[tuple[str, str], dict[str, Any]] = {
        ("abnormal", "<50K"): {
            "wr20": 50.0, "wr50": 35.0, "wr100": 23.0, "samples": 26,
            "avg_peak": "+23%", "avg_pnl": "固定时间PnL不作主指标",
            "entry_window": "1-4h确认窗口；仅后4h暴涨/强涨守住才提高可信度",
            "exit_window": "以+30/+50/+100%到达率和回吐速度判断",
            "priority": "低", "buy_value": "低价值观察",
            "timing_note": "v3显示 abnormal <50K 是弱档，除非前置底部反弹启动且后4h确认走强。",
            "kline_forecast": "基础峰值偏弱；若后4h温和上涨或持续阴跌，WR20会明显下修。",
        },
        ("abnormal", "50K-100K"): {
            "wr20": 61.0, "wr50": 39.0, "wr100": 24.0, "samples": 38,
            "avg_peak": "+38%", "avg_pnl": "看确认后涨幅到达",
            "entry_window": "15-30min先看结构，1-4h确认是否守住",
            "exit_window": "冲高后主动看回吐，固定时间持有容易失真",
            "priority": "中", "buy_value": "低到中价值观察",
            "timing_note": "v3中 abnormal 50K-100K WR20=61%，需要叠加后4h走势过滤。",
            "kline_forecast": "高位加速拉升或强涨守住可加分；温和上涨/持续阴跌降级。",
        },
        ("abnormal", "100K-300K"): {
            "wr20": 66.0, "wr50": 39.0, "wr100": 22.0, "samples": 59,
            "avg_peak": "+33%", "avg_pnl": "看确认后涨幅到达",
            "entry_window": "15-30min识别高位加速，1-4h确认暴涨/强涨守住",
            "exit_window": "以峰值分段观察，避免固定时间口径失真",
            "priority": "中高", "buy_value": "短线高弹性观察",
            "timing_note": "v3中 abnormal 100K-300K WR20=66%，高于<50K但仍弱于new_revival。",
            "kline_forecast": "后4h暴涨/强涨守住是核心加分；持续阴跌WR20约26%。",
        },
        ("abnormal", "300K+"): {
            "wr20": 60.0, "wr50": 30.0, "wr100": 22.0, "samples": 40,
            "avg_peak": "+34%", "avg_pnl": "不作主策略",
            "entry_window": "不作为主确认窗口",
            "exit_window": "仅观察，除非K线和量能明显超预期",
            "priority": "低", "buy_value": "低价值",
            "timing_note": "v3显示 abnormal 300K+ 中位峰值仅+34%，拉盘弹性有限。",
            "kline_forecast": "高市值需要更强量能和更低持仓集中度，否则回吐风险高。",
        },
        ("new_revival", "<50K"): {
            "wr20": 77.0, "wr50": 39.0, "wr100": 25.0, "samples": 69,
            "avg_peak": "+34%", "avg_pnl": "看涨幅到达，不看固定持有PnL",
            "entry_window": "0-30min先判断；底部结构可等1-2h回调确认",
            "exit_window": "分段看+30/+50/+100%，固定时间口径易回吐",
            "priority": "高", "buy_value": "高价值观察",
            "timing_note": "v3显示 new_revival <50K WR20=77%；峰值中位90min，8h后多数行情已结束。",
            "kline_forecast": "底部持续下跌/底部横盘更优；后4h暴涨或冲高急跌分支WR20接近100%。",
        },
        ("new_revival", "50K-100K"): {
            "wr20": 71.0, "wr50": 55.0, "wr100": 29.0, "samples": 31,
            "avg_peak": "+65%", "avg_pnl": "看涨幅到达，不看固定持有PnL",
            "entry_window": "0-30min判断第一段，1-2h看回调/企稳",
            "exit_window": "分段观察峰值，峰值后回吐快",
            "priority": "高", "buy_value": "高价值观察",
            "timing_note": "v3显示 new_revival 50K-100K WR50=55%，中位峰值+65%，属于质量较高档。",
            "kline_forecast": "若前置底部结构配合后4h暴涨，+50%到达率更有参考价值。",
        },
        ("new_revival", "100K-300K"): {
            "wr20": 77.0, "wr50": 52.0, "wr100": 35.0, "samples": 52,
            "avg_peak": "+65%", "avg_pnl": "看涨幅到达，不看固定持有PnL",
            "entry_window": "0-30min看第一段，1-2h看回调/企稳，不能等8h",
            "exit_window": "分段观察峰值，避免峰值后利润回吐",
            "priority": "高", "buy_value": "高价值观察",
            "timing_note": "v3修正：new_revival 100K-300K WR20=77%，不是低价值；但峰值中位仍约90min。",
            "kline_forecast": "高点下跌回落/底部持续下跌后若转暴涨，是重点跟踪结构。",
        },
    }
    default = {
        "wr20": 0.0, "wr50": 0.0, "wr100": 0.0, "samples": 0,
        "avg_peak": "-", "avg_pnl": "-",
        "entry_window": "文档无该组合主策略",
        "exit_window": "仅观察",
        "priority": "低", "buy_value": "回避/仅观察",
        "timing_note": "09-bar-level-strategy.md 未将该组合列为有效主策略。",
        "kline_forecast": "缺少v3统计映射，仅保留观察。",
        "peak_window": peak_window,
    }
    if signal_type in {"quiet_runup", "quiet_breakout"}:
        return {
            **default,
            "wr20": 0.0 if signal_type == "quiet_runup" else 0.0,
            "buy_value": "回避/仅观察",
            "timing_note": "v3不碰清单包含 quiet_runup，推送时通常已明显拉升。",
            "kline_forecast": "追高失效风险高，不纳入底部异动主策略。",
        }
    return {**default, **table.get((signal_type, bucket), {}), "peak_window": peak_window}


def compute_historical_strategy_plan(extra: dict[str, Any], risk_factors: list[str] | None = None) -> dict[str, Any]:
    """Build timing and profit context from onchain_trading_guides/09-bar-level-strategy.md."""
    signal_mcap = to_float(extra.get("current_mcap") or extra.get("entry_mcap"))
    signal_price = to_float(extra.get("price") or extra.get("entry_price"))
    signal_ts = to_int(extra.get("event_ts") or extra.get("signal_ts") or extra.get("pushed_at") or now_ts())
    signal_type = str(extra.get("signal_type") or "unknown")
    bucket = _historical_bottom_bucket(signal_type, signal_mcap)
    primary_dd = -10.0
    balanced_dd = -20.0
    deep_dd = -35.0

    def level(drawdown_pct: float) -> dict[str, Any]:
        multiplier = 1 + drawdown_pct / 100.0
        return {
            "drawdown_pct": drawdown_pct,
            "mcap": round(signal_mcap * multiplier, 2) if signal_mcap > 0 else 0,
            "price": _round_price(signal_price * multiplier) if signal_price > 0 else 0,
        }

    risk_points = list(risk_factors or [])
    risk_points.extend([
        "09策略显示峰值中位时间较短：new_revival约90min，abnormal约150min",
        "固定时间观察历史收益弱，后续判断必须结合涨幅到达率和回撤速度",
        "若推送时已大幅拉升或池/市值过低，WR会被明显折损",
    ])

    return {
        "source_doc": "onchain_trading_guides/09-bar-level-strategy.md",
        "winner_definition": "观察后任意时间价格涨到>=20%",
        "signal_bucket": _mcap_bucket(signal_mcap),
        "buy_value_label": bucket["buy_value"],
        "priority": bucket["priority"],
        "historical_avg_pnl": bucket["avg_pnl"],
        "historical_avg_peak": bucket["avg_peak"],
        "peak_time_window": bucket.get("peak_window", "-"),
        "kline_forecast": bucket.get("kline_forecast", ""),
        "strategy_winrate_summary": (
            f"WR20 {to_float(bucket['wr20']):.0f}% / "
            f"WR50 {to_float(bucket['wr50']):.0f}% / "
            f"WR100 {to_float(bucket['wr100']):.0f}%"
        ),
        "entry_window": bucket["entry_window"],
        "exit_window": bucket["exit_window"],
        "timing_note": bucket["timing_note"],
        "entry": {
            "method": "guide_timing_window",
            "valid_from_ts": signal_ts,
            "expires_at_ts": signal_ts + 12 * 3600,
            "primary": {
                **level(primary_dd),
                "label": "轻回撤观察",
                "historical_trigger_rate_pct": 0,
                "historical_winner_rate_pct": bucket["wr20"],
            },
            "balanced": {
                **level(balanced_dd),
                "label": "中回撤观察",
                "historical_trigger_rate_pct": 0,
                "historical_winner_rate_pct": bucket["wr20"],
            },
            "deep": {
                **level(deep_dd),
                "label": "深回撤风险观察",
                "historical_trigger_rate_pct": 0,
                "historical_winner_rate_pct": bucket["wr20"],
            },
            "entry_time_rule": bucket["entry_window"],
        },
        "exit": {
            "quick_review_ts": signal_ts + 30 * 60,
            "median_peak_ts": signal_ts + (190 * 60 if signal_type == "new_revival" else 400 * 60),
            "time_stop_ts": signal_ts + 12 * 3600,
            "tp1_pct": 30,
            "tp1_close_pct": 50,
            "tp2_pct": 50,
            "tp2_close_pct": 30,
            "runner_close_pct": 20,
            "trailing_from_peak_drawdown_pct": 15,
            "exit_time_rule": bucket["exit_window"],
        },
        "risk_points": risk_points[:8],
    }


def is_deepseek_api_prediction(prediction: dict[str, Any] | None) -> bool:
    if not isinstance(prediction, dict) or not prediction.get("ready"):
        return False
    status = str(prediction.get("status") or "").lower()
    model = str(prediction.get("model") or "").lower()
    return status == "ok" and model and not model.startswith("local_")


def compute_historical_winrate_prediction(extra: dict[str, Any] | None) -> dict[str, Any]:
    """
    Estimate the probability that a bottom-abnormal CA reaches the historical
    WR20 threshold from onchain_trading_guides/09-bar-level-strategy.md.
    This is a data-derived observation score, not trading advice.
    """
    extra = dict(extra or {})
    signal_type = str(extra.get("signal_type") or "unknown")
    mcap = to_float(extra.get("current_mcap") or extra.get("entry_mcap"))
    entry_mcap = to_float(extra.get("entry_mcap") or extra.get("first_signal_mcap"))
    peak_mcap = to_float(extra.get("peak_mcap") or extra.get("post_signal_peak_mcap"))
    bucket = _historical_bottom_bucket(signal_type, mcap)
    score = float(bucket["wr20"])
    sample_count = int(bucket["samples"])
    evidence = [
        f"{signal_type or 'unknown'} × {_mcap_bucket(mcap)} 09策略 WR20 {bucket['wr20']:.1f}%",
        f"09策略中位峰值 {bucket['avg_peak']}，峰值窗口 {bucket.get('peak_window') or '-'}",
    ]
    risk_factors: list[str] = []

    top10 = _pct_value(extra.get("top10_current_pct"))
    if top10 > 0:
        if top10 < 15:
            score += 8
            evidence.append("Top10<15%，历史样本中更偏分散")
        elif top10 <= 30:
            score += 2
            evidence.append("Top10在15%-30%正常区间")
        elif top10 <= 50:
            score -= 8
            risk_factors.append("Top10持仓30%-50%，集中度偏高")
        else:
            score -= 10
            risk_factors.append("Top10持仓>50%，高度集中")

    accumulation = _pct_value(extra.get("accumulation_pct_delta"))
    distribution = _pct_value(extra.get("distribution_pct_delta"))
    if accumulation > 2:
        score += 5
        evidence.append("Top100吸筹delta>2%，符合历史吸筹正向特征")
    elif accumulation < 0:
        score -= 6
        risk_factors.append("Top100吸筹delta为负")
    if distribution > accumulation and distribution > 2:
        score -= 5
        risk_factors.append("Top100减持强于增持")

    if mcap > 0:
        if mcap < 30_000:
            score -= 6
            risk_factors.append("当前市值<30K，深度不足")
        elif signal_type == "new_revival" and mcap < 300_000:
            score += 3
            evidence.append("new_revival<300K 在09策略中WR20均>=71%")
        elif signal_type == "abnormal" and 100_000 <= mcap < 300_000:
            score += 3
            evidence.append("abnormal 100K-300K 是09策略中相对较优档")
        elif signal_type == "abnormal" and mcap < 50_000:
            score -= 6
            risk_factors.append("09策略显示 abnormal <50K WR20仅50%，属于弱档")
        elif mcap >= 300_000:
            score -= 8
            risk_factors.append("市值>=300K，弹性和回吐风险需要更严格过滤")

    if entry_mcap > 0 and mcap > 0:
        pnl_from_signal = (mcap - entry_mcap) / entry_mcap * 100
        if pnl_from_signal <= -50:
            score -= 24
            risk_factors.append(f"相对推送市值已跌{abs(pnl_from_signal):.1f}%，疑似弱势失效")
        elif pnl_from_signal <= -35:
            score -= 16
            risk_factors.append(f"相对推送市值已跌{abs(pnl_from_signal):.1f}%，处于深回撤风险区")
        elif pnl_from_signal >= 80:
            score -= 12
            risk_factors.append(f"相对推送市值已涨{pnl_from_signal:.1f}%，追高回吐风险高")
        elif pnl_from_signal >= 30:
            score -= 6
            risk_factors.append(f"相对推送市值已涨{pnl_from_signal:.1f}%，部分涨幅目标已兑现")

    if peak_mcap > 0 and mcap > 0 and entry_mcap > 0:
        peak_gain_pct = (peak_mcap - entry_mcap) / entry_mcap * 100
        pullback_from_peak_pct = (1 - mcap / peak_mcap) * 100 if peak_mcap > 0 else 0.0
        if peak_gain_pct >= 30 and pullback_from_peak_pct >= 35:
            score -= 10
            risk_factors.append(f"已冲高{peak_gain_pct:.1f}%后从峰值回撤{pullback_from_peak_pct:.1f}%")

    age_hours = to_float(extra.get("age_sec") or extra.get("created_age_sec")) / 3600.0
    if age_hours > 0:
        if age_hours <= 48:
            score += 5
            evidence.append("币龄<=48h，贴近历史赢家更年轻的特征")
        elif age_hours > 2000:
            score -= 6
            risk_factors.append("币龄>2000h，老币复苏效率偏弱")

    price_change = to_float(extra.get("price_change_pct"))
    first_signal_change = to_float(extra.get("first_signal_change_pct"))
    max_push_change = max(price_change, first_signal_change)
    if max_push_change > 200:
        score -= 10
        risk_factors.append("信号涨幅>200%，历史报告中追高风险较高")
    elif 15 <= max_push_change <= 80:
        score += 3
        evidence.append("信号涨幅处于非极端区间")

    vol_1m_ratio = to_float(extra.get("vol_1m_ratio"))
    micro_1m = extra.get("kline_1m_micro") if isinstance(extra.get("kline_1m_micro"), dict) else {}
    breakout_volume = to_float(extra.get("breakout_volume_usd"))
    breakout_ratio = to_float(extra.get("breakout_volume_ratio"))
    if vol_1m_ratio > 0:
        if vol_1m_ratio >= 1.2:
            score += 5
            evidence.append("1m后段量能高于前段，短线买盘延续")
        elif vol_1m_ratio < 0.6:
            score -= 8
            risk_factors.append("1m量能衰减明显")
    if micro_1m.get("ready"):
        micro_delta = to_float(micro_1m.get("score_delta"))
        if micro_delta:
            score += micro_delta
        label_1m = str(micro_1m.get("label") or "")
        doc_wr20_1m = to_float(micro_1m.get("doc_wr20_pct"))
        if doc_wr20_1m > 0:
            evidence.append(f"09 1m确认: {label_1m}，WR20 {doc_wr20_1m:.0f}%")
        else:
            evidence.append(f"09 1m确认: {label_1m}")
        if micro_1m.get("note") and micro_delta < 0:
            risk_factors.append(str(micro_1m.get("note")))
        elif micro_1m.get("note"):
            evidence.append(str(micro_1m.get("note")))
    if 0 < breakout_volume < 5_000:
        score -= 8
        risk_factors.append("突破量能<$5K，历史过滤项偏弱")
    elif breakout_volume >= 10_000:
        score += 2
        evidence.append("突破量能>=10K")
    if breakout_ratio >= 3:
        score += 3
        evidence.append("突破量比>=3x")

    pool_ratio = to_float(extra.get("pool_mcap_ratio"))
    pool_liquidity = to_float(extra.get("liquidity") or extra.get("pool_total_liquidity"))
    if 0 < pool_ratio < 0.07:
        score -= 10
        risk_factors.append("池/市值<7%，流动性不足")
    elif 0.15 <= pool_ratio <= 0.40:
        score += 3
        evidence.append("池/市值处于相对健康区间")
    elif pool_ratio > 0.50:
        score -= 3
        risk_factors.append("池/市值>50%，弹性可能受限")
    if 0 < pool_liquidity < 10_000:
        score -= 8
        risk_factors.append("池子流动性<10K")
    elif pool_liquidity >= 30_000:
        score += 3
        evidence.append("池子流动性>=30K")

    journey_raw = extra.get("kline_journey") if isinstance(extra.get("kline_journey"), dict) else {}
    journey = enrich_kline_journey_for_signal(journey_raw, signal_type) if journey_raw else {}
    if journey.get("ready"):
        structure = str(journey.get("pre_structure") or "其他结构")
        doc_wr20 = to_float(journey.get("doc_wr20_pct"))
        score_delta = to_float(journey.get("score_delta"))
        if score_delta:
            score += score_delta
        evidence.append(
            f"08 K线结构: {structure}，文档WR20 {doc_wr20:.0f}%"
            f"，MedPeak {journey.get('doc_med_peak') or '-'}"
        )
        if journey.get("volume_label"):
            evidence.append(
                f"前4h末段量能: {journey.get('volume_label')} "
                f"({to_float(journey.get('volume_ratio')):.2f}x)"
            )
        note = str(journey.get("doc_note") or "")
        downtrend_prob = to_float(journey.get("doc_downtrend_prob_pct"))
        if downtrend_prob >= 30:
            risk_factors.append(f"{structure} 后4h持续阴跌分支约{downtrend_prob:.0f}%")
        if note and score_delta < 0:
            risk_factors.append(note)
        elif note:
            evidence.append(note)

    predicted = _bounded_winrate(score)
    if bucket["buy_value"].startswith("回避") or predicted < 35:
        label = "低价值/回避"
    elif predicted >= 70:
        label = "高价值观察"
    elif predicted >= 50:
        label = "中等价值观察"
    else:
        label = "低价值观察"

    feature_count = sum(
        1
        for item in (top10, accumulation, mcap, age_hours, max_push_change, vol_1m_ratio, breakout_volume, pool_ratio, pool_liquidity)
        if item
    )
    if sample_count >= 50 and feature_count >= 5:
        confidence = "high"
    elif sample_count >= 20 and feature_count >= 3:
        confidence = "medium"
    else:
        confidence = "low"

    strategy_plan = compute_historical_strategy_plan(extra, risk_factors)
    deepseek_kline = extra.get("deepseek_kline_prediction") if isinstance(extra.get("deepseek_kline_prediction"), dict) else {}
    if is_deepseek_api_prediction(deepseek_kline):
        ds_summary = str(deepseek_kline.get("summary") or "").strip()
        ds_bias = str(deepseek_kline.get("bias") or "unknown")
        ds_confidence = str(deepseek_kline.get("confidence") or "low")
        pattern_5m = deepseek_kline.get("pattern_5m") if isinstance(deepseek_kline.get("pattern_5m"), dict) else {}
        micro_ds = deepseek_kline.get("micro_1m") if isinstance(deepseek_kline.get("micro_1m"), dict) else {}
        if ds_summary:
            evidence.append(f"DeepSeek K线预测({ds_confidence}/{ds_bias}): {ds_summary}")
        if pattern_5m.get("label"):
            evidence.append(f"DeepSeek 5m结构: {pattern_5m.get('label')}")
        if micro_ds.get("label"):
            evidence.append(f"DeepSeek 1m微结构: {micro_ds.get('label')}")
        ds_risks = deepseek_kline.get("risk_factors") if isinstance(deepseek_kline.get("risk_factors"), list) else []
        risk_factors.extend(str(item) for item in ds_risks if item)
        forecast = deepseek_kline.get("forecast") if isinstance(deepseek_kline.get("forecast"), dict) else {}
        forecast_text = ds_summary or forecast.get("next_30m") or strategy_plan.get("kline_forecast") or ""
        strategy_plan = {
            **strategy_plan,
            "kline_forecast": forecast_text,
            "deepseek_forecast": forecast,
            "deepseek_watch_windows": deepseek_kline.get("watch_windows") or [],
            "deepseek_strategy_observations": deepseek_kline.get("strategy_observations") or [],
            "deepseek_bias": ds_bias,
            "deepseek_confidence": ds_confidence,
        }

    return {
        "target": "max_gain_gte_20pct",
        "predicted_winrate_pct": round(predicted, 1),
        "baseline_winrate_pct": round(float(bucket["wr20"]), 1),
        "baseline_sample_count": sample_count,
        "overall_sample_count": 315,
        "winner_definition": "观察后任意时间价格涨到>=20%",
        "label": label,
        "buy_value_label": label,
        "confidence": confidence,
        "source": "hardcoded",
        "analysis_source": "hardcoded",
        "analysis_source_label": "硬编码兜底",
        "analysis_source_status": deepseek_kline.get("status") if deepseek_kline else "no_deepseek_prediction",
        "feature_count": feature_count,
        "evidence": evidence[:6],
        "risk_factors": risk_factors[:6],
        "profit_target_probabilities": {
            "tp20_pct": round(float(bucket["wr20"]), 1),
            "tp50_pct": round(float(bucket["wr50"]), 1),
            "tp100_pct": round(float(bucket["wr100"]), 1),
        },
        "kline_journey": journey,
        "kline_1m_micro": micro_1m,
        "deepseek_kline_prediction": deepseek_kline,
        "strategy_plan": strategy_plan,
        "source_doc": "hardcoded_fallback + onchain_trading_guides/11-ca-analysis-methodology.md + onchain_trading_guides/08-5m-fingerprint-encyclopedia.md",
    }


def enrich_signal_strategy_extra(extra: dict[str, Any] | None) -> dict[str, Any]:
    """Attach risk tags and strategy guide fields for TG/plugin consumers."""
    extra = dict(extra or {})
    risk_tags = extra.get("risk_tags")
    if not isinstance(risk_tags, list):
        risk_tags = compute_risk_tags(extra)
        if risk_tags:
            extra["risk_tags"] = risk_tags
    strategy = compute_strategy_profile(extra, risk_tags)
    extra.update(strategy)

    # Prefer DeepSeek prediction; fall back to hardcoded only when DeepSeek unavailable
    ds_pred = extra.get("deepseek_kline_prediction") if isinstance(extra.get("deepseek_kline_prediction"), dict) else {}
    if is_deepseek_api_prediction(ds_pred):
        extra["winrate_prediction"] = _build_winrate_from_deepseek(extra, ds_pred)
    else:
        extra["winrate_prediction"] = compute_historical_winrate_prediction(extra)
        if str(extra.get("deepseek_async_status") or "").lower() == "pending":
            extra["winrate_prediction"] = {
                **(extra.get("winrate_prediction") or {}),
                "analysis_source": "pending_deepseek",
                "analysis_source_label": "DeepSeek 分析中",
                "analysis_source_status": "pending",
            }
    return extra


def _build_winrate_from_deepseek(extra: dict[str, Any], ds_pred: dict[str, Any]) -> dict[str, Any]:
    """Build winrate_prediction from DeepSeek while preserving local 08/09 guide fields."""
    ds_bias = str(ds_pred.get("bias") or "unknown")
    ds_confidence = str(ds_pred.get("confidence") or "low")
    ds_summary = str(ds_pred.get("summary") or "")
    pattern_5m = ds_pred.get("pattern_5m") if isinstance(ds_pred.get("pattern_5m"), dict) else {}
    micro_1m = ds_pred.get("micro_1m") if isinstance(ds_pred.get("micro_1m"), dict) else {}
    forecast = ds_pred.get("forecast") if isinstance(ds_pred.get("forecast"), dict) else {}
    purchase = ds_pred.get("purchase_value") if isinstance(ds_pred.get("purchase_value"), dict) else {}
    local_fp = ds_pred.get("local_fingerprints") if isinstance(ds_pred.get("local_fingerprints"), dict) else {}
    signal_type = str(extra.get("signal_type") or "")
    mcap = to_float(extra.get("current_mcap") or extra.get("entry_mcap"))
    bucket = _historical_bottom_bucket(signal_type, mcap)
    local_journey_raw = extra.get("kline_journey") if isinstance(extra.get("kline_journey"), dict) else {}
    local_journey = enrich_kline_journey_for_signal(local_journey_raw, signal_type) if local_journey_raw else {}
    local_micro_1m = extra.get("kline_1m_micro") if isinstance(extra.get("kline_1m_micro"), dict) else {}

    # Map DeepSeek bias to predicted WR20
    wr20_map = {"bullish": 67.0, "neutral": 52.0, "bearish": 30.0, "volatile": 45.0, "unknown": 52.0}
    wr50_map = {"bullish": 40.0, "neutral": 27.0, "bearish": 12.0, "volatile": 25.0, "unknown": 27.0}
    predicted_wr20 = wr20_map.get(ds_bias, 52.0)
    predicted_wr50 = wr50_map.get(ds_bias, 27.0)

    # Adjust by confidence
    if ds_confidence == "high":
        predicted_wr20 = min(85, predicted_wr20 * 1.2)
        predicted_wr50 = min(60, predicted_wr50 * 1.3)
    elif ds_confidence == "low":
        predicted_wr20 = max(20, predicted_wr20 * 0.8)
        predicted_wr50 = max(5, predicted_wr50 * 0.7)

    # Adjust by local fingerprints
    if local_fp.get("has_capitulation"):
        predicted_wr20 += 10
        predicted_wr50 += 8
    if local_fp.get("position_zone") == "floor":
        predicted_wr20 += 8
        predicted_wr50 += 5
    elif local_fp.get("position_zone") == "ceiling":
        predicted_wr20 -= 10
        predicted_wr50 -= 8
    if to_float(purchase.get("score_pct")) > 0:
        predicted_wr20 = to_float(purchase.get("score_pct"))
        predicted_wr50 = max(3.0, min(60.0, predicted_wr20 * 0.55))

    predicted_wr20 = max(10, min(85, predicted_wr20))
    predicted_wr50 = max(3, min(60, predicted_wr50))

    evidence = [f"DeepSeek: {ds_summary}"]
    if purchase.get("basis"):
        evidence.append(f"购买价值依据: {purchase.get('basis')}")
    if pattern_5m.get("label"):
        evidence.append(f"5m结构: {pattern_5m.get('label')}")
    if micro_1m.get("label"):
        evidence.append(f"1m微结构: {micro_1m.get('label')}")
    if local_journey.get("ready"):
        evidence.append(
            f"08 K线结构: {local_journey.get('pre_structure') or '-'}，"
            f"文档WR20 {to_float(local_journey.get('doc_wr20_pct')):.0f}%，"
            f"MedPeak {local_journey.get('doc_med_peak') or '-'}"
        )
        if local_journey.get("volume_label"):
            evidence.append(
                f"前4h末段量能: {local_journey.get('volume_label')} "
                f"({to_float(local_journey.get('volume_ratio')):.2f}x)"
            )
    if local_micro_1m.get("ready"):
        evidence.append(
            f"09 1m确认: {local_micro_1m.get('label') or '-'}，"
            f"涨跌 {to_float(local_micro_1m.get('change_pct')):+.1f}%，"
            f"量比 {to_float(local_micro_1m.get('volume_ratio')):.2f}x"
        )
    if local_fp.get("quick_verdict"):
        evidence.append(f"本地指纹: {local_fp.get('quick_verdict')}")

    risk_factors = list(ds_pred.get("risk_factors") or [])
    if ds_bias == "bearish":
        risk_factors.append("DeepSeek偏向看跌")
    if pattern_5m.get("risk_level") == "high":
        risk_factors.append("5m结构高风险")

    label = str(purchase.get("label") or "").strip()
    if not label:
        label = "高价值观察" if ds_bias == "bullish" and ds_confidence in ("high", "medium") else \
                "中等价值观察" if ds_bias in ("bullish", "neutral") else \
                "低价值/回避"

    strategy_plan = compute_historical_strategy_plan(extra, risk_factors)
    strategy_plan = {
        **strategy_plan,
        "strategy_profile": f"DeepSeek {ds_bias}/{ds_confidence}",
        "strategy_action": label,
        "kline_forecast": forecast.get("next_4h") or ds_summary or strategy_plan.get("kline_forecast", ""),
        "deepseek_forecast": forecast,
        "deepseek_bias": ds_bias,
        "deepseek_confidence": ds_confidence,
        "purchase_value_basis": purchase.get("basis") or "",
        "deepseek_watch_windows": ds_pred.get("watch_windows") or [],
        "deepseek_strategy_observations": ds_pred.get("strategy_observations") or [],
    }

    return {
        "target": "max_gain_gte_20pct",
        "predicted_winrate_pct": round(predicted_wr20, 1),
        "predicted_wr50_pct": round(predicted_wr50, 1),
        "predicted_wr100_pct": round(float(bucket["wr100"]), 1),
        "baseline_winrate_pct": round(float(bucket["wr20"]), 1),
        "baseline_sample_count": int(bucket["samples"]),
        "overall_sample_count": 315,
        "winner_definition": "观察后任意时间价格涨到>=20%",
        "label": label,
        "buy_value_label": label,
        "confidence": ds_confidence,
        "source": "deepseek",
        "analysis_source": "deepseek",
        "analysis_source_label": "DeepSeek AI",
        "analysis_source_status": ds_pred.get("status") or "ok",
        "feature_count": sum(
            1
            for item in (
                mcap,
                extra.get("top10_current_pct"),
                extra.get("accumulation_pct_delta"),
                extra.get("pool_mcap_ratio"),
                extra.get("vol_1m_ratio"),
                local_journey.get("doc_wr20_pct"),
                local_micro_1m.get("doc_wr20_pct"),
            )
            if to_float(item)
        ),
        "evidence": evidence[:8],
        "risk_factors": risk_factors[:6],
        "profit_target_probabilities": {
            "tp20_pct": round(float(bucket["wr20"]), 1),
            "tp50_pct": round(float(bucket["wr50"]), 1),
            "tp100_pct": round(float(bucket["wr100"]), 1),
        },
        "deepseek_kline_prediction": ds_pred,
        "deepseek_pattern_5m": pattern_5m,
        "deepseek_micro_1m": micro_1m,
        "kline_journey": local_journey or pattern_5m,
        "kline_1m_micro": local_micro_1m or micro_1m,
        "strategy_plan": strategy_plan,
        "source_doc": "deepseek_api + onchain_trading_guides/08-5m-fingerprint-encyclopedia.md + onchain_trading_guides/09-bar-level-strategy.md",
    }


def publish_frontend_signal_update(
    text: str,
    extra: dict[str, Any],
    status: str = "frontend_update",
    snapshot_id: int = 0,
) -> bool:
    if snapshot_id and not (extra or {}).get("snapshot_id"):
        extra = {**(extra or {}), "snapshot_id": snapshot_id}
    address = str((extra or {}).get("address") or "").strip()
    if not address:
        return False
    signal_type = str((extra or {}).get("signal_type") or "").strip()
    if top100_signal_push_record_exists(address, signal_type, source="bottom_abnormal", chain=CHAIN):
        print(f"{address[:8]} skip frontend push: {signal_type} push already recorded")
        return False
    extra = enrich_signal_strategy_extra(extra)
    risk_tags = extra.get("risk_tags") or []
    try:
        inserted = record_top100_push(text=text, extra=extra, status=status, source="bottom_abnormal", chain=CHAIN)
    except Exception as exc:
        print(f"{address[:8]} top100 push record failed: {exc}")
        inserted = False
    if not inserted:
        print(f"{address[:8]} skip frontend push: duplicate first push record")
        return False
    # Filter 1: ceiling + dead_vol = ~0% success (21 failures, 8 successes killed)
    if "天花板" in risk_tags and "无量" in risk_tags:
        print(f"{address[:8]} skip push: ceiling+dead_vol combo")
        return False
    # Filter 2: extreme dead volume (<$5K) = 78% failure rate, kills only 5/131 successes
    vol = to_float(extra.get("breakout_volume_usd", 0) or extra.get("volume_usd", 0))
    if 0 < vol < 5_000:
        print(f"{address[:8]} skip push: extreme dead vol ${vol:,.0f} < $5K")
        return False
    publish_tg_alert(text, "bottom_abnormal", status=status, ca=address, extra=extra)
    publish_plugin_signal(text, "bottom_abnormal", status=status, ca=address, extra=extra)
    # Live-track: push to Redis for frontend real-time dashboard
    start_bottom_live_tracking(
        address=address,
        symbol=str(extra.get("symbol") or ""),
        entry_mcap=to_float(extra.get("current_mcap")),
        entry_price=to_float(extra.get("price")),
        signal_type=str(extra.get("signal_type") or ""),
        pool_liquidity=to_float(extra.get("liquidity") or extra.get("pool_total_liquidity")),
        created_ts=to_int(extra.get("created_ts")),
        launch_ts=to_int(extra.get("launch_ts")),
        age_sec=to_int(extra.get("created_age_sec") or extra.get("age_sec")),
        narrative_desc=str(extra.get("narrative_desc") or extra.get("narrative") or ""),
        narrative_type=str(extra.get("narrative_type") or ""),
        narrative_category=str(extra.get("narrative_category") or ""),
        winrate_prediction=extra.get("winrate_prediction") or compute_historical_winrate_prediction(extra),
    )

    # Schedule 10-minute follow-up verdict via TG
    threading.Thread(
        target=_send_quick_verdict,
        args=(address, extra),
        daemon=True,
    ).start()
    return True


QUICK_VERDICT_BARS = int(os.getenv("BOTTOM_QUICK_VERDICT_BARS", "6"))
QUICK_VERDICT_DELAY_SEC = int(os.getenv("BOTTOM_QUICK_VERDICT_DELAY_SEC", "600"))


def _send_quick_verdict(address: str, extra: dict[str, Any]) -> None:
    """Wait 10 minutes after push, then analyze 1m/5m volume to classify DCB vs Real."""
    time.sleep(QUICK_VERDICT_DELAY_SEC)

    try:
        # Fetch fresh K-line
        params_1m = {"address": address, "platform": "solana", "interval": "1min", "limit": 24, "pm": "p"}
        params_5m = {"address": address, "platform": "solana", "interval": "5min", "limit": 12, "pm": "p"}
        headers = {"Accept-Encoding": "identity", "User-Agent": "binance-web3/1.1 (Skill)"}

        resp_1m = requests.get(BINANCE_KLINE_URL, params=params_1m, headers=headers, timeout=15)
        resp_5m = requests.get(BINANCE_KLINE_URL, params=params_5m, headers=headers, timeout=15)

        c1m = resp_1m.json().get("data", []) if resp_1m.ok else []
        c5m = resp_5m.json().get("data", []) if resp_5m.ok else []

        if not c1m or not c5m:
            return

        # Extract volumes
        def get_vols(candles, start, count):
            return [float(c[4]) for c in candles[start:start + count] if len(c) > 4]

        # Early (first few bars) vs Late (last few bars)
        mid_1m = len(c1m) // 2
        early_1m = sum(get_vols(c1m, 0, QUICK_VERDICT_BARS)) / max(1, QUICK_VERDICT_BARS)
        late_1m = sum(get_vols(c1m, max(0, len(c1m) - QUICK_VERDICT_BARS), QUICK_VERDICT_BARS)) / max(1, QUICK_VERDICT_BARS)
        r1m = late_1m / early_1m if early_1m > 0 else 0

        mid_5m = len(c5m) // 2
        early_5m = sum(get_vols(c5m, 0, 3)) / 3
        late_5m = sum(get_vols(c5m, max(0, len(c5m) - 3), 3)) / 3
        r5m = late_5m / early_5m if early_5m > 0 else 0

        # Price change
        first_price = float(c5m[0][3]) if c5m else 0
        last_price = float(c5m[-1][3]) if c5m else 0
        peak_price = max(float(c[1]) for c in c5m) if c5m else 0
        price_change = (last_price - first_price) / first_price * 100 if first_price > 0 else 0

        # Classification
        if r1m < 0.4 and price_change < 5:
            verdict = "🔴 死猫跳"
            advice = "量能崩塌({:.0f}%), 买盘承接弱".format((1 - r1m) * 100)
        elif r1m > 1.2 and r5m > 1.0 and price_change > 5:
            verdict = "🟢 真异动"
            advice = "1m+5m量能共振, 买盘延续"
        elif price_change < -5 and r1m < 0.6:
            verdict = "🟡 V反进行中"
            advice = "正在回调, 观察量能恢复"
        elif r1m > 0.6 and r5m > 0.6:
            verdict = "🟡 观望"
            advice = "量能维持但涨幅不足, 继续观察"
        else:
            verdict = "⚪ 不明确"
            advice = "信号混合, 方向不明"

        # Build TG message
        signal_type = extra.get("signal_type", "?")
        symbol = extra.get("symbol", "UNKNOWN")
        mcap = to_float(extra.get("current_mcap", 0))

        msg = (
            "📊 10分钟快速判定 | ${}\n"
            "信号类型: {}\n"
            "CA: {}\n"
            "当前市值: ${:,.0f}\n"
            "10分钟涨跌: {:+.1f}%\n"
            "1m量比(后/前): {:.1f}x\n"
            "5m量比(后/前): {:.1f}x\n"
            "\n判定: {}\n"
            "观察: {}\n"
            "\nhttps://gmgn.ai/sol/token/{}"
        ).format(symbol, signal_type, address, mcap, price_change, r1m, r5m, verdict, advice, address)

        publish_tg_alert(msg, "bottom_abnormal", status="quick_verdict", ca=address,
                         extra={"verdict": verdict, "r1m": r1m, "r5m": r5m, "price_change": price_change})
        print("  [quick_verdict] {}: {} (1m={:.1f}x 5m={:.1f}x)".format(address[:8], verdict, r1m, r5m))

    except Exception as exc:
        print("  [quick_verdict] {} failed: {}".format(address[:8], exc))


def daily_mcap_signal_text(token: dict[str, Any], current_mcap: float, current_fee_sol: float) -> str:
    address = token_address(token)
    active_ts = token_active_ts(token)
    age_text = f"{token_age_sec(token) / 3600:.1f}h" if active_ts > 0 else "未知"
    pool_summary = summarize_pools(token)
    pool_liquidity = to_float(pool_summary.get("total_liquidity"))
    pool_ratio = to_float(pool_summary.get("liquidity_mcap_ratio"))
    trend = token.get("_trend_interval") or "N/A"
    return (
        f"每日过1M市值 | ${token.get('symbol') or 'UNKNOWN'}\n"
        f"来源: {trend} 扫描\n"
        f"CA: {address}\n"
        f"市值: ${current_mcap:,.0f} | 要求: >=${DAILY_MCAP_MILESTONE_USD:,.0f}\n"
        f"手续费: {current_fee_sol:.2f} SOL | 要求: >={DAILY_MCAP_MIN_FEE_SOL:.2f} SOL\n"
        f"池子: ${pool_liquidity:,.0f} | 池/市值: {pool_ratio:.1%} | 要求: >=${BOTTOM_ABNORMAL_MIN_POOL_LIQUIDITY_USD:,.0f} / >{DAILY_MCAP_MIN_POOL_MCAP_RATIO:.1%}\n"
        f"发射年龄: {age_text}\n"
        f"https://gmgn.ai/sol/token/{address}"
    )


def daily_1m_zone(mcap: float):
    if mcap >= 1_000_000: return "green", ">=$1M"
    if mcap >= 500_000: return "yellow", f"${mcap/1000:.0f}K"
    return "red", f"${mcap/1000:.0f}K"


def publish_daily_1m_frontend_update(token, current_mcap, peak_mcap, pool_liquidity: float | None = None):
    address = token_address(token)
    if address and is_watchlist_blacklisted(address):
        print(f"{address[:8]} daily 1M frontend update blacklisted, skipped")
        return
    narrative = resolve_cached_or_db_narrative(address)
    if not narrative or not narrative.get("narrative_desc"):
        try:
            narrative = get_binance_narrative(address, symbol=token.get("symbol"), name=token.get("name"))
        except Exception as exc:
            print(f"{address[:8]} daily 1M binance narrative failed: {exc}")
            narrative = {}
    zone, zone_label = daily_1m_zone(current_mcap)
    drop = round((1 - current_mcap / max(peak_mcap, 1)) * 100, 1)
    milestone_date = token.get("watchlist_daily_mcap_date") or datetime.now().date().isoformat()
    extra = {
        "source_type": "daily_1m",
        "symbol": token.get("symbol"),
        "address": address,
        "current_mcap": current_mcap,
        "peak_mcap": peak_mcap,
        "ath_mcap": max(calc_ath_mcap(token), peak_mcap),
        "milestone_date": milestone_date,
        "zone": zone,
        "zone_label": zone_label,
        "drop_from_peak_pct": drop,
        "liquidity": to_float(pool_liquidity if pool_liquidity is not None else (token.get("liquidity") or token.get("pool_liquidity"))),
        "holders": token.get("holder_count", 0),
        "created_ts": token_created_ts(token),
        "launch_ts": token_launch_ts(token),
        "created_age_sec": token_creation_age_sec(token),
        "launch_age_sec": token_launch_age_sec(token),
        "age_sec": token_age_sec(token),
        "narrative": narrative.get("narrative_desc") or token.get("narrative_desc") or "",
        "narrative_desc": narrative.get("narrative_desc") or token.get("narrative_desc") or "",
        "narrative_type": narrative.get("narrative_type") or token.get("narrative_type") or "",
        "binance_narrative": compact_narrative(narrative),
    }
    text = f"每日1M | ${token.get('symbol', '?')}\n市值: ${current_mcap:,.0f} | 峰值: ${peak_mcap:,.0f} | {zone_label}"
    publish_tg_alert(text, "daily_1m", status="update", ca=address, extra=extra)


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
    if pool_liquidity < BOTTOM_ABNORMAL_MIN_POOL_LIQUIDITY_USD or pool_ratio < DAILY_MCAP_MIN_POOL_MCAP_RATIO:
        print(
            f"{token_label(token)} daily 1M skip pool "
            f"liq=${pool_liquidity:,.0f}/${BOTTOM_ABNORMAL_MIN_POOL_LIQUIDITY_USD:,.0f} "
            f"pool/mcap={pool_ratio:.1%}/{DAILY_MCAP_MIN_POOL_MCAP_RATIO:.1%}"
        )
        return
    upsert_daily_mcap_watchlist_token(
        address,
        token_created_ts(token),
        peak_mcap,
        current_fee_sol,
        symbol=token.get("symbol"),
        threshold_mcap=DAILY_MCAP_MILESTONE_USD,
        launch_ts=token_launch_ts(token),
    )
    print(f"{token_label(token)} watchlist daily 1M recorded peak=${peak_mcap:,.0f} cur=${current_mcap:,.0f} fee={current_fee_sol:.2f} SOL")
    if notify and daily_mcap_watchlist_needs_notify(address):
        narrative = resolve_cached_or_db_narrative(address)
        if not narrative or not narrative.get("narrative_desc"):
            try:
                narrative = get_binance_narrative(address, symbol=token.get("symbol"), name=token.get("name"))
            except Exception as exc:
                print(f"{address[:8]} daily 1M notify binance narrative failed: {exc}")
                narrative = {}
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
            "required_pool_mcap_ratio": DAILY_MCAP_MIN_POOL_MCAP_RATIO,
            "created_ts": token_created_ts(token),
            "launch_ts": token_launch_ts(token),
            "created_age_sec": token_creation_age_sec(token),
            "launch_age_sec": token_launch_age_sec(token),
            "age_sec": age_sec,
            "narrative": narrative.get("narrative_desc") or token.get("narrative_desc") or "",
            "narrative_desc": narrative.get("narrative_desc") or token.get("narrative_desc") or "",
            "narrative_type": narrative.get("narrative_type") or token.get("narrative_type") or "",
            "binance_narrative": compact_narrative(narrative),
        }
        send_tg(daily_mcap_signal_text(token, current_mcap, current_fee_sol), extra=extra)
        publish_daily_1m_frontend_update(token, current_mcap, peak_mcap, pool_liquidity=pool_liquidity)
        mark_daily_mcap_watchlist_notified(address)




def signal_type_text(signal_type: str) -> str:
    mapping = {
        "watch": "观察",
        "abnormal": "老币异动",
        "watchlist_abnormal": "历史代币1m异动",
        "drop_50w": "新币跌破50W",
        "drop_40w": "新币跌破40W",
        "new_revival": "新币异动",
        "quiet_breakout": "横盘异动",
        "quiet_runup": "横盘拉升",
        "ema_golden_cross": "EMA金叉",
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
    trend_interval = token.get("_trend_interval") or TREND_INTERVAL
    return (
        f"底部异动检测 | ${token.get('symbol') or 'UNKNOWN'}\n"
        f"类型: {signal_type_text(analysis.get('signal_type'))}\n"
        f"档位: {analysis.get('abnormal_rule') or '未命中'}\n"
        f"来源: {trend_interval} 扫描\n"
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
    return top100_signal_push_record_exists(
        address,
        signal_type,
        source="bottom_abnormal",
        chain=CHAIN,
    )


def previous_bottom_signal_exists(address: str) -> bool:
    if not address:
        return False

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1
            FROM bottom_top100_snapshots
            WHERE chain=%s AND address=%s AND signal_type <> 'watch'
            LIMIT 1
            """,
            (CHAIN, address),
        )
        return cur.fetchone() is not None

    return bool(db_op(_op))


def first_signal_baseline(address: str, signal_type: str) -> dict[str, Any]:
    if not signal_type or signal_type == "watch":
        return {}

    def _op(conn):
        cur = conn.cursor()
        where_age = "AND snapshot_ts >= %s" if FIRST_SIGNAL_BASELINE_MAX_AGE_SEC > 0 else ""
        params = [CHAIN, address, signal_type]
        if FIRST_SIGNAL_BASELINE_MAX_AGE_SEC > 0:
            params.append(now_ts() - FIRST_SIGNAL_BASELINE_MAX_AGE_SEC)
        cur.execute(
            f"""
            SELECT snapshot_ts, analysis
            FROM bottom_top100_snapshots
            WHERE chain=%s AND address=%s AND signal_type=%s
              {where_age}
            ORDER BY snapshot_ts ASC, id ASC
            LIMIT 1
            """,
            tuple(params),
        )
        row = cur.fetchone()
        if not row:
            return {}
        analysis = row[1] or {}
        if not isinstance(analysis, dict):
            return {}
        return {
            "first_signal_ts": int(row[0] or 0),
            "first_signal_mcap": to_float(analysis.get("current_mcap")),
        }

    return db_op(_op) or {}


def post_signal_peak_mcap(address: str, signal_type: str, first_signal_ts: int) -> float:
    if not address or not signal_type or signal_type == "watch" or first_signal_ts <= 0:
        return 0.0

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            SELECT MAX(NULLIF(summary->>'mcap', '')::numeric)
            FROM bottom_top100_snapshots
            WHERE chain=%s AND address=%s AND signal_type=%s
              AND snapshot_ts >= %s
            """,
            (CHAIN, address, signal_type, max(0, first_signal_ts - 60)),
        )
        row = cur.fetchone()
        return to_float(row[0]) if row and row[0] is not None else 0.0

    return to_float(db_op(_op))


def latest_frontend_signal_baseline(address: str, signal_type: str) -> dict[str, Any]:
    if not address or not signal_type or signal_type == "watch":
        return {}

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            SELECT snapshot_ts, analysis
            FROM bottom_top100_snapshots
            WHERE chain=%s AND address=%s AND signal_type=%s
            ORDER BY snapshot_ts DESC, id DESC
            OFFSET 1
            LIMIT 1
            """,
            (CHAIN, address, signal_type),
        )
        row = cur.fetchone()
        if not row:
            return {}
        analysis = row[1] or {}
        if not isinstance(analysis, dict):
            return {}
        return {
            "last_signal_ts": int(row[0] or 0),
            "last_signal_mcap": to_float(analysis.get("current_mcap")),
        }

    return db_op(_op) or {}


def build_bottom_signal_extra(
    token: dict[str, Any],
    summary: dict[str, Any],
    analysis: dict[str, Any],
    baseline: dict[str, Any] | None = None,
) -> dict[str, Any]:
    address = token_address(token)
    narrative = resolve_cached_or_db_narrative(address)
    if not narrative or not narrative.get("narrative_desc"):
        try:
            narrative = get_binance_narrative(
                address,
                symbol=token.get("symbol"),
                name=token.get("name"),
            )
        except Exception as exc:
            print(f"{address[:8]} binance narrative failed: {exc}")
            narrative = {}
    watchlist_narrative_desc = token.get("watchlist_narrative_desc") or token.get("narrative_desc") or ""
    watchlist_narrative_type = token.get("watchlist_narrative_type") or token.get("narrative_type") or ""
    watchlist_narrative_category = token.get("watchlist_narrative_category") or token.get("narrative_category") or ""
    narrative_desc = narrative.get("narrative_desc") or watchlist_narrative_desc
    narrative_type = narrative.get("narrative_type") or watchlist_narrative_type
    narrative_category = narrative.get("narrative_category") or watchlist_narrative_category or classify_narrative_category(
        narrative_desc,
        narrative_type,
        narrative.get("tags") or [],
    )
    current_mcap = to_float(analysis.get("current_mcap", calc_mcap(token)))
    first_mcap = to_float((baseline or {}).get("first_signal_mcap")) or current_mcap
    first_ts = to_int((baseline or {}).get("first_signal_ts")) or now_ts()
    first_delta = current_mcap - first_mcap if first_mcap > 0 else 0.0
    first_change_pct = (first_delta / first_mcap * 100) if first_mcap > 0 else 0.0
    signal_type = str(analysis.get("signal_type") or "")
    post_peak = max(
        first_mcap,
        current_mcap,
        post_signal_peak_mcap(address, signal_type, first_ts),
    )
    history_gain_pct = (post_peak - first_mcap) / first_mcap * 100 if first_mcap > 0 and post_peak > 0 else 0.0
    pool_summary = summary.get("pool") or {}
    pool_liquidity = to_float(analysis.get("pool_total_liquidity")) or to_float(pool_summary.get("total_liquidity"))
    signal_ts = now_ts()
    kline_summary = summary.get("kline") if isinstance(summary.get("kline"), dict) else {}
    journey_source = summary.get("_5m_kline_journey") if isinstance(summary.get("_5m_kline_journey"), dict) else kline_summary.get("journey")
    kline_journey = enrich_kline_journey_for_signal(journey_source, signal_type)
    micro_1m = summary.get("_1m_micro") if isinstance(summary.get("_1m_micro"), dict) else {}
    deepseek_kline_prediction = analysis.get("deepseek_kline_prediction") if isinstance(analysis.get("deepseek_kline_prediction"), dict) else {}
    deepseek_async_status = ""
    if deepseek_kline_prediction:
        deepseek_async_status = "ok" if is_deepseek_api_prediction(deepseek_kline_prediction) else str(deepseek_kline_prediction.get("status") or "fallback")
    elif BOTTOM_DEEPSEEK_ASYNC_ENABLED and deepseek_signal_eligible(signal_type):
        deepseek_async_status = "pending"
    return {
        "snapshot_id": analysis.get("snapshot_id", 0),
        "event_ts": signal_ts,
        "signal_ts": signal_ts,
        "signal_type": signal_type,
        "abnormal_rule": analysis.get("abnormal_rule"),
        "trend_interval": token.get("_trend_interval") or TREND_INTERVAL,
        "ath_mcap": analysis.get("ath_mcap", 0),
        "post_signal_peak_mcap": post_peak,
        "history_gain_pct": history_gain_pct,
        "min_ath_mcap": analysis.get("min_ath_mcap", 0),
        "current_mcap": current_mcap,
        "liquidity": pool_liquidity,
        "holder_count": summary.get("holder_count", 0),
        "created_ts": summary.get("created_ts", 0),
        "launch_ts": summary.get("launch_ts", 0),
        "created_age_sec": summary.get("created_age_sec", 0),
        "launch_age_sec": summary.get("launch_age_sec", 0),
        "age_sec": summary.get("age_sec", 0),
        "first_signal_mcap": first_mcap,
        "first_signal_ts": first_ts,
        "first_signal_delta_mcap": first_delta,
        "first_signal_change_pct": first_change_pct,
        "min_abnormal_mcap": analysis.get("min_abnormal_mcap", 0),
        "max_abnormal_mcap": analysis.get("max_abnormal_mcap", 0),
        "price_change_pct": analysis.get("price_change_pct", 0),
        "required_price_change_pct": analysis.get("required_price_change_pct", 0),
        "quiet_range_pct": analysis.get("quiet_range_pct", 0),
        "required_quiet_range_pct": analysis.get("required_quiet_range_pct", 0),
        "quiet_avg_volume_usd": analysis.get("quiet_avg_volume_usd", 0),
        "required_quiet_avg_volume_usd": analysis.get("required_quiet_avg_volume_usd", 0),
        "quiet_total_volume_usd": analysis.get("quiet_total_volume_usd", 0),
        "breakout_volume_usd": analysis.get("breakout_volume_usd", 0),
        "breakout_volume_ratio": analysis.get("breakout_volume_ratio", 0),
        "required_breakout_volume_ratio": analysis.get("required_breakout_volume_ratio", 0),
        "required_breakout_volume_usd": analysis.get("required_breakout_volume_usd", 0),
        "quiet_duration_sec": analysis.get("quiet_duration_sec", 0),
        "quiet_bars": analysis.get("quiet_bars", 0),
        "breakout_bars": analysis.get("breakout_bars", 0),
        "quiet_breakout_source_type": analysis.get("source_type", ""),
        "quiet_breakout_trigger_mode": analysis.get("trigger_mode", ""),
        "bottom_low_mcap": analysis.get("bottom_low_mcap", 0),
        "required_bottom_low_mcap": analysis.get("required_bottom_low_mcap", 0),
        "pool_total_liquidity": pool_liquidity,
        "required_pool_liquidity": analysis.get("required_pool_liquidity", 0),
        "pool_count": pool_summary.get("pool_count", 0),
        "pool_main_address": pool_summary.get("main_pool_address", ""),
        "pool_main_exchange": pool_summary.get("main_exchange", ""),
        "pool_main_quote_symbol": pool_summary.get("main_quote_symbol", ""),
        "pool_main_liquidity": pool_summary.get("main_liquidity", 0),
        "pool_main_share": pool_summary.get("main_share", 0),
        "pool_mcap_ratio": analysis.get("pool_mcap_ratio", 0),
        "pool_mcap_ratio_text": analysis.get("pool_mcap_ratio_text", "N/A"),
        "accumulation_pct_delta": analysis.get("accumulation_pct_delta", 0),
        "distribution_pct_delta": analysis.get("distribution_pct_delta", 0),
        "top10_pct_delta": analysis.get("top10_pct_delta", 0),
        "top10_current_pct": analysis.get("top10_current_pct", 0),
        "top10_previous_pct": analysis.get("top10_previous_pct", 0),
        "top20_pct_delta": analysis.get("top20_pct_delta", 0),
        "top20_current_pct": analysis.get("top20_current_pct", 0),
        "top20_previous_pct": analysis.get("top20_previous_pct", 0),
        "top50_pct_delta": analysis.get("top50_pct_delta", 0),
        "top50_current_pct": analysis.get("top50_current_pct", 0),
        "top50_previous_pct": analysis.get("top50_previous_pct", 0),
        "top100_pct_delta": analysis.get("top100_pct_delta", 0),
        "top100_current_pct": analysis.get("top100_current_pct", 0),
        "top100_previous_pct": analysis.get("top100_previous_pct", 0),
        "netflow_usd": analysis.get("netflow_usd", 0),
        "score": analysis.get("score", 0),
        "history_count": analysis.get("history_count", 0),
        "reasons": analysis.get("reasons", []),
        "symbol": token.get("symbol"),
        "address": address,
        "narrative": narrative_desc,
        "narrative_desc": narrative_desc,
        "narrative_type": narrative_type,
        "narrative_category": narrative_category,
        "binance_narrative": compact_narrative(narrative),
        "watchlist_narrative_desc": watchlist_narrative_desc,
        "watchlist_narrative_type": watchlist_narrative_type,
        "watchlist_narrative_category": watchlist_narrative_category,
        # 1m K-line volume data for micro-structure DCB detection
        "kline_journey": kline_journey,
        "kline_pre_structure": kline_journey.get("pre_structure") if kline_journey else "",
        "kline_doc_wr20_pct": kline_journey.get("doc_wr20_pct") if kline_journey else 0,
        "kline_doc_med_peak": kline_journey.get("doc_med_peak") if kline_journey else "",
        "kline_volume_label": kline_journey.get("volume_label") if kline_journey else "",
        "kline_volume_ratio": kline_journey.get("volume_ratio") if kline_journey else 0,
        "kline_1m_micro": micro_1m,
        "kline_1m_label": micro_1m.get("label") if micro_1m else "",
        "kline_1m_decision": micro_1m.get("decision") if micro_1m else "",
        "kline_1m_doc_wr20_pct": micro_1m.get("doc_wr20_pct") if micro_1m else 0,
        "kline_1m_volume_ratio": micro_1m.get("volume_ratio") if micro_1m else 0,
        "kline_1m_change_pct": micro_1m.get("change_pct") if micro_1m else 0,
        "deepseek_kline_prediction": deepseek_kline_prediction,
        "deepseek_async_status": deepseek_async_status,
        "kline_prediction_source_docs": deepseek_kline_prediction.get("source_docs") if deepseek_kline_prediction else [],
        "kline_prediction_summary": deepseek_kline_prediction.get("summary") if deepseek_kline_prediction else "",
        "vol_1m_ratio": to_float(summary.get("_1m_vol_ratio", 0)),
        "vol_1m_early": to_float(summary.get("_1m_vol_early", 0)),
        "vol_1m_late": to_float(summary.get("_1m_vol_late", 0)),
        "vol_1m_candles": int(summary.get("_1m_candles", 0) or 0),
    }


def frontend_repeat_update_allowed(extra: dict[str, Any], analysis: dict[str, Any], latest_baseline: dict[str, Any]) -> tuple[bool, str]:
    current_mcap = to_float(extra.get("current_mcap"))
    last_mcap = to_float(latest_baseline.get("last_signal_mcap"))
    if last_mcap > 0 and current_mcap <= last_mcap:
        return False, f"mcap_not_above_last:${current_mcap:,.0f}<=${last_mcap:,.0f}"
    recent_change_pct = to_float(analysis.get("raw_kline_change_pct"))
    if recent_change_pct <= FRONTEND_REPEAT_MIN_KLINE_CHANGE_PCT:
        return False, f"recent_kline_not_up:{recent_change_pct:.1f}%<={FRONTEND_REPEAT_MIN_KLINE_CHANGE_PCT:.1f}%"
    return True, f"mcap_up:${last_mcap:,.0f}->${current_mcap:,.0f}, recent_kline={recent_change_pct:.1f}%"


def notify_skip_reason(analysis: dict[str, Any]) -> str:
    return "; ".join(analysis.get("reasons") or ["未满足异动检测条件"])


def run_agent_execution(
    *,
    token: dict[str, Any],
    summary: dict[str, Any],
    raw_holders: list[dict[str, Any]],
    holders: list[dict[str, Any]],
    candles: list[dict[str, Any]],
    history: list[dict[str, Any]],
    analysis: dict[str, Any],
    execute: bool,
    already_notified: bool = False,
    has_previous_bottom_signal: bool = False,
    snapshot_id: int = 0,
):
    """Run the Agent decision/execution layer from already-collected monitor data."""
    from agents.action_executor_agent import ActionExecutorAgent
    from agents.chip_analysis_agent import ChipAnalysisAgent
    from agents.context import AgentContext
    from agents.kline_structure_agent import KlineStructureAgent
    from agents.signal_decision_agent import SignalDecisionAgent

    address = token_address(token)
    if snapshot_id:
        analysis = {**analysis, "snapshot_id": snapshot_id}
    context = AgentContext(
        ca=address,
        chain=CHAIN,
        symbol=str(token.get("symbol") or ""),
        source="bottom_monitor",
        token=token,
        gmgn_info=token.get("_gmgn_info") or {},
        gmgn_pool=token.get("_gmgn_pool") or {},
        raw_holders=raw_holders,
        holders=holders,
        candles=candles,
        history=history,
        stats={
            "source_agent": "bottom_monitor_bridge",
            "snapshot_id": snapshot_id,
            "signal_type": analysis.get("signal_type"),
            "abnormal_rule": analysis.get("abnormal_rule"),
            "mcap": analysis.get("current_mcap", 0),
            "ath_mcap": analysis.get("ath_mcap", 0),
            "price_change_pct": analysis.get("price_change_pct", 0),
            "pool_liquidity": analysis.get("pool_total_liquidity", 0),
            "pool_mcap_ratio": analysis.get("pool_mcap_ratio", 0),
            "history_count": analysis.get("history_count", 0),
        },
    )
    context.decision["bottom_signal"] = {
        "token": token,
        "summary": summary,
        "gmgn_info": token.get("_gmgn_info") or {},
        "gmgn_pool": token.get("_gmgn_pool") or {},
        "raw_holders": raw_holders,
        "holders": holders,
        "candles": candles,
        "history": history,
        "analysis": analysis,
        "signal_text": abnormal_signal_text(token, analysis),
        "should_notify": should_notify(analysis),
        "already_notified": already_notified,
        "has_previous_bottom_signal": has_previous_bottom_signal,
    }
    context = KlineStructureAgent().run(context)
    context = ChipAnalysisAgent().run(context)
    context = SignalDecisionAgent().run(context)
    return ActionExecutorAgent(execute=execute).run(context)


def surge_price_threshold_pct(age_sec: int) -> float:
    if age_sec > SURGE_MID_TOKEN_AGE_SEC:
        return SURGE_OLD_TOKEN_PRICE_UP_PCT
    if age_sec > SURGE_NEW_TOKEN_AGE_SEC:
        return SURGE_MID_TOKEN_PRICE_UP_PCT
    return SURGE_NEW_TOKEN_PRICE_UP_PCT


def surge_age_bucket(age_sec: int) -> str:
    if age_sec > SURGE_MID_TOKEN_AGE_SEC:
        return "7d+"
    if age_sec > SURGE_NEW_TOKEN_AGE_SEC:
        return "48h-7d"
    return "0-48h"


def check_old_token_surge(token: dict[str, Any]) -> dict[str, Any] | None:
    """Check old tokens for sudden price surge across 1h/5m/1m resolutions.

    Returns a signal dict if surge detected, None otherwise.
    """
    if not OLD_TOKEN_SURGE_ENABLED:
        return None

    age_sec = token_age_sec(token)
    if age_sec <= OLD_TOKEN_SURGE_MIN_AGE_SEC:
        return None
    required_change_pct = surge_price_threshold_pct(age_sec)

    address = token_address(token)
    current_mcap = calc_mcap(token)
    if current_mcap < OLD_TOKEN_SURGE_MIN_MCAP_USD:
        return None

    # Fetch multi-resolution K-lines and check for surge
    hits = []
    now = int(time.time())
    for resolution in OLD_TOKEN_SURGE_RESOLUTIONS:
        step_sec = kline_resolution_seconds(resolution)
        lookback_sec = max(step_sec * 20, 3600)  # at least 1h of data
        start_ts = now - lookback_sec

        try:
            rows = fetch_kline_range(address, resolution, start_ts, now)
        except Exception:
            continue
        if not rows:
            continue

        candles = []
        for row in (rows if isinstance(rows, list) else []):
            if not isinstance(row, dict):
                continue
            raw_ts = int(to_float(row.get("time") or row.get("timestamp") or row.get("t")))
            ts = raw_ts // 1000 if raw_ts > 10_000_000_000 else raw_ts
            close = to_float(row.get("close") or row.get("c"))
            if ts <= 0 or close <= 0:
                continue
            candles.append({"ts": ts, "close": close, "high": to_float(row.get("high") or row.get("h"), close),
                           "low": to_float(row.get("low") or row.get("l"), close),
                           "volume": to_float(row.get("volume") or row.get("v"))})
        if len(candles) < 2:
            continue
        candles.sort(key=lambda c: c["ts"])

        # Check each recent candle for surge vs its own open (or prior close)
        recent = candles[-3:]  # last 3 candles
        for c in recent:
            if len(candles) >= 2:
                prev_close = candles[-len(recent) - 1 + recent.index(c)]["close"] if recent.index(c) > 0 else candles[-len(recent) - 1]["close"]
            else:
                prev_close = candles[0]["close"]
            if prev_close <= 0:
                continue
            change_pct = (c["close"] - prev_close) / prev_close * 100
            if change_pct >= required_change_pct:
                hits.append({
                    "resolution": resolution,
                    "change_pct": round(change_pct, 1),
                    "required_change_pct": required_change_pct,
                    "age_bucket": surge_age_bucket(age_sec),
                    "from_price": prev_close,
                    "to_price": c["close"],
                    "volume": c["volume"],
                    "ts": c["ts"],
                })

    if not hits:
        return None

    # Keep only best hit per resolution
    best = {}
    for h in hits:
        key = h["resolution"]
        if key not in best or h["change_pct"] > best[key]["change_pct"]:
            best[key] = h

    best_hit = max(best.values(), key=lambda x: x["change_pct"])
    resolutions_hit = list(best.keys())
    return {
        "signal_type": "old_surge",
        "change_pct": best_hit["change_pct"],
        "required_change_pct": best_hit["required_change_pct"],
        "age_bucket": best_hit["age_bucket"],
        "age_sec": age_sec,
        "resolutions": resolutions_hit,
        "from_price": best_hit["from_price"],
        "to_price": best_hit["to_price"],
        "volume": best_hit["volume"],
        "current_mcap": current_mcap,
        "best_resolution": best_hit["resolution"],
        "hits": hits,
    }


def check_watchlist_quiet_breakout(
    token: dict[str, Any],
    summary: dict[str, Any],
    candles: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Watchlist/trending signal: long sideways range, then volume or range breakout."""
    if not QUIET_BREAKOUT_ENABLED or not (is_watchlist_token(token) or is_trending_token(token)):
        return None
    if len(candles) < QUIET_BREAKOUT_MIN_QUIET_BARS + QUIET_BREAKOUT_RECENT_BARS:
        return None

    recent_bars = max(1, QUIET_BREAKOUT_RECENT_BARS)
    quiet_bars = candles[-(QUIET_BREAKOUT_MIN_QUIET_BARS + recent_bars) : -recent_bars]
    recent = candles[-recent_bars:]
    if len(quiet_bars) < QUIET_BREAKOUT_MIN_QUIET_BARS or not recent:
        return None

    quiet_lows = [to_float(c.get("low")) for c in quiet_bars if to_float(c.get("low")) > 0]
    quiet_highs = [to_float(c.get("high")) for c in quiet_bars if to_float(c.get("high")) > 0]
    quiet_volumes = [to_float(c.get("volume")) for c in quiet_bars]
    if not quiet_lows or not quiet_highs:
        return None
    quiet_low = min(quiet_lows)
    quiet_high = max(quiet_highs)
    quiet_range_pct = ((quiet_high - quiet_low) / quiet_low * 100) if quiet_low > 0 else 0.0
    quiet_avg_volume = sum(quiet_volumes) / len(quiet_volumes) if quiet_volumes else 0.0
    quiet_total_volume = sum(quiet_volumes)

    from_price = to_float(quiet_bars[-1].get("close"))
    to_price = to_float(recent[-1].get("close"))
    if from_price <= 0 or to_price <= 0:
        return None
    change_pct = (to_price - from_price) / from_price * 100
    recent_volume = sum(to_float(c.get("volume")) for c in recent)
    current_mcap = to_float(summary.get("mcap")) or calc_mcap(token)
    if current_mcap <= 0:
        return None

    volume_ratio_base = max(quiet_avg_volume * len(recent), 1.0)
    volume_ratio = recent_volume / volume_ratio_base if volume_ratio_base > 0 else 0.0
    if quiet_range_pct > QUIET_BREAKOUT_MAX_RANGE_PCT:
        return None
    if current_mcap < QUIET_BREAKOUT_LOW_MCAP_MAX_USD:
        trigger_mode = "volume"
        if quiet_avg_volume > QUIET_BREAKOUT_MAX_AVG_VOLUME_USD:
            return None
        if recent_volume < QUIET_BREAKOUT_MIN_BREAKOUT_VOLUME_USD:
            return None
        if volume_ratio < QUIET_BREAKOUT_MIN_VOLUME_RATIO:
            return None
    elif current_mcap >= QUIET_BREAKOUT_HIGH_MCAP_MIN_USD:
        trigger_mode = "range"
        if change_pct < QUIET_BREAKOUT_MIN_CHANGE_PCT:
            return None
    else:
        return None

    pool_stats = summary.get("pool") or {}
    quiet_duration_sec = to_int(recent[0].get("ts")) - to_int(quiet_bars[0].get("ts"))
    source_type = "watchlist" if is_watchlist_token(token) else "trending"
    return {
        "score": 100,
        "signal_type": "quiet_breakout",
        "abnormal_rule": f"{source_type.upper()}_QUIET_{trigger_mode.upper()}_BREAKOUT",
        "source_type": source_type,
        "trigger_mode": trigger_mode,
        "current_mcap": current_mcap,
        "ath_mcap": to_float(summary.get("ath_mcap")),
        "price_change_pct": change_pct,
        "required_price_change_pct": QUIET_BREAKOUT_MIN_CHANGE_PCT,
        "low_mcap_max_usd": QUIET_BREAKOUT_LOW_MCAP_MAX_USD,
        "high_mcap_min_usd": QUIET_BREAKOUT_HIGH_MCAP_MIN_USD,
        "quiet_range_pct": quiet_range_pct,
        "required_quiet_range_pct": QUIET_BREAKOUT_MAX_RANGE_PCT,
        "quiet_avg_volume_usd": quiet_avg_volume,
        "required_quiet_avg_volume_usd": QUIET_BREAKOUT_MAX_AVG_VOLUME_USD,
        "quiet_total_volume_usd": quiet_total_volume,
        "breakout_volume_usd": recent_volume,
        "breakout_volume_ratio": volume_ratio,
        "required_breakout_volume_ratio": QUIET_BREAKOUT_MIN_VOLUME_RATIO,
        "required_breakout_volume_usd": QUIET_BREAKOUT_MIN_BREAKOUT_VOLUME_USD,
        "quiet_duration_sec": quiet_duration_sec,
        "quiet_bars": len(quiet_bars),
        "breakout_bars": len(recent),
        "from_price": from_price,
        "to_price": to_price,
        "pool_total_liquidity": to_float(pool_stats.get("total_liquidity")),
        "pool_mcap_ratio": to_float(pool_stats.get("liquidity_mcap_ratio")),
        "pool_mcap_ratio_text": pool_stats.get("liquidity_mcap_ratio_text", "N/A"),
        "reasons": [
            f"{source_type} sideways {quiet_duration_sec / 3600:.1f}h range {quiet_range_pct:.1f}%<={QUIET_BREAKOUT_MAX_RANGE_PCT:.1f}%",
            (
                f"low mcap volume breakout ${recent_volume:,.0f}>={QUIET_BREAKOUT_MIN_BREAKOUT_VOLUME_USD:,.0f}, "
                f"{volume_ratio:.1f}x>={QUIET_BREAKOUT_MIN_VOLUME_RATIO:.1f}x"
                if trigger_mode == "volume"
                else f"high mcap range breakout {change_pct:.1f}%>={QUIET_BREAKOUT_MIN_CHANGE_PCT:.1f}%"
            ),
        ],
    }


def check_quiet_runup(
    token: dict[str, Any],
    summary: dict[str, Any],
    candles: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Find an earlier sideways window followed by a sustained run-up to current price."""
    if not QUIET_RUNUP_ENABLED or not (is_watchlist_token(token) or is_trending_token(token)):
        return None
    min_quiet = max(3, QUIET_RUNUP_MIN_QUIET_BARS)
    lookback = candles[-QUIET_RUNUP_LOOKBACK_BARS:] if QUIET_RUNUP_LOOKBACK_BARS > 0 else candles
    if len(lookback) < min_quiet + 3:
        return None

    current_mcap = to_float(summary.get("mcap")) or calc_mcap(token)
    current_price = to_float(lookback[-1].get("close"))
    if current_mcap <= 0 or current_price <= 0:
        return None

    # Skip if recent trend is declining (run-up already collapsed)
    if len(lookback) >= 6:
        recent_3 = lookback[-3:]
        recent_start = to_float(recent_3[0].get("close"))
        recent_end = to_float(recent_3[-1].get("close"))
        recent_trend = (recent_end - recent_start) / recent_start * 100 if recent_start > 0 else 0
        if recent_trend < -5:
            return None  # Don't push a run-up that already collapsed

    best: dict[str, Any] | None = None
    max_start = len(lookback) - min_quiet - 2
    for start in range(0, max_start + 1):
        quiet = lookback[start : start + min_quiet]
        lows = [to_float(c.get("low")) for c in quiet if to_float(c.get("low")) > 0]
        highs = [to_float(c.get("high")) for c in quiet if to_float(c.get("high")) > 0]
        volumes = [to_float(c.get("volume")) for c in quiet]
        if not lows or not highs:
            continue
        quiet_low = min(lows)
        quiet_high = max(highs)
        quiet_range_pct = ((quiet_high - quiet_low) / quiet_low * 100) if quiet_low > 0 else 0.0
        if quiet_range_pct > QUIET_RUNUP_MAX_RANGE_PCT:
            continue

        quiet_close = to_float(quiet[-1].get("close"))
        if quiet_close <= 0:
            continue
        runup = lookback[start + min_quiet :]
        runup_gain_pct = (current_price - quiet_close) / quiet_close * 100
        if runup_gain_pct < QUIET_RUNUP_MIN_GAIN_PCT:
            continue

        quiet_avg_volume = sum(volumes) / len(volumes) if volumes else 0.0
        runup_volume = sum(to_float(c.get("volume")) for c in runup)
        volume_ratio = runup_volume / max(quiet_avg_volume * len(runup), 1.0)
        if volume_ratio < QUIET_RUNUP_MIN_BREAKOUT_VOLUME_RATIO:
            continue

        item = {
            "quiet": quiet,
            "runup": runup,
            "quiet_range_pct": quiet_range_pct,
            "quiet_avg_volume": quiet_avg_volume,
            "runup_volume": runup_volume,
            "volume_ratio": volume_ratio,
            "gain_pct": runup_gain_pct,
            "quiet_close": quiet_close,
        }
        if best is None or item["gain_pct"] > best["gain_pct"]:
            best = item

    if not best:
        return None

    quiet = best["quiet"]
    runup = best["runup"]
    pool_stats = summary.get("pool") or {}
    source_type = "watchlist" if is_watchlist_token(token) else "trending"
    quiet_duration_sec = to_int(quiet[-1].get("ts")) - to_int(quiet[0].get("ts"))
    runup_duration_sec = to_int(runup[-1].get("ts")) - to_int(runup[0].get("ts")) if runup else 0
    return {
        "score": 100,
        "signal_type": "quiet_runup",
        "abnormal_rule": f"{source_type.upper()}_QUIET_RUNUP",
        "source_type": source_type,
        "trigger_mode": "runup",
        "current_mcap": current_mcap,
        "ath_mcap": to_float(summary.get("ath_mcap")),
        "price_change_pct": best["gain_pct"],
        "required_price_change_pct": QUIET_RUNUP_MIN_GAIN_PCT,
        "quiet_range_pct": best["quiet_range_pct"],
        "required_quiet_range_pct": QUIET_RUNUP_MAX_RANGE_PCT,
        "quiet_avg_volume_usd": best["quiet_avg_volume"],
        "breakout_volume_usd": best["runup_volume"],
        "breakout_volume_ratio": best["volume_ratio"],
        "required_breakout_volume_ratio": QUIET_RUNUP_MIN_BREAKOUT_VOLUME_RATIO,
        "quiet_duration_sec": quiet_duration_sec,
        "runup_duration_sec": runup_duration_sec,
        "quiet_bars": len(quiet),
        "breakout_bars": len(runup),
        "from_price": best["quiet_close"],
        "to_price": current_price,
        "quiet_from_ts": to_int(quiet[0].get("ts")),
        "quiet_to_ts": to_int(quiet[-1].get("ts")),
        "runup_from_ts": to_int(runup[0].get("ts")) if runup else 0,
        "runup_to_ts": to_int(runup[-1].get("ts")) if runup else 0,
        "pool_total_liquidity": to_float(pool_stats.get("total_liquidity")),
        "pool_mcap_ratio": to_float(pool_stats.get("liquidity_mcap_ratio")),
        "pool_mcap_ratio_text": pool_stats.get("liquidity_mcap_ratio_text", "N/A"),
        "reasons": [
            f"{source_type} quiet window {quiet_duration_sec / 3600:.1f}h range {best['quiet_range_pct']:.1f}%<={QUIET_RUNUP_MAX_RANGE_PCT:.1f}%",
            f"runup {best['gain_pct']:.1f}%>={QUIET_RUNUP_MIN_GAIN_PCT:.1f}% after quiet window",
            f"runup volume ${best['runup_volume']:,.0f}, {best['volume_ratio']:.1f}x>={QUIET_RUNUP_MIN_BREAKOUT_VOLUME_RATIO:.1f}x",
        ],
    }


def quiet_breakout_signal_text(token: dict[str, Any], signal: dict[str, Any]) -> str:
    address = token_address(token)
    trend = token.get("_trend_interval") or "N/A"
    signal_type = str(signal.get("signal_type") or "quiet_breakout")
    title = "quiet runup" if signal_type == "quiet_runup" else "quiet breakout"
    return (
        f"{str(signal.get('source_type') or 'watchlist').title()} {title} | ${token.get('symbol') or 'UNKNOWN'}\n"
        f"来源: {trend} 扫描\n"
        f"CA: {address}\n"
        f"MCap: ${signal.get('current_mcap', 0):,.0f}\n"
        f"Mode: {signal.get('trigger_mode') or '-'}\n"
        f"Sideways: {signal.get('quiet_duration_sec', 0) / 3600:.1f}h | range {signal.get('quiet_range_pct', 0):.1f}%\n"
        f"Quiet avg volume: ${signal.get('quiet_avg_volume_usd', 0):,.0f}\n"
        f"Move: {signal.get('price_change_pct', 0):.1f}% | recent volume ${signal.get('breakout_volume_usd', 0):,.0f} ({signal.get('breakout_volume_ratio', 0):.1f}x)\n"
        f"https://gmgn.ai/sol/token/{address}"
    )


def old_surge_signal_text(token: dict[str, Any], surge: dict[str, Any]) -> str:
    address = token_address(token)
    trend = token.get("_trend_interval") or "N/A"
    return (
        f"老币异动拉升 | ${token.get('symbol') or 'UNKNOWN'}\n"
        f"来源: {trend} 扫描\n"
        f"CA: {address}\n"
        f"当前市值: ${surge['current_mcap']:,.0f}\n"
        f"拉升幅度: {surge['change_pct']:.1f}% (最佳分辨率: {surge['best_resolution']})\n"
        f"触发分辨率: {', '.join(surge['resolutions'])}\n"
        f"价格: ${surge['from_price']:.8f} -> ${surge['to_price']:.8f}\n"
        f"成交量: ${surge['volume']:,.0f}\n"
        f"https://gmgn.ai/sol/token/{address}"
    )


def old_surge_cooldown_key(address: str) -> str:
    return f"bottom:old_surge:last:{address}"


def handle_token(scan_id: str, token: dict[str, Any], notify: bool, frontend_update_allowed: bool = False) -> bool:
    address = token_address(token)
    raw_holders = fetch_top100_holders(address)
    if not raw_holders:
        print(f"{token_label(token)} no holders")
        return False
    top_profit_traders, top_loss_traders = fetch_profit_loss_trader_snapshots(address)
    kline_resolution = token_kline_resolution(token)
    candles = fetch_kline(address, kline_resolution, token)
    normalized_resolution = str(kline_resolution or "").lower()
    # Prediction and micro-structure analysis always need both 5m and 1m K-lines.
    candles_5m = candles if normalized_resolution in {"5m", "5min", "5"} else fetch_kline(address, "5m", token)
    candles_1m = candles if normalized_resolution in {"1m", "1min", "1"} else fetch_kline(address, "1m", token)
    # Enrich token with Binance market cap (GMGN token info often returns empty market_cap for pump.fun tokens)
    binance = fetch_binance_dynamic_metrics(address)
    token = apply_binance_dynamic_metrics(token, binance)
    if binance.get("price"):
        token["_binance_price"] = binance["price"]
    if binance.get("pool_liquidity"):
        token["_binance_liquidity"] = binance["pool_liquidity"]
    summary, holders = build_snapshot_json(token, raw_holders, candles, kline_resolution)
    summary["_5m_kline_journey"] = classify_kline_journey(candles_5m, "5m")
    # Add 1m volume ratio to summary for quick verdict
    if candles_1m and len(candles_1m) >= 6:
        mid_1m = len(candles_1m) // 2
        early_vol = sum(to_float(c.get("volume")) for c in candles_1m[:3]) / 3
        late_vol = sum(to_float(c.get("volume")) for c in candles_1m[-3:]) / 3
        summary["_1m_vol_ratio"] = late_vol / early_vol if early_vol > 0 else 0
        summary["_1m_vol_early"] = early_vol
        summary["_1m_vol_late"] = late_vol
        summary["_1m_candles"] = len(candles_1m)
    history = recent_snapshots(address)
    analysis = analyze_abnormal_snapshot(holders, history, summary)
    if isinstance(summary.get("_5m_kline_journey"), dict):
        summary["_5m_kline_journey"] = enrich_kline_journey_for_signal(
            summary.get("_5m_kline_journey"),
            str(analysis.get("signal_type") or ""),
        )
    if candles_1m and len(candles_1m) >= 6:
        summary["_1m_micro"] = classify_1m_micro_strategy(
            candles_1m,
            str(analysis.get("signal_type") or ""),
            to_float(analysis.get("current_mcap", calc_mcap(token))),
        )
    if analysis.get("signal_type") == "abnormal" and is_watchlist_token(token):
        analysis["signal_type"] = "watchlist_abnormal"
    already_notified = previous_signal_exists(address, analysis.get("signal_type", ""))
    has_previous_bottom_signal = previous_bottom_signal_exists(address)
    baseline = first_signal_baseline(address, analysis.get("signal_type", ""))
    snapshot_id = save_snapshot(scan_id, token, summary, holders, analysis, top_profit_traders, top_loss_traders)
    analysis = {**analysis, "snapshot_id": snapshot_id}
    if analysis.get("signal_type") != "watch":
        print(
            f"{token_label(token)} snapshot={snapshot_id} history={len(history)} "
            f"type={analysis.get('signal_type')} score={analysis.get('score')} "
            f"ath=${analysis.get('ath_mcap', 0):,.0f} "
            f"mcap=${analysis.get('current_mcap', 0):,.0f} "
            f"price={analysis.get('price_change_pct', 0):.1f}%/{analysis.get('required_price_change_pct', 0):.1f}% "
            f"pool=${analysis.get('pool_total_liquidity', 0):,.0f} "
            f"pool/mcap={analysis.get('pool_mcap_ratio', 0):.1%}"
        )
    # K-line quality filter: weak candle body + weak pre-trend = fake breakout
    if notify and should_notify(analysis) and candles:
        last_candle = candles[-1] if candles else {}
        last_open = to_float(last_candle.get("open"))
        last_close = to_float(last_candle.get("close"))
        last_body_pct = abs(last_close - last_open) / last_open * 100 if last_open > 0 else 0
        # Pre-signal trend: last 5 bars before the signal window
        pre_bars = candles[-KLINE_SIGNAL_BARS - 6:-KLINE_SIGNAL_BARS] if len(candles) > KLINE_SIGNAL_BARS + 5 else candles[:-KLINE_SIGNAL_BARS] if len(candles) > KLINE_SIGNAL_BARS else []
        pre_return = 0.0
        if len(pre_bars) >= 3:
            pre_first = to_float(pre_bars[0].get("close"))
            pre_last = to_float(pre_bars[-1].get("close"))
            pre_return = (pre_last - pre_first) / pre_first * 100 if pre_first > 0 else 0
        # Separate thresholds by resolution: 1m candles have naturally smaller bodies
        if kline_resolution == "1m":
            body_max = 0.5
            trend_min = 1.5
        else:
            body_max = 2.0
            trend_min = 3.0
        if last_body_pct < body_max and pre_return < trend_min:
            print(f"{token_label(token)} skip push: weak {kline_resolution} kline body={last_body_pct:.1f}% pre_trend={pre_return:.1f}%")
            notify = False

    # Large mcap quiet_breakout filter: 5/7 failures are >$500K
    current_mcap = calc_mcap(token) or to_float(token.get("watchlist_last_mcap"))
    if notify and analysis.get("signal_type") == "quiet_breakout" and current_mcap > 500_000:
        print(f"{token_label(token)} skip push: quiet_breakout large mcap ${current_mcap:,.0f} > $500K")
        notify = False

    if (
        notify
        and should_notify(analysis)
        and not already_notified
        and (BOTTOM_DEEPSEEK_PUSH_LEFT_ENABLED or not BOTTOM_DEEPSEEK_ASYNC_ENABLED)
    ):
        analysis = maybe_attach_deepseek_kline_prediction(
            token=token,
            summary=summary,
            analysis=analysis,
            candles_5m=candles_5m,
            candles_1m=candles_1m,
        )

    if notify and should_notify(analysis) and not already_notified:
        if USE_AGENT_DECISION:
            agent_context = run_agent_execution(
                token=token,
                summary=summary,
                raw_holders=raw_holders,
                holders=holders,
                candles=candles,
                history=history,
                analysis=analysis,
                execute=notify,
                already_notified=already_notified,
                has_previous_bottom_signal=has_previous_bottom_signal,
                snapshot_id=snapshot_id,
            )
            action_execution = agent_context.decision.get("action_executor") if agent_context else {}
            results = action_execution.get("results") if isinstance(action_execution, dict) else []
            frontend_pushed = any(
                isinstance(item, dict) and item.get("step") == "publish_frontend" and item.get("status") == "ok"
                for item in (results or [])
            )
            tg_message_id = next(
                (
                    to_int(item.get("message_id"))
                    for item in (results or [])
                    if isinstance(item, dict) and item.get("step") == "send_tg" and to_int(item.get("message_id")) > 0
                ),
                0,
            )
            if frontend_pushed:
                async_extra = build_bottom_signal_extra(token, summary, analysis, baseline)
                schedule_deepseek_post_push_analysis(
                    token=token,
                    summary=summary,
                    analysis=analysis,
                    candles_5m=candles_5m,
                    candles_1m=candles_1m,
                    base_extra=async_extra,
                    signal_text=abnormal_signal_text(token, analysis),
                    tg_message_id=tg_message_id,
                )
        else:
            web_extra = build_bottom_signal_extra(token, summary, analysis, baseline)
            signal_text = abnormal_signal_text(token, analysis)
            if publish_frontend_signal_update(signal_text, web_extra, snapshot_id=snapshot_id):
                tg_message_id = send_tg(signal_text, extra=web_extra)
                schedule_deepseek_post_push_analysis(
                    token=token,
                    summary=summary,
                    analysis=analysis,
                    candles_5m=candles_5m,
                    candles_1m=candles_1m,
                    base_extra=web_extra,
                    signal_text=signal_text,
                    tg_message_id=tg_message_id,
                )
    elif notify and should_notify(analysis) and already_notified:
        print(f"{token_label(token)} signal {analysis.get('signal_type')} already notified, skip repeat push")
    elif notify and has_previous_bottom_signal:
        if USE_AGENT_DECISION:
            agent_context = run_agent_execution(
                token=token,
                summary=summary,
                raw_holders=raw_holders,
                holders=holders,
                candles=candles,
                history=history,
                analysis=analysis,
                execute=notify,
                already_notified=already_notified,
                has_previous_bottom_signal=has_previous_bottom_signal,
                snapshot_id=snapshot_id,
            )
            action_execution = agent_context.decision.get("action_executor") if agent_context else {}
            print(
                f"{token_label(token)} previous bottom signal now {analysis.get('signal_type')}, "
                f"agent action={action_execution.get('action')} results={action_execution.get('results')}"
            )
        else:
            web_extra = build_bottom_signal_extra(token, summary, analysis, baseline)
            publish_frontend_signal_update(abnormal_signal_text(token, analysis), web_extra, snapshot_id=snapshot_id)
            print(
                f"{token_label(token)} previous bottom signal now {analysis.get('signal_type')}, "
                f"frontend updated mcap ${web_extra.get('current_mcap', 0):,.0f}"
            )

    maybe_reply_post_push_drawdown(token, summary, analysis)

    quiet_breakout = check_watchlist_quiet_breakout(token, summary, candles)
    if notify and quiet_breakout:
        quiet_type = quiet_breakout["signal_type"]
        quiet_already = previous_signal_exists(address, quiet_type)
        quiet_baseline = first_signal_baseline(address, quiet_type)
        if not quiet_already:
            quiet_snapshot_id = save_snapshot(
                scan_id + "_quiet",
                token,
                summary,
                holders,
                quiet_breakout,
                top_profit_traders,
                top_loss_traders,
            )
            quiet_breakout = {**quiet_breakout, "snapshot_id": quiet_snapshot_id}
            if not BOTTOM_DEEPSEEK_ASYNC_ENABLED:
                quiet_breakout = maybe_attach_deepseek_kline_prediction(
                    token=token,
                    summary=summary,
                    analysis=quiet_breakout,
                    candles_5m=candles_5m,
                    candles_1m=candles_1m,
                )
            quiet_extra = build_bottom_signal_extra(token, summary, quiet_breakout, quiet_baseline)
            quiet_text = quiet_breakout_signal_text(token, quiet_breakout)
            if publish_frontend_signal_update(quiet_text, quiet_extra, status="frontend_update", snapshot_id=quiet_snapshot_id):
                quiet_message_id = send_tg(quiet_text, extra=quiet_extra)
                schedule_deepseek_post_push_analysis(
                    token=token,
                    summary=summary,
                    analysis=quiet_breakout,
                    candles_5m=candles_5m,
                    candles_1m=candles_1m,
                    base_extra=quiet_extra,
                    signal_text=quiet_text,
                    tg_message_id=quiet_message_id,
                )
            print(
                f"{token_label(token)} quiet_breakout {quiet_breakout['price_change_pct']:.1f}% "
                f"after sideways {quiet_breakout['quiet_duration_sec'] / 3600:.1f}h "
                f"range={quiet_breakout['quiet_range_pct']:.1f}% avg_vol=${quiet_breakout['quiet_avg_volume_usd']:,.0f}"
            )
        else:
            print(f"{token_label(token)} quiet_breakout already notified")

    quiet_runup = check_quiet_runup(token, summary, candles)
    if notify and quiet_runup:
        runup_type = quiet_runup["signal_type"]
        runup_already = previous_signal_exists(address, runup_type)
        runup_baseline = first_signal_baseline(address, runup_type)
        if not runup_already:
            runup_snapshot_id = save_snapshot(
                scan_id + "_runup",
                token,
                summary,
                holders,
                quiet_runup,
                top_profit_traders,
                top_loss_traders,
            )
            quiet_runup = {**quiet_runup, "snapshot_id": runup_snapshot_id}
            runup_extra = build_bottom_signal_extra(token, summary, quiet_runup, runup_baseline)
            runup_text = quiet_breakout_signal_text(token, quiet_runup)
            # quiet_runup: 60-78% dead rate → DB record only, no TG, no frontend
            record_top100_push(
                text=runup_text,
                extra=runup_extra,
                status="db_only",
                source="bottom_abnormal",
                chain=CHAIN,
            )
            print(
                f"{token_label(token)} quiet_runup {quiet_runup['price_change_pct']:.1f}% "
                f"(TG blocked) "
                f"after quiet range={quiet_runup['quiet_range_pct']:.1f}% "
                f"vol_ratio={quiet_runup['breakout_volume_ratio']:.1f}x"
            )
        else:
            print(f"{token_label(token)} quiet_runup already notified, skip repeat push")

    # Old token surge detection (independent of abnormal/EMA signal)
    if notify and OLD_TOKEN_SURGE_ENABLED:
        surge = check_old_token_surge(token)
        if surge:
            surge_type = "old_surge"
            surge_already = previous_signal_exists(address, surge_type)
            if not surge_already:
                surge_text = old_surge_signal_text(token, surge)
                surge_extra = {
                    "event_ts": now_ts(),
                    "signal_type": surge_type,
                    "change_pct": surge["change_pct"],
                    "price_change_pct": surge["change_pct"],
                    "required_change_pct": surge.get("required_change_pct"),
                    "age_bucket": surge.get("age_bucket"),
                    "age_sec": surge.get("age_sec"),
                    "resolutions": surge["resolutions"],
                    "best_resolution": surge["best_resolution"],
                    "current_mcap": surge["current_mcap"],
                    "symbol": token.get("symbol"),
                    "address": address,
                }
                send_tg(surge_text, extra=surge_extra)
                record_top100_push(
                    text=surge_text,
                    extra=surge_extra,
                    status="tg_sent",
                    source="bottom_abnormal",
                    chain=CHAIN,
                )
                print(
                    f"{token_label(token)} old_surge {surge['change_pct']:.1f}%/"
                    f"{surge.get('required_change_pct', 0):.1f}% {surge.get('age_bucket', '')} "
                    f"at {surge['best_resolution']} mcap=${surge['current_mcap']:,.0f}"
                )
            else:
                print(
                    f"{token_label(token)} old_surge {surge['change_pct']:.1f}%/"
                    f"{surge.get('required_change_pct', 0):.1f}% already notified"
                )

    # EMA 9/26 crossover detection is disabled by default; keep it behind
    # an explicit flag so bottom abnormal pushes are not driven by K-line crosses.
    if EMA_GOLDEN_CROSS_ENABLED and notify and (frontend_update_allowed or should_notify(analysis)) and candles and len(candles) >= 30:
        prices = [c["close"] for c in candles]
        crossover = detect_ema_crossover(prices)
        if crossover and crossover["type"] == "golden_cross":
            crossover_signal_type = f"ema_golden_cross"
            ema_already = previous_signal_exists(address, crossover_signal_type)
            ema_baseline = first_signal_baseline(address, crossover_signal_type)
            current_mcap = to_float(summary.get("mcap"))
            pool_liq = to_float(summary.get("pool", {}).get("total_liquidity"))
            pool_rat = to_float(summary.get("pool", {}).get("liquidity_mcap_ratio"))
            first_mcap = to_float(ema_baseline.get("first_signal_mcap")) or current_mcap
            first_delta = current_mcap - first_mcap if first_mcap > 0 else 0.0
            first_change_pct = (first_delta / first_mcap * 100) if first_mcap > 0 else 0.0
            narrative = resolve_cached_or_db_narrative(address)
            if not narrative or not narrative.get("narrative_desc"):
                try:
                    narrative = get_binance_narrative(address, symbol=token.get("symbol"), name=token.get("name"))
                except Exception as exc:
                    print(f"{address[:8]} EMA binance narrative failed: {exc}")
                    narrative = {}
            # Extract the actual timestamp of the golden cross candle
            crossover_bar_idx = crossover.get("bar_index", 0)
            crossover_ts = int(candles[crossover_bar_idx]["ts"]) if 0 <= crossover_bar_idx < len(candles) else 0
            ema_extra = {
                "signal_type": crossover_signal_type,
                "trend_interval": token.get("_trend_interval") or TREND_INTERVAL,
                "crossover_type": crossover["type"],
                "crossover_ts": crossover_ts,
                "ema9": crossover["ema9"],
                "ema26": crossover["ema26"],
                "strength": crossover["strength"],
                "bars_below": crossover.get("bars_below_before_cross", 0),
                "symbol": token.get("symbol"),
                "address": address,
                "current_mcap": current_mcap,
                "liquidity": pool_liq,
                "holder_count": summary.get("holder_count", 0),
                "created_ts": summary.get("created_ts", 0),
                "launch_ts": summary.get("launch_ts", 0),
                "created_age_sec": summary.get("created_age_sec", 0),
                "launch_age_sec": summary.get("launch_age_sec", 0),
                "age_sec": summary.get("age_sec", 0),
                "first_signal_mcap": first_mcap,
                "first_signal_ts": to_int(ema_baseline.get("first_signal_ts")),
                "first_signal_delta_mcap": first_delta,
                "first_signal_change_pct": first_change_pct,
                "pool_liquidity": pool_liq,
                "pool_total_liquidity": pool_liq,
                "pool_mcap_ratio": pool_rat,
                "narrative": narrative.get("narrative_desc") or token.get("narrative_desc") or "",
                "narrative_desc": narrative.get("narrative_desc") or token.get("narrative_desc") or "",
                "narrative_type": narrative.get("narrative_type") or token.get("narrative_type") or "",
                "binance_narrative": compact_narrative(narrative),
            }
            # Always push frontend update for golden cross
            signal_text = ema_crossover_signal_text(token, crossover, current_mcap, pool_liq, pool_rat, crossover_ts)
            if not ema_already:
                ema_snapshot_id = save_snapshot(
                    scan_id + "_ema",
                    token,
                    summary,
                    holders,
                    {"signal_type": crossover_signal_type, "score": 80, "crossover": crossover},
                    top_profit_traders,
                    top_loss_traders,
                )
                ema_extra = {**ema_extra, "snapshot_id": ema_snapshot_id}
                # Push to frontend on first detection
                if publish_frontend_signal_update(signal_text, ema_extra, status="frontend_update", snapshot_id=ema_snapshot_id):
                    send_tg(signal_text, extra=ema_extra)
                print(f"{token_label(token)} EMA golden cross detected! bars_below={crossover.get('bars_below_before_cross', 0)} crossover_ts={crossover_ts}")
            else:
                print(
                    f"{token_label(token)} EMA golden cross already notified, skip repeat push"
                )

    return True


def prune_recent_seen(recent_seen: dict[str, float], ttl_sec: int) -> None:
    if ttl_sec <= 0:
        recent_seen.clear()
        return
    cutoff = time.monotonic() - ttl_sec
    stale = [address for address, seen_at in recent_seen.items() if seen_at < cutoff]
    for address in stale:
        recent_seen.pop(address, None)


def apply_token_limit(tokens: list[dict[str, Any]], max_tokens: int) -> list[dict[str, Any]]:
    if max_tokens <= 0:
        return tokens
    return tokens[:max_tokens]


def scan_once(
    args: argparse.Namespace,
    intervals: tuple[str, ...] | list[str] | None = None,
    include_watchlist: bool = True,
    mode_name: str = "trending+watchlist",
    recent_seen: dict[str, float] | None = None,
    skip_recent_seen: bool = False,
    recent_seen_ttl_sec: int = TREND_CROSS_WINDOW_DEDUP_SEC,
) -> None:
    scan_id = str(uuid.uuid4())
    active_intervals = tuple(intervals or TREND_INTERVALS)
    trending_tokens = fetch_trending_tokens(active_intervals)
    watchlist_tokens = fetch_watchlist_tokens() if include_watchlist else []
    alpha_abnormal_tokens = fetch_alpha_abnormal_tokens()

    # Dedup: remove watchlist CAs from trending to avoid double-processing
    watchlist_addresses = {token_address(t) for t in watchlist_tokens if token_address(t)}
    trending_tokens = [t for t in trending_tokens if token_address(t) not in watchlist_addresses]

    prefiltered_trending = []
    prefiltered_skipped = 0
    for token in trending_tokens:
        skip_reason = prefilter_trending_token(token)
        if skip_reason:
            prefiltered_skipped += 1
            continue
        prefiltered_trending.append(token)

    # Recent seen dedup for trending only
    dedupe_skipped = 0
    if recent_seen is not None:
        prune_recent_seen(recent_seen, recent_seen_ttl_sec)
        if skip_recent_seen:
            filtered_trending = []
            for token in prefiltered_trending:
                address = token_address(token)
                if address and address in recent_seen:
                    dedupe_skipped += 1
                    continue
                filtered_trending.append(token)
            prefiltered_trending = filtered_trending

    selected_trending = apply_token_limit(prefiltered_trending, args.max_tokens)

    if recent_seen is not None:
        seen_at = time.monotonic()
        for token in selected_trending:
            address = token_address(token)
            if address:
                recent_seen[address] = seen_at

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] scan_id={scan_id} "
        f"mode={mode_name} "
        f"intervals={','.join(active_intervals)} "
        f"trending={len(trending_tokens)} prefiltered={len(prefiltered_trending)} "
        f"selected={len(selected_trending)} "
        f"prefilter_skip={prefiltered_skipped} watchlist={len(watchlist_tokens)} "
        f"alpha_abnormal={len(alpha_abnormal_tokens)} "
        f"dedupe_skip={dedupe_skipped}"
    )

    processed = 0
    skipped = 0

    # ========================================================================
    # Phase 1: Process ALL watchlist tokens first (no max_tokens limit).
    #   Update mcap/pool via GMGN CLI, then only run abnormal analysis
    #   for tokens with mcap < $300K (底部异动范围).
    # ========================================================================
    WATCHLIST_ABNORMAL_MAX_MCAP = 300_000

    for token in watchlist_tokens:
        try:
            address = token_address(token)
            if token.get("blacklisted") or is_watchlist_blacklisted(address):
                print(f"{token_label(token)} blacklisted, skipped")
                skipped += 1
                continue
            pre_skip_reason = recent_snapshot_skip_reason(address, token)
            if pre_skip_reason:
                skipped += 1
                print(f"{token_label(token)} skip {pre_skip_reason}")
                continue
            info, security = fetch_token_metadata(address)
            token = merge_token_metadata(token, info, security)
            fill_watchlist_create_at(token)
            created_ts = token_created_ts(info)
            launch_ts = token_launch_ts(info)
            gmgn_ath_mcap = current_token_ath_mcap(info)
            if created_ts > 0 or launch_ts > 0 or gmgn_ath_mcap > 0:
                store_fill_token_created_at(address, created_ts, gmgn_ath_mcap, launch_ts)
                token["_gmgn_created_ts"] = created_ts
                token["_gmgn_open_ts"] = launch_ts
                if gmgn_ath_mcap > 0:
                    token["_gmgn_ath_mcap"] = gmgn_ath_mcap
            current_mcap = calc_mcap(token)
            pool_data = fetch_token_pool(address)
            token = attach_token_pool(token, pool_data)
            pool_summary, pool_reliable, pool_unreliable_reason = summarize_gmgn_pool_data(pool_data, token)
            pool_liquidity = to_float(pool_summary.get("total_liquidity"))
            pool_mcap_ratio = to_float(pool_summary.get("liquidity_mcap_ratio"))
            if not pool_reliable:
                previous_pool_liquidity = to_float(token.get("watchlist_last_pool_liquidity"))
                previous_pool_ratio = to_float(token.get("watchlist_last_pool_mcap_ratio"))
                if previous_pool_liquidity > 0:
                    pool_liquidity = previous_pool_liquidity
                    pool_mcap_ratio = previous_pool_ratio
                print(f"{address[:8]} pool check skipped: {pool_unreliable_reason}")
            # ---- Always update DB immediately after fetching GMGN data ----
            update_watchlist_seen(
                address, current_mcap,
                pool_liquidity=pool_liquidity, pool_mcap_ratio=pool_mcap_ratio,
                fee_sol=fee_sol(token), symbol=token.get("symbol"),
            )
            maybe_record_daily_mcap_milestone(token, current_mcap, args.notify)
            # ---- Filters below only control whether to proceed to abnormal analysis ----
            if launch_ts <= 0:
                skipped += 1
                token["_trench"] = True
                print(f"{token_label(token)} skip abnormal: open_ts missing (发射时间缺失)")
                continue
            open_age_sec = now_ts() - launch_ts
            if open_age_sec < 1 * 3600:
                skipped += 1
                print(f"{token_label(token)} skip abnormal: open_age={open_age_sec/3600:.1f}h < 1h (发射时间小于1H)")
                continue
            if pool_reliable and 0 < pool_mcap_ratio < 0.07:
                skipped += 1
                print(f"{token_label(token)} skip abnormal: pool/mcap={pool_mcap_ratio:.1%} < 7% (流动性不足)")
                continue
            if pool_reliable and pool_liquidity < WATCHLIST_DELETE_BELOW_POOL_LIQUIDITY_USD:
                deleted = delete_watchlist_token(
                    address, "pool_liquidity_below_threshold",
                    current_mcap=current_mcap, pool_liquidity=pool_liquidity,
                    pool_mcap_ratio=pool_mcap_ratio,
                    metadata={"threshold": WATCHLIST_DELETE_BELOW_POOL_LIQUIDITY_USD, "trigger": "scan_once", "pool_reliable": pool_reliable},
                )
                if deleted:
                    print(f"{address[:8]} watchlist deleted: pool ${pool_liquidity:,.0f}<${WATCHLIST_DELETE_BELOW_POOL_LIQUIDITY_USD:,.0f}")
                skipped += 1
                continue
            daily_mcap_date = str(token.get("watchlist_daily_mcap_date") or "")
            if daily_mcap_date == datetime.now().date().isoformat() and current_mcap >= DAILY_MCAP_MILESTONE_USD * 0.3:
                active_ts = token_active_ts({**token, **(info or {})})
                age_sec = (now_ts() - active_ts) if active_ts > 0 else 0
                if 0 < age_sec <= NEW_TOKEN_AGE_CUTOFF_SEC:
                    pool_summary2 = summarize_pools(token)
                    pool_liq = to_float(pool_summary2.get("total_liquidity"))
                    pool_ratio = to_float(pool_summary2.get("liquidity_mcap_ratio"))
                    if pool_liq >= BOTTOM_ABNORMAL_MIN_POOL_LIQUIDITY_USD and pool_ratio >= DAILY_MCAP_MIN_POOL_MCAP_RATIO:
                        peak = max(to_float(token.get("watchlist_peak_mcap")), to_float(token.get("peak_mcap")), current_mcap)
                        publish_daily_1m_frontend_update(token, current_mcap, peak)
            if current_mcap > 0 and current_mcap < WATCHLIST_DELETE_BELOW_MCAP_USD:
                if token.get("watchlist_daily_mcap_date"):
                    skipped += 1
                    print(f"{address[:8]} watchlist daily mcap record kept: mcap ${current_mcap:,.0f}<${WATCHLIST_DELETE_BELOW_MCAP_USD:,.0f}")
                    continue
                deleted = delete_watchlist_token(
                    address, "mcap_below_threshold",
                    current_mcap=current_mcap, pool_liquidity=pool_liquidity,
                    pool_mcap_ratio=pool_mcap_ratio,
                    metadata={"threshold": WATCHLIST_DELETE_BELOW_MCAP_USD, "trigger": "scan_once"},
                )
                if deleted:
                    print(f"{address[:8]} watchlist deleted: mcap ${current_mcap:,.0f}<${WATCHLIST_DELETE_BELOW_MCAP_USD:,.0f}")
                skipped += 1
                continue
            # Only run abnormal analysis for tokens with mcap < $300K
            if current_mcap > 0 and current_mcap >= WATCHLIST_ABNORMAL_MAX_MCAP:
                skipped += 1
                print(f"{token_label(token)} watchlist mcap ${current_mcap:,.0f} >= $300K, metadata updated, skip abnormal analysis")
                continue
            skip_reason = recent_snapshot_skip_reason(token_address(token), token)
            if skip_reason:
                skipped += 1
                print(f"{token_label(token)} skip {skip_reason}")
                continue
            if handle_token(scan_id, token, args.notify, frontend_update_allowed=True):
                processed += 1
        except Exception as exc:
            print(f"{token_label(token)} failed: {exc}")
        time.sleep(args.token_delay)

    # ========================================================================
    # Phase 2: Process trending tokens (deduped from watchlist; max_tokens <= 0 means no limit)
    # ========================================================================
    for token in selected_trending:
        try:
            address = token_address(token)
            if token.get("blacklisted") or is_watchlist_blacklisted(address):
                print(f"{token_label(token)} blacklisted, skipped")
                skipped += 1
                continue
            pre_skip_reason = recent_snapshot_skip_reason(address, token)
            if pre_skip_reason:
                skipped += 1
                print(f"{token_label(token)} skip {pre_skip_reason}")
                continue
            info, security = fetch_token_metadata(address)
            token = merge_token_metadata(token, info, security)
            fill_watchlist_create_at(token)
            created_ts = token_created_ts(info)
            launch_ts = token_launch_ts(info)
            if launch_ts <= 0:
                skipped += 1
                token["_trench"] = True
                print(f"{token_label(token)} skip open_ts missing (发射时间小于1H)")
                continue
            open_age_sec = now_ts() - launch_ts
            if open_age_sec < 1 * 3600:
                skipped += 1
                print(f"{token_label(token)} skip open_age={open_age_sec/3600:.1f}h < 4h (发射时间小于4H)")
                continue
            gmgn_ath_mcap = current_token_ath_mcap(info)
            if created_ts > 0 or launch_ts > 0 or gmgn_ath_mcap > 0:
                store_fill_token_created_at(address, created_ts, gmgn_ath_mcap, launch_ts)
                token["_gmgn_created_ts"] = created_ts
                token["_gmgn_open_ts"] = launch_ts
                if gmgn_ath_mcap > 0:
                    token["_gmgn_ath_mcap"] = gmgn_ath_mcap
            current_mcap = calc_mcap(token)
            pool_data = fetch_token_pool(address)
            token = attach_token_pool(token, pool_data)
            pool_summary, pool_reliable, pool_unreliable_reason = summarize_gmgn_pool_data(pool_data, token)
            pool_liquidity = to_float(pool_summary.get("total_liquidity"))
            pool_mcap_ratio = to_float(pool_summary.get("liquidity_mcap_ratio"))
            if pool_reliable and 0 < pool_mcap_ratio < 0.07:
                skipped += 1
                print(f"{token_label(token)} skip pool/mcap={pool_mcap_ratio:.1%} < 7% (流动性不足)")
                continue
            # Trending-specific filters (basic / fee / pool)
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
            skip_reason = token_pool_filter_reason(pool_liquidity, pool_reliable, pool_unreliable_reason)
            if skip_reason:
                skipped += 1
                print(f"{token_label(token)} skip {skip_reason}")
                continue
            maybe_record_daily_mcap_milestone(token, current_mcap, args.notify)
            skip_reason = recent_snapshot_skip_reason(token_address(token), token)
            if skip_reason:
                skipped += 1
                print(f"{token_label(token)} skip {skip_reason}")
                continue
            if handle_token(scan_id, token, args.notify, frontend_update_allowed=False):
                processed += 1
        except Exception as exc:
            print(f"{token_label(token)} failed: {exc}")
        time.sleep(args.token_delay)

    # ========================================================================
    # Phase 3: Alpha abnormal tokens (deduped from both watchlist and trending)
    # ========================================================================
    all_processed = watchlist_addresses | {token_address(t) for t in selected_trending if token_address(t)}
    for token in alpha_abnormal_tokens:
        if token_address(token) in all_processed:
            continue
        try:
            if handle_token(scan_id, token, args.notify, frontend_update_allowed=False):
                processed += 1
        except Exception as exc:
            print(f"{token_label(token)} alpha_abnormal failed: {exc}")
        time.sleep(args.token_delay)

    total_tokens = len(watchlist_tokens) + len(selected_trending)
    print(f"scan_id={scan_id} processed={processed}/{total_tokens} skipped={skipped}")


def _fast_snapshot_skip_reason(address: str, token: dict[str, Any]) -> str | None:
    """Shorter interval check for fast-scan path."""
    latest_ts = latest_snapshot_ts(address)
    if not latest_ts:
        return None
    age = now_ts() - latest_ts
    if age < FAST_SCAN_SNAPSHOT_INTERVAL_SEC:
        return f"fast快照{age / 60:.1f}m<{FAST_SCAN_SNAPSHOT_INTERVAL_SEC / 60:.1f}m"
    return None


def fast_scan_once(args: argparse.Namespace) -> None:
    """Scan only watchlist tokens in [FAST_SCAN_MIN_MCAP, FAST_SCAN_MAX_MCAP] MCap range."""
    scan_id = str(uuid.uuid4())

    watchlist_tokens = fetch_watchlist_tokens()
    if not watchlist_tokens:
        return

    # First pass: get MCap for all watchlist tokens without heavy API calls
    candidates = []
    for token in watchlist_tokens:
        address = token_address(token)
        if not valid_sol_ca(address):
            continue
        if token.get("blacklisted") or is_watchlist_blacklisted(address):
            continue

        # Estimate MCap from watchlist cached value before doing heavier API calls.
        last_mcap = calc_mcap(token) or to_float(token.get("watchlist_last_mcap"))
        if last_mcap < FAST_SCAN_MIN_MCAP or last_mcap > FAST_SCAN_MAX_MCAP:
            continue

        candidates.append(token)

    if not candidates:
        return

    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] scan_id={scan_id} "
        f"mode=fast_scan "
        f"watchlist={len(watchlist_tokens)} in_range={len(candidates)} "
        f"mcap_range=${FAST_SCAN_MIN_MCAP:,.0f}-${FAST_SCAN_MAX_MCAP:,.0f}"
    )

    max_tokens = FAST_SCAN_MAX_TOKENS if FAST_SCAN_MAX_TOKENS > 0 else len(candidates)
    processed = 0
    skipped = 0
    for token in candidates[:max_tokens]:
        try:
            address = token_address(token)

            # Lightweight metadata fetch (skip security, pool for speed)
            info, _security = fetch_token_metadata(address)
            if not info:
                skipped += 1
                continue
            token = merge_token_metadata(token, info, {})
            fill_watchlist_create_at(token)

            # Launch/open time filter (fast scan)
            launch_ts = token_launch_ts(info)
            if launch_ts <= 0:
                skipped += 1
                continue
            if now_ts() - launch_ts < 4 * 3600:
                skipped += 1
                continue

            current_mcap = calc_mcap(token) or to_float(token.get("watchlist_last_mcap"))
            # Re-check MCap after fresh fetch
            if current_mcap > 0 and (current_mcap < FAST_SCAN_MIN_MCAP or current_mcap > FAST_SCAN_MAX_MCAP):
                update_watchlist_seen(
                    address, current_mcap,
                    pool_liquidity=0, pool_mcap_ratio=0, fee_sol=fee_sol(token),
                    symbol=token.get("symbol"),
                )
                skipped += 1
                continue

            # Pool data (lightweight)
            pool_data = fetch_token_pool(address)
            token = attach_token_pool(token, pool_data)
            pool_summary, pool_reliable, pool_unreliable_reason = summarize_gmgn_pool_data(pool_data, token)
            pool_liquidity = to_float(pool_summary.get("total_liquidity"))
            pool_mcap_ratio = to_float(pool_summary.get("liquidity_mcap_ratio"))
            if not pool_reliable:
                pool_liquidity = to_float(token.get("watchlist_last_pool_liquidity"))
                pool_mcap_ratio = to_float(token.get("watchlist_last_pool_mcap_ratio"))

            # Delete check
            if current_mcap > 0 and current_mcap < WATCHLIST_DELETE_BELOW_MCAP_USD:
                deleted = delete_watchlist_token(
                    address, "mcap_below_threshold",
                    current_mcap=current_mcap,
                    pool_liquidity=pool_liquidity,
                    pool_mcap_ratio=pool_mcap_ratio,
                    metadata={"threshold": WATCHLIST_DELETE_BELOW_MCAP_USD, "trigger": "fast_scan"},
                )
                if deleted:
                    print(f"  {address[:8]} fast_scan deleted: mcap ${current_mcap:,.0f}")
                skipped += 1
                continue

            # Update watchlist seen timestamp + MCap
            update_watchlist_seen(
                address, current_mcap,
                pool_liquidity=pool_liquidity, pool_mcap_ratio=pool_mcap_ratio,
                fee_sol=fee_sol(token), symbol=token.get("symbol"),
            )

            may_notify = bool(args.notify)
            # Use fast-scan specific snapshot interval
            skip_reason = _fast_snapshot_skip_reason(address, token)
            if skip_reason:
                skipped += 1
                continue

            if handle_token(scan_id, token, may_notify, frontend_update_allowed=True):
                processed += 1
        except Exception as exc:
            print(f"  {token_label(token)} fast_scan failed: {exc}")
        time.sleep(args.fast_token_delay)

    if processed or skipped:
        print(f"  scan_id={scan_id} fast_scan processed={processed}/{len(candidates)} skipped={skipped}")


def parse_trend_interval_schedules() -> list[tuple[str, int]]:
    schedules: list[tuple[str, int]] = []
    seen = set()
    for raw in TREND_INTERVAL_SCHEDULES_RAW.split(","):
        item = raw.strip()
        if not item:
            continue
        if ":" in item:
            interval, seconds = item.split(":", 1)
        else:
            interval, seconds = item, str(DEFAULT_INTERVAL_SEC)
        interval = interval.strip()
        if not interval or interval in seen:
            continue
        try:
            every_sec = max(1, int(float(seconds.strip())))
        except (TypeError, ValueError):
            every_sec = DEFAULT_INTERVAL_SEC
        schedules.append((interval, every_sec))
        seen.add(interval)

    for interval in TREND_INTERVALS:
        if interval and interval not in seen:
            schedules.append((interval, DEFAULT_INTERVAL_SEC))
            seen.add(interval)
    return schedules or [(TREND_INTERVAL or "5m", DEFAULT_INTERVAL_SEC)]


def run_scheduled_scans(args: argparse.Namespace) -> None:
    schedules = parse_trend_interval_schedules()
    primary_interval = TREND_PRIMARY_INTERVAL or schedules[0][0]
    next_due = {interval: 0.0 for interval, _every_sec in schedules}
    next_fast_scan_due = 0.0
    recent_seen: dict[str, float] = {}
    print(
        "trend scheduler enabled: "
        + ", ".join(f"{interval}:{every_sec}s" for interval, every_sec in schedules)
        + f" primary={primary_interval} dedup={TREND_CROSS_WINDOW_DEDUP_SEC}s"
    )

    while True:
        now = time.monotonic()
        if FAST_SCAN_ENABLED and now >= next_fast_scan_due:
            started_at = time.monotonic()
            fast_scan_once(args)
            next_fast_scan_due = started_at + max(1, FAST_SCAN_INTERVAL_SEC)
            now = time.monotonic()

        due = [(interval, every_sec) for interval, every_sec in schedules if now >= next_due.get(interval, 0.0)]
        if not due:
            sleep_left = min(
                TREND_SCHEDULER_IDLE_SLEEP_SEC,
                max(0.5, min(next_due.values()) - now),
            )
            time.sleep(sleep_left)
            continue

        for interval, every_sec in due:
            started_at = time.monotonic()
            include_watchlist = interval == primary_interval
            skip_recent_seen = interval != primary_interval
            mode_name = f"scheduled_{interval}{'+watchlist' if include_watchlist else ''}"
            scan_once(
                args,
                intervals=(interval,),
                include_watchlist=include_watchlist,
                mode_name=mode_name,
                recent_seen=recent_seen,
                skip_recent_seen=skip_recent_seen,
                recent_seen_ttl_sec=TREND_CROSS_WINDOW_DEDUP_SEC,
            )
            next_due[interval] = started_at + every_sec
            if args.once:
                return


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Monitor multi-interval trending tokens with one JSON snapshot table.")
    parser.add_argument("--once", action="store_true", help="Run once and exit.")
    parser.add_argument("--watch", action="store_true", help="Run forever.")
    parser.add_argument("--notify", action="store_true", help="Send Telegram messages for new signals.")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL_SEC, help="Watch interval seconds.")
    parser.add_argument("--max-tokens", type=int, default=MAX_TOKENS, help="Max trending tokens per scan; <=0 means no limit.")
    parser.add_argument("--token-delay", type=float, default=0.5, help="Delay between holder calls.")
    parser.add_argument("--min-mcap", type=float, default=MIN_MCAP_USD, help="Skip tokens below this market cap in USD.")
    parser.add_argument("--min-age-hours", type=float, default=MIN_TOKEN_AGE_SEC / 3600, help="Skip tokens younger than this many hours.")
    parser.add_argument("--min-fee-sol", type=float, default=MIN_FEE_SOL, help="Skip tokens below this SOL fee value.")
    parser.add_argument("--min-pool-liquidity", type=float, default=MIN_POOL_LIQUIDITY_USD, help="Skip non-watchlist tokens below this pool liquidity in USD.")
    parser.add_argument("--fast-scan", action="store_true", default=FAST_SCAN_ENABLED, help="Enable 1-min fast scan for bottom-range watchlist tokens.")
    parser.add_argument("--fast-interval", type=int, default=FAST_SCAN_INTERVAL_SEC, help="Fast scan interval seconds.")
    parser.add_argument("--fast-min-mcap", type=float, default=FAST_SCAN_MIN_MCAP, help="Fast scan minimum MCap in USD.")
    parser.add_argument("--fast-max-mcap", type=float, default=FAST_SCAN_MAX_MCAP, help="Fast scan maximum MCap in USD.")
    parser.add_argument("--fast-snapshot-interval", type=int, default=FAST_SCAN_SNAPSHOT_INTERVAL_SEC, help="Fast scan per-token snapshot interval seconds.")
    parser.add_argument("--fast-token-delay", type=float, default=FAST_SCAN_TOKEN_DELAY, help="Fast scan delay between tokens.")
    parser.add_argument("--fast-max-tokens", type=int, default=FAST_SCAN_MAX_TOKENS, help="Fast scan max tokens per cycle.")
    parser.add_argument("--verbose", action="store_true", help="Verbose logging.")
    parser.add_argument("--enable-ema-golden-cross", action="store_true", help="Enable EMA9/EMA26 golden-cross Telegram/frontend pushes.")
    parser.add_argument(
        "--trend-order-bys",
        default=",".join(TREND_ORDER_BYS),
        help="Comma-separated GMGN trending sort fields, for example: default,change1h,volume.",
    )
    parser.add_argument(
        "--trend-intervals",
        default=",".join(TREND_INTERVALS),
        help="Comma-separated GMGN trending intervals, for example: 1h,6h,24h.",
    )
    return parser


def cleanup_stale_watchlist_tokens() -> None:
    """Remove watchlist tokens whose MCap has dropped near zero."""
    from bottom_detection.bottom_watchlist_store import delete_watchlist_token
    tokens = fetch_watchlist_records()
    cleaned = 0
    for row in tokens:
        ca = str(row.get("ca") or "").strip()
        if not ca: continue
        last_mcap = to_float(row.get("last_mcap"))
        peak_mcap = to_float(row.get("peak_mcap"))
        pool_liquidity = to_float(row.get("last_pool_liquidity"))
        # Skip if last_mcap is missing (never scanned) — not a death signal
        if last_mcap <= 0:
            continue
        # Skip if pool still has meaningful liquidity — token is alive
        if pool_liquidity >= 10_000:
            continue
        # Delete truly dead tokens (absolute fall below $10K, or >99.9% drop = fake MCap)
        is_dead_absolute = peak_mcap >= 500_000 and last_mcap < 10_000
        is_dead_fake_mcap = peak_mcap >= 500_000 and last_mcap > 0 and (last_mcap / peak_mcap) < 0.001
        if is_dead_absolute or is_dead_fake_mcap:
            delete_watchlist_token(
                ca,
                "startup_dead_token_cleanup",
                current_mcap=last_mcap,
                metadata={
                    "trigger": "cleanup_stale_watchlist_tokens",
                    "is_dead_absolute": is_dead_absolute,
                    "is_dead_fake_mcap": is_dead_fake_mcap,
                    "peak_mcap_threshold": 500_000,
                    "last_mcap_threshold": 10_000,
                    "drop_ratio_threshold": 0.001,
                },
            )
            print(f"  Cleanup: removed dead token {ca[:16]}... (peak=${peak_mcap:,.0f} -> last=${last_mcap:,.0f})")
            cleaned += 1
    if cleaned:
        print(f"  Cleanup: removed {cleaned} dead watchlist tokens")


def main() -> None:
    global MIN_MCAP_USD, MIN_TOKEN_AGE_SEC, MIN_FEE_SOL, MIN_POOL_LIQUIDITY_USD, TREND_ORDER_BYS, TREND_INTERVALS, TREND_INTERVAL, EMA_GOLDEN_CROSS_ENABLED, FAST_SCAN_ENABLED, FAST_SCAN_INTERVAL_SEC, FAST_SCAN_MIN_MCAP, FAST_SCAN_MAX_MCAP, FAST_SCAN_SNAPSHOT_INTERVAL_SEC, FAST_SCAN_TOKEN_DELAY, FAST_SCAN_MAX_TOKENS
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] bottom accumulation monitor starting...", flush=True)
    args = build_parser().parse_args()
    MIN_MCAP_USD = args.min_mcap
    MIN_TOKEN_AGE_SEC = int(args.min_age_hours * 3600)
    MIN_FEE_SOL = args.min_fee_sol
    MIN_POOL_LIQUIDITY_USD = args.min_pool_liquidity
    TREND_ORDER_BYS = tuple(item.strip() for item in str(args.trend_order_bys).split(",") if item.strip())
    TREND_INTERVALS = tuple(item.strip() for item in str(args.trend_intervals).split(",") if item.strip())
    TREND_INTERVAL = TREND_INTERVALS[0] if TREND_INTERVALS else TREND_INTERVAL
    EMA_GOLDEN_CROSS_ENABLED = EMA_GOLDEN_CROSS_ENABLED or bool(args.enable_ema_golden_cross)
    FAST_SCAN_ENABLED = bool(args.fast_scan)
    FAST_SCAN_INTERVAL_SEC = args.fast_interval
    FAST_SCAN_MIN_MCAP = args.fast_min_mcap
    FAST_SCAN_MAX_MCAP = args.fast_max_mcap
    FAST_SCAN_SNAPSHOT_INTERVAL_SEC = args.fast_snapshot_interval
    FAST_SCAN_TOKEN_DELAY = args.fast_token_delay
    FAST_SCAN_MAX_TOKENS = args.fast_max_tokens
    print(f"[{datetime.now().strftime('%H:%M:%S')}] init: kline cache table...", flush=True)
    ensure_kline_cache_table()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] init: watchlist daily mcap columns...", flush=True)
    ensure_watchlist_daily_mcap_columns()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] DB tables ready, cleaning stale watchlist...", flush=True)
    cleanup_stale_watchlist_tokens()
    if args.notify:
        start_post_push_entry_drawdown_monitor()
    if args.once or not args.watch:
        scan_once(args)
        return
    run_scheduled_scans(args)


if __name__ == "__main__":
    main()
