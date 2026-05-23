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
    clean_redis_stream_for_ca,
    daily_mcap_watchlist_needs_notify,
    delete_watchlist_token,
    ensure_watchlist_daily_mcap_columns,
    fetch_watchlist_records,
    fill_watchlist_create_at as store_fill_watchlist_create_at,
    fill_watchlist_token_created_at as store_fill_token_created_at,
    is_watchlist_blacklisted,
    mark_daily_mcap_watchlist_notified,
    set_watchlist_blacklisted,
    update_watchlist_seen,
    upsert_daily_mcap_watchlist_token,
)
from bottom_detection.top100_push_record_store import (
    record_top100_push,
    top100_signal_push_record_exists,
)


CHAIN = "sol"
_BOTTOM_TOP100_SNAPSHOT_COMMENTS_READY = False
TREND_INTERVALS = tuple(
    item.strip()
    for item in os.getenv("BOTTOM_TREND_INTERVALS", os.getenv("BOTTOM_TREND_INTERVAL", "5m")).split(",")
    if item.strip()
)
TREND_INTERVAL = TREND_INTERVALS[0] if TREND_INTERVALS else "5m"
TREND_INTERVAL_SCHEDULES_RAW = os.getenv("BOTTOM_TREND_INTERVAL_SCHEDULES", "1m:60,5m:120,1h:300")
TREND_PRIMARY_INTERVAL = os.getenv("BOTTOM_TREND_PRIMARY_INTERVAL", "1m")
TREND_CROSS_WINDOW_DEDUP_SEC = int(os.getenv("BOTTOM_TREND_CROSS_WINDOW_DEDUP_SEC", "180"))
TREND_SCHEDULER_IDLE_SLEEP_SEC = float(os.getenv("BOTTOM_TREND_SCHEDULER_IDLE_SLEEP_SEC", "2"))
TREND_ORDER_BYS = tuple(
    item.strip()
    for item in os.getenv("BOTTOM_TREND_ORDER_BYS", "default,change5m").split(",")
    if item.strip()
)
TREND_LIMIT = int(os.getenv("BOTTOM_TREND_LIMIT", "100"))
MAX_TOKENS = int(os.getenv("BOTTOM_MAX_TOKENS", str(TREND_LIMIT)))
DEFAULT_INTERVAL_SEC = int(os.getenv("BOTTOM_SCAN_INTERVAL", "300"))
TOP_HOLDER_LIMIT = int(os.getenv("BOTTOM_TOP_HOLDER_LIMIT", "100"))
RECENT_COMPARE_LIMIT = int(os.getenv("BOTTOM_RECENT_COMPARE_LIMIT", "100"))
NEW_TOKEN_AGE_CUTOFF_SEC = int(os.getenv("BOTTOM_NEW_TOKEN_AGE_CUTOFF_SEC", str(48 * 3600)))
MID_TOKEN_AGE_CUTOFF_SEC = int(os.getenv("BOTTOM_MID_TOKEN_AGE_CUTOFF_SEC", str(5 * 24 * 3600)))
NEW_TOKEN_SNAPSHOT_INTERVAL_SEC = int(os.getenv("BOTTOM_NEW_TOKEN_SNAPSHOT_INTERVAL_SEC", "300"))
OLD_TOKEN_SNAPSHOT_INTERVAL_SEC = int(os.getenv("BOTTOM_OLD_TOKEN_SNAPSHOT_INTERVAL_SEC", "900"))
# Fast scan: 100K-300K watchlist tokens checked every 1 min
FAST_SCAN_ENABLED = os.getenv("BOTTOM_FAST_SCAN_ENABLED", "1") != "0"
FAST_SCAN_INTERVAL_SEC = int(os.getenv("BOTTOM_FAST_SCAN_INTERVAL_SEC", "60"))
FAST_SCAN_MIN_MCAP = float(os.getenv("BOTTOM_FAST_SCAN_MIN_MCAP", "100000"))
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
BINANCE_KLINE_URL = "https://dquery.sintral.io/u-kline/v1/k-line/candles"
BINANCE_HEADERS = {"Accept-Encoding": "identity", "User-Agent": BINANCE_WEB3_USER_AGENT}
_POST_PUSH_MONITOR_STARTED = False

# ---------------------------------------------------------------------------
# Live tracking for frontend real-time dashboard (8h window for bottom signals)
# ---------------------------------------------------------------------------
BOTTOM_LIVE_TRACK_ENABLED = os.getenv("BOTTOM_LIVE_TRACK_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
BOTTOM_LIVE_TRACK_REDIS_PREFIX = os.getenv("BOTTOM_LIVE_TRACK_REDIS_PREFIX", "bottom:live_track")
BOTTOM_LIVE_TRACK_TTL_SEC = int(os.getenv("BOTTOM_LIVE_TRACK_TTL_SEC", str(8 * 3600)))  # 8h
BOTTOM_LIVE_TRACK_REMOVE_DEAD_MCAP_USD = float(os.getenv("BOTTOM_LIVE_TRACK_DEAD_MCAP", "6000"))  # < 6K = dead
BOTTOM_LIVE_TRACK_REMOVE_LOW_MCAP_USD = float(os.getenv("BOTTOM_LIVE_TRACK_LOW_MCAP", "10000"))  # < 10K within 30min
BOTTOM_LIVE_TRACK_LOW_MCAP_WINDOW_SEC = int(os.getenv("BOTTOM_LIVE_TRACK_LOW_WINDOW", "1800"))  # 30min

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


def fetch_token_metadata(address: str) -> tuple[dict[str, Any], dict[str, Any]]:
    info = run_gmgn(["token", "info", "--chain", CHAIN, "--address", address], timeout=75)
    sec = run_gmgn(["token", "security", "--chain", CHAIN, "--address", address], timeout=75)
    return (info if isinstance(info, dict) else {}, sec if isinstance(sec, dict) else {})


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
    start_ts = token_launch_ts(token) or token_created_ts(token)
    if start_ts > 0:
        return min(start_ts, end_ts - kline_resolution_seconds(token_kline_resolution(token)))
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
    }


def merge_token_metadata(token: dict[str, Any], info: dict[str, Any], security: dict[str, Any]) -> dict[str, Any]:
    merged = dict(token)
    for source in (security, info):
        for key, value in source.items():
            if key not in merged or merged.get(key) in (None, "", 0):
                merged[key] = value
    # Flatten nested price object from gmgn token-info (e.g. {"price": "0.0068", "price_1m": "..."})
    price_val = merged.get("price")
    if isinstance(price_val, dict):
        merged["price"] = price_val.get("price") or price_val.get("price_1m") or 0
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
    new_revival_ready = (
        is_under_24h
        and rebound_ready
        and bottom_low_mcap > 0
        and bottom_low_mcap <= BOTTOM_NEW_REVIVAL_MAX_LOW_MCAP_USD
        and new_revival_price_ready
        and pool_ready
    )
    if drop_level > 0 and pool_ready:
        signal_type = f"drop_{int(drop_level / 10000)}w"
    elif new_revival_ready:
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
    if drop_level > 0:
        rule_name = f"NEW_ATH1M_DROP_{int(drop_level / 10000)}W"
        min_ath_mcap = BOTTOM_NEW_DROP_ATH_MCAP_USD
        min_mcap = 0
        max_mcap = drop_level
        rule_reason = (
            f"新币回落{rule_name}: 创建{token_age / 3600:.1f}h, "
            f"ATH${ath_mcap:,.0f}>={min_ath_mcap:,.0f}, 当前市值${current_mcap:,.0f}<=${drop_level:,.0f}"
        )
    elif new_revival_ready:
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

def save_snapshot(scan_id: str, token: dict[str, Any], summary: dict[str, Any], holders: list[dict[str, Any]], analysis: dict[str, Any]) -> int:
    address = token_address(token)
    ensure_bottom_top100_snapshot_comments()

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
# Bottom live-track Redis helpers (8h frontend tracking)
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
) -> None:
    """Store a bottom-abnormal CA in Redis for 8h real-time tracking."""
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
        "pushed_at": now_ts(),
        "current_mcap": to_float(entry_mcap),
        "current_price": to_float(entry_price),
        "peak_mcap": to_float(entry_mcap),
        "pool_liquidity": to_float(pool_liquidity),
        "holders": 0,
        "volume_5m": 0,
        "volume_1h": 0,
        "pnl_pct": 0.0,
        "last_updated": now_ts(),
        "status": "tracking",
        "remove_reason": "",
    }
    try:
        key = _bottom_live_track_key(address)
        client.setex(key, BOTTOM_LIVE_TRACK_TTL_SEC, json.dumps(payload, ensure_ascii=False))
        client.sadd(_bottom_live_track_index_key(), address)
        client.expire(_bottom_live_track_index_key(), BOTTOM_LIVE_TRACK_TTL_SEC)
        print(f"  [BottomLiveTrack] 开始实时追踪 ${symbol} {address[:8]}... signal={signal_type}")
    except Exception as exc:
        print(f"  [BottomLiveTrack] Redis写入失败 {address[:8]}: {exc}")


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
        return {
            "price": price,
            "market_cap": market_cap,
            "pool_liquidity": liquidity,
            "pool_mcap_ratio": liquidity / market_cap if liquidity > 0 and market_cap > 0 else 0.0,
            "holders": to_int(data.get("holders") or data.get("holderCount")),
            "volume_5m": to_float(data.get("volume5m")),
            "volume_1h": to_float(data.get("volume1h")),
            "symbol": data.get("symbol") or "",
        }
    except Exception as exc:
        print(f"{address[:8]} binance dynamic fetch failed: {exc}")
        return {}


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
        f"高点回撤: {drawdown_pct:.1f}% | 入场下跌: {entry_loss_pct:.1f}%\n"
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
        publish_tg_alert(tg_text, "bottom_abnormal", status="dry_run", chat_id=TG_CHAT_ID, extra=extra)
        return None
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": tg_text, "disable_web_page_preview": True},
            timeout=15,
        )
        if not resp.ok:
            print(f"tg failed: {resp.status_code} {resp.text[:200]}")
            publish_tg_alert(tg_text, "bottom_abnormal", status=f"failed_http_{resp.status_code}", chat_id=TG_CHAT_ID, extra=extra)
            return None
        payload = resp.json()
        message_id = payload.get("result", {}).get("message_id") if isinstance(payload, dict) else None
        publish_tg_alert(tg_text, "bottom_abnormal", status="sent", chat_id=TG_CHAT_ID, message_id=message_id, extra=extra)
        register_post_push_track(address, extra, message_id)
        # Live-track: push to Redis for frontend real-time dashboard
        start_bottom_live_tracking(
            address=address,
            symbol=str(extra.get("symbol") or ""),
            entry_mcap=to_float(extra.get("current_mcap")),
            entry_price=to_float(extra.get("price")),
            signal_type=str(extra.get("signal_type") or ""),
            pool_liquidity=to_float(extra.get("liquidity") or extra.get("pool_total_liquidity")),
        )
        return int(message_id) if message_id else None
    except Exception as exc:
        print(f"tg exception: {exc}")
        publish_tg_alert(tg_text, "bottom_abnormal", status="exception", chat_id=TG_CHAT_ID, extra={**extra, "error": str(exc)})
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
        avoid_reasons.append("5m内冲顶不追")
    if "大市值" in risk_tags:
        avoid_reasons.append(">$500K拉盘成本高")
    if "天花板" in risk_tags:
        avoid_reasons.append("ATH/现值<1.5x")
    if "无量" in risk_tags:
        avoid_reasons.append("量能<$10K")
    if signal_type == "quiet_breakout":
        avoid_reasons.append("quiet_breakout历史样本弱")

    if profile in {"低优先级", "快峰风险", "高市值谨慎"}:
        action_hint = "不追第一波，等待回踩和二次放量确认"
    elif profile == "优先观察":
        action_hint = "关注-5%~-15%回调区，确认止跌后观察"
    else:
        action_hint = "等待-5%~-15%回调，不回调不追"

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
    return extra


def publish_frontend_signal_update(
    text: str,
    extra: dict[str, Any],
    status: str = "frontend_update",
    snapshot_id: int = 0,
) -> None:
    if snapshot_id and not (extra or {}).get("snapshot_id"):
        extra = {**(extra or {}), "snapshot_id": snapshot_id}
    address = str((extra or {}).get("address") or "").strip()
    if not address:
        return
    signal_type = str((extra or {}).get("signal_type") or "").strip()
    if top100_signal_push_record_exists(address, signal_type, source="bottom_abnormal", chain=CHAIN):
        print(f"{address[:8]} skip frontend push: {signal_type} push already recorded")
        return
    extra = enrich_signal_strategy_extra(extra)
    risk_tags = extra.get("risk_tags") or []
    try:
        inserted = record_top100_push(text=text, extra=extra, status=status, source="bottom_abnormal", chain=CHAIN)
    except Exception as exc:
        print(f"{address[:8]} top100 push record failed: {exc}")
        inserted = True
    if not inserted:
        print(f"{address[:8]} skip frontend push: duplicate first push record")
        return
    # Filter 1: ceiling + dead_vol = ~0% success (21 failures, 8 successes killed)
    if "天花板" in risk_tags and "无量" in risk_tags:
        print(f"{address[:8]} skip push: ceiling+dead_vol combo")
        return
    # Filter 2: extreme dead volume (<$5K) = 78% failure rate, kills only 5/131 successes
    vol = to_float(extra.get("breakout_volume_usd", 0) or extra.get("volume_usd", 0))
    if 0 < vol < 5_000:
        print(f"{address[:8]} skip push: extreme dead vol ${vol:,.0f} < $5K")
        return
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
    )

    # Schedule 10-minute follow-up verdict via TG
    threading.Thread(
        target=_send_quick_verdict,
        args=(address, extra),
        daemon=True,
    ).start()


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
            advice = "量能崩塌({:.0f}%), 不建仓".format((1 - r1m) * 100)
        elif r1m > 1.2 and r5m > 1.0 and price_change > 5:
            verdict = "🟢 真异动"
            advice = "1m+5m量能共振, 可小仓试探"
        elif price_change < -5 and r1m < 0.6:
            verdict = "🟡 V反进行中"
            advice = "正在回调, 等量能恢复再入"
        elif r1m > 0.6 and r5m > 0.6:
            verdict = "🟡 观望"
            advice = "量能维持但涨幅不足, 继续观察"
        else:
            verdict = "⚪ 不明确"
            advice = "信号混合, 不建议操作"

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
            "建议: {}\n"
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
        "abnormal": "异动检测",
        "drop_50w": "新币跌破50W",
        "drop_40w": "新币跌破40W",
        "new_revival": "新币底部启动",
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
    kline_resolution = token_kline_resolution(token)
    candles = fetch_kline(address, kline_resolution, token)
    # Also fetch 1m K-line for micro-structure volume analysis (DCB vs V-reversal)
    candles_1m = fetch_kline(address, "1m", token) if kline_resolution != "1m" else candles
    summary, holders = build_snapshot_json(token, raw_holders, candles, kline_resolution)
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
    already_notified = previous_signal_exists(address, analysis.get("signal_type", ""))
    has_previous_bottom_signal = previous_bottom_signal_exists(address)
    baseline = first_signal_baseline(address, analysis.get("signal_type", ""))
    snapshot_id = save_snapshot(scan_id, token, summary, holders, analysis)
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

    if notify and should_notify(analysis) and not already_notified:
        if USE_AGENT_DECISION:
            run_agent_execution(
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
        else:
            web_extra = build_bottom_signal_extra(token, summary, analysis, baseline)
            signal_text = abnormal_signal_text(token, analysis)
            publish_frontend_signal_update(signal_text, web_extra, snapshot_id=snapshot_id)
            send_tg(signal_text, extra=web_extra)
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
            quiet_snapshot_id = save_snapshot(scan_id + "_quiet", token, summary, holders, quiet_breakout)
            quiet_breakout = {**quiet_breakout, "snapshot_id": quiet_snapshot_id}
            quiet_extra = build_bottom_signal_extra(token, summary, quiet_breakout, quiet_baseline)
            quiet_text = quiet_breakout_signal_text(token, quiet_breakout)
            publish_frontend_signal_update(quiet_text, quiet_extra, status="frontend_update", snapshot_id=quiet_snapshot_id)
            send_tg(quiet_text, extra=quiet_extra)
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
            runup_snapshot_id = save_snapshot(scan_id + "_runup", token, summary, holders, quiet_runup)
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
                )
                ema_extra = {**ema_extra, "snapshot_id": ema_snapshot_id}
                # Push to frontend on first detection
                publish_frontend_signal_update(signal_text, ema_extra, status="frontend_update", snapshot_id=ema_snapshot_id)
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
    prefiltered_trending = []
    prefiltered_skipped = 0
    for token in trending_tokens:
        skip_reason = prefilter_trending_token(token)
        if skip_reason:
            prefiltered_skipped += 1
            continue
        prefiltered_trending.append(token)
    tokens = merge_token_sources(prefiltered_trending, watchlist_tokens, alpha_abnormal_tokens)
    dedupe_skipped = 0
    if recent_seen is not None:
        prune_recent_seen(recent_seen, recent_seen_ttl_sec)
        if skip_recent_seen:
            filtered_tokens = []
            for token in tokens:
                address = token_address(token)
                if address and address in recent_seen:
                    dedupe_skipped += 1
                    continue
                filtered_tokens.append(token)
            tokens = filtered_tokens
        seen_at = time.monotonic()
        for token in tokens[: args.max_tokens]:
            address = token_address(token)
            if address:
                recent_seen[address] = seen_at
    print(
        f"[{datetime.now().strftime('%H:%M:%S')}] scan_id={scan_id} "
        f"mode={mode_name} "
        f"intervals={','.join(active_intervals)} "
        f"trending={len(trending_tokens)} prefiltered={len(prefiltered_trending)} "
        f"prefilter_skip={prefiltered_skipped} watchlist={len(watchlist_tokens)} "
        f"alpha_abnormal={len(alpha_abnormal_tokens)} "
        f"dedupe_skip={dedupe_skipped} merged={len(tokens)}"
    )
    processed = 0
    skipped = 0
    for token in tokens[: args.max_tokens]:
        try:
            address = token_address(token)
            is_watchlist = "watchlist" in set(token.get("_sources", []))
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
            # Launch/open time filter: token must be >=4h past pool migration.
            created_ts = token_created_ts(info)
            launch_ts = token_launch_ts(info)
            if launch_ts <= 0:
                skipped += 1
                token["_trench"] = True
                print(f"{token_label(token)} skip open_ts missing (发射时间小于4H)")
                continue
            open_age_sec = now_ts() - launch_ts
            if open_age_sec < 4 * 3600:
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
            if is_watchlist and not pool_reliable:
                previous_pool_liquidity = to_float(token.get("watchlist_last_pool_liquidity"))
                previous_pool_ratio = to_float(token.get("watchlist_last_pool_mcap_ratio"))
                if previous_pool_liquidity > 0:
                    pool_liquidity = previous_pool_liquidity
                    pool_mcap_ratio = previous_pool_ratio
                print(f"{address[:8]} pool check skipped: {pool_unreliable_reason}")
            # Pool/Mcap ratio filter: skip tokens with liquidity < 7% of mcap
            if pool_reliable and 0 < pool_mcap_ratio < 0.07:
                skipped += 1
                print(f"{token_label(token)} skip pool/mcap={pool_mcap_ratio:.1%} < 7% (流动性不足)")
                continue
            if (
                is_watchlist
                and pool_reliable
                and pool_liquidity < WATCHLIST_DELETE_BELOW_POOL_LIQUIDITY_USD
            ):
                deleted = delete_watchlist_token(
                    address,
                    "pool_liquidity_below_threshold",
                    current_mcap=current_mcap,
                    pool_liquidity=pool_liquidity,
                    pool_mcap_ratio=pool_mcap_ratio,
                    metadata={
                        "threshold": WATCHLIST_DELETE_BELOW_POOL_LIQUIDITY_USD,
                        "trigger": "scan_once",
                        "pool_reliable": pool_reliable,
                    },
                )
                if deleted:
                    print(
                        f"{address[:8]} watchlist deleted: "
                        f"pool ${pool_liquidity:,.0f}<${WATCHLIST_DELETE_BELOW_POOL_LIQUIDITY_USD:,.0f}"
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
                skip_reason = token_pool_filter_reason(pool_liquidity, pool_reliable, pool_unreliable_reason)
                if skip_reason:
                    skipped += 1
                    print(f"{token_label(token)} skip {skip_reason}")
                    continue
            maybe_record_daily_mcap_milestone(token, current_mcap, args.notify)
            if is_watchlist:
                update_watchlist_seen(
                    address,
                    current_mcap,
                    pool_liquidity=pool_liquidity,
                    pool_mcap_ratio=pool_mcap_ratio,
                    fee_sol=fee_sol(token),
                    symbol=token.get("symbol"),
                )
                daily_mcap_date = str(token.get("watchlist_daily_mcap_date") or "")
                if daily_mcap_date == datetime.now().date().isoformat() and current_mcap >= DAILY_MCAP_MILESTONE_USD * 0.3:
                    active_ts = token_active_ts({**token, **(info or {})})
                    age_sec = (now_ts() - active_ts) if active_ts > 0 else 0
                    if 0 < age_sec <= NEW_TOKEN_AGE_CUTOFF_SEC:
                        pool_summary = summarize_pools(token)
                        pool_liq = to_float(pool_summary.get("total_liquidity"))
                        pool_ratio = to_float(pool_summary.get("liquidity_mcap_ratio"))
                        if pool_liq >= BOTTOM_ABNORMAL_MIN_POOL_LIQUIDITY_USD and pool_ratio >= DAILY_MCAP_MIN_POOL_MCAP_RATIO:
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
                    deleted = delete_watchlist_token(
                        address,
                        "mcap_below_threshold",
                        current_mcap=current_mcap,
                        pool_liquidity=pool_liquidity,
                        pool_mcap_ratio=pool_mcap_ratio,
                        metadata={
                            "threshold": WATCHLIST_DELETE_BELOW_MCAP_USD,
                            "trigger": "scan_once",
                        },
                    )
                    if deleted:
                        print(
                            f"{address[:8]} watchlist deleted: "
                            f"mcap ${current_mcap:,.0f}<${WATCHLIST_DELETE_BELOW_MCAP_USD:,.0f}"
                        )
                    skipped += 1
                    continue
            skip_reason = recent_snapshot_skip_reason(token_address(token), token)
            if skip_reason:
                skipped += 1
                print(f"{token_label(token)} skip {skip_reason}")
                continue
            if handle_token(scan_id, token, args.notify, frontend_update_allowed=is_watchlist):
                processed += 1
        except Exception as exc:
            print(f"{token_label(token)} failed: {exc}")
        time.sleep(args.token_delay)
    print(f"scan_id={scan_id} processed={processed}/{len(tokens)} skipped={skipped}")


def _fast_snapshot_skip_reason(address: str, token: dict[str, Any]) -> str | None:
    """Shorter interval check for fast-scan path (100K-300K tokens)."""
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
    recent_seen: dict[str, float] = {}
    print(
        "trend scheduler enabled: "
        + ", ".join(f"{interval}:{every_sec}s" for interval, every_sec in schedules)
        + f" primary={primary_interval} dedup={TREND_CROSS_WINDOW_DEDUP_SEC}s"
    )

    while True:
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
    parser.add_argument("--max-tokens", type=int, default=MAX_TOKENS)
    parser.add_argument("--token-delay", type=float, default=0.5, help="Delay between holder calls.")
    parser.add_argument("--min-mcap", type=float, default=MIN_MCAP_USD, help="Skip tokens below this market cap in USD.")
    parser.add_argument("--min-age-hours", type=float, default=MIN_TOKEN_AGE_SEC / 3600, help="Skip tokens younger than this many hours.")
    parser.add_argument("--min-fee-sol", type=float, default=MIN_FEE_SOL, help="Skip tokens below this SOL fee value.")
    parser.add_argument("--min-pool-liquidity", type=float, default=MIN_POOL_LIQUIDITY_USD, help="Skip non-watchlist tokens below this pool liquidity in USD.")
    parser.add_argument("--fast-scan", action="store_true", default=FAST_SCAN_ENABLED, help="Enable 1-min fast scan for 100K-300K watchlist tokens.")
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
    ensure_kline_cache_table()
    ensure_watchlist_daily_mcap_columns()
    cleanup_stale_watchlist_tokens()
    if args.notify:
        start_post_push_entry_drawdown_monitor()
    if args.once or not args.watch:
        scan_once(args)
        return
    run_scheduled_scans(args)


if __name__ == "__main__":
    main()
