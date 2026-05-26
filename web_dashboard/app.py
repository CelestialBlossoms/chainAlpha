#!/usr/bin/env python3
"""
Lightweight dashboard for Telegram alert stream.
"""

from __future__ import annotations

import asyncio
import json
import os
import requests
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from redis_client import get_redis_client, get_redis_disabled_reason
from tg_alert_stream import TG_ALERT_STREAM_KEY, read_recent_tg_alerts
from plugin_signal_stream import PLUGIN_SIGNAL_STREAM_KEY, read_recent_plugin_signals
from db_client import db_op
from ca_analyzer.cluster_api import analyze_ca_clusters


BASE_DIR = Path(__file__).resolve().parent
SSE_BLOCK_MS = int(os.getenv("TG_DASHBOARD_SSE_BLOCK_MS", "30000"))
PLUGIN_BOTTOM_ABNORMAL_CACHE_TTL_SEC = float(os.getenv("PLUGIN_BOTTOM_ABNORMAL_CACHE_TTL_SEC", "3"))
PLUGIN_BOTTOM_WATCHLIST_HIGHLIGHT_MCAP_USD = float(os.getenv("PLUGIN_BOTTOM_WATCHLIST_HIGHLIGHT_MCAP_USD", "300000"))
_PLUGIN_BOTTOM_ABNORMAL_CACHE: dict[str, Any] = {"ts": 0.0, "limit": 0, "items": []}
PLUGIN_ALPHA_NEW_TOKEN_CACHE_TTL_SEC = float(os.getenv("PLUGIN_ALPHA_NEW_TOKEN_CACHE_TTL_SEC", "3"))
_PLUGIN_ALPHA_NEW_TOKEN_CACHE: dict[str, Any] = {"ts": 0.0, "limit": 0, "items": []}
PUSH_CA_METRIC_CACHE_TTL_SEC = float(os.getenv("PUSH_CA_METRIC_CACHE_TTL_SEC", "60"))
PUSH_CA_RESPONSE_CACHE_TTL_SEC = float(os.getenv("PUSH_CA_RESPONSE_CACHE_TTL_SEC", "300"))
PUSH_CA_MAX_WORKERS = int(os.getenv("PUSH_CA_MAX_WORKERS", "6"))
BINANCE_SOL_CHAIN_ID = os.getenv("BINANCE_SOL_CHAIN_ID", "CT_501")
BINANCE_WEB3_USER_AGENT = os.getenv("BINANCE_WEB3_USER_AGENT", "binance-web3/1.1 (Skill)")
BINANCE_DYNAMIC_URL = "https://web3.binance.com/bapi/defi/v4/public/wallet-direct/buw/wallet/market/token/dynamic/info/ai"
BINANCE_KLINE_URL = "https://dquery.sintral.io/u-kline/v1/k-line/candles"
BINANCE_HEADERS = {"Accept-Encoding": "identity", "User-Agent": BINANCE_WEB3_USER_AGENT}
_PUSH_CA_METRIC_CACHE: dict[str, dict[str, Any]] = {}
_PUSH_CA_RESPONSE_CACHE: dict[str, dict[str, Any]] = {}
_DASHBOARD_KLINE_CACHE_TABLE_READY = False
BOTTOM_ONLY_FRONTEND = os.getenv("BOTTOM_ONLY_FRONTEND", "1").lower() not in {"0", "false", "no"}

# ---------------------------------------------------------------------------
# Live-track configuration (mirrors deep_alpha_pro settings)
# ---------------------------------------------------------------------------
LIVE_TRACK_REDIS_PREFIX = os.getenv("DEEP_ALPHA_LIVE_TRACK_REDIS_PREFIX", "deep_alpha:live_track")
LIVE_TRACK_REDIS_TTL_SEC = int(os.getenv("DEEP_ALPHA_LIVE_TRACK_TTL_SEC", str(24 * 3600)))  # 24h
LIVE_TRACK_REMOVE_DEAD_MCAP_USD = float(os.getenv("DEEP_ALPHA_LIVE_TRACK_DEAD_MCAP", "6000"))
LIVE_TRACK_REMOVE_LOW_MCAP_USD = float(os.getenv("DEEP_ALPHA_LIVE_TRACK_LOW_MCAP", "10000"))
LIVE_TRACK_LOW_MCAP_WINDOW_SEC = int(os.getenv("DEEP_ALPHA_LIVE_TRACK_LOW_WINDOW", "1800"))  # 30min
LIVE_TRACK_REFRESH_INTERVAL_SEC = int(os.getenv("DEEP_ALPHA_LIVE_TRACK_REFRESH_SEC", "30"))
LIVE_TRACK_PUBSUB_CHANNEL = os.getenv("DEEP_ALPHA_LIVE_TRACK_PUBSUB", "deep_alpha:live_track:updates")
LIVE_TRACK_MAX_WORKERS = int(os.getenv("DEEP_ALPHA_LIVE_TRACK_MAX_WORKERS", "4"))
_LIVE_TRACK_BG_STARTED = False

# ---------------------------------------------------------------------------
# Bottom live-track configuration (12h window for bottom abnormal signals)
# ---------------------------------------------------------------------------
BOTTOM_LIVE_TRACK_REDIS_PREFIX = os.getenv("BOTTOM_LIVE_TRACK_REDIS_PREFIX", "bottom:live_track")
BOTTOM_LIVE_TRACK_TTL_SEC = int(os.getenv("BOTTOM_LIVE_TRACK_TTL_SEC", str(12 * 3600)))  # 12h
BOTTOM_LIVE_TRACK_REMOVE_DEAD_MCAP_USD = float(os.getenv("BOTTOM_LIVE_TRACK_DEAD_MCAP", "6000"))
BOTTOM_LIVE_TRACK_REMOVE_LOW_MCAP_USD = float(os.getenv("BOTTOM_LIVE_TRACK_LOW_MCAP", "10000"))
BOTTOM_LIVE_TRACK_LOW_MCAP_WINDOW_SEC = int(os.getenv("BOTTOM_LIVE_TRACK_LOW_WINDOW", "1800"))  # 30min
BOTTOM_LIVE_TRACK_REFRESH_INTERVAL_SEC = int(os.getenv("BOTTOM_LIVE_TRACK_REFRESH_SEC", "30"))
BOTTOM_LIVE_TRACK_PUBSUB_CHANNEL = os.getenv("BOTTOM_LIVE_TRACK_PUBSUB", "bottom:live_track:updates")
BOTTOM_LIVE_TRACK_MAX_WORKERS = int(os.getenv("BOTTOM_LIVE_TRACK_MAX_WORKERS", "4"))
BOTTOM_LIVE_TRACK_KLINE_REFRESH_SEC = int(os.getenv("BOTTOM_LIVE_TRACK_KLINE_REFRESH_SEC", str(4 * 3600)))
BOTTOM_LIVE_TRACK_KLINE_WINDOW_SEC = int(os.getenv("BOTTOM_LIVE_TRACK_KLINE_WINDOW_SEC", str(12 * 3600)))
BOTTOM_LIVE_TRACK_BG_ENABLED = os.getenv("BOTTOM_LIVE_TRACK_BG_ENABLED", "0").strip().lower() not in {"0", "false", "no", "off"}
_BOTTOM_LIVE_TRACK_BG_STARTED = False

try:
    REPORT_TZ = ZoneInfo(os.getenv("REPORT_TZ") or os.getenv("TZ") or "Asia/Shanghai")
except Exception:
    REPORT_TZ = ZoneInfo("Asia/Shanghai")

app = FastAPI(title="Chain Alpha TG Dashboard")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["chrome-extension://*", "https://gmgn.ai", "https://www.gmgn.ai", "http://localhost:*", "http://127.0.0.1:*"],
    allow_origin_regex=r"chrome-extension://.*|https://([a-z0-9-]+\.)?gmgn\.ai|http://(localhost|127\.0\.0\.1)(:\d+)?",
    allow_methods=["GET"],
    allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")


def _sort_live_track_by_push_time(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(items, key=lambda item: _safe_int(item.get("pushed_at")), reverse=True)


def normalize_alert(stream_id: str, fields: dict[str, Any]) -> dict[str, Any]:
    item = dict(fields)
    item["id"] = stream_id
    extra = item.get("extra")
    if isinstance(extra, str) and extra:
        try:
            item["extra"] = json.loads(extra)
        except json.JSONDecodeError:
            item["extra"] = {}
    else:
        item["extra"] = {}
    return item


def _safe_float(value: Any) -> float:
    try:
        if value is None or value == "":
            return 0.0
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value: Any) -> int:
    try:
        if value is None or value == "":
            return 0
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _bottom_abnormal_ca(item: dict[str, Any]) -> str:
    extra = item.get("extra") if isinstance(item.get("extra"), dict) else {}
    return str(extra.get("address") or item.get("ca") or "").strip()


def _bottom_signal_sort_key(item: dict[str, Any]) -> tuple[int, int, str]:
    stream_id = str(item.get("id") or "")
    stream_ms = _safe_int(stream_id.split("-", 1)[0]) if "-" in stream_id else 0
    return (_safe_int(item.get("ts")), stream_ms, stream_id)


def _format_change_history(history: list[dict[str, Any]]) -> str:
    parts = []
    for point in history:
        pct = _safe_float(point.get("change_pct"))
        precision = 1 if abs(pct) >= 10 else 2
        parts.append(f"{pct:+.{precision}f}%")
    return ",".join(parts)


def enrich_bottom_abnormal_history(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Attach per-CA abnormal mcap change history to bottom abnormal plugin rows."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        if item.get("source") != "bottom_abnormal":
            continue
        extra = item.get("extra") if isinstance(item.get("extra"), dict) else {}
        if str(extra.get("signal_type") or "") == "watch":
            continue
        ca = _bottom_abnormal_ca(item)
        if ca:
            grouped.setdefault(ca, []).append(item)

    for rows in grouped.values():
        rows.sort(key=_bottom_signal_sort_key)
        history: list[dict[str, Any]] = []
        previous_mcap = 0.0
        for row_item in rows:
            extra = row_item.setdefault("extra", {})
            if not isinstance(extra, dict):
                extra = {}
                row_item["extra"] = extra
            current_mcap = _safe_float(extra.get("current_mcap"))
            if current_mcap <= 0:
                continue
            if previous_mcap > 0:
                change_pct = (current_mcap - previous_mcap) / previous_mcap * 100
                basis = "previous_abnormal_mcap"
            else:
                change_pct = _safe_float(extra.get("price_change_pct"))
                basis = "signal_price_change_pct"
            history.append(
                {
                    "ts": _safe_int(row_item.get("ts")),
                    "mcap": current_mcap,
                    "from_mcap": previous_mcap,
                    "change_pct": round(change_pct, 4),
                    "basis": basis,
                    "signal_type": extra.get("signal_type") or "",
                }
            )
            visible_history = history[-12:]
            extra["previous_abnormal_mcap"] = previous_mcap
            extra["abnormal_mcap_change_pct"] = round(change_pct, 4)
            extra["abnormal_mcap_change_history"] = visible_history
            extra["abnormal_mcap_change_text"] = _format_change_history(visible_history)
            extra["abnormal_signal_count"] = len(history)
            previous_mcap = current_mcap
    return items


def enrich_bottom_watchlist_highlights(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Mark plugin abnormal rows that are in bottom_watchlist_tokens below the highlight MCap."""
    addresses = sorted({_bottom_abnormal_ca(item) for item in items if _bottom_abnormal_ca(item)})
    if not addresses:
        return items

    watchlist_by_ca: dict[str, dict[str, Any]] = {}

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            SELECT ca, COALESCE(NULLIF(current_mcap, 0), NULLIF(last_mcap, 0), 0), source, symbol
            FROM bottom_watchlist_tokens
            WHERE ca = ANY(%s)
            """,
            (addresses,),
        )
        for ca, current_mcap, source, symbol in cur.fetchall():
            watchlist_by_ca[str(ca)] = {
                "current_mcap": _safe_float(current_mcap),
                "source": source or "",
                "symbol": symbol or "",
            }

    try:
        db_op(_op)
    except Exception as exc:
        print(f"bottom watchlist highlight enrich failed: {exc}")
        return items

    for item in items:
        ca = _bottom_abnormal_ca(item)
        watch = watchlist_by_ca.get(ca)
        if not watch:
            continue
        extra = item.setdefault("extra", {})
        if not isinstance(extra, dict):
            extra = {}
            item["extra"] = extra
        watch_mcap = _safe_float(watch.get("current_mcap"))
        extra["in_bottom_watchlist"] = True
        extra["watchlist_current_mcap"] = watch_mcap
        extra["watchlist_source"] = watch.get("source") or ""
        extra["watchlist_symbol"] = watch.get("symbol") or ""
        extra["watchlist_low_mcap_threshold"] = PLUGIN_BOTTOM_WATCHLIST_HIGHLIGHT_MCAP_USD
        extra["watchlist_low_mcap_highlight"] = 0 < watch_mcap < PLUGIN_BOTTOM_WATCHLIST_HIGHLIGHT_MCAP_USD
    return items


def compact_bottom_abnormal_item(item: dict[str, Any]) -> dict[str, Any]:
    extra = item.get("extra") if isinstance(item.get("extra"), dict) else {}
    return {
        "id": item.get("id") or "",
        "ts": item.get("ts") or 0,
        "source": "bottom_abnormal",
        "status": item.get("status") or "",
        "ca": extra.get("address") or item.get("ca") or "",
        "title": item.get("title") or "",
        "text": item.get("text") or "",
        "extra": {
            "signal_type": extra.get("signal_type") or "",
            "abnormal_rule": extra.get("abnormal_rule") or "",
            "address": extra.get("address") or item.get("ca") or "",
            "symbol": extra.get("symbol") or item.get("title") or "UNKNOWN",
            "narrative": extra.get("narrative") or extra.get("narrative_desc") or "",
            "narrative_desc": extra.get("narrative_desc") or extra.get("narrative") or "",
            "narrative_category": extra.get("narrative_category") or "",
            "narrative_type": extra.get("narrative_type") or "",
            "current_mcap": extra.get("current_mcap") or 0,
            "first_signal_mcap": extra.get("first_signal_mcap") or 0,
            "first_signal_ts": extra.get("first_signal_ts") or 0,
            "ath_mcap": extra.get("ath_mcap") or 0,
            "max_abnormal_mcap": extra.get("max_abnormal_mcap") or 0,
            "pool_total_liquidity": extra.get("pool_total_liquidity") or extra.get("pool_liquidity") or 0,
            "pool_liquidity": extra.get("pool_liquidity") or 0,
            "price_change_pct": extra.get("price_change_pct") or 0,
            "age_sec": extra.get("age_sec") or 0,
            "pool_mcap_ratio": extra.get("pool_mcap_ratio") or 0,
            "top10_pct_delta": extra.get("top10_pct_delta") or 0,
            "top10_current_pct": extra.get("top10_current_pct") or 0,
            "top10_previous_pct": extra.get("top10_previous_pct") or 0,
            "top20_pct_delta": extra.get("top20_pct_delta") or 0,
            "top20_current_pct": extra.get("top20_current_pct") or 0,
            "top20_previous_pct": extra.get("top20_previous_pct") or 0,
            "top50_pct_delta": extra.get("top50_pct_delta") or 0,
            "top50_current_pct": extra.get("top50_current_pct") or 0,
            "top50_previous_pct": extra.get("top50_previous_pct") or 0,
            "top100_pct_delta": extra.get("top100_pct_delta") or 0,
            "top100_current_pct": extra.get("top100_current_pct") or 0,
            "top100_previous_pct": extra.get("top100_previous_pct") or 0,
            "abnormal_mcap_change_history": extra.get("abnormal_mcap_change_history") or [],
            "abnormal_mcap_change_text": extra.get("abnormal_mcap_change_text") or "",
            "abnormal_signal_count": extra.get("abnormal_signal_count") or 0,
            "in_bottom_watchlist": bool(extra.get("in_bottom_watchlist")),
            "watchlist_current_mcap": extra.get("watchlist_current_mcap") or 0,
            "watchlist_source": extra.get("watchlist_source") or "",
            "watchlist_low_mcap_threshold": extra.get("watchlist_low_mcap_threshold") or PLUGIN_BOTTOM_WATCHLIST_HIGHLIGHT_MCAP_USD,
            "watchlist_low_mcap_highlight": bool(extra.get("watchlist_low_mcap_highlight")),
            "risk_tags": extra.get("risk_tags") or [],
        },
    }


def sse_message(event: str, data: dict[str, Any]) -> str:
    event_id = str(data.get("id") or "").strip()
    id_line = f"id: {event_id}\n" if event_id else ""
    return f"{id_line}event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


def json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def fetch_bottom_watchlist(limit: int = 500) -> list[dict[str, Any]]:
    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            ALTER TABLE bottom_watchlist_tokens
                ADD COLUMN IF NOT EXISTS symbol TEXT,
                ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'auto_ath_mcap',
                ADD COLUMN IF NOT EXISTS peak_mcap NUMERIC DEFAULT 0,
                ADD COLUMN IF NOT EXISTS last_mcap NUMERIC DEFAULT 0,
                ADD COLUMN IF NOT EXISTS highest_mcap NUMERIC DEFAULT 0,
                ADD COLUMN IF NOT EXISTS current_mcap NUMERIC DEFAULT 0,
                ADD COLUMN IF NOT EXISTS ath_mcap NUMERIC DEFAULT 0,
                ADD COLUMN IF NOT EXISTS last_pool_liquidity NUMERIC DEFAULT 0,
                ADD COLUMN IF NOT EXISTS last_pool_mcap_ratio NUMERIC DEFAULT 0,
                ADD COLUMN IF NOT EXISTS narrative_desc TEXT,
                ADD COLUMN IF NOT EXISTS narrative_type TEXT,
                ADD COLUMN IF NOT EXISTS narrative_category TEXT,
                ADD COLUMN IF NOT EXISTS remark TEXT,
                ADD COLUMN IF NOT EXISTS note TEXT,
                ADD COLUMN IF NOT EXISTS blacklisted BOOLEAN DEFAULT false,
                ADD COLUMN IF NOT EXISTS added_at TIMESTAMPTZ DEFAULT now(),
                ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ,
                ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now();
            """
        )
        cur.execute(
            """
            SELECT
                ca,
                symbol,
                COALESCE(NULLIF(narrative_desc, ''), NULLIF(remark, ''), NULLIF(note, ''), '') AS narrative,
                COALESCE(NULLIF(narrative_type, ''), source, '') AS token_type,
                COALESCE(NULLIF(narrative_category, ''), '') AS narrative_category,
                COALESCE(current_mcap, last_mcap, 0) AS current_mcap,
                GREATEST(
                    COALESCE(highest_mcap, 0),
                    COALESCE(ath_mcap, 0),
                    COALESCE(peak_mcap, 0),
                    COALESCE(current_mcap, 0),
                    COALESCE(last_mcap, 0)
                ) AS max_mcap,
                COALESCE(ath_mcap, 0) AS ath_mcap,
                COALESCE(peak_mcap, 0) AS peak_mcap,
                COALESCE(last_pool_liquidity, 0) AS liquidity,
                COALESCE(last_pool_mcap_ratio, 0) AS pool_mcap_ratio,
                COALESCE(blacklisted, false) AS blacklisted,
                added_at,
                last_seen_at,
                updated_at,
                source
            FROM bottom_watchlist_tokens
            WHERE ca IS NOT NULL
            ORDER BY GREATEST(
                COALESCE(highest_mcap, 0),
                COALESCE(ath_mcap, 0),
                COALESCE(peak_mcap, 0),
                COALESCE(current_mcap, 0),
                COALESCE(last_mcap, 0)
            ) DESC, updated_at DESC NULLS LAST, last_seen_at DESC NULLS LAST
            LIMIT %s
            """,
            (limit,),
        )
        columns = [desc[0] for desc in cur.description]
        return [
            {key: json_safe(value) for key, value in zip(columns, row)}
            for row in cur.fetchall()
        ]

    return db_op(_op)


def fetch_alpha_new_token_events(limit: int = 100) -> list[dict[str, Any]]:
    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                id,
                extract(epoch from pushed_at)::bigint AS ts,
                address,
                chain,
                symbol,
                source,
                trend_interval,
                alert_no,
                repeat_alert,
                repeat_alert_type,
                entry_mcap,
                entry_price,
                holder_count,
                fee_sol,
                buy_score,
                tg_chat_id,
                tg_message_id,
                raw_stats->>'narrative' AS raw_narrative,
                raw_stats->>'narrative_desc' AS raw_narrative_desc,
                raw_stats->>'verdict' AS raw_verdict,
                raw_stats->>'market_structure' AS raw_market_structure,
                raw_stats->>'pool_label' AS raw_pool_label,
                raw_stats->>'pool_liquidity' AS raw_pool_liquidity,
                raw_stats->>'pool_mcap_ratio' AS raw_pool_mcap_ratio,
                raw_stats->>'trade_volume_usd' AS raw_trade_volume_usd,
                raw_stats->>'control_ratio' AS raw_control_ratio,
                raw_stats->>'top10_rate' AS raw_top10_rate,
                raw_stats->>'created_time' AS raw_created_time,
                raw_stats->>'created_at' AS raw_created_at,
                raw_stats->>'price_observation_change_pct' AS raw_price_observation_change_pct,
                COALESCE(raw_stats->'mcap_alert_history', '[]'::jsonb) AS raw_mcap_alert_history,
                COALESCE(raw_stats->'price_alert_history', '[]'::jsonb) AS raw_price_alert_history,
                pushed_at
            FROM alpha_push_events
            WHERE trend_interval = '1m'
              AND COALESCE(source, '1m') = '1m'
            ORDER BY pushed_at DESC, id DESC
            LIMIT %s
            """,
            (limit,),
        )
        columns = [desc[0] for desc in cur.description]
        rows = []
        for row in cur.fetchall():
            item = {key: json_safe(value) for key, value in zip(columns, row)}
            raw_narrative = item.pop("raw_narrative", "")
            raw_narrative_desc = item.pop("raw_narrative_desc", "")
            item["narrative"] = raw_narrative or raw_narrative_desc or ""
            item["verdict"] = item.pop("raw_verdict", "") or ""
            item["market_structure"] = item.pop("raw_market_structure", "") or ""
            item["pool_label"] = item.pop("raw_pool_label", "") or ""
            item["pool_liquidity"] = _safe_float(item.pop("raw_pool_liquidity", 0))
            item["pool_mcap_ratio"] = _safe_float(item.pop("raw_pool_mcap_ratio", 0))
            if not _safe_float(item["pool_mcap_ratio"]):
                entry_mcap = _safe_float(item.get("entry_mcap"))
                pool_liquidity = _safe_float(item.get("pool_liquidity"))
                if entry_mcap > 0 and pool_liquidity > 0:
                    item["pool_mcap_ratio"] = pool_liquidity / entry_mcap
            item["trade_volume_usd"] = _safe_float(item.pop("raw_trade_volume_usd", 0))
            item["control_ratio"] = _safe_float(item.pop("raw_control_ratio", 0))
            item["top10_rate"] = _safe_float(item.pop("raw_top10_rate", 0))
            item["created_time"] = item.pop("raw_created_time", "") or ""
            item["created_at"] = _safe_int(item.pop("raw_created_at", 0))
            item["price_observation_change_pct"] = _safe_float(item.pop("raw_price_observation_change_pct", 0))
            item["mcap_alert_history"] = item.pop("raw_mcap_alert_history", []) or []
            item["price_alert_history"] = item.pop("raw_price_alert_history", []) or []
            item.pop("tg_chat_id", None)
            item.pop("tg_message_id", None)
            rows.append(item)
        return rows

    return db_op(_op)


def compact_alpha_new_token_stream_item(item: dict[str, Any]) -> dict[str, Any] | None:
    normalized = normalize_alert(str(item.get("id") or ""), item)
    if normalized.get("source") != "alpha_new_tokens":
        return None
    extra = normalized.get("extra") if isinstance(normalized.get("extra"), dict) else {}
    address = str(extra.get("address") or normalized.get("ca") or "").strip()
    if not address:
        return None
    ts = _safe_int(normalized.get("ts") or extra.get("ts"))
    stream_id = str(normalized.get("id") or "")
    pushed_at = datetime.fromtimestamp(ts).isoformat() if ts > 0 else ""
    return {
        "id": f"stream:{stream_id or address}",
        "ts": ts,
        "address": address,
        "chain": extra.get("chain") or "sol",
        "symbol": extra.get("symbol") or "UNKNOWN",
        "source": extra.get("source") or "1m",
        "trend_interval": extra.get("trend_interval") or "1m",
        "alert_no": _safe_int(extra.get("alert_no")),
        "repeat_alert": bool(extra.get("repeat_alert")),
        "repeat_alert_type": extra.get("repeat_alert_type") or "",
        "entry_mcap": _safe_float(extra.get("entry_mcap")),
        "entry_price": _safe_float(extra.get("entry_price")),
        "holder_count": _safe_int(extra.get("holder_count")),
        "fee_sol": _safe_float(extra.get("fee_sol")),
        "buy_score": _safe_int(extra.get("buy_score")),
        "pushed_at": pushed_at,
        "narrative": extra.get("narrative") or "",
        "verdict": extra.get("verdict") or "",
        "market_structure": extra.get("market_structure") or "",
        "pool_label": extra.get("pool_label") or "",
        "pool_liquidity": _safe_float(extra.get("pool_liquidity")),
        "pool_mcap_ratio": _safe_float(extra.get("pool_mcap_ratio")),
        "trade_volume_usd": _safe_float(extra.get("trade_volume_usd")),
        "control_ratio": _safe_float(extra.get("control_ratio")),
        "top10_rate": _safe_float(extra.get("top10_rate")),
        "created_time": extra.get("created_time") or "",
        "created_at": _safe_int(extra.get("created_at")),
        "price_observation_change_pct": _safe_float(extra.get("price_observation_change_pct")),
        "mcap_alert_history": extra.get("mcap_alert_history") if isinstance(extra.get("mcap_alert_history"), list) else [],
        "price_alert_history": extra.get("price_alert_history") if isinstance(extra.get("price_alert_history"), list) else [],
    }


def fetch_alpha_new_token_stream_events(limit: int = 100) -> list[dict[str, Any]]:
    history_limit = min(1000, max(limit * 3, 200))
    rows = []
    for item in read_recent_plugin_signals(history_limit):
        compact = compact_alpha_new_token_stream_item(item)
        if compact:
            rows.append(compact)
    return sorted(rows, key=_alpha_new_token_sort_key, reverse=True)[:limit]


def _alpha_new_token_sort_key(item: dict[str, Any]) -> tuple[int, int, str]:
    item_id = str(item.get("id") or "")
    stream_id = item_id.split("stream:", 1)[-1] if item_id.startswith("stream:") else item_id
    stream_ms = _safe_int(stream_id.split("-", 1)[0]) if "-" in stream_id else 0
    return (_safe_int(item.get("ts")), stream_ms, item_id)


def merge_alpha_new_token_events(
    db_items: list[dict[str, Any]],
    stream_items: list[dict[str, Any]],
    limit: int,
) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for item in [*db_items, *stream_items]:
        address = str(item.get("address") or item.get("ca") or "").strip()
        if not address:
            continue
        current = merged.get(address)
        if current is None or _alpha_new_token_sort_key(item) > _alpha_new_token_sort_key(current):
            merged[address] = item
    return sorted(merged.values(), key=_alpha_new_token_sort_key, reverse=True)[:limit]


def _to_ts(value: Any) -> int:
    if isinstance(value, datetime):
        return int(value.timestamp())
    try:
        ts = int(float(value))
        return ts // 1000 if ts > 10_000_000_000 else ts
    except (TypeError, ValueError):
        return 0


def _format_dashboard_time(ts: Any) -> str:
    value = _to_ts(ts)
    if value <= 0:
        return ""
    return datetime.fromtimestamp(value, REPORT_TZ).strftime("%Y-%m-%d %H:%M:%S")


def _push_ca_day_window(day_text: str = "") -> tuple[str, int, int, datetime, datetime]:
    text = str(day_text or "").strip()
    try:
        day = date.fromisoformat(text) if text else datetime.now(REPORT_TZ).date()
    except ValueError:
        raise HTTPException(status_code=400, detail="date must be YYYY-MM-DD")
    start_dt = datetime(day.year, day.month, day.day, tzinfo=REPORT_TZ)
    end_dt = start_dt + timedelta(days=1)
    return day.isoformat(), int(start_dt.timestamp()), int(end_dt.timestamp()), start_dt, end_dt


def _pct_change(value: float, base: float) -> float:
    return (value / base - 1) * 100 if value > 0 and base > 0 else 0.0


def _fetch_binance_dynamic(address: str) -> dict[str, Any]:
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
        return {
            "price": _safe_float(data.get("price")),
            "market_cap": _safe_float(data.get("marketCap")),
            "pool_liquidity": _safe_float(
                data.get("poolLiquidity")
                or data.get("pool_liquidity")
                or data.get("liquidity")
                or data.get("liquidityUsd")
                or data.get("poolTotalLiquidity")
            ),
            "holders": _safe_int(data.get("holders")),
            "volume_5m": _safe_float(data.get("volume5m")),
            "volume_1h": _safe_float(data.get("volume1h")),
            "symbol": data.get("symbol") or "",
        }
    except Exception:
        return {}


def _parse_binance_kline(raw: Any) -> list[dict[str, float]]:
    candles = []
    for item in raw or []:
        if not isinstance(item, list) or len(item) < 6:
            continue
        try:
            ts = int(item[5] / 1000) if item[5] > 10**10 else int(item[5])
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
        except (TypeError, ValueError):
            continue
    candles.sort(key=lambda candle: int(candle["ts"]))
    return candles


def _fetch_binance_kline_range(address: str, from_ts: int, to_ts: int, interval: str = "1min") -> list[dict[str, float]]:
    params = {
        "address": address,
        "platform": "solana",
        "interval": interval,
        "pm": "p",
        "from": max(0, int(from_ts)) * 1000,
        "to": max(0, int(to_ts)) * 1000,
    }
    try:
        resp = requests.get(BINANCE_KLINE_URL, params=params, headers=BINANCE_HEADERS, timeout=25)
        if not resp.ok:
            return []
        return _parse_binance_kline(resp.json().get("data"))
    except Exception:
        return []


def _kline_resolution_seconds(resolution: str) -> int:
    return {
        "1m": 60,
        "1min": 60,
        "5m": 300,
        "5min": 300,
        "15m": 900,
        "15min": 900,
        "1h": 3600,
    }.get(str(resolution or "").lower(), 60)


def _binance_interval(resolution: str) -> str:
    return {
        "1m": "1min",
        "5m": "5min",
        "15m": "15min",
        "1h": "1h",
    }.get(str(resolution or "").lower(), resolution or "1min")


def ensure_dashboard_kline_cache_table() -> None:
    global _DASHBOARD_KLINE_CACHE_TABLE_READY
    if _DASHBOARD_KLINE_CACHE_TABLE_READY:
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
    _DASHBOARD_KLINE_CACHE_TABLE_READY = True


def load_dashboard_kline_cache(address: str, resolution: str, from_ts: int, to_ts: int) -> list[dict[str, float]]:
    if not address:
        return []
    ensure_dashboard_kline_cache_table()

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            SELECT ts, open, high, low, close, volume, amount
            FROM bottom_kline_cache
            WHERE chain=%s
              AND address=%s
              AND resolution=%s
              AND ts >= %s
              AND ts <= %s
            ORDER BY ts ASC
            """,
            ("sol", address, resolution, int(from_ts), int(to_ts)),
        )
        return [
            {
                "ts": int(row[0]),
                "open": _safe_float(row[1]),
                "high": _safe_float(row[2]),
                "low": _safe_float(row[3]),
                "close": _safe_float(row[4]),
                "volume": _safe_float(row[5]),
                "amount": _safe_float(row[6]),
            }
            for row in cur.fetchall()
        ]

    return db_op(_op) or []


def load_dashboard_kline_cache_many(
    addresses: list[str],
    resolution: str,
    from_ts: int,
    to_ts: int,
) -> dict[str, list[dict[str, float]]]:
    addresses = sorted({str(address or "").strip() for address in addresses if str(address or "").strip()})
    if not addresses:
        return {}
    ensure_dashboard_kline_cache_table()

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            SELECT address, ts, open, high, low, close, volume, amount
            FROM bottom_kline_cache
            WHERE chain=%s
              AND address = ANY(%s)
              AND resolution=%s
              AND ts >= %s
              AND ts <= %s
            ORDER BY address ASC, ts ASC
            """,
            ("sol", addresses, resolution, int(from_ts), int(to_ts)),
        )
        grouped: dict[str, list[dict[str, float]]] = {}
        for row in cur.fetchall():
            grouped.setdefault(str(row[0]), []).append(
                {
                    "ts": int(row[1]),
                    "open": _safe_float(row[2]),
                    "high": _safe_float(row[3]),
                    "low": _safe_float(row[4]),
                    "close": _safe_float(row[5]),
                    "volume": _safe_float(row[6]),
                    "amount": _safe_float(row[7]),
                }
            )
        return grouped

    return db_op(_op) or {}


def _push_item_row_key(item: dict[str, Any]) -> str:
    return f"{item.get('source_key') or ''}:{item.get('id') or ''}:{item.get('address') or ''}:{item.get('pushed_ts') or 0}"


def load_dashboard_kline_stats_many(
    items: list[dict[str, Any]],
    resolution: str,
    to_ts: int,
) -> dict[str, dict[str, Any]]:
    rows = [
        (_push_item_row_key(item), str(item.get("address") or "").strip(), _to_ts(item.get("pushed_ts")))
        for item in items
        if str(item.get("address") or "").strip() and _to_ts(item.get("pushed_ts")) > 0
    ]
    if not rows:
        return {}
    ensure_dashboard_kline_cache_table()
    values_sql = ",".join(["(%s,%s,%s)"] * len(rows))
    params: list[Any] = []
    for row in rows:
        params.extend(row)
    params.extend(["sol", resolution, int(to_ts)])

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            f"""
            WITH inputs(row_key, address, pushed_ts) AS (
                VALUES {values_sql}
            ),
            filtered AS (
                SELECT
                    i.row_key,
                    k.ts,
                    k.open,
                    k.high,
                    k.low,
                    k.close,
                    k.volume
                FROM inputs i
                JOIN bottom_kline_cache k
                  ON k.chain = %s
                 AND k.address = i.address
                 AND k.resolution = %s
                 AND k.ts >= GREATEST(0, i.pushed_ts::bigint - 120)
                 AND k.ts <= %s
                 AND k.ts + 60 > i.pushed_ts::bigint
            )
            SELECT
                row_key,
                (array_agg(open ORDER BY ts ASC))[1] AS first_open,
                (array_agg(close ORDER BY ts ASC))[1] AS first_close,
                (array_agg(close ORDER BY ts DESC))[1] AS last_close,
                (array_agg(ts ORDER BY ts DESC))[1] AS last_ts,
                (array_agg(high ORDER BY high DESC NULLS LAST, ts ASC))[1] AS peak_high,
                (array_agg(ts ORDER BY high DESC NULLS LAST, ts ASC))[1] AS peak_ts,
                COUNT(*) AS candle_count,
                COALESCE(SUM(volume), 0) AS volume_sum
            FROM filtered
            GROUP BY row_key
            """,
            params,
        )
        return {
            str(row[0]): {
                "first_open": _safe_float(row[1]),
                "first_close": _safe_float(row[2]),
                "last_close": _safe_float(row[3]),
                "last_ts": _to_ts(row[4]),
                "peak_high": _safe_float(row[5]),
                "peak_ts": _to_ts(row[6]),
                "candle_count": _safe_int(row[7]),
                "volume_sum": _safe_float(row[8]),
            }
            for row in cur.fetchall()
        }

    return db_op(_op) or {}


def save_dashboard_kline_cache(address: str, resolution: str, candles: list[dict[str, Any]]) -> int:
    if not address or not candles:
        return 0
    ensure_dashboard_kline_cache_table()

    def _op(conn):
        cur = conn.cursor()
        rows = [
            (
                "sol",
                address,
                resolution,
                _to_ts(candle.get("ts")),
                candle.get("open"),
                candle.get("high"),
                candle.get("low"),
                candle.get("close"),
                candle.get("volume"),
                candle.get("amount"),
            )
            for candle in candles
            if _to_ts(candle.get("ts")) > 0
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


def insert_dashboard_kline_cache_missing_only(address: str, resolution: str, candles: list[dict[str, Any]]) -> int:
    if not address or not candles:
        return 0
    ensure_dashboard_kline_cache_table()

    def _op(conn):
        cur = conn.cursor()
        rows = [
            (
                "sol",
                address,
                resolution,
                _to_ts(candle.get("ts")),
                candle.get("open"),
                candle.get("high"),
                candle.get("low"),
                candle.get("close"),
                candle.get("volume"),
                candle.get("amount"),
            )
            for candle in candles
            if _to_ts(candle.get("ts")) > 0
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


def fetch_dashboard_kline_range(
    address: str,
    from_ts: int,
    to_ts: int,
    resolution: str = "1m",
    allow_external: bool = True,
) -> tuple[list[dict[str, float]], str]:
    """Read DB K-line cache first, then fetch missing/recent candles from Binance Web3."""
    if not address or from_ts <= 0 or to_ts <= 0:
        return [], "no_address"
    step = _kline_resolution_seconds(resolution)
    cached = load_dashboard_kline_cache(address, resolution, from_ts, to_ts)
    latest_cached_ts = max((_to_ts(candle.get("ts")) for candle in cached), default=0)
    earliest_cached_ts = min((_to_ts(candle.get("ts")) for candle in cached), default=0)
    fresh: list[dict[str, float]] = []

    cache_has_start = bool(cached and earliest_cached_ts <= from_ts + step * 2)
    cache_is_recent = bool(cached and latest_cached_ts >= to_ts - step * 2)
    if allow_external and (not cache_has_start or not cache_is_recent):
        fetch_from = from_ts if latest_cached_ts <= 0 else max(from_ts, latest_cached_ts - step * 2)
        if not cache_has_start:
            fetch_from = from_ts
        fresh = _fetch_binance_kline_range(
            address,
            fetch_from,
            to_ts,
            interval=_binance_interval(resolution),
        )
        save_dashboard_kline_cache(address, resolution, fresh)
        cached = load_dashboard_kline_cache(address, resolution, from_ts, to_ts)

    seen = set()
    merged = []
    for candle in sorted([*cached, *fresh], key=lambda item: _to_ts(item.get("ts"))):
        ts = _to_ts(candle.get("ts"))
        if ts <= 0 or ts in seen or ts < from_ts or ts > to_ts:
            continue
        seen.add(ts)
        merged.append(candle)

    if fresh and cached:
        return merged, "db_cache+binance_kline"
    if cached:
        return merged, "db_cache"
    if not allow_external:
        return [], "db_cache_miss"
    if fresh:
        return merged, "binance_kline"
    return [], "empty"


def enrich_push_ca_item_from_candles(
    item: dict[str, Any],
    candles: list[dict[str, float]],
    kline_source: str,
    now: float,
) -> dict[str, Any]:
    pushed_ts = _to_ts(item.get("pushed_ts"))
    signal_mcap = _safe_float(item.get("signal_mcap"))
    entry_price = _safe_float(item.get("entry_price"))
    post = [candle for candle in candles if int(candle.get("ts") or 0) + 60 > pushed_ts]

    entry_price_used = entry_price
    if entry_price_used <= 0 and post:
        first = post[0]
        entry_price_used = _safe_float(first.get("open")) or _safe_float(first.get("close"))

    peak_price = 0.0
    peak_ts = 0
    current_price = 0.0
    volume_usd = 0.0
    if post:
        peak_candle = max(post, key=lambda candle: _safe_float(candle.get("high")))
        peak_price = _safe_float(peak_candle.get("high"))
        peak_ts = _to_ts(peak_candle.get("ts"))
        current_price = _safe_float(post[-1].get("close"))
        volume_usd = sum(_safe_float(candle.get("volume")) for candle in post)

    peak_gain_pct = _pct_change(peak_price, entry_price_used)
    current_gain_pct = _pct_change(current_price, entry_price_used)
    peak_mcap = signal_mcap * (1 + peak_gain_pct / 100) if signal_mcap > 0 and peak_gain_pct else 0.0
    current_mcap = signal_mcap * (1 + current_gain_pct / 100) if signal_mcap > 0 and current_gain_pct else 0.0
    current_mcap = current_mcap or signal_mcap
    if peak_mcap <= 0 and signal_mcap > 0:
        peak_mcap = max(signal_mcap, current_mcap)
        peak_gain_pct = _pct_change(peak_mcap, signal_mcap)

    current_drop_pct = ((signal_mcap - current_mcap) / signal_mcap * 100) if signal_mcap > 0 and current_mcap > 0 else 0.0
    metrics = {
        "signal_mcap": signal_mcap,
        "post_peak_mcap": peak_mcap,
        "post_peak_gain_pct": peak_gain_pct,
        "current_mcap": current_mcap,
        "current_drop_pct": current_drop_pct,
        "current_vs_signal_pct": -current_drop_pct,
        "entry_price_used": entry_price_used,
        "current_price": current_price,
        "post_peak_price": peak_price,
        "post_peak_ts": peak_ts,
        "post_peak_time": _format_dashboard_time(peak_ts),
        "post_volume_usd": volume_usd,
        "kline_candles": len(post),
        "metrics_source": kline_source if post else "db_cache_miss",
        "refreshed_at": _format_dashboard_time(int(now)),
    }
    return {**item, **metrics}


def enrich_push_ca_item_from_kline_stats(
    item: dict[str, Any],
    stats: dict[str, Any] | None,
    now: float,
) -> dict[str, Any]:
    signal_mcap = _safe_float(item.get("signal_mcap"))
    entry_price = _safe_float(item.get("entry_price"))
    stats = stats or {}
    entry_price_used = entry_price or _safe_float(stats.get("first_open")) or _safe_float(stats.get("first_close"))
    peak_price = _safe_float(stats.get("peak_high"))
    current_price = _safe_float(stats.get("last_close"))
    peak_gain_pct = _pct_change(peak_price, entry_price_used)
    current_gain_pct = _pct_change(current_price, entry_price_used)
    peak_mcap = signal_mcap * (1 + peak_gain_pct / 100) if signal_mcap > 0 and peak_gain_pct else 0.0
    current_mcap = signal_mcap * (1 + current_gain_pct / 100) if signal_mcap > 0 and current_gain_pct else 0.0
    current_mcap = current_mcap or signal_mcap
    if peak_mcap <= 0 and signal_mcap > 0:
        peak_mcap = max(signal_mcap, current_mcap)
        peak_gain_pct = _pct_change(peak_mcap, signal_mcap)
    current_drop_pct = ((signal_mcap - current_mcap) / signal_mcap * 100) if signal_mcap > 0 and current_mcap > 0 else 0.0
    candle_count = _safe_int(stats.get("candle_count"))
    return {
        **item,
        "signal_mcap": signal_mcap,
        "post_peak_mcap": peak_mcap,
        "post_peak_gain_pct": peak_gain_pct,
        "current_mcap": current_mcap,
        "current_drop_pct": current_drop_pct,
        "current_vs_signal_pct": -current_drop_pct,
        "entry_price_used": entry_price_used,
        "current_price": current_price,
        "post_peak_price": peak_price,
        "post_peak_ts": _to_ts(stats.get("peak_ts")),
        "post_peak_time": _format_dashboard_time(stats.get("peak_ts")),
        "post_volume_usd": _safe_float(stats.get("volume_sum")),
        "kline_candles": candle_count,
        "metrics_source": "db_cache" if candle_count > 0 else "db_cache_miss",
        "refreshed_at": _format_dashboard_time(int(now)),
    }


def _push_metric_cache_key(item: dict[str, Any]) -> str:
    return "|".join(
        [
            str(item.get("source_key") or ""),
            str(item.get("id") or ""),
            str(item.get("address") or ""),
            str(item.get("pushed_ts") or 0),
            str(round(_safe_float(item.get("signal_mcap")), 6)),
        ]
    )


def enrich_push_ca_item(item: dict[str, Any], refresh: bool = False, allow_external: bool = True) -> dict[str, Any]:
    address = str(item.get("address") or "").strip()
    pushed_ts = _to_ts(item.get("pushed_ts"))
    signal_mcap = _safe_float(item.get("signal_mcap"))
    entry_price = _safe_float(item.get("entry_price"))
    cached_key = _push_metric_cache_key(item)
    now = time.time()
    cached = _PUSH_CA_METRIC_CACHE.get(cached_key)
    if (
        allow_external
        and not refresh
        and cached
        and now - float(cached.get("cached_at") or 0) <= PUSH_CA_METRIC_CACHE_TTL_SEC
    ):
        return {**item, **cached.get("metrics", {})}

    dynamic = _fetch_binance_dynamic(address) if allow_external and address else {}
    current_price = _safe_float(dynamic.get("price"))
    dynamic_mcap = _safe_float(dynamic.get("market_cap"))
    candles: list[dict[str, float]] = []
    post: list[dict[str, float]] = []
    kline_source = ""
    if address and pushed_ts > 0:
        candles, kline_source = fetch_dashboard_kline_range(
            address,
            max(0, pushed_ts - 120),
            int(now),
            resolution="1m",
            allow_external=allow_external,
        )
        post = [candle for candle in candles if int(candle.get("ts") or 0) + 60 > pushed_ts]

    entry_price_used = entry_price
    if entry_price_used <= 0 and post:
        first = post[0]
        entry_price_used = _safe_float(first.get("open")) or _safe_float(first.get("close"))

    peak_price = 0.0
    peak_ts = 0
    current_kline_price = 0.0
    volume_usd = 0.0
    if post:
        peak_candle = max(post, key=lambda candle: _safe_float(candle.get("high")))
        peak_price = _safe_float(peak_candle.get("high"))
        peak_ts = _to_ts(peak_candle.get("ts"))
        current_kline_price = _safe_float(post[-1].get("close"))
        volume_usd = sum(_safe_float(candle.get("volume")) for candle in post)

    if current_price <= 0:
        current_price = current_kline_price

    peak_gain_pct = _pct_change(peak_price, entry_price_used)
    current_gain_pct = _pct_change(current_price, entry_price_used)
    peak_mcap = signal_mcap * (1 + peak_gain_pct / 100) if signal_mcap > 0 and peak_gain_pct else 0.0
    kline_current_mcap = signal_mcap * (1 + current_gain_pct / 100) if signal_mcap > 0 and current_gain_pct else 0.0
    current_mcap = dynamic_mcap or kline_current_mcap or signal_mcap
    if peak_mcap <= 0 and signal_mcap > 0:
        peak_mcap = max(signal_mcap, current_mcap)
        peak_gain_pct = _pct_change(peak_mcap, signal_mcap)

    current_drop_pct = ((signal_mcap - current_mcap) / signal_mcap * 100) if signal_mcap > 0 and current_mcap > 0 else 0.0
    current_vs_signal_pct = -current_drop_pct
    metrics = {
        "signal_mcap": signal_mcap,
        "post_peak_mcap": peak_mcap,
        "post_peak_gain_pct": peak_gain_pct,
        "current_mcap": current_mcap,
        "current_drop_pct": current_drop_pct,
        "current_vs_signal_pct": current_vs_signal_pct,
        "entry_price_used": entry_price_used,
        "current_price": current_price,
        "post_peak_price": peak_price,
        "post_peak_ts": peak_ts,
        "post_peak_time": _format_dashboard_time(peak_ts),
        "post_volume_usd": volume_usd,
        "kline_candles": len(post),
        "metrics_source": (
            f"binance_dynamic+{kline_source}" if post and dynamic_mcap
            else (kline_source if post else ("binance_dynamic" if dynamic_mcap else "fallback"))
        ),
        "refreshed_at": _format_dashboard_time(int(now)),
    }
    _PUSH_CA_METRIC_CACHE[cached_key] = {"cached_at": now, "metrics": metrics}
    return {**item, **metrics}


def enrich_push_ca_items(
    items: list[dict[str, Any]],
    refresh: bool = False,
    allow_external: bool = True,
) -> list[dict[str, Any]]:
    if not items:
        return []
    if not allow_external:
        now = time.time()
        stats_by_key = load_dashboard_kline_stats_many(items, "1m", int(now))
        return [
            enrich_push_ca_item_from_kline_stats(item, stats_by_key.get(_push_item_row_key(item)), now)
            for item in items
        ]

    max_workers = max(1, min(PUSH_CA_MAX_WORKERS, len(items)))
    enriched: list[dict[str, Any] | None] = [None] * len(items)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(enrich_push_ca_item, item, refresh, allow_external): index
            for index, item in enumerate(items)
        }
        for future in as_completed(future_map):
            index = future_map[future]
            try:
                enriched[index] = future.result()
            except Exception as exc:
                fallback = dict(items[index])
                fallback["metrics_error"] = str(exc)
                fallback["current_mcap"] = fallback.get("signal_mcap") or 0
                fallback["post_peak_mcap"] = fallback.get("signal_mcap") or 0
                enriched[index] = fallback
    return [item for item in enriched if item is not None]


def fetch_bottom_push_ca_items(limit: int = 50, q: str = "", day: str = "") -> tuple[str, list[dict[str, Any]]]:
    day_iso, start_ts, end_ts, _, _ = _push_ca_day_window(day)

    def _op(conn):
        cur = conn.cursor()
        cur.execute("SELECT to_regclass('public.bottom_top100_push_records')")
        if not cur.fetchone()[0]:
            return []
        cur.execute(
            """
            ALTER TABLE bottom_top100_push_records
                ADD COLUMN IF NOT EXISTS snapshot_id BIGINT,
                ADD COLUMN IF NOT EXISTS liquidity NUMERIC DEFAULT 0;
            """
        )
        where = [
            "COALESCE(NULLIF(event_ts, 0), extract(epoch from pushed_at)::bigint) >= %s",
            "COALESCE(NULLIF(event_ts, 0), extract(epoch from pushed_at)::bigint) < %s",
        ]
        params: list[Any] = [start_ts, end_ts]
        query = str(q or "").strip()
        if query:
            like = f"%{query}%"
            where.append(
                """
                (
                    address ILIKE %s OR symbol ILIKE %s OR signal_type ILIKE %s
                    OR abnormal_rule ILIKE %s OR COALESCE(extra->>'narrative_desc', '') ILIKE %s
                )
                """
            )
            params.extend([like, like, like, like, like])
        params.append(limit)
        cur.execute(
            f"""
            SELECT
                id,
                address,
                COALESCE(chain, 'sol') AS chain,
                COALESCE(symbol, '') AS symbol,
                COALESCE(source, 'bottom_abnormal') AS source,
                COALESCE(signal_type, '') AS signal_type,
                COALESCE(abnormal_rule, '') AS abnormal_rule,
                COALESCE(trend_interval, '') AS trend_interval,
                COALESCE(current_mcap, 0) AS signal_mcap,
                COALESCE(first_signal_mcap, 0) AS first_signal_mcap,
                COALESCE(price_change_pct, 0) AS price_change_pct,
                COALESCE(max_abnormal_mcap, 0) AS max_abnormal_mcap,
                COALESCE(ath_mcap, 0) AS ath_mcap,
                COALESCE(liquidity, pool_total_liquidity, 0) AS liquidity,
                COALESCE(pool_mcap_ratio, 0) AS pool_mcap_ratio,
                COALESCE(NULLIF(event_ts, 0), extract(epoch from pushed_at)::bigint) AS pushed_ts,
                pushed_at,
                COALESCE(extra, '{{}}'::jsonb) AS extra,
                COALESCE(text, '') AS text
            FROM bottom_top100_push_records
            WHERE {' AND '.join(where)}
            ORDER BY COALESCE(NULLIF(event_ts, 0), extract(epoch from pushed_at)::bigint) DESC, id DESC
            LIMIT %s
            """,
            params,
        )
        columns = [desc[0] for desc in cur.description]
        return [
            {key: json_safe(value) for key, value in zip(columns, row)}
            for row in cur.fetchall()
        ]

    rows = db_op(_op) or []
    items = []
    for row in rows:
        extra = row.get("extra") if isinstance(row.get("extra"), dict) else {}
        pushed_ts = _to_ts(row.get("pushed_ts"))
        address = str(row.get("address") or "").strip()
        if not address:
            continue
        items.append(
            {
                "id": row.get("id"),
                "source_key": "bottom_push",
                "source_label": "底部推送",
                "address": address,
                "chain": row.get("chain") or "sol",
                "symbol": row.get("symbol") or extra.get("symbol") or "UNKNOWN",
                "signal_type": row.get("signal_type") or "",
                "signal_label": row.get("abnormal_rule") or row.get("signal_type") or "",
                "trend_interval": row.get("trend_interval") or "",
                "signal_mcap": _safe_float(row.get("signal_mcap")),
                "first_signal_mcap": _safe_float(row.get("first_signal_mcap")),
                "price_change_pct": _safe_float(row.get("price_change_pct")),
                "raw_peak_hint_mcap": max(_safe_float(row.get("max_abnormal_mcap")), _safe_float(row.get("ath_mcap"))),
                "liquidity": _safe_float(row.get("liquidity")),
                "pool_mcap_ratio": _safe_float(row.get("pool_mcap_ratio")),
                "pushed_ts": pushed_ts,
                "pushed_time": _format_dashboard_time(pushed_ts),
                "narrative": extra.get("narrative_desc") or extra.get("narrative") or row.get("text") or "",
            }
        )
    return day_iso, items


def fetch_deep_alpha_1m_ca_items(limit: int = 50, q: str = "", day: str = "") -> tuple[str, list[dict[str, Any]]]:
    day_iso, _, _, start_dt, end_dt = _push_ca_day_window(day)

    def _op(conn):
        cur = conn.cursor()
        cur.execute("SELECT to_regclass('public.alpha_push_events')")
        if not cur.fetchone()[0]:
            return []
        where = [
            "trend_interval = '1m'",
            "COALESCE(source, '1m') = '1m'",
            "pushed_at >= %s",
            "pushed_at < %s",
        ]
        params: list[Any] = [start_dt, end_dt]
        query = str(q or "").strip()
        if query:
            like = f"%{query}%"
            where.append(
                """
                (
                    address ILIKE %s OR symbol ILIKE %s OR repeat_alert_type ILIKE %s
                    OR COALESCE(raw_stats->>'narrative_desc', raw_stats->>'narrative', '') ILIKE %s
                )
                """
            )
            params.extend([like, like, like, like])
        params.append(limit)
        cur.execute(
            f"""
            SELECT
                id,
                address,
                COALESCE(chain, 'sol') AS chain,
                COALESCE(symbol, '') AS symbol,
                COALESCE(source, '1m') AS source,
                COALESCE(trend_interval, '1m') AS trend_interval,
                COALESCE(alert_no, 1) AS alert_no,
                COALESCE(repeat_alert, false) AS repeat_alert,
                COALESCE(repeat_alert_type, '') AS repeat_alert_type,
                COALESCE(entry_mcap, 0) AS signal_mcap,
                COALESCE(entry_price, 0) AS entry_price,
                COALESCE(holder_count, 0) AS holder_count,
                COALESCE(fee_sol, 0) AS fee_sol,
                COALESCE(buy_score, 0) AS buy_score,
                COALESCE(raw_stats, '{{}}'::jsonb) AS raw_stats,
                extract(epoch from pushed_at)::bigint AS pushed_ts,
                pushed_at
            FROM alpha_push_events
            WHERE {' AND '.join(where)}
            ORDER BY pushed_at DESC, id DESC
            LIMIT %s
            """,
            params,
        )
        columns = [desc[0] for desc in cur.description]
        return [
            {key: json_safe(value) for key, value in zip(columns, row)}
            for row in cur.fetchall()
        ]

    rows = db_op(_op) or []
    items = []
    for row in rows:
        raw = row.get("raw_stats") if isinstance(row.get("raw_stats"), dict) else {}
        narrative_obj = raw.get("binance_narrative") if isinstance(raw.get("binance_narrative"), dict) else {}
        pushed_ts = _to_ts(row.get("pushed_ts"))
        address = str(row.get("address") or "").strip()
        if not address:
            continue
        repeat_alert = bool(row.get("repeat_alert"))
        items.append(
            {
                "id": row.get("id"),
                "source_key": "deep_alpha_1m",
                "source_label": "Deep Alpha Pro 1m",
                "address": address,
                "chain": row.get("chain") or "sol",
                "symbol": row.get("symbol") or raw.get("symbol") or "UNKNOWN",
                "signal_type": "repeat" if repeat_alert else "first",
                "signal_label": row.get("repeat_alert_type") or ("复推" if repeat_alert else "首推"),
                "trend_interval": row.get("trend_interval") or "1m",
                "alert_no": _safe_int(row.get("alert_no")),
                "signal_mcap": _safe_float(row.get("signal_mcap")),
                "entry_price": _safe_float(row.get("entry_price")),
                "holder_count": _safe_int(row.get("holder_count")),
                "fee_sol": _safe_float(row.get("fee_sol")),
                "buy_score": _safe_int(row.get("buy_score")),
                "liquidity": _safe_float(raw.get("pool_liquidity")),
                "pool_mcap_ratio": _safe_float(raw.get("pool_mcap_ratio")),
                "pushed_ts": pushed_ts,
                "pushed_time": _format_dashboard_time(pushed_ts),
                "narrative": raw.get("narrative_desc") or raw.get("narrative") or narrative_obj.get("narrative_desc") or "",
            }
        )
    return day_iso, items


def fetch_onchain_trading_guides(
    limit: int = 200,
    query: str = "",
    category: str = "",
    chain: str = "",
    include_archived: bool = False,
) -> list[dict[str, Any]]:
    def _op(conn):
        cur = conn.cursor()
        where = []
        params: list[Any] = []
        if not include_archived:
            where.append("is_archived = false")
        if query:
            params.append(f"%{query}%")
            where.append(
                """
                (
                    title ILIKE %s OR note ILIKE %s OR category ILIKE %s OR chain ILIKE %s
                    OR token_address ILIKE %s OR source_url ILIKE %s
                    OR EXISTS (
                        SELECT 1 FROM unnest(tags) AS tag
                        WHERE tag ILIKE %s
                    )
                )
                """
            )
            params.extend([params[-1]] * 6)
        if category:
            where.append("category = %s")
            params.append(category)
        if chain:
            where.append("chain = %s")
            params.append(chain)

        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        params.append(limit)
        cur.execute(
            f"""
            SELECT
                id,
                title,
                note,
                category,
                chain,
                token_address,
                source_url,
                tags,
                metadata,
                is_archived,
                created_at,
                updated_at
            FROM onchain_trading_guides
            {where_sql}
            ORDER BY created_at DESC, id DESC
            LIMIT %s
            """,
            params,
        )
        columns = [desc[0] for desc in cur.description]
        return [
            {key: json_safe(value) for key, value in zip(columns, row)}
            for row in cur.fetchall()
        ]

    return db_op(_op)


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    if BOTTOM_ONLY_FRONTEND:
        return RedirectResponse(url="/bottom-live-track", status_code=302)
    return templates.TemplateResponse(request, "onchain_guides.html", {})


@app.get("/api/health")
def health_api(request: Request):
    client = get_redis_client()
    return {
        "ok": True,
        "service": "chain-alpha-ca-clusters",
        "redis_ok": client is not None,
        "redis_error": "" if client is not None else get_redis_disabled_reason(),
    }


@app.get("/onchain-guides", response_class=HTMLResponse)
def onchain_guides(request: Request):
    if BOTTOM_ONLY_FRONTEND:
        return RedirectResponse(url="/bottom-live-track", status_code=302)
    return templates.TemplateResponse(request, "onchain_guides.html", {})


@app.get("/bottom-push-ca", response_class=HTMLResponse)
def bottom_push_ca_page(request: Request):
    if BOTTOM_ONLY_FRONTEND:
        return RedirectResponse(url="/bottom-live-track", status_code=302)
    return templates.TemplateResponse(
        request,
        "push_ca_table.html",
        {
            "page_title": "底部推送 CA",
            "page_desc": "今日 bottom_top100_push_records 推送后的市值表现，默认显示最近 50 条。",
            "api_path": "/api/push-ca/bottom",
            "source_label": "底部推送",
        },
    )


@app.get("/deep-alpha-1m-ca", response_class=HTMLResponse)
def deep_alpha_1m_ca_page(request: Request):
    if BOTTOM_ONLY_FRONTEND:
        return RedirectResponse(url="/bottom-live-track", status_code=302)
    return templates.TemplateResponse(
        request,
        "push_ca_table.html",
        {
            "page_title": "Deep Alpha Pro 1m 打新 CA",
            "page_desc": "今日 deep_alpha_pro 1m 推送后的市值表现，默认显示最近 50 条。",
            "api_path": "/api/push-ca/deep-alpha-1m",
            "source_label": "Deep Alpha Pro 1m",
        },
    )


@app.get("/api/recent")
def recent(request: Request, limit: int = 100):
    limit = max(1, min(limit, 500))
    return {"items": [normalize_alert(item.get("id", ""), item) for item in read_recent_tg_alerts(limit)]}


@app.get("/api/plugin/bottom-abnormal")
def plugin_bottom_abnormal(request: Request, limit: int = 100):
    limit = max(1, min(limit, 500))
    now = time.monotonic()
    cache_limit = int(_PLUGIN_BOTTOM_ABNORMAL_CACHE.get("limit") or 0)
    cache_ts = float(_PLUGIN_BOTTOM_ABNORMAL_CACHE.get("ts") or 0)
    if (
        limit <= cache_limit
        and now - cache_ts <= PLUGIN_BOTTOM_ABNORMAL_CACHE_TTL_SEC
        and isinstance(_PLUGIN_BOTTOM_ABNORMAL_CACHE.get("items"), list)
    ):
        return {"items": _PLUGIN_BOTTOM_ABNORMAL_CACHE["items"][-limit:], "cached": True}

    history_limit = min(500, max(limit, limit * 3))
    items = []
    for item in read_recent_plugin_signals(history_limit):
        if item.get("source") != "bottom_abnormal":
            continue
        normalized = normalize_alert(item.get("id", ""), item)
        extra = normalized.get("extra") if isinstance(normalized.get("extra"), dict) else {}
        if str(extra.get("signal_type") or "") == "watch":
            continue
        items.append(normalized)
    items = enrich_bottom_watchlist_highlights(enrich_bottom_abnormal_history(items))
    items = [compact_bottom_abnormal_item(item) for item in items][-limit:]
    _PLUGIN_BOTTOM_ABNORMAL_CACHE.update({"ts": now, "limit": limit, "items": items})
    return {"items": items, "cached": False}


@app.get("/api/plugin/alpha-new-tokens")
def plugin_alpha_new_tokens(request: Request, limit: int = 100):
    limit = max(1, min(limit, 300))
    now = time.monotonic()
    cache_limit = int(_PLUGIN_ALPHA_NEW_TOKEN_CACHE.get("limit") or 0)
    cache_ts = float(_PLUGIN_ALPHA_NEW_TOKEN_CACHE.get("ts") or 0)
    if (
        limit <= cache_limit
        and now - cache_ts <= PLUGIN_ALPHA_NEW_TOKEN_CACHE_TTL_SEC
        and isinstance(_PLUGIN_ALPHA_NEW_TOKEN_CACHE.get("items"), list)
    ):
        return {"items": _PLUGIN_ALPHA_NEW_TOKEN_CACHE["items"][:limit], "cached": True}
    items = merge_alpha_new_token_events(
        fetch_alpha_new_token_events(limit),
        fetch_alpha_new_token_stream_events(limit),
        limit,
    )
    _PLUGIN_ALPHA_NEW_TOKEN_CACHE.update({"ts": now, "limit": limit, "items": items})
    return {"items": items, "cached": False}


def _sort_push_ca_items(items: list[dict[str, Any]], sort: str) -> list[dict[str, Any]]:
    key = str(sort or "time_desc")
    sorters = {
        "time_desc": (lambda item: _to_ts(item.get("pushed_ts")), True),
        "time_asc": (lambda item: _to_ts(item.get("pushed_ts")), False),
        "peak_gain_desc": (lambda item: _safe_float(item.get("post_peak_gain_pct")), True),
        "current_drop_desc": (lambda item: _safe_float(item.get("current_drop_pct")), True),
        "current_mcap_desc": (lambda item: _safe_float(item.get("current_mcap")), True),
    }
    getter, reverse = sorters.get(key, sorters["time_desc"])
    return sorted(items, key=getter, reverse=reverse)


def _push_ca_response_cache_get(key: str) -> dict[str, Any] | None:
    cached = _PUSH_CA_RESPONSE_CACHE.get(key)
    if not cached:
        return None
    if time.monotonic() - float(cached.get("ts") or 0) > PUSH_CA_RESPONSE_CACHE_TTL_SEC:
        _PUSH_CA_RESPONSE_CACHE.pop(key, None)
        return None
    payload = cached.get("payload")
    if isinstance(payload, dict):
        return {**payload, "cached": True}
    return None


def _push_ca_response_cache_set(key: str, payload: dict[str, Any]) -> dict[str, Any]:
    _PUSH_CA_RESPONSE_CACHE[key] = {"ts": time.monotonic(), "payload": payload}
    return payload


@app.get("/api/push-ca/bottom")
def bottom_push_ca_api(
    request: Request,
    limit: int = 50,
    q: str = "",
    date: str = "",
    sort: str = "time_desc",
    refresh: bool = False,
    live: bool = False,
):
    limit = max(1, min(limit, 200))
    cache_key = f"bottom|{limit}|{q.strip()}|{date.strip()}|{sort}|{bool(refresh)}|{bool(live)}"
    if not refresh and not live:
        cached = _push_ca_response_cache_get(cache_key)
        if cached:
            return cached
    day_iso, items = fetch_bottom_push_ca_items(limit=limit, q=q.strip(), day=date.strip())
    allow_external = bool(refresh or live)
    items = enrich_push_ca_items(items, refresh=refresh, allow_external=allow_external)
    payload = {
        "items": _sort_push_ca_items(items, sort),
        "count": len(items),
        "date": day_iso,
        "limit": limit,
        "source": "bottom_push",
        "refreshed": refresh,
        "live": allow_external,
        "cached": False,
    }
    if not allow_external:
        return _push_ca_response_cache_set(cache_key, payload)
    return payload


@app.get("/api/push-ca/deep-alpha-1m")
def deep_alpha_1m_ca_api(
    request: Request,
    limit: int = 50,
    q: str = "",
    date: str = "",
    sort: str = "time_desc",
    refresh: bool = False,
    live: bool = False,
):
    limit = max(1, min(limit, 200))
    cache_key = f"deep_alpha_1m|{limit}|{q.strip()}|{date.strip()}|{sort}|{bool(refresh)}|{bool(live)}"
    if not refresh and not live:
        cached = _push_ca_response_cache_get(cache_key)
        if cached:
            return cached
    day_iso, items = fetch_deep_alpha_1m_ca_items(limit=limit, q=q.strip(), day=date.strip())
    allow_external = bool(refresh or live)
    items = enrich_push_ca_items(items, refresh=refresh, allow_external=allow_external)
    payload = {
        "items": _sort_push_ca_items(items, sort),
        "count": len(items),
        "date": day_iso,
        "limit": limit,
        "source": "deep_alpha_1m",
        "refreshed": refresh,
        "live": allow_external,
        "cached": False,
    }
    if not allow_external:
        return _push_ca_response_cache_set(cache_key, payload)
    return payload


@app.get("/api/plugin/health")
def plugin_health(request: Request, limit: int = 20):
    limit = max(1, min(limit, 100))
    client = get_redis_client()
    recent_items = [normalize_alert(item.get("id", ""), item) for item in read_recent_plugin_signals(limit)]
    bottom_abnormal_count = sum(1 for item in recent_items if item.get("source") == "bottom_abnormal")
    return {
        "ok": client is not None,
        "redis_ok": client is not None,
        "redis_error": "" if client is not None else get_redis_disabled_reason(),
        "recent_plugin_count": len(recent_items),
        "plugin_bottom_abnormal_count": bottom_abnormal_count,
        "items": recent_items,
    }


# ---------------------------------------------------------------------------
# gmgn-cli live refresh helpers
# ---------------------------------------------------------------------------

def _gmgn_exe() -> list:
    exe = shutil.which("gmgn-cli") or shutil.which("gmgn-cli.cmd") or "gmgn-cli"
    return [exe]


def _run_gmgn_sync(args_list: list, timeout: int = 45) -> dict:
    cmd = _gmgn_exe() + args_list + ["--raw"]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8",
                           errors="replace", timeout=timeout)
    except subprocess.TimeoutExpired:
        return {}
    if r.returncode != 0:
        return {}
    try:
        return json.loads(r.stdout)
    except json.JSONDecodeError:
        return {}


def _fetch_live_mcap(address: str) -> tuple[str, float, float, str]:
    """Query gmgn-cli for a single token. Returns (address, mcap, liquidity, symbol)."""
    info = _run_gmgn_sync(["token", "info", "--chain", "sol", "--address", address])
    if not info:
        return (address, 0.0, 0.0, "")
    try:
        price = float(info.get("price") or 0)
        supply = float(info.get("circulating_supply") or 0)
        liq = float(info.get("liquidity") or 0)
        symbol = str(info.get("symbol") or "")
        mcap = price * supply
        return (address, mcap, liq, symbol)
    except (ValueError, TypeError):
        return (address, 0.0, 0.0, "")


def _refresh_watchlist_mcaps(addresses: list[str], max_workers: int = 5) -> dict[str, tuple[float, float, str]]:
    """Fetch live market cap for multiple addresses via gmgn-cli in parallel.
    Returns dict[address, (mcap, liquidity, symbol)].
    Updates DB for each token as results come in.
    """
    results: dict[str, tuple[float, float, str]] = {}

    def _update_db(address: str, mcap: float, liq: float, symbol: str):
        if mcap <= 0:
            return
        def _op(conn):
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE bottom_watchlist_tokens
                SET current_mcap = %s,
                    last_mcap = %s,
                    peak_mcap = GREATEST(COALESCE(peak_mcap, 0), %s),
                    highest_mcap = GREATEST(COALESCE(highest_mcap, 0), %s),
                    last_pool_liquidity = CASE WHEN %s > 0 THEN %s ELSE COALESCE(last_pool_liquidity, 0) END,
                    symbol = CASE WHEN %s != '' THEN %s ELSE COALESCE(symbol, '') END,
                    last_seen_at = now(),
                    updated_at = now()
                WHERE ca = %s
                """,
                (mcap, mcap, mcap, mcap, liq, liq, symbol, symbol, address),
            )
        db_op(_op)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(_fetch_live_mcap, addr): addr for addr in addresses}
        for future in as_completed(future_map):
            try:
                addr, mcap, liq, symbol = future.result()
                results[addr] = (mcap, liq, symbol)
                _update_db(addr, mcap, liq, symbol)
            except Exception:
                pass

    return results


@app.get("/api/bottom-watchlist")
def bottom_watchlist_api(request: Request, limit: int = 500, refresh: bool = False):
    limit = max(1, min(limit, 2000))
    if refresh:
        # Get all CA addresses from watchlist, then refresh via gmgn-cli
        items = fetch_bottom_watchlist(limit)
        addresses = [item["ca"] for item in items if item.get("ca")]
        if addresses:
            _refresh_watchlist_mcaps(addresses)
        # Re-fetch from DB after update
        return {"items": fetch_bottom_watchlist(limit), "refreshed": True}
    return {"items": fetch_bottom_watchlist(limit)}


@app.get("/api/bottom-watchlist/deleted-today")
def bottom_watchlist_deleted_today_api(request: Request):
    from bottom_detection.bottom_watchlist_store import fetch_today_deleted_watchlist_tokens
    return {"items": fetch_today_deleted_watchlist_tokens()}


@app.get("/api/onchain-guides")
def onchain_guides_api(
    request: Request,
    limit: int = 200,
    q: str = "",
    category: str = "",
    chain: str = "",
    include_archived: bool = False,
):
    limit = max(1, min(limit, 500))
    return {
        "items": fetch_onchain_trading_guides(
            limit=limit,
            query=q.strip(),
            category=category.strip(),
            chain=chain.strip(),
            include_archived=include_archived,
        )
    }


@app.get("/api/ca-clusters")
async def ca_clusters_api(request: Request, address: str, chain: str = "sol", limit: int = 100):
    address = address.strip()
    chain = chain.strip().lower() or "sol"
    if not address or len(address) < 32:
        raise HTTPException(status_code=400, detail="invalid token address")
    limit = max(20, min(limit, 200))
    result = await asyncio.to_thread(analyze_ca_clusters, address, chain, limit)
    if not result.get("ok"):
        if result.get("error_type") == "empty_holders":
            raise HTTPException(status_code=404, detail=result.get("error") or "no holder data")
        raise HTTPException(status_code=502, detail=result.get("error") or "cluster analysis failed")
    return result


@app.get("/events")
async def events(request: Request, last_id: str = "$"):
    async def generator():
        client = get_redis_client()
        if client is None:
            yield sse_message("error", {"message": f"redis unavailable: {get_redis_disabled_reason()}"})
            return

        current_id = last_id or "$"
        yield sse_message("ready", {"stream": TG_ALERT_STREAM_KEY, "last_id": current_id})
        while True:
            if await request.is_disconnected():
                break
            try:
                rows = await asyncio.to_thread(
                    client.xread,
                    {TG_ALERT_STREAM_KEY: current_id},
                    10,
                    SSE_BLOCK_MS,
                )
            except Exception as exc:
                yield sse_message("error", {"message": str(exc)})
                await asyncio.sleep(2)
                continue

            if not rows:
                yield ": keepalive\n\n"
                continue

            for _, messages in rows:
                for stream_id, fields in messages:
                    current_id = stream_id
                    yield sse_message("alert", normalize_alert(stream_id, fields))

    return StreamingResponse(generator(), media_type="text/event-stream")


@app.get("/api/plugin/events")
async def plugin_events(request: Request, last_id: str = "$"):
    async def generator():
        client = get_redis_client()
        if client is None:
            yield sse_message("error", {"message": f"redis unavailable: {get_redis_disabled_reason()}"})
            return

        current_id = last_id or "$"
        yield sse_message("ready", {"stream": PLUGIN_SIGNAL_STREAM_KEY, "last_id": current_id})
        while True:
            if await request.is_disconnected():
                break
            try:
                rows = await asyncio.to_thread(
                    client.xread,
                    {PLUGIN_SIGNAL_STREAM_KEY: current_id},
                    20,
                    SSE_BLOCK_MS,
                )
            except Exception as exc:
                yield sse_message("error", {"message": str(exc)})
                await asyncio.sleep(2)
                continue

            if not rows:
                yield ": keepalive\n\n"
                continue

            for _, messages in rows:
                for stream_id, fields in messages:
                    current_id = stream_id
                    yield sse_message("signal", normalize_alert(stream_id, fields))

    return StreamingResponse(generator(), media_type="text/event-stream")


# ---------------------------------------------------------------------------
# Alpha live-track: background refresh & endpoints
# ---------------------------------------------------------------------------
def _live_track_redis_key(address: str) -> str:
    return f"{LIVE_TRACK_REDIS_PREFIX}:{address}"


def _live_track_index_key() -> str:
    return f"{LIVE_TRACK_REDIS_PREFIX}:__index__"


def _live_track_list_addresses() -> list[str]:
    client = get_redis_client()
    if client is None:
        return []
    try:
        members = client.smembers(_live_track_index_key())
        return [str(m) for m in members if m]
    except Exception:
        return []


def _live_track_load(address: str) -> dict[str, Any] | None:
    client = get_redis_client()
    if client is None:
        return None
    try:
        raw = client.get(_live_track_redis_key(address))
        return json.loads(raw) if raw else None
    except Exception:
        return None


def _live_track_save(address: str, data: dict[str, Any]) -> None:
    client = get_redis_client()
    if client is None:
        return
    try:
        ttl = client.ttl(_live_track_redis_key(address))
        if ttl is None or ttl <= 0:
            ttl = LIVE_TRACK_REDIS_TTL_SEC
        client.setex(
            _live_track_redis_key(address),
            ttl,
            json.dumps(data, ensure_ascii=False),
        )
    except Exception:
        pass


def _alpha_deleted_today_redis_key(address: str) -> str:
    return f"deep_alpha:live_track:deleted_today:{address}"


def _alpha_deleted_today_index_key() -> str:
    return "deep_alpha:live_track:deleted_today:__index__"


def _alpha_deleted_today_save(address: str, track: dict[str, Any]) -> None:
    client = get_redis_client()
    if client is None:
        return
    try:
        from datetime import datetime, timedelta
        # Calculate time left for today to expire at midnight
        now = time.time()
        tomorrow_midnight = time.mktime((datetime.now() + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0).timetuple())
        ttl = int(tomorrow_midnight - now)
        if ttl <= 0:
            ttl = 86400  # Fallback to 24h
            
        key = _alpha_deleted_today_redis_key(address)
        client.setex(key, ttl, json.dumps(track, ensure_ascii=False))
        
        index_key = _alpha_deleted_today_index_key()
        client.sadd(index_key, address)
        client.expire(index_key, ttl)
    except Exception as exc:
        print(f"Failed to save alpha deleted today for {address[:8]}: {exc}")


def _alpha_deleted_today_list() -> list[dict[str, Any]]:
    client = get_redis_client()
    if client is None:
        return []
    try:
        index_key = _alpha_deleted_today_index_key()
        members = client.smembers(index_key)
        addresses = [str(m.decode() if isinstance(m, bytes) else m) for m in members if m]
        
        items = []
        for addr in addresses:
            raw = client.get(_alpha_deleted_today_redis_key(addr))
            if raw:
                try:
                    items.append(json.loads(raw))
                except Exception:
                    pass
        items.sort(key=lambda x: int(x.get("last_updated") or 0), reverse=True)
        return items
    except Exception:
        return []


def _live_track_remove(address: str, reason: str = "") -> None:
    client = get_redis_client()
    if client is None:
        return
    try:
        track = _live_track_load(address)
        if track:
            track["status"] = "removed"
            track["remove_reason"] = reason
            track["last_updated"] = int(time.time())
            
            # Archive this removed token to "deleted today" Redis store
            _alpha_deleted_today_save(address, track)
            
            # Keep for 60s so frontend sees the removal
            client.setex(_live_track_redis_key(address), 60, json.dumps(track, ensure_ascii=False))
        client.srem(_live_track_index_key(), address)
    except Exception:
        pass


def _live_track_refresh_one(address: str) -> dict[str, Any] | None:
    track = _live_track_load(address)
    if not track or track.get("status") != "tracking":
        return track
    dynamic = _fetch_binance_dynamic(address)
    if not dynamic:
        return track
    now_ts = int(time.time())
    entry_mcap = _safe_float(track.get("entry_mcap"))
    current_mcap = _safe_float(dynamic.get("market_cap"))
    current_price = _safe_float(dynamic.get("price"))
    holders = _safe_int(dynamic.get("holders"))
    volume_5m = _safe_float(dynamic.get("volume_5m"))
    volume_1h = _safe_float(dynamic.get("volume_1h"))
    pool_liquidity = _safe_float(dynamic.get("pool_liquidity")) or _safe_float(track.get("pool_liquidity"))
    prev_peak_mcap = _safe_float(track.get("peak_mcap"))
    peak_mcap = prev_peak_mcap
    peak_mcap_at = _safe_int(track.get("peak_mcap_at"))
    if current_mcap > prev_peak_mcap:
        peak_mcap = current_mcap
        peak_mcap_at = now_ts
        
    pnl_pct = ((current_mcap - entry_mcap) / entry_mcap * 100) if entry_mcap > 0 and current_mcap > 0 else 0.0

    track.update({
        "current_mcap": current_mcap,
        "current_price": current_price,
        "peak_mcap": peak_mcap,
        "peak_mcap_at": peak_mcap_at or int(track.get("pushed_at") or now_ts),
        "pool_liquidity": pool_liquidity,
        "holders": holders,
        "volume_5m": volume_5m,
        "volume_1h": volume_1h,
        "pnl_pct": round(pnl_pct, 2),
        "last_updated": now_ts,
    })
    if dynamic.get("symbol"):
        track["symbol"] = dynamic["symbol"]

    # --- Removal check ---
    pushed_at = int(track.get("pushed_at") or now_ts)
    age_seconds = now_ts - pushed_at

    # Rule 1.2: Market cap < 6K at any time → dead
    if 0 < current_mcap < LIVE_TRACK_REMOVE_DEAD_MCAP_USD:
        track["status"] = "removed"
        track["remove_reason"] = f"市值归零 ${current_mcap:,.0f} < ${LIVE_TRACK_REMOVE_DEAD_MCAP_USD:,.0f}"
        _live_track_save(address, track)
        _live_track_remove(address, track["remove_reason"])
        print(f"  [LiveTrack] 移除(归零) ${track.get('symbol', '')} {address[:8]}: mcap=${current_mcap:,.0f}")
        return track

    # Rule 1.1: Market cap < 10K within 30 minutes → too weak
    if age_seconds <= LIVE_TRACK_LOW_MCAP_WINDOW_SEC and 0 < current_mcap < LIVE_TRACK_REMOVE_LOW_MCAP_USD:
        track["status"] = "removed"
        track["remove_reason"] = f"30分钟内市值过低 ${current_mcap:,.0f} < ${LIVE_TRACK_REMOVE_LOW_MCAP_USD:,.0f}"
        _live_track_save(address, track)
        _live_track_remove(address, track["remove_reason"])
        print(f"  [LiveTrack] 移除(低市值) ${track.get('symbol', '')} {address[:8]}: mcap=${current_mcap:,.0f} age={age_seconds}s")
        return track

    _live_track_save(address, track)
    return track


def _live_track_refresh_all() -> list[dict[str, Any]]:
    addresses = _live_track_list_addresses()
    if not addresses:
        return []
    results: list[dict[str, Any]] = []
    max_workers = max(1, min(LIVE_TRACK_MAX_WORKERS, len(addresses)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(_live_track_refresh_one, addr): addr for addr in addresses}
        for future in as_completed(future_map):
            try:
                track = future.result()
                if track:
                    results.append(track)
            except Exception:
                pass
    return _sort_live_track_by_push_time(results)


def _live_track_broadcast(items: list[dict[str, Any]]) -> None:
    client = get_redis_client()
    if client is None or not items:
        return
    try:
        payload = json.dumps(
            {"ts": int(time.time()), "items": _sort_live_track_by_push_time(items), "track_ttl_sec": LIVE_TRACK_REDIS_TTL_SEC},
            ensure_ascii=False,
        )
        client.publish(LIVE_TRACK_PUBSUB_CHANNEL, payload)
    except Exception:
        pass


def _live_track_bg_loop() -> None:
    import threading
    while True:
        try:
            items = _live_track_refresh_all()
            if items:
                _live_track_broadcast(items)
                active = [item for item in items if item.get("status") == "tracking"]
                removed = [item for item in items if item.get("status") == "removed"]
                print(f"  [LiveTrack] 刷新完成: {len(active)}个追踪中, {len(removed)}个已移除")
        except Exception as exc:
            print(f"  [LiveTrack] 后台刷新异常: {exc}")
        time.sleep(LIVE_TRACK_REFRESH_INTERVAL_SEC)


@app.on_event("startup")
def start_live_track_bg():
    global _LIVE_TRACK_BG_STARTED
    if _LIVE_TRACK_BG_STARTED:
        return
    _LIVE_TRACK_BG_STARTED = True
    import threading
    thread = threading.Thread(target=_live_track_bg_loop, daemon=True, name="live_track_bg")
    thread.start()
    print("[LiveTrack] 后台刷新线程已启动")


@app.get("/alpha-live-track", response_class=HTMLResponse)
def alpha_live_track_page(request: Request):
    if BOTTOM_ONLY_FRONTEND:
        return RedirectResponse(url="/bottom-live-track", status_code=302)
    return templates.TemplateResponse(
        request,
        "alpha_live_track.html",
        {"alpha_live_track_ttl_sec": LIVE_TRACK_REDIS_TTL_SEC},
    )


@app.get("/api/alpha-live-track")
def alpha_live_track_api(request: Request):
    addresses = _live_track_list_addresses()
    items = []
    for addr in addresses:
        track = _live_track_load(addr)
        if track:
            items.append(track)
    items = _sort_live_track_by_push_time(items)
    return {
        "items": items,
        "count": len(items),
        "ts": int(time.time()),
        "track_ttl_sec": LIVE_TRACK_REDIS_TTL_SEC,
    }


@app.get("/api/alpha-live-track/deleted-today")
def alpha_live_track_deleted_today_api(request: Request):
    return {"items": _alpha_deleted_today_list()}


@app.get("/api/alpha-live-track/events")
async def alpha_live_track_events(request: Request):
    async def generator():
        client = get_redis_client()
        if client is None:
            yield sse_message("error", {"message": f"redis unavailable: {get_redis_disabled_reason()}"})
            return

        # Send initial snapshot
        addresses = _live_track_list_addresses()
        snapshot = []
        for addr in addresses:
            track = _live_track_load(addr)
            if track:
                snapshot.append(track)
        snapshot = _sort_live_track_by_push_time(snapshot)
        yield sse_message(
            "snapshot",
            {"items": snapshot, "ts": int(time.time()), "track_ttl_sec": LIVE_TRACK_REDIS_TTL_SEC},
        )

        # Subscribe to pubsub for real-time updates
        pubsub = client.pubsub()
        try:
            pubsub.subscribe(LIVE_TRACK_PUBSUB_CHANNEL)
        except Exception as exc:
            yield sse_message("error", {"message": str(exc)})
            return

        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = await asyncio.to_thread(pubsub.get_message, ignore_subscribe_messages=True, timeout=30)
                except Exception as exc:
                    yield sse_message("error", {"message": str(exc)})
                    await asyncio.sleep(2)
                    continue

                if msg is None:
                    yield ": keepalive\n\n"
                    continue

                if msg.get("type") != "message":
                    continue

                data_raw = msg.get("data") or ""
                try:
                    data = json.loads(data_raw) if isinstance(data_raw, (str, bytes)) else {}
                except (json.JSONDecodeError, TypeError):
                    data = {}

                if data.get("items"):
                    yield sse_message("update", data)
        finally:
            try:
                pubsub.unsubscribe(LIVE_TRACK_PUBSUB_CHANNEL)
                pubsub.close()
            except Exception:
                pass

    return StreamingResponse(generator(), media_type="text/event-stream")


# ---------------------------------------------------------------------------
# Bottom live-track: background refresh & endpoints
# ---------------------------------------------------------------------------
def _bottom_live_track_redis_key(address: str) -> str:
    return f"{BOTTOM_LIVE_TRACK_REDIS_PREFIX}:{address}"


def _bottom_live_track_index_key() -> str:
    return f"{BOTTOM_LIVE_TRACK_REDIS_PREFIX}:__index__"


def _bottom_live_track_list_addresses() -> list[str]:
    client = get_redis_client()
    if client is None:
        return []
    try:
        members = client.smembers(_bottom_live_track_index_key())
        return [str(m) for m in members if m]
    except Exception:
        return []


def _bottom_live_track_load(address: str) -> dict[str, Any] | None:
    client = get_redis_client()
    if client is None:
        return None
    try:
        raw = client.get(_bottom_live_track_redis_key(address))
        return json.loads(raw) if raw else None
    except Exception:
        return None


def _bottom_live_track_extra_map(items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    addresses = sorted({
        str((item or {}).get("address") or "").strip()
        for item in items
        if item
        and (
            not item.get("winrate_prediction")
            or not ((item.get("winrate_prediction") or {}).get("strategy_plan"))
            or not (item.get("narrative") or item.get("narrative_desc"))
        )
        and str((item or {}).get("address") or "").strip()
    })
    if not addresses:
        return {}
    address_set = set(addresses)
    result: dict[str, dict[str, Any]] = {}

    for signal in read_recent_plugin_signals(max(500, len(addresses) * 12)):
        extra = signal.get("extra") if isinstance(signal, dict) else {}
        if isinstance(extra, str):
            try:
                extra = json.loads(extra) if extra else {}
            except json.JSONDecodeError:
                extra = {}
        if not isinstance(extra, dict):
            continue
        address = str(extra.get("address") or signal.get("ca") or "").strip()
        if not address or address not in address_set or address in result:
            continue
        result[address] = extra

    def _query(conn):
        cur = conn.cursor()
        cur.execute(
            """
            SELECT DISTINCT ON (address) address, extra
            FROM bottom_top100_push_records
            WHERE address = ANY(%s)
            ORDER BY address, COALESCE(NULLIF(event_ts, 0), EXTRACT(EPOCH FROM pushed_at)::bigint) DESC
            """,
            [addresses],
        )
        rows = cur.fetchall()
        db_result = {}
        for address, extra in rows:
            if isinstance(extra, str):
                try:
                    extra = json.loads(extra) if extra else {}
                except json.JSONDecodeError:
                    extra = {}
            db_result[str(address)] = extra if isinstance(extra, dict) else {}
        return db_result

    try:
        for address, extra in (db_op(_query) or {}).items():
            result.setdefault(address, extra)
        return result
    except Exception:
        return result


def _bottom_live_track_with_prediction(track: dict[str, Any] | None, extra: dict[str, Any] | None = None) -> dict[str, Any] | None:
    if not track:
        return track
    extra = extra or {}
    narrative_desc = (
        track.get("narrative_desc")
        or track.get("narrative")
        or extra.get("narrative_desc")
        or extra.get("narrative")
        or extra.get("watchlist_narrative_desc")
        or extra.get("alpha_abnormal_narrative_desc")
        or ""
    )
    if narrative_desc:
        track["narrative"] = str(narrative_desc)
        track["narrative_desc"] = str(narrative_desc)
    narrative_type = extra.get("narrative_type") or extra.get("watchlist_narrative_type") or extra.get("alpha_abnormal_narrative_type") or ""
    narrative_category = (
        extra.get("narrative_category")
        or extra.get("watchlist_narrative_category")
        or extra.get("alpha_abnormal_narrative_category")
        or ""
    )
    if not track.get("narrative_type") and narrative_type:
        track["narrative_type"] = str(narrative_type)
    if not track.get("narrative_category") and narrative_category:
        track["narrative_category"] = str(narrative_category)
    existing_prediction = track.get("winrate_prediction") or {}
    if existing_prediction and existing_prediction.get("strategy_plan"):
        return track
    try:
        from bottom_detection.bottom_accumulation_monitor import compute_historical_winrate_prediction
    except Exception:
        return track

    merged = {**extra, **track}
    current_mcap = _safe_float(merged.get("current_mcap")) or _safe_float(merged.get("entry_mcap"))
    pool_liquidity = _safe_float(merged.get("pool_liquidity") or merged.get("liquidity") or merged.get("pool_total_liquidity"))
    if current_mcap > 0 and pool_liquidity > 0 and not _safe_float(merged.get("pool_mcap_ratio")):
        merged["pool_mcap_ratio"] = pool_liquidity / current_mcap
    track["winrate_prediction"] = compute_historical_winrate_prediction(merged)
    return track


def _bottom_live_track_attach_predictions(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    extra_by_address = _bottom_live_track_extra_map(items)
    enriched = []
    for item in items:
        address = str((item or {}).get("address") or "").strip()
        current_prediction = (item or {}).get("winrate_prediction") or {}
        had_prediction = bool(current_prediction and current_prediction.get("strategy_plan"))
        had_narrative = bool((item or {}).get("narrative") or (item or {}).get("narrative_desc"))
        next_item = _bottom_live_track_with_prediction(item, extra_by_address.get(address)) or item
        has_new_prediction = not had_prediction and bool(next_item.get("winrate_prediction"))
        has_new_narrative = not had_narrative and bool(next_item.get("narrative") or next_item.get("narrative_desc"))
        if address and (has_new_prediction or has_new_narrative):
            _bottom_live_track_save(address, next_item)
        enriched.append(next_item)
    return enriched


def _bottom_live_track_save(address: str, data: dict[str, Any]) -> None:
    client = get_redis_client()
    if client is None:
        return
    try:
        ttl = client.ttl(_bottom_live_track_redis_key(address))
        if ttl is None or ttl <= 0:
            ttl = BOTTOM_LIVE_TRACK_TTL_SEC
        client.setex(
            _bottom_live_track_redis_key(address),
            ttl,
            json.dumps(data, ensure_ascii=False),
        )
    except Exception:
        pass


def _bottom_live_track_remove(address: str, reason: str = "") -> None:
    client = get_redis_client()
    if client is None:
        return
    try:
        track = _bottom_live_track_load(address)
        if track:
            track["status"] = "removed"
            track["remove_reason"] = reason
            track["last_updated"] = int(time.time())
            client.setex(_bottom_live_track_redis_key(address), 60, json.dumps(track, ensure_ascii=False))
        client.srem(_bottom_live_track_index_key(), address)
    except Exception:
        pass


def _bottom_live_track_sync_5m_kline(address: str, track: dict[str, Any], now_ts: int, pushed_at: int) -> dict[str, Any]:
    if not address or pushed_at <= 0:
        return track
    if BOTTOM_LIVE_TRACK_KLINE_REFRESH_SEC <= 0 or BOTTOM_LIVE_TRACK_KLINE_WINDOW_SEC <= 0:
        return track
    track_age = now_ts - pushed_at
    if track_age < 0 or track_age > BOTTOM_LIVE_TRACK_KLINE_WINDOW_SEC:
        return track
    last_sync_at = _safe_int(track.get("last_5m_kline_sync_at"))
    if last_sync_at > 0 and now_ts - last_sync_at < BOTTOM_LIVE_TRACK_KLINE_REFRESH_SEC:
        return track

    from_ts = max(0, pushed_at - _kline_resolution_seconds("5m"))
    candles = _fetch_binance_kline_range(address, from_ts, now_ts, interval="5min")
    inserted = insert_dashboard_kline_cache_missing_only(address, "5m", candles)
    track["last_5m_kline_sync_at"] = now_ts
    track["last_5m_kline_inserted"] = inserted
    track["last_5m_kline_candles"] = len(candles)
    if candles:
        track["last_5m_kline_ts"] = max(_to_ts(candle.get("ts")) for candle in candles)
    print(
        f"  [BottomLiveTrack] 5m kline sync {address[:8]}: "
        f"candles={len(candles)} inserted={inserted}"
    )
    return track


def _bottom_live_track_refresh_one(address: str) -> dict[str, Any] | None:
    track = _bottom_live_track_load(address)
    if not track or track.get("status") != "tracking":
        return track
    dynamic = _fetch_binance_dynamic(address)
    if not dynamic:
        return track
    now_ts = int(time.time())
    entry_mcap = _safe_float(track.get("entry_mcap"))
    current_mcap = _safe_float(dynamic.get("market_cap"))
    current_price = _safe_float(dynamic.get("price"))
    holders = _safe_int(dynamic.get("holders"))
    volume_5m = _safe_float(dynamic.get("volume_5m"))
    volume_1h = _safe_float(dynamic.get("volume_1h"))
    pool_liquidity = _safe_float(dynamic.get("pool_liquidity")) or _safe_float(track.get("pool_liquidity"))
    prev_peak_mcap = _safe_float(track.get("peak_mcap"))
    peak_mcap = prev_peak_mcap
    peak_mcap_at = _safe_int(track.get("peak_mcap_at"))
    if current_mcap > prev_peak_mcap:
        peak_mcap = current_mcap
        peak_mcap_at = now_ts
        
    pnl_pct = ((current_mcap - entry_mcap) / entry_mcap * 100) if entry_mcap > 0 and current_mcap > 0 else 0.0

    track.update({
        "current_mcap": current_mcap,
        "current_price": current_price,
        "peak_mcap": peak_mcap,
        "peak_mcap_at": peak_mcap_at or int(track.get("pushed_at") or now_ts),
        "pool_liquidity": pool_liquidity,
        "holders": holders,
        "volume_5m": volume_5m,
        "volume_1h": volume_1h,
        "pnl_pct": round(pnl_pct, 2),
        "last_updated": now_ts,
    })
    if dynamic.get("symbol"):
        track["symbol"] = dynamic["symbol"]

    pushed_at = int(track.get("pushed_at") or now_ts)
    age_seconds = now_ts - pushed_at

    if age_seconds >= BOTTOM_LIVE_TRACK_TTL_SEC:
        track["status"] = "removed"
        track["remove_reason"] = f"追踪满 {BOTTOM_LIVE_TRACK_TTL_SEC // 3600} 小时"
        _bottom_live_track_save(address, track)
        _bottom_live_track_remove(address, track["remove_reason"])
        print(f"  [BottomLiveTrack] 移除(到期) ${track.get('symbol', '')} {address[:8]}: age={age_seconds}s")
        return track

    track = _bottom_live_track_sync_5m_kline(address, track, now_ts, pushed_at)

    # Rule 1.2: Market cap < 6K at any time -> dead
    if 0 < current_mcap < BOTTOM_LIVE_TRACK_REMOVE_DEAD_MCAP_USD:
        track["status"] = "removed"
        track["remove_reason"] = f"市值归零 ${current_mcap:,.0f} < ${BOTTOM_LIVE_TRACK_REMOVE_DEAD_MCAP_USD:,.0f}"
        _bottom_live_track_save(address, track)
        _bottom_live_track_remove(address, track["remove_reason"])
        print(f"  [BottomLiveTrack] 移除(归零) ${track.get('symbol', '')} {address[:8]}: mcap=${current_mcap:,.0f}")
        return track

    # Rule 1.1: Market cap < 10K within 30 minutes -> too weak
    if age_seconds <= BOTTOM_LIVE_TRACK_LOW_MCAP_WINDOW_SEC and 0 < current_mcap < BOTTOM_LIVE_TRACK_REMOVE_LOW_MCAP_USD:
        track["status"] = "removed"
        track["remove_reason"] = f"30分钟内市值过低 ${current_mcap:,.0f} < ${BOTTOM_LIVE_TRACK_REMOVE_LOW_MCAP_USD:,.0f}"
        _bottom_live_track_save(address, track)
        _bottom_live_track_remove(address, track["remove_reason"])
        print(f"  [BottomLiveTrack] 移除(低市值) ${track.get('symbol', '')} {address[:8]}: mcap=${current_mcap:,.0f} age={age_seconds}s")
        return track

    _bottom_live_track_save(address, track)
    return track


def _bottom_live_track_refresh_all() -> list[dict[str, Any]]:
    addresses = _bottom_live_track_list_addresses()
    if not addresses:
        return []
    results: list[dict[str, Any]] = []
    max_workers = max(1, min(BOTTOM_LIVE_TRACK_MAX_WORKERS, len(addresses)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(_bottom_live_track_refresh_one, addr): addr for addr in addresses}
        for future in as_completed(future_map):
            try:
                track = future.result()
                if track:
                    results.append(track)
            except Exception:
                pass
    return _sort_live_track_by_push_time(results)


def _bottom_live_track_broadcast(items: list[dict[str, Any]]) -> None:
    client = get_redis_client()
    if client is None or not items:
        return
    try:
        payload = json.dumps(
            {"ts": int(time.time()), "items": _sort_live_track_by_push_time(items), "track_ttl_sec": BOTTOM_LIVE_TRACK_TTL_SEC},
            ensure_ascii=False,
        )
        client.publish(BOTTOM_LIVE_TRACK_PUBSUB_CHANNEL, payload)
    except Exception:
        pass


def _bottom_live_track_bg_loop() -> None:
    while True:
        try:
            items = _bottom_live_track_refresh_all()
            if items:
                _bottom_live_track_broadcast(items)
                active = [item for item in items if item.get("status") == "tracking"]
                removed = [item for item in items if item.get("status") == "removed"]
                print(f"  [BottomLiveTrack] 刷新完成: {len(active)}个追踪中, {len(removed)}个已移除")
        except Exception as exc:
            print(f"  [BottomLiveTrack] 后台刷新异常: {exc}")
        time.sleep(BOTTOM_LIVE_TRACK_REFRESH_INTERVAL_SEC)


@app.on_event("startup")
def start_bottom_live_track_bg():
    if not BOTTOM_LIVE_TRACK_BG_ENABLED:
        print("[BottomLiveTrack] background refresh disabled in dashboard process")
        return
    global _BOTTOM_LIVE_TRACK_BG_STARTED
    if _BOTTOM_LIVE_TRACK_BG_STARTED:
        return
    _BOTTOM_LIVE_TRACK_BG_STARTED = True
    import threading
    thread = threading.Thread(target=_bottom_live_track_bg_loop, daemon=True, name="bottom_live_track_bg")
    thread.start()
    print("[BottomLiveTrack] 后台刷新线程已启动")


@app.get("/bottom-live-track", response_class=HTMLResponse)
def bottom_live_track_page(request: Request):
    return templates.TemplateResponse(
        request,
        "bottom_live_track.html",
        {"bottom_live_track_ttl_sec": BOTTOM_LIVE_TRACK_TTL_SEC},
    )


@app.get("/api/bottom-live-track")
def bottom_live_track_api(request: Request):
    addresses = _bottom_live_track_list_addresses()
    items = []
    for addr in addresses:
        track = _bottom_live_track_load(addr)
        if track:
            items.append(track)
    items = _bottom_live_track_attach_predictions(items)
    items = _sort_live_track_by_push_time(items)
    return {
        "items": items,
        "count": len(items),
        "ts": int(time.time()),
        "track_ttl_sec": BOTTOM_LIVE_TRACK_TTL_SEC,
    }


@app.get("/api/bottom-live-track/{address}/detail")
def bottom_live_track_detail_api(address: str):
    track = _bottom_live_track_load(address)
    if not track:
        raise HTTPException(status_code=404, detail="track not found")

    now_ts = int(time.time())
    pushed_at = _safe_int(track.get("pushed_at")) or max(0, now_ts - BOTTOM_LIVE_TRACK_KLINE_WINDOW_SEC)
    from_ts = max(0, pushed_at - _kline_resolution_seconds("5m"))
    to_ts = now_ts
    candles: list[dict[str, float]] = []
    kline_source = "empty"
    try:
        candles, kline_source = fetch_dashboard_kline_range(
            address,
            from_ts,
            to_ts,
            resolution="5m",
            allow_external=True,
        )
    except Exception as exc:
        kline_source = f"error:{exc.__class__.__name__}"

    return {
        "address": address,
        "kline_source": kline_source,
        "candles": candles[-180:],
        "ts": now_ts,
    }


@app.get("/api/bottom-live-track/events")
async def bottom_live_track_events(request: Request):
    async def generator():
        client = get_redis_client()
        if client is None:
            yield sse_message("error", {"message": f"redis unavailable: {get_redis_disabled_reason()}"})
            return

        addresses = _bottom_live_track_list_addresses()
        snapshot = []
        for addr in addresses:
            track = _bottom_live_track_load(addr)
            if track:
                snapshot.append(track)
        snapshot = _bottom_live_track_attach_predictions(snapshot)
        snapshot = _sort_live_track_by_push_time(snapshot)
        yield sse_message(
            "snapshot",
            {"items": snapshot, "ts": int(time.time()), "track_ttl_sec": BOTTOM_LIVE_TRACK_TTL_SEC},
        )

        pubsub = client.pubsub()
        try:
            pubsub.subscribe(BOTTOM_LIVE_TRACK_PUBSUB_CHANNEL)
        except Exception as exc:
            yield sse_message("error", {"message": str(exc)})
            return

        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = await asyncio.to_thread(pubsub.get_message, ignore_subscribe_messages=True, timeout=30)
                except Exception as exc:
                    yield sse_message("error", {"message": str(exc)})
                    await asyncio.sleep(2)
                    continue

                if msg is None:
                    yield ": keepalive\n\n"
                    continue

                if msg.get("type") != "message":
                    continue

                data_raw = msg.get("data") or ""
                try:
                    data = json.loads(data_raw) if isinstance(data_raw, (str, bytes)) else {}
                except (json.JSONDecodeError, TypeError):
                    data = {}

                if data.get("items"):
                    yield sse_message("update", data)
        finally:
            try:
                pubsub.unsubscribe(BOTTOM_LIVE_TRACK_PUBSUB_CHANNEL)
                pubsub.close()
            except Exception:
                pass

    return StreamingResponse(generator(), media_type="text/event-stream")
