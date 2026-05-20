#!/usr/bin/env python3
"""
Lightweight dashboard for Telegram alert stream.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
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
    return templates.TemplateResponse(request, "onchain_guides.html", {})


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
