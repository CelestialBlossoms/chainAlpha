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
from plugin_signal_stream import read_recent_plugin_signals
from db_client import db_op
from ca_analyzer.cluster_api import analyze_ca_clusters


BASE_DIR = Path(__file__).resolve().parent
SSE_BLOCK_MS = int(os.getenv("TG_DASHBOARD_SSE_BLOCK_MS", "30000"))

app = FastAPI(title="Chain Alpha TG Dashboard")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["chrome-extension://*", "http://localhost:*", "http://127.0.0.1:*"],
    allow_origin_regex=r"chrome-extension://.*|http://(localhost|127\.0\.0\.1)(:\d+)?",
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


def sse_message(event: str, data: dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


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
                ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ;
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
                source
            FROM bottom_watchlist_tokens
            WHERE ca IS NOT NULL
            ORDER BY GREATEST(
                COALESCE(highest_mcap, 0),
                COALESCE(ath_mcap, 0),
                COALESCE(peak_mcap, 0),
                COALESCE(current_mcap, 0),
                COALESCE(last_mcap, 0)
            ) DESC, last_seen_at DESC NULLS LAST
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
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "stream_key": TG_ALERT_STREAM_KEY,
        },
    )


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


@app.get("/api/plugin/new-1m")
def plugin_new_1m(request: Request, limit: int = 100):
    limit = max(1, min(limit, 500))
    items = [
        normalize_alert(item.get("id", ""), item)
        for item in read_recent_plugin_signals(limit)
        if item.get("source") == "plugin_new_1m"
    ]
    return {"items": items}


@app.get("/api/plugin/bottom-abnormal")
def plugin_bottom_abnormal(request: Request, limit: int = 100):
    limit = max(1, min(limit, 500))
    history_limit = min(500, max(limit, limit * 5))
    items = [
        normalize_alert(item.get("id", ""), item)
        for item in read_recent_plugin_signals(history_limit)
        if item.get("source") == "bottom_abnormal"
    ]
    return {"items": enrich_bottom_abnormal_history(items)[-limit:]}


@app.get("/api/plugin/health")
def plugin_health(request: Request, limit: int = 20):
    limit = max(1, min(limit, 100))
    client = get_redis_client()
    recent_items = [normalize_alert(item.get("id", ""), item) for item in read_recent_plugin_signals(limit)]
    new_1m_count = sum(1 for item in recent_items if item.get("source") == "plugin_new_1m")
    bottom_abnormal_count = sum(1 for item in recent_items if item.get("source") == "bottom_abnormal")
    return {
        "ok": client is not None,
        "redis_ok": client is not None,
        "redis_error": "" if client is not None else get_redis_disabled_reason(),
        "recent_plugin_count": len(recent_items),
        "plugin_new_1m_count": new_1m_count,
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
                    last_seen_at = now()
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
