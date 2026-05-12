#!/usr/bin/env python3
"""
Lightweight dashboard for Telegram alert stream.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
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
        "auth_enabled": False,
    }


@app.get("/bottom-watchlist", response_class=HTMLResponse)
def bottom_watchlist(request: Request):
    return templates.TemplateResponse(request, "bottom_watchlist.html", {})


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


@app.get("/api/plugin/health")
def plugin_health(request: Request, limit: int = 20):
    limit = max(1, min(limit, 100))
    client = get_redis_client()
    recent_items = [normalize_alert(item.get("id", ""), item) for item in read_recent_plugin_signals(limit)]
    new_1m_count = sum(1 for item in recent_items if item.get("source") == "plugin_new_1m")
    return {
        "ok": client is not None,
        "redis_ok": client is not None,
        "redis_error": "" if client is not None else get_redis_disabled_reason(),
        "auth_enabled": False,
        "recent_plugin_count": len(recent_items),
        "plugin_new_1m_count": new_1m_count,
        "items": recent_items,
    }


@app.get("/api/bottom-watchlist")
def bottom_watchlist_api(request: Request, limit: int = 500):
    limit = max(1, min(limit, 2000))
    return {"items": fetch_bottom_watchlist(limit)}


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
