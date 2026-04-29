#!/usr/bin/env python3
"""
Lightweight dashboard for Telegram alert stream.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from redis_client import get_redis_client, get_redis_disabled_reason
from tg_alert_stream import TG_ALERT_STREAM_KEY, read_recent_tg_alerts


BASE_DIR = Path(__file__).resolve().parent
SSE_BLOCK_MS = int(os.getenv("TG_DASHBOARD_SSE_BLOCK_MS", "30000"))

app = FastAPI(title="Chain Alpha TG Dashboard")
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


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "stream_key": TG_ALERT_STREAM_KEY,
        },
    )


@app.get("/api/recent")
def recent(limit: int = 100):
    limit = max(1, min(limit, 500))
    return {"items": [normalize_alert(item.get("id", ""), item) for item in read_recent_tg_alerts(limit)]}


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
