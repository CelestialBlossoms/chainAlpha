#!/usr/bin/env python3
"""
Redis Stream helpers for Telegram alert messages.
"""

from __future__ import annotations

import json
import os
import re
import time
from typing import Any

from redis_client import get_redis_client


TG_ALERT_STREAM_KEY = os.getenv("TG_ALERT_STREAM_KEY", "chain_alpha:tg_alerts")
TG_ALERT_STREAM_MAXLEN = int(os.getenv("TG_ALERT_STREAM_MAXLEN", "1000"))
TG_ALERT_STREAM_MAX_AGE_SEC = int(os.getenv("TG_ALERT_STREAM_MAX_AGE_SEC", str(60 * 60)))

CA_RE = re.compile(r"[1-9A-HJ-NP-Za-km-z]{32,50}")


def extract_ca(text: str) -> str:
    match = CA_RE.search(text or "")
    return match.group(0) if match else ""


def compact_title(text: str) -> str:
    first_line = (text or "").strip().splitlines()[0] if (text or "").strip() else ""
    return first_line[:160]


def publish_tg_alert(
    text: str,
    source: str,
    *,
    status: str = "sent",
    ca: str | None = None,
    message_id: int | str | None = None,
    chat_id: int | str | None = None,
    extra: dict[str, Any] | None = None,
) -> str | None:
    client = get_redis_client()
    if client is None:
        return None
    payload = {
        "ts": str(int(time.time())),
        "source": source,
        "status": status,
        "ca": ca or extract_ca(text),
        "title": compact_title(text),
        "text": text or "",
        "message_id": str(message_id or ""),
        "chat_id": str(chat_id or ""),
        "extra": json.dumps(extra or {}, ensure_ascii=False, default=str),
    }
    try:
        stream_id = client.xadd(TG_ALERT_STREAM_KEY, payload, maxlen=TG_ALERT_STREAM_MAXLEN, approximate=True)
        trim_old_tg_alerts(client)
        if TG_ALERT_STREAM_MAX_AGE_SEC > 0:
            client.expire(TG_ALERT_STREAM_KEY, TG_ALERT_STREAM_MAX_AGE_SEC * 2)
        return stream_id
    except Exception as exc:
        print(f"[tg_stream] publish failed: {exc}")
        return None


def trim_old_tg_alerts(client=None) -> None:
    if TG_ALERT_STREAM_MAX_AGE_SEC <= 0:
        return
    client = client or get_redis_client()
    if client is None:
        return
    min_id = f"{max(0, int((time.time() - TG_ALERT_STREAM_MAX_AGE_SEC) * 1000))}-0"
    try:
        client.xtrim(TG_ALERT_STREAM_KEY, minid=min_id, approximate=True)
    except TypeError:
        client.execute_command("XTRIM", TG_ALERT_STREAM_KEY, "MINID", "~", min_id)
    except Exception as exc:
        print(f"[tg_stream] trim old failed: {exc}")


def read_recent_tg_alerts(count: int = 100) -> list[dict[str, Any]]:
    client = get_redis_client()
    if client is None:
        return []
    try:
        trim_old_tg_alerts(client)
        rows = client.xrevrange(TG_ALERT_STREAM_KEY, count=count)
    except Exception as exc:
        print(f"[tg_stream] read recent failed: {exc}")
        return []
    items = []
    min_ts = int(time.time()) - TG_ALERT_STREAM_MAX_AGE_SEC if TG_ALERT_STREAM_MAX_AGE_SEC > 0 else 0
    for stream_id, fields in reversed(rows):
        item = dict(fields)
        if min_ts > 0:
            try:
                if int(float(item.get("ts") or 0)) < min_ts:
                    continue
            except (TypeError, ValueError):
                continue
        item["id"] = stream_id
        items.append(item)
    return items
