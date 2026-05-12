#!/usr/bin/env python3
"""
Redis Stream helpers for Chrome-extension-only signals.
"""

from __future__ import annotations

import json
import os
import time
from typing import Any

from redis_client import get_redis_client


PLUGIN_SIGNAL_STREAM_KEY = os.getenv("PLUGIN_SIGNAL_STREAM_KEY", "chain_alpha:plugin_signals")
PLUGIN_SIGNAL_STREAM_MAXLEN = int(os.getenv("PLUGIN_SIGNAL_STREAM_MAXLEN", "1000"))
PLUGIN_SIGNAL_STREAM_MAX_AGE_SEC = int(os.getenv("PLUGIN_SIGNAL_STREAM_MAX_AGE_SEC", str(60 * 60)))


def publish_plugin_signal(
    title: str,
    source: str,
    *,
    ca: str = "",
    status: str = "signal",
    extra: dict[str, Any] | None = None,
) -> str | None:
    client = get_redis_client()
    if client is None:
        return None
    payload = {
        "ts": str(int(time.time())),
        "source": source,
        "status": status,
        "ca": ca,
        "title": (title or "")[:160],
        "text": title or "",
        "extra": json.dumps(extra or {}, ensure_ascii=False, default=str),
    }
    try:
        stream_id = client.xadd(PLUGIN_SIGNAL_STREAM_KEY, payload, maxlen=PLUGIN_SIGNAL_STREAM_MAXLEN, approximate=True)
        trim_old_plugin_signals(client)
        if PLUGIN_SIGNAL_STREAM_MAX_AGE_SEC > 0:
            client.expire(PLUGIN_SIGNAL_STREAM_KEY, PLUGIN_SIGNAL_STREAM_MAX_AGE_SEC * 2)
        return stream_id
    except Exception as exc:
        print(f"[plugin_stream] publish failed: {exc}")
        return None


def trim_old_plugin_signals(client=None) -> None:
    if PLUGIN_SIGNAL_STREAM_MAX_AGE_SEC <= 0:
        return
    client = client or get_redis_client()
    if client is None:
        return
    min_id = f"{max(0, int((time.time() - PLUGIN_SIGNAL_STREAM_MAX_AGE_SEC) * 1000))}-0"
    try:
        client.xtrim(PLUGIN_SIGNAL_STREAM_KEY, minid=min_id, approximate=True)
    except TypeError:
        client.execute_command("XTRIM", PLUGIN_SIGNAL_STREAM_KEY, "MINID", "~", min_id)
    except Exception as exc:
        print(f"[plugin_stream] trim old failed: {exc}")


def read_recent_plugin_signals(count: int = 100) -> list[dict[str, Any]]:
    client = get_redis_client()
    if client is None:
        return []
    try:
        trim_old_plugin_signals(client)
        rows = client.xrevrange(PLUGIN_SIGNAL_STREAM_KEY, count=count)
    except Exception as exc:
        print(f"[plugin_stream] read recent failed: {exc}")
        return []
    items = []
    min_ts = int(time.time()) - PLUGIN_SIGNAL_STREAM_MAX_AGE_SEC if PLUGIN_SIGNAL_STREAM_MAX_AGE_SEC > 0 else 0
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
