#!/usr/bin/env python3
"""Hourly Deep Alpha K-line cache worker.

Writes post-push K-lines into deep_alpha_cache_5m or deep_alpha_cache_1m.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from db_client import db_op
from deep_alpha import deep_alpha_pro as dap
from redis_client import get_redis_client


@dataclass(frozen=True)
class KlineTarget:
    address: str
    chain: str
    pushed_at: int
    source: str


def _safe_int(value: Any) -> int:
    try:
        if value in (None, ""):
            return 0
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _resolution_seconds(resolution: str) -> int:
    return 60 if str(resolution or "").lower() in {"1", "1m", "1min"} else 300


def _normalize_resolution(resolution: str) -> str:
    return "1m" if str(resolution or "").lower() in {"1", "1m", "1min"} else "5m"


def _target_from_item(item: dict[str, Any], source: str, now_ts: int) -> KlineTarget | None:
    address = str(item.get("address") or item.get("token_address") or "").strip()
    if not address:
        return None
    pushed_at = _safe_int(
        item.get("pushed_at")
        or item.get("smart_signal_trigger_at")
        or item.get("market_signal_trigger_at")
        or item.get("trigger_at")
    )
    if pushed_at <= 0:
        pushed_at = now_ts
    return KlineTarget(
        address=address,
        chain=str(item.get("chain") or "sol"),
        pushed_at=pushed_at,
        source=source,
    )


def _load_db_targets(history_hours: float, limit: int) -> list[KlineTarget]:
    since_sec = int(max(0, history_hours) * 3600)

    def _op(conn):
        cur = conn.cursor()
        cur.execute("SELECT to_regclass('public.alpha_push_events')")
        if not cur.fetchone()[0]:
            return []
        params: list[Any] = []
        where = ["address IS NOT NULL", "address <> ''"]
        if since_sec > 0:
            where.append("pushed_at >= NOW() - (%s * INTERVAL '1 second')")
            params.append(since_sec)
        params.append(max(1, limit))
        cur.execute(
            f"""
            SELECT address, COALESCE(chain, 'sol'), extract(epoch from pushed_at)::bigint
            FROM alpha_push_events
            WHERE {' AND '.join(where)}
            ORDER BY pushed_at DESC, id DESC
            LIMIT %s
            """,
            params,
        )
        return [
            KlineTarget(address=str(row[0]), chain=str(row[1] or "sol"), pushed_at=int(row[2] or 0), source="alpha_push_events")
            for row in cur.fetchall()
            if row[0] and row[2]
        ]

    return db_op(_op) or []


def _load_redis_targets(now_ts: int) -> list[KlineTarget]:
    client = get_redis_client()
    if client is None:
        return []
    targets: list[KlineTarget] = []
    try:
        for raw_addr in client.smembers(dap.live_track_index_key()):
            address = str(raw_addr.decode() if isinstance(raw_addr, bytes) else raw_addr).strip()
            if not address:
                continue
            raw = client.get(dap.live_track_redis_key(address))
            if not raw:
                continue
            item = json.loads(raw)
            target = _target_from_item(item if isinstance(item, dict) else {}, "live_track", now_ts)
            if target:
                targets.append(target)
    except Exception as exc:
        print(f"[DeepAlphaKlineCache] live-track target load failed: {exc}")

    for source, key_fn in (
        ("smart_money_signal", dap.smart_signal_redis_key),
        ("market_signal", dap.market_signal_redis_key),
    ):
        try:
            raw = client.get(key_fn("sol"))
            if not raw:
                continue
            payload = json.loads(raw)
            items = payload.get("items") if isinstance(payload, dict) else payload
            for item in items if isinstance(items, list) else []:
                if not isinstance(item, dict):
                    continue
                target = _target_from_item(item, source, now_ts)
                if target:
                    targets.append(target)
        except Exception as exc:
            print(f"[DeepAlphaKlineCache] {source} target load failed: {exc}")
    return targets


def collect_targets(history_hours: float, limit: int) -> list[KlineTarget]:
    now_ts = int(time.time())
    by_address: dict[str, KlineTarget] = {}
    for target in [*_load_db_targets(history_hours, limit), *_load_redis_targets(now_ts)]:
        if not target.address or target.pushed_at <= 0:
            continue
        existing = by_address.get(target.address)
        if existing is None or target.pushed_at < existing.pushed_at:
            by_address[target.address] = target
    return sorted(by_address.values(), key=lambda item: item.pushed_at, reverse=True)[: max(1, limit)]


def sync_target(
    target: KlineTarget,
    resolution: str,
    window_hours: float,
    overlap_bars: int,
) -> dict[str, Any]:
    now_ts = int(time.time())
    step = _resolution_seconds(resolution)
    window_sec = int(max(0, window_hours) * 3600)
    end_ts = min(now_ts, target.pushed_at + window_sec) if window_sec > 0 else now_ts
    if end_ts <= target.pushed_at:
        return {"address": target.address, "status": "skip_future"}

    latest_before = dap.latest_deep_alpha_kline_ts(target.address, resolution, chain=target.chain)
    if latest_before > 0 and latest_before >= end_ts - step and end_ts < now_ts:
        return {"address": target.address, "status": "skip_complete", "latest_ts": latest_before}

    from_ts = max(0, target.pushed_at - step)
    if latest_before > 0:
        from_ts = max(from_ts, latest_before - max(0, overlap_bars) * step)

    candles = dap.fetch_kline_range(
        target.address,
        from_ts,
        end_ts,
        resolution=resolution,
        chain=target.chain,
        use_cache=True,
    )
    latest_after = dap.latest_deep_alpha_kline_ts(target.address, resolution, chain=target.chain)
    return {
        "address": target.address,
        "source": target.source,
        "status": "ok",
        "candles": len(candles),
        "from_ts": from_ts,
        "to_ts": end_ts,
        "latest_before": latest_before,
        "latest_after": latest_after,
    }


def run_once(args: argparse.Namespace) -> None:
    resolution = _normalize_resolution(args.resolution)
    dap.ensure_deep_alpha_kline_cache_tables()
    targets = collect_targets(args.history_hours, args.limit)
    print(
        f"[DeepAlphaKlineCache] resolution={resolution} targets={len(targets)} "
        f"history_hours={args.history_hours:g} window_hours={args.window_hours:g}"
    )
    ok = 0
    skipped = 0
    for target in targets:
        try:
            result = sync_target(target, resolution, args.window_hours, args.overlap_bars)
            if result.get("status") == "ok":
                ok += 1
            else:
                skipped += 1
            print(
                f"[DeepAlphaKlineCache] {resolution} {target.address[:8]} "
                f"{result.get('status')} candles={result.get('candles', 0)} "
                f"latest={result.get('latest_after') or result.get('latest_ts') or 0}"
            )
        except Exception as exc:
            print(f"[DeepAlphaKlineCache] {resolution} {target.address[:8]} failed: {exc}")
        if args.delay > 0:
            time.sleep(args.delay)
    print(f"[DeepAlphaKlineCache] resolution={resolution} done ok={ok} skipped={skipped}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync Deep Alpha post-push K-lines into deep_alpha_cache tables.")
    parser.add_argument("--resolution", default=os.getenv("DEEP_ALPHA_KLINE_CACHE_RESOLUTION", "5m"))
    parser.add_argument("--interval-sec", type=int, default=int(os.getenv("DEEP_ALPHA_KLINE_CACHE_INTERVAL_SEC", "3600")))
    parser.add_argument("--window-hours", type=float, default=float(os.getenv("DEEP_ALPHA_KLINE_CACHE_WINDOW_HOURS", "24")))
    parser.add_argument("--history-hours", type=float, default=float(os.getenv("DEEP_ALPHA_KLINE_CACHE_HISTORY_HOURS", "48")))
    parser.add_argument("--limit", type=int, default=int(os.getenv("DEEP_ALPHA_KLINE_CACHE_LIMIT", "500")))
    parser.add_argument("--overlap-bars", type=int, default=int(os.getenv("DEEP_ALPHA_KLINE_CACHE_OVERLAP_BARS", "2")))
    parser.add_argument("--delay", type=float, default=float(os.getenv("DEEP_ALPHA_KLINE_CACHE_DELAY_SEC", "0.1")))
    parser.add_argument("--once", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.once:
        run_once(args)
        return
    while True:
        started = time.time()
        run_once(args)
        sleep_for = max(1, int(args.interval_sec - (time.time() - started)))
        print(f"[DeepAlphaKlineCache] sleep {sleep_for}s")
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()
