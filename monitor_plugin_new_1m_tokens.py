#!/usr/bin/env python3
"""
Chrome-extension-only 1m new-token monitor.

The detection path intentionally reuses bottom_accumulation_monitor's 1m
snapshot/anomaly logic. Only the output sink is different: plugin stream only,
no Telegram and no dashboard/frontend alert stream.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

os.environ.setdefault("BOTTOM_TREND_INTERVALS", "1m")
os.environ.setdefault("BOTTOM_TREND_ORDER_BYS", "default")
os.environ.setdefault("BOTTOM_SCAN_INTERVAL", "60")
os.environ.setdefault("BOTTOM_NEW_TOKEN_KLINE_RESOLUTION", "1m")
os.environ.setdefault("BOTTOM_NEW_TOKEN_SNAPSHOT_INTERVAL_SEC", "60")

from plugin_signal_stream import publish_plugin_signal
from bottom_detection.bottom_accumulation_monitor import (
    CHAIN,
    analyze_abnormal_snapshot,
    attach_token_pool,
    build_bottom_signal_extra,
    build_snapshot_json,
    check_quiet_runup,
    check_watchlist_quiet_breakout,
    fetch_kline,
    fetch_token_metadata,
    fetch_token_pool,
    fetch_top100_holders,
    fetch_trending_tokens,
    first_signal_baseline,
    merge_token_metadata,
    now_ts,
    previous_signal_exists,
    recent_snapshots,
    save_snapshot,
    should_notify,
    token_address,
    token_age_sec,
    token_kline_resolution,
    token_label,
    valid_sol_ca,
)


PLUGIN_NEW_1M_INTERVAL_SEC = int(os.getenv("PLUGIN_NEW_1M_INTERVAL_SEC", "60"))
PLUGIN_NEW_1M_TREND_LIMIT = int(os.getenv("PLUGIN_NEW_1M_TREND_LIMIT", "100"))
PLUGIN_NEW_1M_MAX_TOKENS = int(os.getenv("PLUGIN_NEW_1M_MAX_TOKENS", "80"))
PLUGIN_NEW_1M_MAX_AGE_SEC = int(os.getenv("PLUGIN_NEW_1M_MAX_AGE_SEC", str(48 * 3600)))
PLUGIN_NEW_1M_DEDUP_SEC = int(os.getenv("PLUGIN_NEW_1M_DEDUP_SEC", "900"))
PLUGIN_NEW_1M_TOKEN_DELAY = float(os.getenv("PLUGIN_NEW_1M_TOKEN_DELAY", "0.25"))

_LAST_SENT: dict[str, int] = {}


def fetch_1m_trending(limit: int) -> list[dict[str, Any]]:
    return fetch_trending_tokens()[:limit]


def enrich_token(token: dict[str, Any]) -> dict[str, Any]:
    address = token_address(token)
    info, security = fetch_token_metadata(address)
    merged = merge_token_metadata(token, info, security)
    pool_data = fetch_token_pool(address)
    return attach_token_pool(merged, pool_data)


def new_token_skip_reason(token: dict[str, Any]) -> str | None:
    age = token_age_sec(token)
    if age <= 0:
        return "missing_created_at"
    if age > PLUGIN_NEW_1M_MAX_AGE_SEC:
        return f"age>{PLUGIN_NEW_1M_MAX_AGE_SEC}s"
    return None


def publish_plugin_token(token: dict[str, Any], summary: dict[str, Any], analysis: dict[str, Any]) -> bool:
    address = token_address(token)
    signal_type = str(analysis.get("signal_type") or "")
    if not signal_type or signal_type == "watch":
        return False

    ts = now_ts()
    dedup_key = f"{address}:{signal_type}"
    if ts - _LAST_SENT.get(dedup_key, 0) < PLUGIN_NEW_1M_DEDUP_SEC:
        return False
    _LAST_SENT[dedup_key] = ts

    baseline = first_signal_baseline(address, signal_type)
    extra = build_bottom_signal_extra(token, summary, analysis, baseline)
    extra["source_type"] = "plugin_new_1m"
    extra["plugin_only"] = True
    extra["resolution"] = summary.get("kline", {}).get("resolution") or "1m"
    title = (
        f"1m new token ${extra.get('symbol') or 'UNKNOWN'} {address[:8]} "
        f"type={signal_type} mcap=${extra.get('current_mcap', 0):,.0f} "
        f"change={extra.get('price_change_pct', 0):.1f}%"
    )
    return bool(publish_plugin_signal(title, "plugin_new_1m", ca=address, extra=extra))


def handle_token_plugin_only(scan_id: str, token: dict[str, Any]) -> bool:
    address = token_address(token)
    raw_holders = fetch_top100_holders(address)
    if not raw_holders:
        print(f"{token_label(token)} no holders")
        return False

    kline_resolution = token_kline_resolution(token)
    candles = fetch_kline(address, kline_resolution, token)
    summary, holders = build_snapshot_json(token, raw_holders, candles, kline_resolution)
    history = recent_snapshots(address)
    analysis = analyze_abnormal_snapshot(holders, history, summary)
    snapshot_id = save_snapshot(scan_id, token, summary, holders, analysis)
    print(
        f"{token_label(token)} plugin snapshot={snapshot_id} "
        f"type={analysis.get('signal_type')} mcap=${analysis.get('current_mcap', 0):,.0f} "
        f"price={analysis.get('price_change_pct', 0):.1f}%"
    )

    published = False
    if should_notify(analysis):
        published = publish_plugin_token(token, summary, analysis) or published

    quiet_breakout = check_watchlist_quiet_breakout(token, summary, candles)
    if quiet_breakout and not previous_signal_exists(address, quiet_breakout.get("signal_type", "")):
        save_snapshot(scan_id + "_quiet", token, summary, holders, quiet_breakout)
        published = publish_plugin_token(token, summary, quiet_breakout) or published

    quiet_runup = check_quiet_runup(token, summary, candles)
    if quiet_runup and not previous_signal_exists(address, quiet_runup.get("signal_type", "")):
        save_snapshot(scan_id + "_runup", token, summary, holders, quiet_runup)
        published = publish_plugin_token(token, summary, quiet_runup) or published

    return published


def scan_once(args: argparse.Namespace) -> None:
    scan_id = f"plugin_new_1m_{now_ts()}"
    tokens = fetch_1m_trending(args.limit)
    if args.max_tokens > 0:
        tokens = tokens[: args.max_tokens]
    print(f"plugin new 1m scan: trending={len(tokens)}")
    for token in tokens:
        address = token_address(token)
        if not valid_sol_ca(address):
            continue
        try:
            enriched = enrich_token(token)
            skip_reason = new_token_skip_reason(enriched)
            if skip_reason:
                print(f"{token_label(enriched)} plugin_new_1m skip reason={skip_reason}")
                continue
            if handle_token_plugin_only(scan_id, enriched):
                print(f"{token_label(enriched)} plugin_new_1m published")
            else:
                print(f"{token_label(enriched)} plugin_new_1m no plugin signal")
        except Exception as exc:
            print(f"{address[:8]} plugin_new_1m failed: {exc}")
        if args.token_delay > 0:
            time.sleep(args.token_delay)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Monitor GMGN 1m new-token movement for Chrome extension only.")
    parser.add_argument("--watch", action="store_true", help="Run forever.")
    parser.add_argument("--once", action="store_true", help="Run one scan and exit.")
    parser.add_argument("--interval", type=int, default=PLUGIN_NEW_1M_INTERVAL_SEC, help="Loop interval seconds.")
    parser.add_argument("--limit", type=int, default=PLUGIN_NEW_1M_TREND_LIMIT, help="1m trending limit.")
    parser.add_argument("--max-tokens", type=int, default=PLUGIN_NEW_1M_MAX_TOKENS, help="Max tokens to enrich per scan; 0 means all.")
    parser.add_argument("--token-delay", type=float, default=PLUGIN_NEW_1M_TOKEN_DELAY, help="Delay between token enrich calls.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    while True:
        scan_once(args)
        if args.once or not args.watch:
            break
        time.sleep(max(10, args.interval))


if __name__ == "__main__":
    main()
