#!/usr/bin/env python3
"""
Ingest one or many Solana token CAs into bottom accumulation monitor tables.

Input formats:
- One CA per line.
- Multiple CAs separated by comma, whitespace, or semicolon.
- Lines may contain labels; valid Solana-looking addresses are extracted.

Run bottom_detection/init_bottom_accumulation_db.py once before this script.
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path
from typing import Any

from psycopg2.extras import Json

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from db_client import db_op
from bottom_accumulation_monitor import (
    CHAIN,
    analyze_accumulation,
    detect_price_setup,
    fetch_and_cache_klines,
    load_recent_candles,
    maybe_refresh_holders,
    save_signal,
    send_tg,
    valid_sol_ca,
)


SOL_CA_RE = re.compile(r"[1-9A-HJ-NP-Za-km-z]{32,44}")


def extract_cas(text: str) -> list[str]:
    seen: set[str] = set()
    addresses: list[str] = []
    for match in SOL_CA_RE.findall(text):
        address = match.strip()
        if address in seen or not valid_sol_ca(address):
            continue
        seen.add(address)
        addresses.append(address)
    return addresses


def read_ca_file(path: Path) -> list[str]:
    return extract_cas(path.read_text(encoding="utf-8"))


def save_manual_ingest_signal(
    address: str,
    latest_bucket_ts: int,
    accumulation: dict[str, Any],
    raw: dict[str, Any],
) -> bool:
    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO bottom_accumulation_signals (
                chain, address, signal_type, window_minutes, signal_bucket_ts,
                accumulation_score, accumulation_pct_delta,
                distribution_pct_delta, rotation_score, netflow_usd,
                top_buyers, raw_analysis
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (chain, address, signal_type, window_minutes, signal_bucket_ts)
            DO NOTHING
            RETURNING id
            """,
            (
                CHAIN,
                address,
                "manual_ingest",
                0,
                latest_bucket_ts,
                accumulation["score"],
                accumulation["accumulation_pct_delta"],
                accumulation["distribution_pct_delta"],
                accumulation["rotation_score"],
                accumulation["netflow_usd"],
                Json(accumulation.get("top_buyers", [])),
                Json(raw),
            ),
        )
        return cur.fetchone() is not None

    return db_op(_op)


def signal_text(address: str, signal: dict[str, Any], accumulation: dict[str, Any]) -> str:
    return (
        "Bottom accumulation file ingest signal\n"
        f"CA: {address}\n"
        f"type: {signal['signal_type']} window={signal['window_minutes']}m\n"
        f"spike: {signal.get('spike_pct', 0):.1%} | "
        f"base_range: {signal.get('base_range_pct', 0):.1%} | "
        f"base_drift: {signal.get('base_drift_pct', 0):.1%}\n"
        f"accum_score: {accumulation['score']} | "
        f"accum_delta: {accumulation['accumulation_pct_delta']:.2%} | "
        f"distribution_delta: {accumulation['distribution_pct_delta']:.2%} | "
        f"netflow: ${accumulation['netflow_usd']:,.0f}\n"
        f"reasons: {', '.join(accumulation['reasons']) or 'baseline ingest'}\n"
        f"https://gmgn.ai/sol/token/{address}"
    )


def ingest_address(address: str, args: argparse.Namespace) -> dict[str, Any]:
    cached_count = fetch_and_cache_klines(address, args.backfill_hours)
    snapshot_id = maybe_refresh_holders(address, args.holder_refresh_sec)
    candles = load_recent_candles(address, hours=max(12, args.backfill_hours))
    latest_bucket_ts = candles[-1].ts if candles else int(time.time())
    price_signals = detect_price_setup(candles)
    accumulation = analyze_accumulation(address)

    inserted_signals = 0
    for price_signal in price_signals:
        if save_signal(address, price_signal, accumulation):
            inserted_signals += 1
            if args.notify:
                send_tg(signal_text(address, price_signal, accumulation))

    inserted_baseline = False
    if args.record_baseline:
        inserted_baseline = save_manual_ingest_signal(
            address,
            latest_bucket_ts,
            accumulation,
            {
                "source": "file_ingest",
                "cached_klines": cached_count,
                "holder_snapshot_id": snapshot_id,
                "price_signal_count": len(price_signals),
                "accumulation": accumulation,
            },
        )

    return {
        "address": address,
        "cached_klines": cached_count,
        "holder_snapshot_id": snapshot_id,
        "candles": len(candles),
        "price_signals": len(price_signals),
        "inserted_price_signals": inserted_signals,
        "inserted_baseline": inserted_baseline,
        "accumulation_score": accumulation["score"],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Import CA file into bottom accumulation tables.")
    parser.add_argument("file", nargs="?", help="Text file containing one or many CAs.")
    parser.add_argument("--ca", action="append", default=[], help="Direct CA input. Can be repeated or comma-separated.")
    parser.add_argument("--backfill-hours", type=int, default=48, help="Initial kline history range.")
    parser.add_argument("--holder-refresh-sec", type=int, default=0, help="0 forces a fresh holder snapshot.")
    parser.add_argument("--delay", type=float, default=0.25, help="Delay between CAs to reduce rate-limit risk.")
    parser.add_argument("--limit", type=int, default=0, help="Max CAs to process. 0 means no limit.")
    parser.add_argument("--notify", action="store_true", help="Send Telegram notifications for detected price signals.")
    parser.add_argument(
        "--no-baseline",
        dest="record_baseline",
        action="store_false",
        help="Do not insert manual_ingest rows into bottom_accumulation_signals.",
    )
    parser.set_defaults(record_baseline=True)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    source_text = ""
    if args.file:
        source_text += Path(args.file).read_text(encoding="utf-8")
    if args.ca:
        source_text += "\n" + "\n".join(args.ca)

    addresses = extract_cas(source_text)
    if args.limit > 0:
        addresses = addresses[: args.limit]
    if not addresses:
        raise SystemExit("No valid Solana CA found in input.")

    print(f"found {len(addresses)} CA(s)")
    for index, address in enumerate(addresses, start=1):
        try:
            result = ingest_address(address, args)
            print(
                f"[{index}/{len(addresses)}] {address} "
                f"klines={result['cached_klines']} candles={result['candles']} "
                f"snapshot={result['holder_snapshot_id']} "
                f"signals={result['inserted_price_signals']}/{result['price_signals']} "
                f"baseline={result['inserted_baseline']} "
                f"score={result['accumulation_score']}"
            )
        except Exception as exc:
            print(f"[{index}/{len(addresses)}] {address} failed: {exc}")
        time.sleep(args.delay)


if __name__ == "__main__":
    main()
