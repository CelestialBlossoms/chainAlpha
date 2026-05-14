#!/usr/bin/env python3
"""
Query bottom_watchlist_tokens where current_mcap = 0,
fetch real market cap via gmgn-cli, and update the DB.
"""
from __future__ import annotations

import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from db_client import db_op


# ---------------------------------------------------------------------------
# gmgn-cli helpers
# ---------------------------------------------------------------------------

def _gmgn_exe() -> list:
    exe = shutil.which("gmgn-cli") or shutil.which("gmgn-cli.cmd") or "gmgn-cli"
    return [exe]


def run_gmgn(args_list: list, timeout: int = 45) -> dict:
    cmd = _gmgn_exe() + args_list + ["--raw"]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8",
                           errors="replace", timeout=timeout)
    except subprocess.TimeoutExpired:
        print(f"  [TIMEOUT] gmgn-cli {' '.join(args_list[:4])}")
        return {}
    if r.returncode != 0:
        stderr = r.stderr.strip()
        if "429" in stderr or "RATE_LIMIT" in stderr:
            print(f"  [RATE LIMITED] {stderr[:200]}")
        elif stderr:
            print(f"  [ERROR] {stderr[:300]}")
        return {}
    try:
        return json.loads(r.stdout)
    except json.JSONDecodeError:
        print(f"  [JSON ERROR] {r.stdout[:200]}")
        return {}


def to_f(v, default=0.0):
    try:
        if v in (None, ""):
            return default
        return float(v)
    except (ValueError, TypeError):
        return default


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------

def get_zero_mcap_tokens() -> list[dict]:
    """Fetch tokens from bottom_watchlist_tokens where current_mcap = 0."""
    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            SELECT ca, symbol, source, peak_mcap, last_mcap,
                   COALESCE(blacklisted, false)
            FROM bottom_watchlist_tokens
            WHERE current_mcap = 0
              AND ca IS NOT NULL
            ORDER BY added_at DESC NULLS LAST
            """
        )
        return [
            {
                "ca": row[0], "symbol": row[1] or "", "source": row[2],
                "peak_mcap": float(row[3] or 0), "last_mcap": float(row[4] or 0),
                "blacklisted": bool(row[5]),
            }
            for row in cur.fetchall()
        ]
    return db_op(_op)


def update_token_mcap(address: str, mcap: float, price: float, liquidity: float, symbol: str = "") -> None:
    """Update current_mcap and related fields for a watchlist token."""
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
                symbol = CASE WHEN %s != '' AND symbol IS NULL THEN %s ELSE COALESCE(symbol, '') END,
                last_seen_at = now()
            WHERE ca = %s
            """,
            (mcap, mcap, mcap, mcap, liquidity, liquidity, symbol, symbol, address),
        )
        return cur.rowcount
    rows = db_op(_op)
    return rows


def main():
    print("=" * 60)
    print("Fixing zero market cap tokens in bottom_watchlist_tokens")
    print("=" * 60)

    tokens = get_zero_mcap_tokens()
    print(f"\nFound {len(tokens)} tokens with current_mcap = 0\n")

    if not tokens:
        print("Nothing to fix.")
        return

    updated = 0
    failed = 0
    skipped = 0

    for i, t in enumerate(tokens):
        ca = t["ca"]
        sym = t["symbol"]
        print(f"[{i+1}/{len(tokens)}] {sym or ca[:12]}... ({ca[:16]}...)")

        if t["blacklisted"]:
            print(f"  -> SKIP (blacklisted)")
            skipped += 1
            continue

        info = run_gmgn(["token", "info", "--chain", "sol", "--address", ca])

        if not info:
            print(f"  -> FAILED (no response)")
            failed += 1
            time.sleep(1)
            continue

        price = to_f(info.get("price"))
        supply = to_f(info.get("circulating_supply"))
        mcap = price * supply
        liq = to_f(info.get("liquidity"))
        new_symbol = info.get("symbol", "") or ""

        if mcap <= 0:
            print(f"  -> SKIP (mcap still 0: price={price}, supply={supply})")
            skipped += 1
            time.sleep(0.5)
            continue

        rows = update_token_mcap(ca, mcap, price, liq, new_symbol)
        print(f"  -> OK: mcap=${mcap:,.0f}, price=${price:.8f}, liq=${liq:,.0f} ({rows} row updated)")
        updated += 1
        time.sleep(0.5)

    print(f"\n{'=' * 60}")
    print(f"Done: {updated} updated, {failed} failed, {skipped} skipped")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
