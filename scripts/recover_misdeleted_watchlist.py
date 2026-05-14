#!/usr/bin/env python3
"""
Recover mistakenly deleted watchlist tokens.

Reads bottom_watchlist_delete_audit, checks each token's current pool liquidity
and market cap via gmgn-cli, then restores tokens that are still alive.

Usage:
  python scripts/recover_misdeleted_watchlist.py          # dry-run
  python scripts/recover_misdeleted_watchlist.py --apply  # actually restore
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from db_client import db_op


CHAIN = "sol"
# ---- alive thresholds ----
MIN_POOL_LIQUIDITY_USD = 10_000  # pool >= $10K → token is alive
MIN_MCAP_USD = 20_000            # mcap >= $20K → token is alive (backup signal)
# -------------------------


def to_float(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def gmgn_command_prefix() -> list[str]:
    exe = shutil.which("gmgn-cli") or shutil.which("gmgn-cli.cmd") or shutil.which("gmgn-cli.ps1")
    if not exe:
        return ["gmgn-cli"]
    if exe.lower().endswith(".ps1"):
        return ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", exe]
    return [exe]


def run_gmgn(args: list[str], timeout: int = 60) -> dict[str, Any] | list[Any] | None:
    cmd = [*gmgn_command_prefix(), *args, "--raw"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except Exception as exc:
        print(f"  gmgn error: {exc}")
        return None
    if result.returncode != 0:
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return None


def fetch_pool(address: str) -> dict[str, Any]:
    return run_gmgn(["token", "pool", "--chain", CHAIN, "--address", address], timeout=60) or {}


def fetch_info(address: str) -> dict[str, Any]:
    return run_gmgn(["token", "info", "--chain", CHAIN, "--address", address], timeout=60) or {}


def calc_mcap(info: dict[str, Any]) -> float:
    price = to_float(info.get("price"))
    supply = to_float(info.get("circulating_supply"))
    if price > 0 and supply > 0:
        return price * supply
    for key in ("market_cap", "usd_market_cap", "mcap", "fdv"):
        v = to_float(info.get(key))
        if v > 0:
            return v
    return 0.0


def extract_liquidity(pool_data: dict[str, Any]) -> float:
    """Extract total pool liquidity from gmgn-cli token pool response."""
    if not pool_data:
        return 0.0
    rows: list[dict[str, Any]] = []
    if isinstance(pool_data, list):
        rows = pool_data
    elif isinstance(pool_data, dict):
        data = pool_data.get("data") if isinstance(pool_data.get("data"), dict) else {}
        rows = (
            pool_data.get("list")
            or pool_data.get("pools")
            or pool_data.get("pairs")
            or data.get("list")
            or data.get("pools")
            or data.get("pairs")
        ) or []
        if isinstance(rows, dict):
            rows = [rows]
        if not rows and any(k in pool_data for k in ("pool_address", "liquidity", "exchange")):
            rows = [pool_data]
    if not isinstance(rows, list):
        return 0.0
    total = 0.0
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key in ("liquidity", "liquidity_usd", "usd_liquidity", "reserve_usd", "pool_liquidity", "total_liquidity"):
            v = row.get(key)
            if v not in (None, ""):
                total += to_float(v)
                break
        else:
            total += to_float(row.get("base_reserve_value")) + to_float(row.get("quote_reserve_value"))
    return total


def check_alive(address: str) -> tuple[bool, float, float, str]:
    """Returns (is_alive, pool_liquidity, mcap, detail_str)."""
    pool_data = fetch_pool(address)
    pool_liq = extract_liquidity(pool_data)

    info = fetch_info(address)
    mcap = calc_mcap(info)
    symbol = str(info.get("symbol") or "?")

    if pool_liq >= MIN_POOL_LIQUIDITY_USD:
        return True, pool_liq, mcap, f"pool=${pool_liq:,.0f}>={MIN_POOL_LIQUIDITY_USD:,}"
    if mcap >= MIN_MCAP_USD:
        return True, pool_liq, mcap, f"mcap=${mcap:,.0f}>={MIN_MCAP_USD:,}"
    return False, pool_liq, mcap, f"dead: pool=${pool_liq:,.0f} mcap=${mcap:,.0f}"


def get_audit_records() -> list[dict[str, Any]]:
    def _op(conn):
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT ON (ca)
                ca, reason, source, symbol,
                peak_mcap, last_mcap, current_mcap,
                pool_liquidity, pool_mcap_ratio,
                daily_mcap_date, note, metadata,
                deleted_at
            FROM bottom_watchlist_delete_audit
            ORDER BY ca, id DESC
        """)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    return db_op(_op)


def restore_token(record: dict[str, Any], pool_liq: float, mcap: float) -> bool:
    ca = str(record["ca"] or "").strip()
    if not ca:
        return False

    def _op(conn):
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO bottom_watchlist_tokens (
                ca, added_at, last_seen_at, source, symbol,
                peak_mcap, last_mcap, highest_mcap, current_mcap,
                last_pool_liquidity, last_pool_mcap_ratio,
                daily_mcap_date, note
            ) VALUES (
                %s, NOW(), NOW(), %s, %s,
                %s, %s, %s, %s,
                %s, %s,
                %s, %s
            )
            ON CONFLICT (ca) DO UPDATE SET
                last_seen_at = NOW(),
                highest_mcap = GREATEST(COALESCE(bottom_watchlist_tokens.highest_mcap, 0), EXCLUDED.highest_mcap),
                current_mcap = CASE WHEN EXCLUDED.current_mcap > 0 THEN EXCLUDED.current_mcap ELSE bottom_watchlist_tokens.current_mcap END,
                last_pool_liquidity = CASE WHEN EXCLUDED.last_pool_liquidity > 0 THEN EXCLUDED.last_pool_liquidity ELSE bottom_watchlist_tokens.last_pool_liquidity END,
                last_pool_mcap_ratio = CASE WHEN EXCLUDED.last_pool_mcap_ratio > 0 THEN EXCLUDED.last_pool_mcap_ratio ELSE bottom_watchlist_tokens.last_pool_mcap_ratio END,
                source = CASE WHEN bottom_watchlist_tokens.source IS NULL THEN EXCLUDED.source ELSE bottom_watchlist_tokens.source END
        """, (
            ca,
            str(record.get("source") or "recovered"),
            str(record.get("symbol") or ""),
            to_float(record.get("peak_mcap")),
            mcap if mcap > 0 else to_float(record.get("last_mcap")),
            max(to_float(record.get("peak_mcap")), mcap),
            mcap if mcap > 0 else to_float(record.get("current_mcap")),
            pool_liq if pool_liq > 0 else to_float(record.get("pool_liquidity")),
            to_float(record.get("pool_mcap_ratio")),
            record.get("daily_mcap_date"),
            f"{record.get('note') or ''} [recovered {datetime.now(timezone.utc).strftime('%Y-%m-%d')} via gmgn balance check]",
        ))
        return cur.rowcount > 0
    return bool(db_op(_op))


def main():
    apply_mode = "--apply" in sys.argv
    action_word = "Restoring" if apply_mode else "[DRY RUN] Would restore"

    records = get_audit_records()
    print(f"Audit records (unique CA): {len(records)}\n")

    alive = []
    dead = []

    for i, rec in enumerate(records):
        ca = str(rec["ca"] or "").strip()
        if not ca:
            continue
        symbol = str(rec.get("symbol") or "?")
        reason = str(rec.get("reason") or "?")
        peak = to_float(rec.get("peak_mcap"))
        print(f"[{i+1}/{len(records)}] {ca[:12]}... {symbol:<12} peak=${peak:,.0f} reason={reason}")

        is_alive, pool_liq, mcap, detail = check_alive(ca)
        print(f"  => {detail}")

        if is_alive:
            alive.append((rec, pool_liq, mcap))
            print(f"  {action_word} {symbol} ...")
            if apply_mode:
                restore_token(rec, pool_liq, mcap)
                print(f"  [OK] restored")
        else:
            dead.append((rec, pool_liq, mcap))
            print(f"  keep deleted")

    print(f"\n{'='*60}")
    print(f"Summary: {len(alive)} alive (to restore), {len(dead)} dead (keep deleted)")
    if alive:
        print(f"\n{action_word}:")
        for rec, pool_liq, mcap in alive:
            print(f"  {str(rec['symbol'] or '?'):<12} {rec['ca'][:16]}... pool=${pool_liq:,.0f} mcap=${mcap:,.0f}")
    if not apply_mode:
        print(f"\nRun with --apply to actually restore.")


if __name__ == "__main__":
    main()
