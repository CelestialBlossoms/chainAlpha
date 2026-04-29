#!/usr/bin/env python3
"""
Check whether one or more token addresses appear in GMGN trending tokens.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from typing import Any


TARGET_CAS = [
    "E95sJahssFKUk6jcWYbyfmjtcCsr4Z226HD9Qbjupump",
]


def gmgn_command_prefix() -> list[str]:
    executable = shutil.which("gmgn-cli") or shutil.which("gmgn-cli.cmd") or shutil.which("gmgn-cli.ps1")
    if not executable:
        return ["gmgn-cli"]
    if executable.lower().endswith(".ps1"):
        return ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", executable]
    return [executable]


def run_gmgn(args: list[str], timeout: int = 75) -> dict[str, Any] | list[Any] | None:
    cmd = [*gmgn_command_prefix(), *args, "--raw"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", timeout=timeout)
    except Exception as exc:
        print(f"gmgn exception: {' '.join(cmd)} -> {exc}", file=sys.stderr)
        return None
    if result.returncode != 0:
        err = (result.stderr or result.stdout or "").strip()
        print(f"gmgn failed rc={result.returncode}: {' '.join(cmd)}", file=sys.stderr)
        if err:
            print(err[:1000], file=sys.stderr)
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        print(f"gmgn json decode failed: {exc}", file=sys.stderr)
        return None


def token_address(row: dict[str, Any]) -> str:
    return str(row.get("address") or row.get("token_address") or row.get("ca") or "").strip()


def token_symbol(row: dict[str, Any]) -> str:
    return str(row.get("symbol") or row.get("ticker") or row.get("name") or "UNKNOWN").strip()


def to_float(value: Any) -> float:
    try:
        if value in (None, ""):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def calc_mcap(row: dict[str, Any]) -> float:
    price = to_float(row.get("price"))
    supply = to_float(row.get("circulating_supply"))
    if price > 0 and supply > 0:
        return price * supply
    for key in ("market_cap", "usd_market_cap", "mcap", "fdv", "fully_diluted_valuation"):
        value = to_float(row.get(key))
        if value > 0:
            return value
    return 0.0


def trending_rows(chain: str, interval: str, limit: int, platform: str | None = None) -> list[dict[str, Any]]:
    args = ["market", "trending", "--chain", chain, "--interval", interval, "--limit", str(limit)]
    if platform:
        args.extend(["--platform", platform])
    data = run_gmgn(args)
    if not isinstance(data, dict):
        return []
    rows = data.get("data", {}).get("rank") or data.get("rank") or data.get("list") or []
    return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Check CA presence in GMGN trending tokens.")
    parser.add_argument("--chain", default="sol", help="GMGN chain, default: sol.")
    parser.add_argument("--interval", default="1m", help="Trending interval, default: 1m.")
    parser.add_argument("--limit", type=int, default=100, help="Trending limit, default: 100.")
    parser.add_argument("--platform", default=None, help="Optional GMGN platform filter, e.g. pump.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    targets = {ca.strip(): ca.strip().lower() for ca in TARGET_CAS if ca.strip()}
    if not targets:
        print("TARGET_CAS is empty.", file=sys.stderr)
        return 2
    rows = trending_rows(args.chain, args.interval, args.limit, args.platform)
    by_address = {token_address(row).lower(): row for row in rows if token_address(row)}

    print(f"GMGN trending chain={args.chain} interval={args.interval} limit={args.limit} count={len(rows)}")
    found_any = False
    for raw_ca, normalized in targets.items():
        row = by_address.get(normalized)
        if not row:
            print(f"NOT FOUND {raw_ca}")
            continue
        found_any = True
        rank = row.get("rank") or row.get("rank_no") or row.get("index") or "N/A"
        print(
            f"FOUND {raw_ca} | rank={rank} | symbol=${token_symbol(row)} | "
            f"mcap=${calc_mcap(row):,.0f} | price={to_float(row.get('price')):.12g}"
        )
    return 0 if found_any else 1


if __name__ == "__main__":
    raise SystemExit(main())
