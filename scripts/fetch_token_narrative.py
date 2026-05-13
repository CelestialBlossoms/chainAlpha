#!/usr/bin/env python3
"""
Fetch GMGN token info + Binance narrative and save to bottom_watchlist_tokens.

Usage:
    D:/software/anaconda/envs/py312/python.exe scripts/fetch_token_narrative.py <CA>
    D:/software/anaconda/envs/py312/python.exe scripts/fetch_token_narrative.py <CA> --symbol LOBSTER --name LobsterCoin
    D:/software/anaconda/envs/py312/python.exe scripts/fetch_token_narrative.py <CA> --chain bsc
"""

from __future__ import annotations

import argparse, json, shutil, subprocess, sys, time, uuid
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

import requests
from config import GMGN_API_KEY
from binance_narrative import get_binance_narrative, compact_narrative
from bottom_detection.bottom_watchlist_store import save_watchlist_narrative

GMGN_HOST = "https://openapi.gmgn.ai"
GMGN_TIMEOUT = 30


def gmgn_cli(args_list: list[str], timeout: int = GMGN_TIMEOUT) -> dict | None:
    exe = shutil.which("gmgn-cli") or shutil.which("gmgn-cli.cmd") or "gmgn-cli"
    prefix = [exe] if not str(exe).lower().endswith(".ps1") else ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", exe]
    try:
        r = subprocess.run([*prefix, *args_list, "--raw"], capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout)
        if r.returncode != 0:
            return None
        return json.loads(r.stdout)
    except Exception as exc:
        print(f"  [ERR] gmgn-cli: {exc}")
        return None


def gmgn_api(endpoint: str, address: str, chain: str = "sol", api_key: str = "") -> dict:
    key = api_key or GMGN_API_KEY
    headers = {"X-APIKEY": key} if key else {}
    params = {
        "chain": chain,
        "address": address,
        "timestamp": int(time.time()),
        "client_id": str(uuid.uuid4()),
    }
    r = requests.get(f"{GMGN_HOST}{endpoint}", params=params, headers=headers, timeout=GMGN_TIMEOUT)
    return r.json()


def fetch_token_meta(address: str, chain: str = "sol") -> dict:
    """Try gmgn-cli first, fall back to direct API. Returns full token metadata + pool."""
    info = gmgn_cli(["token", "info", "--chain", chain, "--address", address])
    if info and info.get("code") != 0:
        info = None
    if not info:
        info = gmgn_api("/v1/token/info", address, chain)
    if not info or info.get("code") != 0:
        return {}
    d = info.get("data", info) if isinstance(info, dict) else {}
    pool = d.get("pool", {}) or {}
    ts = int(d.get("creation_timestamp") or d.get("open_timestamp") or 0)
    return {
        "symbol": str(d.get("symbol") or "").strip(),
        "name": str(d.get("name") or "").strip(),
        "price": float(d.get("price") or 0),
        "supply": float(d.get("circulating_supply") or 0),
        "mcap": float(d.get("price") or 0) * float(d.get("circulating_supply") or 0),
        "liquidity": float(d.get("liquidity") or 0),
        "launchpad": str(d.get("launchpad_platform") or "").strip(),
        "twitter": str((d.get("link") or {}).get("twitter_username") or "").strip(),
        "holders": d.get("holder_count", 0),
        "created_ts": ts,
    }


def main():
    parser = argparse.ArgumentParser(description="Fetch and save token narrative to bottom_watchlist_tokens")
    parser.add_argument("address", help="Token contract address")
    parser.add_argument("--chain", default="sol", help="Chain: sol/bsc/base/eth")
    parser.add_argument("--symbol", help="Override token symbol")
    parser.add_argument("--name", help="Override token name")
    parser.add_argument("--force", action="store_true", help="Force refresh Binance narrative (skip cache)")
    args = parser.parse_args()

    address = args.address.strip()
    chain = args.chain.strip()

    # 1. GMGN token info
    print(f"[1/3] Fetching GMGN token info ({chain})...")
    meta = fetch_token_meta(address, chain)
    symbol = args.symbol or meta.get("symbol", "")
    name = args.name or meta.get("name", "")
    if symbol:
        print(f"  Token: {symbol}" + (f" ({name})" if name else ""))
    if meta.get("mcap"):
        print(f"  MCap: ${meta['mcap']:,.0f}")
    if meta.get("launchpad"):
        print(f"  Launchpad: {meta['launchpad']}")
    if meta.get("twitter"):
        print(f"  Twitter: @{meta['twitter']}")

    # 2. Binance narrative
    print(f"[2/3] Fetching Binance narrative...")
    narrative = get_binance_narrative(
        address,
        symbol=symbol or None,
        name=name or None,
        force=args.force,
        save=True,
    )
    c = compact_narrative(narrative)

    desc = c.get("narrative_desc", "")
    ntype = c.get("narrative_type", "")
    tags = c.get("tags", [])
    source = c.get("source", "")

    if desc:
        print(f"  desc: {desc[:200]}")
    if ntype:
        print(f"  type: {ntype}")
    if tags:
        print(f"  tags: {tags}")

    if not desc and not ntype:
        print(f"  No Binance narrative found for this token")
        return

    # 3. Save to bottom-watchlist with full metadata fill
    print(f"[3/3] Saving to bottom_watchlist_tokens...")
    save_watchlist_narrative(
        address,
        narrative_desc=desc,
        narrative_type=ntype,
        source=source or "narrative_fetch",
        symbol=meta.get("symbol", ""),
        name=meta.get("name", ""),
        mcap=meta.get("mcap", 0),
        liquidity=meta.get("liquidity", 0),
        holders=meta.get("holders", 0),
        launchpad=meta.get("launchpad", ""),
        created_ts=meta.get("created_ts", 0),
    )
    print(f"  Done! narrative + metadata saved to bottom_watchlist_tokens for {address[:12]}..")


if __name__ == "__main__":
    main()
