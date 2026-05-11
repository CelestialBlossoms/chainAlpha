"""
Comprehensive token analysis: K-line + holders + bottom wallets + whale detection.

Combines K-line phase breakdown, holder concentration analysis, smart money/KOL
tracking, bottom wallet (best entry) detection, and new whale monitoring into a
single report. Fills the gap between analyze_token_whales.py (holder-focused)
and scripts/analyze_kline.py (basic K-line only).

Usage:
    D:/software/anaconda/envs/py312/python.exe analyze_token_full.py <CA>
    D:/software/anaconda/envs/py312/python.exe analyze_token_full.py <CA> --chain bsc
    D:/software/anaconda/envs/py312/python.exe analyze_token_full.py <CA> --no-kline
"""
import argparse
import json
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from collections import defaultdict


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _gmgn_exe() -> list:
    """Find the gmgn-cli executable, handling Windows .cmd resolution."""
    exe = shutil.which("gmgn-cli") or shutil.which("gmgn-cli.cmd") or "gmgn-cli"
    return [exe]


def run_gmgn(args_list: list, timeout: int = 45) -> dict:
    """Run a gmgn-cli command and return parsed JSON."""
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


def short(addr: str, n: int = 8) -> str:
    """Shorten an address for display."""
    if not addr or len(addr) <= n + 4:
        return addr or "N/A"
    return addr[:n] + ".." + addr[-4:]


def fmt_usd(val) -> str:
    """Format a USD value."""
    if val is None:
        return "$0"
    return f"${val:,.2f}"


def fmt_time(ts) -> str:
    """Format a Unix timestamp (seconds)."""
    if not ts or ts == 0:
        return "N/A"
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%m-%d %H:%M")


def to_f(v, default=0.0):
    """Safely cast to float."""
    try:
        if v in (None, ""):
            return default
        return float(v)
    except (ValueError, TypeError):
        return default


# ---------------------------------------------------------------------------
# Token Info
# ---------------------------------------------------------------------------

def fetch_token_info(chain: str, address: str) -> dict:
    """Fetch token info + security + pool data."""
    print("[1/4] Fetching token info + security...")
    info = run_gmgn(["token", "info", "--chain", chain, "--address", address])
    time.sleep(0.5)
    security = run_gmgn(["token", "security", "--chain", chain, "--address", address])
    return {"info": info, "security": security}


def print_token_overview(info: dict, security: dict):
    """Print token basic info and security summary."""
    dev = info.get("dev", {})
    stat = info.get("stat", {})
    wts = info.get("wallet_tags_stat", {})
    link = info.get("link", {})
    pool = info.get("pool", {})

    price = to_f(info.get("price"))
    supply = to_f(info.get("circulating_supply"))
    mcap = price * supply
    liq = to_f(info.get("liquidity"))
    holders = info.get("holder_count", 0)
    ath = to_f(info.get("ath_price"))

    print()
    print(f"{'='*70}")
    print(f"  {info.get('symbol','?')} ({info.get('name','?')})")
    print(f"  Chain: SOL | Launchpad: {info.get('launchpad_platform','?')}")
    print(f"{'='*70}")
    print(f"  Price: ${price:.8f}  |  MCap: ${mcap:,.0f}  |  Liq: ${liq:,.0f}")
    print(f"  ATH:   ${ath:.8f}  |  Holders: {holders}")
    print(f"  Pool:  {pool.get('exchange','?')}  |  Quote: {pool.get('quote_symbol','?')}")

    # Creator
    creator = dev.get("creator_address", "")
    creator_status = dev.get("creator_token_status", "")
    cto = dev.get("cto_flag", 0)
    prev_tokens = dev.get("creator_open_count", 0)
    ath_token = dev.get("ath_token_info", {})
    print(f"  Creator: {short(creator)} | Status: {creator_status} | CTO: {'Yes' if cto else 'No'}")
    print(f"  Prev tokens: {prev_tokens} | Best ATH MCap: ${to_f(ath_token.get('ath_mc','0')):,.0f}")

    # Social
    tw = link.get("twitter_username", "")
    web = link.get("website", "")
    tg = link.get("telegram", "")
    if tw:
        print(f"  Twitter: @{tw}")
    if web:
        print(f"  Website: {web[:80]}")
    if tg:
        print(f"  Telegram: {tg}")

    # Wallet stats
    print(f"  Smart Money: {wts.get('smart_wallets',0)} | KOL: {wts.get('renowned_wallets',0)} | "
          f"Snipers: {wts.get('sniper_wallets',0)} | Bundlers: {wts.get('bundler_wallets',0)}")
    print(f"  Rat Traders: {wts.get('rat_trader_wallets',0)} | Fresh: {wts.get('fresh_wallets',0)}")

    # Security
    if security:
        print(f"\n  --- Security ---")
        rm = security.get("renounced_mint", False)
        rf = security.get("renounced_freeze_account", False)
        bt = security.get("buy_tax", "0")
        st = security.get("sell_tax", "0")
        burn = security.get("burn_status", "")
        print(f"  Mint renounced: {rm} | Freeze renounced: {rf} | Tax: {bt}/{st}")
        print(f"  LP burn: {burn or 'N/A'}")

    # Risk signals
    print(f"\n  --- Risk Signals ---")
    bot_rate = to_f(stat.get("top_bot_degen_percentage")) * 100
    bundler_rate = to_f(stat.get("top_bundler_trader_percentage")) * 100
    rat_rate = to_f(stat.get("top_rat_trader_percentage")) * 100
    entrap_rate = to_f(stat.get("top_entrapment_trader_percentage")) * 100
    top10_rate = to_f(stat.get("top_10_holder_rate")) * 100
    print(f"  Bot/Degen: {bot_rate:.1f}% | Bundler vol: {bundler_rate:.1f}% | "
          f"Rat trader: {rat_rate:.1f}% | Entrapment: {entrap_rate:.1f}%")
    print(f"  Top-10 concentration: {top10_rate:.1f}%")


# ---------------------------------------------------------------------------
# Liquidity / Pool Analysis
# ---------------------------------------------------------------------------

def print_liquidity_analysis(info: dict):
    """Print pool health & liquidity analysis."""
    pool = info.get("pool", {})
    if not pool:
        return

    price = to_f(info.get("price"))
    supply = to_f(info.get("circulating_supply"))
    mcap = price * supply
    liq = to_f(info.get("liquidity"))

    base_reserve = to_f(pool.get("base_reserve"))
    quote_reserve = to_f(pool.get("quote_reserve"))
    base_value = to_f(pool.get("base_reserve_value"))
    quote_value = to_f(pool.get("quote_reserve_value"))

    init_liq = to_f(pool.get("initial_liquidity"))
    init_base = to_f(pool.get("initial_base_reserve"))
    init_quote = to_f(pool.get("initial_quote_reserve"))

    mcap_liq_ratio = mcap / liq if liq > 0 else 0
    base_quote_ratio = base_value / quote_value if quote_value > 0 else 0
    liq_growth = (liq - init_liq) / init_liq * 100 if init_liq > 0 else 0
    pool_supply_pct = base_reserve / supply * 100 if supply > 0 else 0
    init_pool_supply_pct = init_base / supply * 100 if supply > 0 and init_base > 0 else 0
    base_change = (base_reserve - init_base) / init_base * 100 if init_base > 0 else 0
    quote_change = (quote_reserve - init_quote) / init_quote * 100 if init_quote > 0 else 0

    print(f"\n  {'='*60}")
    print(f"  LIQUIDITY / POOL HEALTH ANALYSIS")
    print(f"  {'='*60}")

    # Core metrics
    print(f"\n  --- Pool Metrics ---")
    print(f"  MCap:          ${mcap:>14,.0f}")
    print(f"  LP Total:      ${liq:>14,.0f}")
    print(f"  MCap/LP Ratio: {mcap_liq_ratio:>13.1f}x  ", end="")
    if mcap_liq_ratio < 5:
        print("[HEALTHY — ample LP backing]")
    elif mcap_liq_ratio < 10:
        print("[MODERATE — LP mildly thin]")
    elif mcap_liq_ratio < 20:
        print("[WARNING — LP thin vs market cap]")
    else:
        print("[DANGER — severely under-collateralized]")

    # Base/Quote balance
    print(f"\n  --- Pool Balance ---")
    print(f"  Base (Token):  ${base_value:>12,.0f}  ({base_reserve:,.0f} tokens, {pool_supply_pct:.1f}% of supply)")
    print(f"  Quote (SOL):   ${quote_value:>12,.0f}  ({quote_reserve:,.4f} SOL)")
    print(f"  Base/Quote:    {base_quote_ratio:>12.2f}x  ", end="")
    if 0.9 <= base_quote_ratio <= 1.1:
        print("[BALANCED — pool is symmetric, no one-sided dump]")
    elif base_quote_ratio > 1.1:
        print("[IMBALANCE — token side is heavier, possible accumulation or trapped sellers]")
    else:
        print("[IMBALANCE — SOL side is heavier, possible buys or LP withdrawal]")

    # LP Evolution
    if init_liq > 0:
        print(f"\n  --- LP Evolution (from creation) ---")
        print(f"  Initial LP:    ${init_liq:>12,.0f}  ({init_base:,.0f} tokens + {init_quote:,.4f} SOL)")
        print(f"  Current LP:    ${liq:>12,.0f}  ({base_reserve:,.0f} tokens + {quote_reserve:,.4f} SOL)")
        print(f"  LP Growth:     {liq_growth:>+12.0f}%  ", end="")
        if liq_growth > 100:
            print("[STRONG GROWTH — organic demand driving LP expansion]")
        elif liq_growth > 0:
            print("[MODERATE GROWTH — LP increasing naturally]")
        elif liq_growth > -20:
            print("[MILD DECLINE — some LP withdrawn, not alarming]")
        else:
            print("[SHARP DECLINE — significant LP removal, possible rug/liquidity exit]")

        print(f"  Token side:    {base_change:>+11.0f}%  ", end="")
        if base_change > 50:
            print("[tokens ADDED to pool — accumulation/sell pressure absorbed]")
        elif base_change < -30:
            print("[tokens DRAINED from pool — aggressive buying or LP removal]")
        else:
            print("[moderate token-side change]")

        print(f"  SOL side:      {quote_change:>+11.0f}%  ", end="")
        if quote_change > 50:
            print("[SOL ADDED — deep liquidity build-up]")
        elif quote_change < -30:
            print("[SOL DRAINED — significant sells or LP removal]")
        else:
            print("[moderate SOL-side change]")

        if init_pool_supply_pct > 0:
            print(f"  Pool supply %: {init_pool_supply_pct:.1f}% -> {pool_supply_pct:.1f}%", end="")
            if pool_supply_pct > init_pool_supply_pct * 1.5:
                print(" [pool holds MORE supply — possible accumulation or trapped sellers]")
            elif pool_supply_pct < init_pool_supply_pct * 0.5:
                print(" [pool holds LESS supply — tokens moved to wallets or burned]")
            else:
                print(" [stable]")

    # Verdict
    print(f"\n  --- Liquidity Verdict ---")
    issues = []
    if mcap > 0 and liq > 0:
        lp_depth_pct = liq / mcap * 100
        if lp_depth_pct > 20:
            issues.append(f"DEEP liquidity ({lp_depth_pct:.0f}% of MCap) — excellent trade execution")
        elif lp_depth_pct > 10:
            issues.append(f"Good liquidity ({lp_depth_pct:.0f}% of MCap)")
        elif lp_depth_pct > 5:
            issues.append(f"Thin liquidity ({lp_depth_pct:.0f}% of MCap) — large orders will slip")
        else:
            issues.append(f"VERY thin liquidity ({lp_depth_pct:.0f}% of MCap) — high slippage risk")

    if not 0.9 <= base_quote_ratio <= 1.1:
        direction = "token-heavy" if base_quote_ratio > 1 else "SOL-heavy"
        issues.append(f"Pool is {direction} ({base_quote_ratio:.2f}x) — potential imbalance risk")

    if liq_growth < -20:
        issues.append(f"LP declining {liq_growth:.0f}% — liquidity exit detected")
    elif liq_growth > 50:
        issues.append(f"LP growing strongly +{liq_growth:.0f}% — healthy demand")

    for issue in issues:
        print(f"  - {issue}")

    if not issues:
        print(f"  Pool looks healthy with no warning signals")


# ---------------------------------------------------------------------------
# Holder Analysis
# ---------------------------------------------------------------------------

def fetch_holders(chain: str, address: str) -> dict:
    """Fetch holders: top-50 by amount, smart_degen, and renowned."""
    print("[2/4] Fetching holder data...")
    top = run_gmgn(["token", "holders", "--chain", chain, "--address", address,
                    "--limit", "50", "--order-by", "amount_percentage", "--direction", "desc"])
    time.sleep(1)
    sm = run_gmgn(["token", "holders", "--chain", chain, "--address", address,
                   "--limit", "20", "--tag", "smart_degen", "--order-by", "amount_percentage", "--direction", "desc"])
    time.sleep(1)
    kol = run_gmgn(["token", "holders", "--chain", chain, "--address", address,
                    "--limit", "20", "--tag", "renowned", "--order-by", "amount_percentage", "--direction", "desc"])
    return {"top": top, "smart_degen": sm, "renowned": kol}


def print_holder_concentration(holders: list):
    """Print holder concentration metrics."""
    if not holders:
        return
    top3 = sum(h.get("amount_percentage", 0) for h in holders[:3]) * 100
    top5 = sum(h.get("amount_percentage", 0) for h in holders[:5]) * 100
    top10 = sum(h.get("amount_percentage", 0) for h in holders[:10]) * 100
    top20 = sum(h.get("amount_percentage", 0) for h in holders[:20]) * 100

    # Exclude pool addresses
    wallets = [h for h in holders if h.get("addr_type") != 2]
    holding = [h for h in wallets if h.get("balance", 0) > 0]
    exited = [h for h in wallets if h.get("balance", 0) == 0]

    print(f"\n  --- Holder Concentration (top 50) ---")
    print(f"  Top 3: {top3:.1f}% | Top 5: {top5:.1f}% | Top 10: {top10:.1f}% | Top 20: {top20:.1f}%")
    print(f"  Still holding: {len(holding)}/{len(wallets)} | Fully exited: {len(exited)}/{len(wallets)}")


def print_top_holders(holders: list, current_price: float):
    """Print top-30 holders table."""
    print(f"\n  --- Top 30 Holders ---")
    print(f"  {'#':<3} {'Wallet':<20} {'Hold%':>7} {'USD Value':>10} {'Real PnL':>10} {'Unreal PnL':>10} {'Tags'}")
    print(f"  {'-'*3} {'-'*20} {'-'*7} {'-'*10} {'-'*10} {'-'*10} {'-'*25}")

    for i, h in enumerate(holders[:30]):
        addr = short(h.get("address", ""), 8)
        pct = to_f(h.get("amount_percentage")) * 100
        usd = to_f(h.get("usd_value"))
        rpnl = to_f(h.get("realized_profit"))
        upnl = to_f(h.get("unrealized_profit"))
        tags = ",".join(h.get("tags", [])[:2])
        mtags = ",".join(h.get("maker_token_tags", [])[:2])
        tag_str = (tags + " | " + mtags).strip(" |")
        print(f"  {i+1:<3} {addr:<20} {pct:>6.2f}% {usd:>10,.0f} {rpnl:>10,.0f} {upnl:>10,.0f}  {tag_str}")


def print_bottom_wallets(holders: list, current_price: float):
    """Print wallets with the lowest average buy price (best entries)."""
    valid = [h for h in holders
             if h.get("avg_cost") and to_f(h.get("balance")) > 0
             and h.get("addr_type") != 2]
    if not valid:
        return
    valid.sort(key=lambda h: to_f(h.get("avg_cost")))

    print(f"\n  --- Bottom Wallets (Lowest Entry Price / Best Cost Basis) ---")
    print(f"  {'#':<3} {'Wallet':<20} {'Hold%':>7} {'USD Value':>10} {'Real PnL':>10} {'Unreal PnL':>10} {'Avg Cost':>12} {'Mult':>6} {'Tags'}")
    print(f"  {'-'*3} {'-'*20} {'-'*7} {'-'*10} {'-'*10} {'-'*10} {'-'*12} {'-'*6} {'-'*20}")

    for i, h in enumerate(valid[:15]):
        addr = short(h.get("address", ""), 8)
        pct = to_f(h.get("amount_percentage")) * 100
        usd = to_f(h.get("usd_value"))
        rpnl = to_f(h.get("realized_profit"))
        upnl = to_f(h.get("unrealized_profit"))
        avg_cost = to_f(h.get("avg_cost"))
        mult = current_price / avg_cost if avg_cost > 0 and current_price > 0 else 0
        tags = ",".join(h.get("tags", [])[:2])
        mtags = ",".join(h.get("maker_token_tags", [])[:2])
        tag_str = (tags + " | " + mtags).strip(" |")
        print(f"  {i+1:<3} {addr:<20} {pct:>6.2f}% {usd:>10,.0f} {rpnl:>10,.0f} {upnl:>10,.0f} "
              f"${avg_cost:>10.7f} {mult:>5.1f}x {tag_str}")

    _print_bottom_wallets_analysis(valid, current_price)


def _print_bottom_wallets_analysis(bottom_wallets: list, current_price: float):
    """Print descriptive analysis of bottom-position wallets."""
    if len(bottom_wallets) < 3:
        return

    # Count tags
    bot_count = sum(1 for h in bottom_wallets if "bundler" in (h.get("tags") or []) + (h.get("maker_token_tags") or []))
    sniper_count = sum(1 for h in bottom_wallets if "sniper" in (h.get("tags") or []) + (h.get("maker_token_tags") or []))
    dev_count = sum(1 for h in bottom_wallets if "dev_team" in (h.get("tags") or []) + (h.get("maker_token_tags") or []))

    # Realized vs unrealized
    total_realized = sum(to_f(h.get("realized_profit")) for h in bottom_wallets)
    total_unreal = sum(to_f(h.get("unrealized_profit")) for h in bottom_wallets)
    total_cost = sum(to_f(h.get("total_cost")) or to_f(h.get("history_bought_cost")) for h in bottom_wallets)
    sold_any = sum(1 for h in bottom_wallets if to_f(h.get("history_sold_income")) > 0)
    avg_mult = current_price / (sum(to_f(h.get("avg_cost")) for h in bottom_wallets) / len(bottom_wallets)) if bottom_wallets else 0

    lines = []
    lines.append(f"\n  >>> Bottom Chips Analysis <<<")
    lines.append(f"  Tag makeup: {bot_count} bundlers, {sniper_count} snipers, {dev_count} dev_team")
    lines.append(f"  Avg return multiple: {avg_mult:.1f}x | Sold any: {sold_any}/{len(bottom_wallets)}")

    if total_realized > 0 and total_realized < total_unreal * 0.5:
        lines.append(f"  Selling pressure: LOW — realized profit (${total_realized:,.0f}) << unrealized (${total_unreal:,.0f}), bottom chips are HOLDING")
    elif total_realized > total_unreal:
        lines.append(f"  Selling pressure: HIGH — realized profit (${total_realized:,.0f}) > unrealized (${total_unreal:,.0f}), bottom chips are DISTRIBUTING")
    else:
        lines.append(f"  Selling pressure: MODERATE — realized ${total_realized:,.0f} vs unrealized ${total_unreal:,.0f}")

    if bot_count > len(bottom_wallets) * 0.6:
        lines.append(f"  [!] {bot_count}/{len(bottom_wallets)} bottom wallets are bot/bundler-tagged — likely coordinated entry, not organic")
    if sold_any == 0 and total_cost > 0:
        lines.append(f"  [!] ZERO sellers among bottom wallets — typical of bundled wallets waiting to exit together")

    for line in lines:
        print(f"  {line}")


def print_new_whales(holders: list):
    """Detect and print recently-entered wallets with significant holdings."""
    whales = [h for h in holders
              if to_f(h.get("amount_percentage")) > 0.005
              and to_f(h.get("balance")) > 0
              and to_f(h.get("sell_amount_percentage", 1)) < 0.3
              and h.get("addr_type") != 2]
    if not whales:
        return
    whales.sort(key=lambda h: h.get("start_holding_at") or 0, reverse=True)

    print(f"\n  --- New Whales (Hold >0.5%, Recent Entry, Low Sell%) ---")
    print(f"  {'#':<3} {'Wallet':<20} {'Hold%':>6} {'USD Value':>10} {'Buy Vol':>10} {'Unreal PnL':>10} {'Tags'}")
    print(f"  {'-'*3} {'-'*20} {'-'*6} {'-'*10} {'-'*10} {'-'*10} {'-'*15}")

    for i, h in enumerate(whales[:10]):
        addr = short(h.get("address", ""), 8)
        pct = to_f(h.get("amount_percentage")) * 100
        usd = to_f(h.get("usd_value"))
        buy_vol = to_f(h.get("buy_volume_cur"))
        upnl = to_f(h.get("unrealized_profit"))
        tags = ",".join(h.get("tags", [])[:2])
        print(f"  {i+1:<3} {addr:<20} {pct:>5.1f}% {usd:>10,.0f} {buy_vol:>10,.0f} {upnl:>10,.0f}  {tags}")


def print_smart_kol(sm_list: list, kol_list: list):
    """Print smart money and KOL holder summaries."""
    if sm_list:
        print(f"\n  --- Smart Money Holders ---")
        print(f"  {'#':<3} {'Wallet':<20} {'Hold%':>7} {'Real PnL':>10} {'Unreal PnL':>10} {'Sold%':>7} {'Tags'}")
        print(f"  {'-'*3} {'-'*20} {'-'*7} {'-'*10} {'-'*10} {'-'*7} {'-'*15}")
        for i, h in enumerate(sm_list[:10]):
            addr = short(h.get("address", ""), 8)
            pct = to_f(h.get("amount_percentage")) * 100
            rpnl = to_f(h.get("realized_profit"))
            upnl = to_f(h.get("unrealized_profit"))
            sold = to_f(h.get("sell_amount_percentage")) * 100
            tags = ",".join(h.get("tags", [])[:2])
            print(f"  {i+1:<3} {addr:<20} {pct:>6.2f}% {rpnl:>10,.0f} {upnl:>10,.0f} {sold:>6.0f}%  {tags}")

    if kol_list:
        print(f"\n  --- KOL / Renowned Holders ---")
        print(f"  {'#':<3} {'Wallet':<20} {'Name':<18} {'Hold%':>7} {'Real PnL':>10} {'Unreal PnL':>10} {'Sold%':>7}")
        print(f"  {'-'*3} {'-'*20} {'-'*18} {'-'*7} {'-'*10} {'-'*10} {'-'*7}")
        for i, h in enumerate(kol_list[:10]):
            addr = short(h.get("address", ""), 8)
            name = (h.get("twitter_username") or h.get("name") or "N/A")[:17]
            pct = to_f(h.get("amount_percentage")) * 100
            rpnl = to_f(h.get("realized_profit"))
            upnl = to_f(h.get("unrealized_profit"))
            sold = to_f(h.get("sell_amount_percentage")) * 100
            print(f"  {i+1:<3} {addr:<20} {name:<18} {pct:>6.2f}% {rpnl:>10,.0f} {upnl:>10,.0f} {sold:>6.0f}%")


# ---------------------------------------------------------------------------
# K-line Analysis
# ---------------------------------------------------------------------------

def fetch_kline(chain: str, address: str, resolution: str = "5m",
                lookback_hours: int = 24) -> list:
    """Fetch K-line data and return normalized candles."""
    print(f"[3/4] Fetching {resolution} K-line data...")
    now = int(time.time())
    start = now - lookback_hours * 3600
    data = run_gmgn(["market", "kline", "--chain", chain, "--address", address,
                     "--resolution", resolution, "--from", str(start), "--to", str(now)],
                    timeout=30)
    if not data:
        return []

    rows = data.get("list") or data.get("data", {}).get("list") or []
    candles = []
    for row in (rows if isinstance(rows, list) else []):
        if not isinstance(row, dict):
            continue
        raw_ts = int(to_f(row.get("time") or row.get("timestamp") or row.get("t")))
        ts = raw_ts // 1000 if raw_ts > 10_000_000_000 else raw_ts
        close = to_f(row.get("close") or row.get("c"))
        if ts <= 0 or close <= 0:
            continue
        candles.append({
            "ts": ts,
            "open": to_f(row.get("open") or row.get("o"), close),
            "high": to_f(row.get("high") or row.get("h"), close),
            "low": to_f(row.get("low") or row.get("l"), close),
            "close": close,
            "volume": to_f(row.get("volume") or row.get("v")),
        })
    candles.sort(key=lambda c: c["ts"])
    return candles


def analyze_kline(candles: list) -> dict:
    """Comprehensive K-line analysis with phase breakdown."""
    if len(candles) < 3:
        return {"error": f"Only {len(candles)} candles available"}

    opens = [c["open"] for c in candles]
    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    volumes = [c["volume"] for c in candles]

    ath = max(highs)
    atl = min(lows)
    ath_idx = highs.index(ath)
    first_open = opens[0]
    last_close = closes[-1]
    total_vol = sum(volumes)

    # Post-ATH drawdown
    post_ath_lows = [lows[i] for i in range(ath_idx, len(lows))]
    max_dd = (ath - min(post_ath_lows)) / ath * 100 if ath > 0 else 0

    # Candle stats
    green = sum(1 for c in candles if c["close"] > c["open"])
    red = sum(1 for c in candles if c["close"] < c["open"])

    # SMAs
    def sma(data, period):
        if len(data) >= period:
            return sum(data[-period:]) / period
        return sum(data) / len(data)

    sma20 = sma(closes, 20)
    sma50 = sma(closes, 50)

    # Volume trend
    half = len(volumes) // 2
    first_half_vol = sum(volumes[:half])
    second_half_vol = sum(volumes[half:])
    recent_vol_avg = sum(volumes[-10:]) / 10 if len(volumes) >= 10 else sum(volumes) / len(volumes)
    early_vol_avg = sum(volumes[:10]) / 10 if len(volumes) >= 10 else sum(volumes) / len(volumes)
    vol_ratio = recent_vol_avg / early_vol_avg if early_vol_avg > 0 else 1

    # Key levels
    recent_low = min(lows[-20:]) if len(lows) >= 20 else min(lows)
    recent_high = max(highs[-20:]) if len(highs) >= 20 else max(highs)

    # Phase detection
    phases = _detect_phases(candles, ath_idx, ath, last_close)

    # Trend determination
    if last_close < sma20 and last_close < sma50:
        trend = "BEARISH (below both SMAs)"
    elif last_close > sma20 and last_close < sma50:
        trend = "NEUTRAL (above SMA-20, below SMA-50)"
    elif last_close > sma50:
        trend = "BULLISH (above SMA-50)"
    else:
        trend = "MIXED"

    return {
        "resolution": "5m",
        "candle_count": len(candles),
        "first_open": first_open,
        "ath": ath,
        "ath_idx": ath_idx,
        "atl": atl,
        "last_close": last_close,
        "total_change_pct": (last_close - first_open) / first_open * 100 if first_open > 0 else 0,
        "ath_gain": ath / first_open if first_open > 0 else 0,
        "max_drawdown_pct": max_dd,
        "ath_to_current_pct": (ath - last_close) / ath * 100 if ath > 0 else 0,
        "total_volume": total_vol,
        "avg_volume": total_vol / len(candles),
        "max_volume": max(volumes),
        "vol_ratio": vol_ratio,
        "green_candles": green,
        "red_candles": red,
        "green_ratio": green / len(candles),
        "sma20": sma20,
        "sma50": sma50,
        "resistance": recent_high,
        "support": recent_low,
        "trend": trend,
        "phases": phases,
        "last_candles": candles[-8:],
    }


def _detect_phases(candles: list, ath_idx: int, ath: float, current: float) -> list:
    """Detect distinct price phases from candle history."""
    phases = []
    n = len(candles)
    if n < 6:
        return phases

    closes = [c["close"] for c in candles]

    # Phase 1: Launch pump (first 3 candles or up to first significant drop)
    end_p1 = min(3, n // 4)
    if end_p1 > 0:
        p1_start = candles[0]["close"]
        p1_end = candles[end_p1 - 1]["close"]
        if p1_start > 0:
            phases.append({
                "name": "Launch Pump",
                "from_price": p1_start,
                "to_price": p1_end,
                "change": f"{p1_end / p1_start:.0f}x" if p1_end >= p1_start else f"{(p1_end/p1_start - 1)*100:.0f}%"
            })

    # Find first major dump after launch
    dump_start_idx = end_p1
    lowest_before_recovery = end_p1
    min_close = closes[end_p1]
    for i in range(end_p1, ath_idx):
        if closes[i] < min_close:
            min_close = closes[i]
            lowest_before_recovery = i

    if lowest_before_recovery > end_p1:
        p2_start = closes[end_p1]
        p2_end = closes[lowest_before_recovery]
        phases.append({
            "name": "First Dump",
            "from_price": p2_start,
            "to_price": p2_end,
            "change": f"{(p2_end/p2_start - 1)*100:.0f}%"
        })

    # Phase 3: ATH run
    p3_start_idx = max(lowest_before_recovery + 1, end_p1 + 1)
    if ath_idx > p3_start_idx:
        p3_start = closes[p3_start_idx]
        phases.append({
            "name": "ATH Run",
            "from_price": p3_start,
            "to_price": ath,
            "change": f"{ath / p3_start:.0f}x" if p3_start > 0 else "N/A"
        })

    # Phase 4: Post-ATH crash
    if ath_idx < n - 3:
        post_lows = [closes[i] for i in range(ath_idx, n)]
        crash_end = min(post_lows)
        crash_idx = post_lows.index(crash_end) + ath_idx
        if crash_end < ath * 0.85:  # Only if real crash
            phases.append({
                "name": "Post-ATH Crash",
                "from_price": ath,
                "to_price": crash_end,
                "change": f"{(crash_end/ath - 1)*100:.0f}%"
            })

    # Phase 5: Current consolidation
    last_10 = closes[-12:] if n >= 12 else closes[-5:]
    phases.append({
        "name": "Consolidation (Current)",
        "from_price": min(last_10),
        "to_price": max(last_10),
        "change": f"Range: ${min(last_10):.8f} - ${max(last_10):.8f}"
    })

    return phases


def print_kline_report(candles: list, a: dict):
    """Print the K-line analysis report."""
    print(f"\n{'='*70}")
    print(f"  5-MINUTE K-LINE ANALYSIS  ({a['candle_count']} candles)")
    print(f"{'='*70}")

    if "error" in a:
        print(f"  ERROR: {a['error']}")
        return

    print(f"  Launch:    ${a['first_open']:.8f}")
    print(f"  ATH:       ${a['ath']:.8f}  (candle #{a['ath_idx']+1})  [{a['ath_gain']:.0f}x from launch]")
    print(f"  ATL:       ${a['atl']:.8f}")
    print(f"  Current:   ${a['last_close']:.8f}")
    print(f"  Change:    +{a['total_change_pct']:.0f}% from launch")
    print(f"  Drawdown:  -{a['max_drawdown_pct']:.1f}% from ATH")

    # Phases
    print(f"\n  --- Phase Breakdown ---")
    for p in a["phases"]:
        print(f"  {p['name']:<25} ${p['from_price']:.8f} -> ${p['to_price']:.8f}  ({p['change']})")

    # Volume
    print(f"\n  --- Volume ---")
    print(f"  Total: ${a['total_volume']:,.0f} | Avg/5m: ${a['avg_volume']:,.0f} | Max: ${a['max_volume']:,.0f}")
    vol_status = "DRYING UP" if a['vol_ratio'] < 0.5 else "HEALTHY"
    print(f"  Vol Ratio (recent/early): {a['vol_ratio']:.2f}x  [{vol_status}]")

    # Candles
    print(f"\n  --- Candle Pattern ---")
    print(f"  Green: {a['green_candles']} ({a['green_ratio']:.0%}) | Red: {a['red_candles']}")

    # Last 8 candles
    print(f"\n  --- Last 8 Candles ---")
    print(f"  {'Time':<10} {'Open':>12} {'High':>12} {'Low':>12} {'Close':>12} {'Chg%':>7} {'Vol':>10}")
    for c in a["last_candles"]:
        t = datetime.fromtimestamp(c["ts"], tz=timezone.utc).strftime("%H:%M UTC")
        chg = (c["close"] - c["open"]) / c["open"] * 100 if c["open"] > 0 else 0
        sign = "+" if chg >= 0 else ""
        print(f"  {t:<10} {c['open']:>12.8f} {c['high']:>12.8f} {c['low']:>12.8f} {c['close']:>12.8f} {sign}{chg:>6.1f}% {c['volume']:>10,.0f}")

    # Key levels
    print(f"\n  --- Key Levels ---")
    print(f"  Resistance:  ${a['resistance']:.8f}")
    print(f"  SMA-50:      ${a['sma50']:.8f}  (price {'ABOVE' if a['last_close'] > a['sma50'] else 'BELOW'})")
    print(f"  SMA-20:      ${a['sma20']:.8f}  (price {'ABOVE' if a['last_close'] > a['sma20'] else 'BELOW'})")
    print(f"  Support:     ${a['support']:.8f}")

    dist_res = (a['resistance'] - a['last_close']) / a['last_close'] * 100 if a['last_close'] > 0 else 0
    dist_sup = (a['last_close'] - a['support']) / a['last_close'] * 100 if a['last_close'] > 0 else 0
    print(f"  To resistance: +{dist_res:.1f}% | To support: -{dist_sup:.1f}%")

    print(f"\n  --- Trend: {a['trend']} ---")


# ---------------------------------------------------------------------------
# Summary & Scoring
# ---------------------------------------------------------------------------

def print_summary(info: dict, security: dict, kline_result: dict):
    """Print final summary and risk assessment."""
    print(f"\n{'='*70}")
    print(f"  SUMMARY & RISK ASSESSMENT")
    print(f"{'='*70}")

    dev = info.get("dev", {})
    stat = info.get("stat", {})
    wts = info.get("wallet_tags_stat", {})
    price = to_f(info.get("price"))
    supply = to_f(info.get("circulating_supply"))
    mcap = price * supply

    score = 0
    flags = []

    # Security checks (from security dict, not info)
    if security.get("renounced_mint") and security.get("renounced_freeze_account"):
        score += 15
    elif security.get("renounced_mint") or security.get("renounced_freeze_account"):
        score += 8
        flags.append(("WARN", "Partial authority renounce"))
    else:
        flags.append(("WARN", "Mint/Freeze not renounced"))

    if security.get("burn_status") == "burn":
        score += 5
    else:
        flags.append(("WARN", "LP not burned"))

    creator_status = dev.get("creator_token_status", "")
    if creator_status == "creator_close":
        score += 10
    else:
        flags.append(("WARN", "Dev still holding"))

    top10 = to_f(stat.get("top_10_holder_rate")) * 100
    if top10 < 20:
        score += 10
    elif top10 < 50:
        score += 5
        flags.append(("WARN", f"Top-10 holds {top10:.0f}%"))
    else:
        flags.append(("DANGER", f"Top-10 holds {top10:.0f}% (high concentration)"))

    # Smart money / KOL
    sm = wts.get("smart_wallets", 0)
    kol = wts.get("renowned_wallets", 0)
    if sm >= 5:
        score += 10
    elif sm >= 2:
        score += 5
    else:
        flags.append(("INFO", "Low smart money interest"))
    if kol >= 3:
        score += 5

    # Bot/Degen risk
    bot_rate = to_f(stat.get("top_bot_degen_percentage")) * 100
    bundler_rate = to_f(stat.get("top_bundler_trader_percentage")) * 100
    if bot_rate > 40:
        flags.append(("WARN", f"Bot/Degen rate {bot_rate:.0f}%"))
        score -= 5
    if bundler_rate > 8:
        flags.append(("WARN", f"Bundler volume {bundler_rate:.0f}%"))
        score -= 3

    # K-line trend
    kline_ok = kline_result and "error" not in kline_result
    if kline_ok:
        trend = kline_result.get("trend", "")
        if "BEARISH" in trend:
            score -= 5
            flags.append(("WARN", "K-line bearish"))
        elif "BULLISH" in trend:
            score += 5
        if kline_result.get("vol_ratio", 1) < 0.3:
            flags.append(("WARN", "Volume severely drying up"))

    # MCap assessment
    if mcap < 100_000:
        flags.append(("INFO", f"Micro cap (${mcap:,.0f})"))
    elif mcap > 1_000_000:
        score += 5

    print(f"\n  Composite Score: {score}/50")
    print(f"  MCap: ${mcap:,.0f} | Smart Money: {sm} | KOL: {kol}")

    if flags:
        print(f"\n  Flags:")
        for level, msg in flags:
            prefix = {"DANGER": "[STOP]", "WARN": "[-]", "INFO": "[i]"}.get(level, "[?]")
            print(f"    {prefix} {msg}")

    # Verdict
    print(f"\n  --- Verdict ---")
    if score >= 35:
        print(f"  [PASS] Worth deeper research. Run whale/cluster analysis next.")
    elif score >= 20:
        print(f"  [CAUTION] Mixed signals. Monitor before entering.")
    else:
        print(f"  [SKIP] Too many red flags. Not recommended.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Comprehensive token analysis")
    parser.add_argument("address", help="Token contract address")
    parser.add_argument("--chain", default="sol", help="Chain: sol/bsc/base/eth")
    parser.add_argument("--no-kline", action="store_true", help="Skip K-line analysis")
    args = parser.parse_args()

    chain = args.chain
    addr = args.address

    # 1. Token info + security
    token_data = fetch_token_info(chain, addr)
    info = token_data.get("info", {})
    security = token_data.get("security", {})
    if not info:
        print("Failed to fetch token info. Check address and API key.")
        sys.exit(1)

    price = to_f(info.get("price"))
    print_token_overview(info, security)
    print_liquidity_analysis(info)

    # 2. Holders
    holders_data = fetch_holders(chain, addr)
    top_holders = holders_data.get("top", {}).get("list", [])
    sm_holders = holders_data.get("smart_degen", {}).get("list", [])
    kol_holders = holders_data.get("renowned", {}).get("list", [])

    if top_holders:
        print_holder_concentration(top_holders)
        print_top_holders(top_holders, price)
        print_bottom_wallets(top_holders, price)
        print_new_whales(top_holders)
        print_smart_kol(sm_holders, kol_holders)

    # 3. K-line
    kline_result = {}
    if not args.no_kline:
        candles = fetch_kline(chain, addr)
        if candles:
            kline_result = analyze_kline(candles)
            print_kline_report(candles, kline_result)
        else:
            print("\n  [SKIP] No K-line data available (token may be too new)")

    # 4. Summary
    print_summary(info, security, kline_result)


if __name__ == "__main__":
    main()
