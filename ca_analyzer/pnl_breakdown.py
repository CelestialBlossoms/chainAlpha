"""
Analyze top-100 holders & traders PnL data for a token.
Verifies which profit/loss fields gmgn-cli returns and computes
aggregate profit/loss statistics.

Usage:
    D:/software/anaconda/envs/py312/python.exe scripts/analyze_pnl_breakdown.py <CA>
    D:/software/anaconda/envs/py312/python.exe scripts/analyze_pnl_breakdown.py <CA> --chain sol
"""
import argparse, json, shutil, subprocess, sys
from collections import defaultdict


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def gmgn_exe():
    exe = shutil.which("gmgn-cli") or shutil.which("gmgn-cli.cmd") or "gmgn-cli"
    return [exe]


def run_gmgn(args_list, timeout=60):
    cmd = gmgn_exe() + args_list + ["--raw"]
    r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8",
                       errors="replace", timeout=timeout)
    if r.returncode != 0:
        print(f"  [ERR] {' '.join(args_list[:4])}: {r.stderr[:200]}")
        return {}
    try:
        return json.loads(r.stdout)
    except json.JSONDecodeError:
        return {}


def to_f(v, default=0.0):
    try:
        if v in (None, ""):
            return default
        return float(v)
    except (ValueError, TypeError):
        return default


def short(addr, n=8):
    if not addr or len(addr) <= n + 4:
        return addr or "N/A"
    return addr[:n] + ".." + addr[-4:]


# ---------------------------------------------------------------------------
# Core analysis
# ---------------------------------------------------------------------------
def analyze_wallets(wallets, label, exclude_pool=True):
    """Analyze PnL fields across a list of wallets."""
    results = {
        "label": label,
        "total": 0,
        "wallets_with_balance": 0,     # still holding
        "wallets_exited": 0,            # fully sold
        "pool_addresses": 0,
        # PnL aggregates
        "total_profit": 0.0,
        "total_realized_profit": 0.0,
        "total_unrealized_profit": 0.0,
        "total_cost": 0.0,
        "total_buy_vol": 0.0,
        "total_sell_vol": 0.0,
        # Profit breakdown
        "profitable_count": 0,          # total profit > 0
        "losing_count": 0,              # total profit < 0
        "breakeven_count": 0,           # total profit == 0
        "realized_profitable_count": 0,  # realized_profit > 0
        "realized_losing_count": 0,     # realized_profit < 0
        "unrealized_profitable_count": 0,  # unrealized_profit > 0
        "unrealized_losing_count": 0,   # unrealized_profit < 0
        # Sums
        "sum_profitable_profit": 0.0,
        "sum_losing_loss": 0.0,
        "sum_realized_profitable": 0.0,
        "sum_realized_losing": 0.0,
        "sum_unrealized_profitable": 0.0,
        "sum_unrealized_losing": 0.0,
        # Top/bottom
        "top_earners": [],
        "top_losers": [],
        # Field coverage
        "fields_present": set(),
        "fields_always_zero": set(),
    }

    field_list = ["profit", "realized_profit", "unrealized_profit",
                  "profit_change", "realized_pnl", "unrealized_pnl",
                  "total_cost", "avg_cost", "avg_sold",
                  "buy_volume_cur", "sell_volume_cur",
                  "history_bought_cost", "history_sold_income",
                  "netflow_usd", "usd_value", "balance"]

    field_nonzero = {f: 0 for f in field_list}
    field_total = {f: 0 for f in field_list}

    for w in wallets:
        results["total"] += 1

        # Skip pool addresses
        if exclude_pool and w.get("addr_type") == 2:
            results["pool_addresses"] += 1
            continue

        balance = to_f(w.get("balance"))
        sell_pct = to_f(w.get("sell_amount_percentage"))
        if sell_pct >= 1.0 or balance == 0:
            results["wallets_exited"] += 1
        else:
            results["wallets_with_balance"] += 1

        # Collect fields
        for f in field_list:
            val = w.get(f)
            if val is not None and val != 0 and val != "":
                field_nonzero[f] += 1
            field_total[f] += 1

        profit = to_f(w.get("profit"))
        realized = to_f(w.get("realized_profit"))
        unrealized = to_f(w.get("unrealized_profit"))
        total_cost = to_f(w.get("total_cost"))
        buy_vol = to_f(w.get("buy_volume_cur"))
        sell_vol = to_f(w.get("sell_volume_cur"))

        results["total_profit"] += profit
        results["total_realized_profit"] += realized
        results["total_unrealized_profit"] += unrealized
        results["total_cost"] += total_cost
        results["total_buy_vol"] += buy_vol
        results["total_sell_vol"] += sell_vol

        # Categorize
        if profit > 0:
            results["profitable_count"] += 1
            results["sum_profitable_profit"] += profit
        elif profit < 0:
            results["losing_count"] += 1
            results["sum_losing_loss"] += profit
        else:
            results["breakeven_count"] += 1

        if realized > 0:
            results["realized_profitable_count"] += 1
            results["sum_realized_profitable"] += realized
        elif realized < 0:
            results["realized_losing_count"] += 1
            results["sum_realized_losing"] += realized

        if unrealized > 0:
            results["unrealized_profitable_count"] += 1
            results["sum_unrealized_profitable"] += unrealized
        elif unrealized < 0:
            results["unrealized_losing_count"] += 1
            results["sum_unrealized_losing"] += unrealized

        # Track top/bottom
        info = {
            "addr": w.get("address", ""),
            "profit": profit,
            "realized_profit": realized,
            "unrealized_profit": unrealized,
            "total_cost": total_cost,
            "balance": balance,
            "usd_value": to_f(w.get("usd_value")),
            "tags": w.get("tags", []),
            "maker_tags": w.get("maker_token_tags", []),
            "buy_vol": buy_vol,
            "sell_vol": sell_vol,
            "sell_pct": sell_pct,
            "name": w.get("twitter_username") or w.get("name") or "",
        }

    # Detect field coverage
    for f in field_list:
        if field_nonzero[f] > 0:
            results["fields_present"].add(f)
        if field_total[f] > 0 and field_nonzero[f] == 0:
            results["fields_always_zero"].add(f)

    return results


def print_field_analysis(results, label):
    """Print which PnL fields gmgn-cli returns and their coverage."""
    print(f"\n{'='*70}")
    print(f"  FIELD COVERAGE CHECK: {label}")
    print(f"{'='*70}")
    print(f"  Fields PRESENT (non-zero in some wallets):")
    for f in sorted(results["fields_present"]):
        print(f"    [OK] {f}")
    if results["fields_always_zero"]:
        print(f"  Fields ALWAYS ZERO:")
        for f in sorted(results["fields_always_zero"]):
            print(f"    [--] {f}")
    missing = {"profit", "realized_profit", "unrealized_profit", "profit_change",
               "total_cost", "avg_cost"} - results["fields_present"]
    if missing:
        print(f"  Fields MISSING entirely:")
        for f in sorted(missing):
            print(f"    [!!] {f}")


def print_pnl_summary(r, label):
    """Print PnL aggregate statistics."""
    active = r["wallets_with_balance"] + r["wallets_exited"]
    print(f"\n{'='*70}")
    print(f"  P&L SUMMARY: {label}")
    print(f"{'='*70}")
    print(f"  Total wallets: {r['total']} (pool: {r['pool_addresses']}, active: {active})")
    print(f"  Still holding: {r['wallets_with_balance']} | Fully exited: {r['wallets_exited']}")
    print()
    print(f"  --- Aggregate P&L ---")
    print(f"  Total Profit (all wallets):     ${r['total_profit']:>14,.2f}")
    print(f"  Total Realized Profit:          ${r['total_realized_profit']:>14,.2f}")
    print(f"  Total Unrealized Profit:        ${r['total_unrealized_profit']:>14,.2f}")
    print(f"  Total Cost Basis:               ${r['total_cost']:>14,.2f}")
    print(f"  Total Buy Volume:               ${r['total_buy_vol']:>14,.2f}")
    print(f"  Total Sell Volume:              ${r['total_sell_vol']:>14,.2f}")
    if r["total_cost"] > 0:
        roi = r["total_profit"] / r["total_cost"] * 100
        print(f"  Overall ROI:                    {roi:>14.1f}%")
    print()
    print(f"  --- Winners vs Losers ---")
    print(f"  Profitable wallets:  {r['profitable_count']:>4}  sum: ${r['sum_profitable_profit']:>14,.2f}")
    print(f"  Losing wallets:      {r['losing_count']:>4}  sum: ${r['sum_losing_loss']:>14,.2f}")
    print(f"  Breakeven:           {r['breakeven_count']:>4}")
    if r["profitable_count"] + r["losing_count"] > 0:
        win_rate = r["profitable_count"] / (r["profitable_count"] + r["losing_count"]) * 100
        print(f"  Win Rate:            {win_rate:>5.1f}%")
        profit_ratio = abs(r['sum_profitable_profit'] / r['sum_losing_loss']) if r['sum_losing_loss'] != 0 else float('inf')
        print(f"  Profit/Loss Ratio:   {profit_ratio:>6.1f}x")
    print()
    print(f"  --- Realized vs Unrealized Breakdown ---")
    print(f"  Realized profitable:   {r['realized_profitable_count']:>4}  sum: ${r['sum_realized_profitable']:>14,.2f}")
    print(f"  Realized losing:       {r['realized_losing_count']:>4}  sum: ${r['sum_realized_losing']:>14,.2f}")
    print(f"  Unrealized profitable: {r['unrealized_profitable_count']:>4}  sum: ${r['sum_unrealized_profitable']:>14,.2f}")
    print(f"  Unrealized losing:     {r['unrealized_losing_count']:>4}  sum: ${r['sum_unrealized_losing']:>14,.2f}")

    _print_pnl_summary_analysis(r, label)


def _print_pnl_summary_analysis(r, label):
    """Print descriptive interpretation of PnL summary statistics."""
    lines = [f"\n  >>> PnL Health Assessment ({label}) <<<"]

    total = r["total"]
    profitable = r["profitable_count"]
    losing = r["losing_count"]
    exited = r["wallets_exited"]
    holding = r["wallets_with_balance"]
    total_profit = r["total_profit"]
    total_realized = r["total_realized_profit"]
    total_unrealized = r["total_unrealized_profit"]
    total_buy = r["total_buy_vol"]
    total_sell = r["total_sell_vol"]

    # Win rate assessment
    if total > 0:
        win_rate = profitable / (profitable + losing) * 100 if (profitable + losing) > 0 else 0
    else:
        win_rate = 0

    if win_rate >= 80:
        lines.append(f"  Win rate: {win_rate:.0f}% — overwhelmingly profitable, early-entry dominance")
    elif win_rate >= 50:
        lines.append(f"  Win rate: {win_rate:.0f}% — reasonably balanced, typical early-stage distribution")
    elif win_rate >= 30:
        lines.append(f"  Win rate: {win_rate:.0f}% — tilted toward losers, late buyers are underwater")
    elif win_rate > 0:
        lines.append(f"  Win rate: {win_rate:.0f}% — most holders are losing, price declining from entry")

    # Exit analysis
    exit_pct = exited / (exited + holding) * 100 if (exited + holding) > 0 else 0
    if exit_pct > 60:
        lines.append(f"  Exit rate: {exit_pct:.0f}% fully exited — mass departure, early players have cashed out")
    elif exit_pct > 30:
        lines.append(f"  Exit rate: {exit_pct:.0f}% fully exited — moderate rotation, partial profit-taking")
    elif exit_pct > 0:
        lines.append(f"  Exit rate: {exit_pct:.0f}% fully exited — most holders still in position")
    else:
        lines.append(f"  Exit rate: 0% — NO ONE has fully exited, positions are locked or accumulating")

    # Realized vs Unrealized
    if total_profit > 0:
        real_ratio = total_realized / total_profit * 100 if total_profit > 0 else 0
        if real_ratio > 70:
            lines.append(f"  Profit composition: {real_ratio:.0f}% realized — profits have been TAKEN, selling pressure was absorbed")
        elif real_ratio > 30:
            lines.append(f"  Profit composition: {real_ratio:.0f}% realized — balanced between taken and paper profits")
        else:
            lines.append(f"  Profit composition: {real_ratio:.0f}% realized — mostly UNREALIZED (paper gains), risk of future sell-off")

    # Flow direction
    netflow = total_buy - total_sell
    if total_buy > 0 and total_sell > 0:
        if netflow > 0:
            lines.append(f"  Net flow: +${netflow:,.0f} — net BUYING, money flowing in")
        else:
            lines.append(f"  Net flow: -${abs(netflow):,.0f} — net SELLING, money flowing out")
    elif total_sell == 0 and total_buy > 0:
        lines.append(f"  Net flow: PURE BUY — no selling activity at all")

    # Profit/Loss ratio
    if r.get("sum_losing_loss") and r["sum_losing_loss"] < 0:
        pl_ratio = abs(r.get("sum_profitable_profit", 0) / r["sum_losing_loss"])
        if pl_ratio > 5:
            lines.append(f"  P/L ratio: {pl_ratio:.1f}x — winners massively outweigh losers, asymmetric upside")
        elif pl_ratio > 2:
            lines.append(f"  P/L ratio: {pl_ratio:.1f}x — healthy risk/reward, winners 2-5x losers")
        elif pl_ratio < 1.5:
            lines.append(f"  P/L ratio: {pl_ratio:.1f}x — winners barely exceed losers, marginal profitability")

    for line in lines:
        print(f"  {line}")


def print_top_bottom(wallets, label, current_price, top_n=15):
    """Print top earners and top losers."""
    active = [w for w in wallets if w.get("addr_type") != 2]

    # Sort by profit
    by_profit = sorted(active, key=lambda w: to_f(w.get("profit")), reverse=True)
    by_loss = sorted(active, key=lambda w: to_f(w.get("profit")))

    print(f"\n  --- TOP {top_n} EARNERS ({label}) ---")
    print(f"  {'#':<3} {'Wallet':<20} {'Total P&L':>10} {'Realized':>10} {'Unreal':>10} {'Cost':>10} {'Balance':>10} {'Tags'}")
    print(f"  {'-'*3} {'-'*20} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*20}")
    for i, w in enumerate(by_profit[:top_n]):
        addr = short(w.get("address", ""), 8)
        profit = to_f(w.get("profit"))
        realized = to_f(w.get("realized_profit"))
        unrealized = to_f(w.get("unrealized_profit"))
        cost = to_f(w.get("total_cost"))
        balance = to_f(w.get("usd_value"))
        tags = ",".join((w.get("tags") or [])[:2])
        mtags = ",".join((w.get("maker_token_tags") or [])[:1])
        tag_str = (tags + " | " + mtags).strip(" |")
        print(f"  {i+1:<3} {addr:<20} ${profit:>9,.0f} ${realized:>9,.0f} ${unrealized:>9,.0f} ${cost:>9,.0f} ${balance:>9,.0f}  {tag_str}")

    print(f"\n  --- TOP {top_n} LOSERS ({label}) ---")
    print(f"  {'#':<3} {'Wallet':<20} {'Total P&L':>10} {'Realized':>10} {'Unreal':>10} {'Cost':>10} {'Balance':>10} {'Tags'}")
    print(f"  {'-'*3} {'-'*20} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*20}")
    for i, w in enumerate(by_loss[:top_n]):
        addr = short(w.get("address", ""), 8)
        profit = to_f(w.get("profit"))
        realized = to_f(w.get("realized_profit"))
        unrealized = to_f(w.get("unrealized_profit"))
        cost = to_f(w.get("total_cost"))
        balance = to_f(w.get("usd_value"))
        tags = ",".join((w.get("tags") or [])[:2])
        mtags = ",".join((w.get("maker_token_tags") or [])[:1])
        tag_str = (tags + " | " + mtags).strip(" |")
        print(f"  {i+1:<3} {addr:<20} ${profit:>9,.0f} ${realized:>9,.0f} ${unrealized:>9,.0f} ${cost:>9,.0f} ${balance:>9,.0f}  {tag_str}")


def print_distribution(wallets, label):
    """Print PnL distribution histogram."""
    active = [w for w in wallets if w.get("addr_type") != 2]
    profits = [to_f(w.get("profit")) for w in active]

    if not profits:
        return

    max_p = max(profits)
    min_p = min(profits)

    # Create buckets
    buckets = [
        (" < -$1000", lambda p: p < -1000),
        ("-$1000 ~ -$100", lambda p: -1000 <= p < -100),
        ("-$100 ~ -$10", lambda p: -100 <= p < -10),
        (" -$10 ~ $0", lambda p: -10 <= p < 0),
        ("   $0 (breakeven)", lambda p: p == 0),
        ("  $0 ~ $10", lambda p: 0 < p <= 10),
        (" $10 ~ $100", lambda p: 10 < p <= 100),
        ("$100 ~ $1000", lambda p: 100 < p <= 1000),
        ("$1000 ~ $10000", lambda p: 1000 < p <= 10000),
        (" > $10000", lambda p: p > 10000),
    ]

    print(f"\n  --- P&L DISTRIBUTION ({label}) ---")
    for name, fn in buckets:
        count = sum(1 for p in profits if fn(p))
        pct = count / len(profits) * 100 if profits else 0
        bar = "#" * int(pct / 2) if pct > 0 else ""
        print(f"  {name:<20} {count:>4} ({pct:>5.1f}%)  {bar}")

    _print_distribution_analysis(wallets, label)


def _print_distribution_analysis(wallets, label):
    """Print descriptive interpretation of PnL distribution shape."""
    active = [w for w in wallets if w.get("addr_type") != 2]
    profits = [to_f(w.get("profit")) for w in active]
    if not profits:
        return

    total = len(profits)
    big_winners = sum(1 for p in profits if p > 100)
    big_losers = sum(1 for p in profits if p < -100)
    small_win = sum(1 for p in profits if 0 < p <= 100)
    small_loss = sum(1 for p in profits if -100 <= p < 0)
    breakeven = sum(1 for p in profits if p == 0)
    avg_pnl = sum(profits) / total if total else 0

    lines = [f"\n  >>> Distribution Shape Analysis ({label}) <<<"]

    # Skew analysis
    if big_winners > big_losers * 3 and big_winners > 5:
        lines.append(f"  Distribution: RIGHT-SKEWED — {big_winners} big winners vs {big_losers} big losers")
        lines.append(f"       Early-entry whales dominate profits, late buyers mostly small losses. Typical of pump-and-hold.")

    if small_loss > small_win * 2:
        lines.append(f"  Distribution: MASS RETAIL LOSS — {small_loss} small losers vs {small_win} small winners")
        lines.append(f"       Most wallets down small amounts: classic retail fade pattern, late entrants underwater")

    if breakeven > total * 0.3:
        lines.append(f"  Distribution: FLAT — {breakeven}/{total} at breakeven, suggesting snipers who haven't sold yet")

    # Profit concentration
    winner_avg = sum(p for p in profits if p > 0) / max(sum(1 for p in profits if p > 0), 1)
    loser_avg = abs(sum(p for p in profits if p < 0)) / max(sum(1 for p in profits if p < 0), 1)
    if winner_avg > loser_avg * 3:
        lines.append(f"  Profit concentration: winner avg ${winner_avg:,.0f} >> loser avg ${loser_avg:,.0f}")
        lines.append(f"       Asymmetric payoff — winning trades pay 3x+ more than losing trades cost")

    # Overall assessment
    if avg_pnl > 100:
        lines.append(f"  Overall: STRONG PROFIT — avg P&L ${avg_pnl:,.0f}/wallet, cohort is deep in profit")
    elif avg_pnl > 10:
        lines.append(f"  Overall: MODERATE PROFIT — avg P&L ${avg_pnl:,.0f}/wallet, modestly positive")
    elif avg_pnl > -10:
        lines.append(f"  Overall: NEAR BREAKEVEN — avg P&L ${avg_pnl:,.0f}/wallet, cohort roughly flat")
    else:
        lines.append(f"  Overall: NEGATIVE — avg P&L ${avg_pnl:,.0f}/wallet, cohort is underwater")

    for line in lines:
        print(f"  {line}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="PnL breakdown analysis for top-100 holders & traders")
    parser.add_argument("address")
    parser.add_argument("--chain", default="sol")
    args = parser.parse_args()

    chain = args.chain
    addr = args.address

    # Token info
    print("[1/3] Fetching token info...")
    info = run_gmgn(["token", "info", "--chain", chain, "--address", addr])
    price = to_f(info.get("price"))
    symbol = info.get("symbol", "?")
    name = info.get("name", "?")

    print(f"\n  Token: {symbol} ({name})")
    print(f"  Price: ${price:.8f}  |  MCap: ${price * to_f(info.get('circulating_supply')):,.0f}")
    print(f"  Holders: {info.get('holder_count', '?')}  |  Age: {info.get('creation_timestamp', '?')}")

    # Top-100 holders
    print("[2/3] Fetching top-100 holders...")
    holders_raw = run_gmgn(["token", "holders", "--chain", chain, "--address", addr,
                            "--limit", "100", "--order-by", "amount_percentage", "--direction", "desc"])
    holders_list = holders_raw.get("list", []) if holders_raw else []

    # Top-100 traders
    print("[3/3] Fetching top-100 traders (by profit)...")
    traders_raw = run_gmgn(["token", "traders", "--chain", chain, "--address", addr,
                            "--limit", "100", "--order-by", "profit", "--direction", "desc"])
    traders_list = traders_raw.get("list", []) if traders_raw else []

    # =====================================================================
    # ANALYSIS
    # =====================================================================
    print(f"\n{'#'*70}")
    print(f"#  P&L DATA VERIFICATION & ANALYSIS")
    print(f"#  Token: {symbol} | Chain: {chain}")
    print(f"#  Holders fetched: {len(holders_list)} | Traders fetched: {len(traders_list)}")
    print(f"{'#'*70}")

    # ---- 1. Field coverage check ----
    print(f"\n{'='*70}")
    print(f"  PART 1: Does gmgn-cli return profit/loss data?")
    print(f"{'='*70}")

    # Check a single wallet to show all fields
    sample = None
    for h in holders_list:
        if h.get("addr_type") != 2:
            sample = h
            break
    if sample:
        pnl_fields = ["profit", "realized_profit", "unrealized_profit",
                      "profit_change", "realized_pnl", "unrealized_pnl",
                      "total_cost", "avg_cost", "avg_sold",
                      "history_bought_cost", "history_sold_income",
                      "buy_volume_cur", "sell_volume_cur", "netflow_usd"]
        print(f"\n  Sample wallet: {short(sample.get('address','?'))}")
        print(f"  {'Field':<30} {'Value':>15} {'Type'}")
        print(f"  {'-'*30} {'-'*15} {'-'*10}")
        for f in pnl_fields:
            val = sample.get(f)
            vtype = type(val).__name__
            if val is None:
                display = "None"
            elif isinstance(val, float):
                display = f"${val:,.2f}"
            elif isinstance(val, int):
                display = f"{val}"
            else:
                display = str(val)[:40]
            print(f"  {f:<30} {display:>15}  ({vtype})")

    # ---- 2. Aggregate PnL Analysis ----
    holders_r = analyze_wallets(holders_list, "Holders (top-100 by supply)")
    traders_r = analyze_wallets(traders_list, "Traders (top-100 by profit)")

    print(f"\n{'='*70}")
    print(f"  PART 2: PnL Aggregate Statistics")
    print(f"{'='*70}")

    print_field_analysis(holders_r, "HOLDERS")
    print_field_analysis(traders_r, "TRADERS")

    print_pnl_summary(holders_r, "HOLDERS (top-100 by supply %)")
    print_pnl_summary(traders_r, "TRADERS (top-100 by profit)")

    # ---- 3. Top/Bottom ----
    print(f"\n{'='*70}")
    print(f"  PART 3: Top Earners & Losers")
    print(f"{'='*70}")
    print_top_bottom(holders_list, "HOLDERS", price, top_n=15)
    print_top_bottom(traders_list, "TRADERS", price, top_n=15)

    # ---- 4. Distribution ----
    print(f"\n{'='*70}")
    print(f"  PART 4: P&L Distribution")
    print(f"{'='*70}")
    print_distribution(holders_list, "HOLDERS")
    print_distribution(traders_list, "TRADERS")

    # ---- 5. Key Findings ----
    print(f"\n{'='*70}")
    print(f"  PART 5: Key Findings")
    print(f"{'='*70}")
    print(f"""
  GMGN CLI P&L Data Availability:
  ┌────────────────────────────────────────────────────────────────────┐
  │ profit:           YES - Total P&L per wallet                      │
  │ realized_profit:  YES - Profit from completed sells                │
  │ unrealized_profit: YES - Floating P&L on current holdings          │
  │ profit_change:    YES - Profit ratio (profit/total_cost)            │
  │ total_cost:       YES - Cost basis including fees                  │
  │ avg_cost / avg_sold: YES - Average buy/sell price per token        │
  │ history_bought_cost: YES - Total USD spent on buys                 │
  │ history_sold_income: YES - Total USD received from sells            │
  │ buy_volume_cur / sell_volume_cur: YES - Buy/sell USD volume        │
  │ netflow_usd:      YES - Net USD flow                               │
  └────────────────────────────────────────────────────────────────────┘

  HOLDERS (top-100 by supply %):
    Total wallets analyzed: {holders_r['total']} (pool: {holders_r['pool_addresses']})
    Still holding: {holders_r['wallets_with_balance']} | Exited: {holders_r['wallets_exited']}
    Total P&L: ${holders_r['total_profit']:,.2f}
      - Realized:  ${holders_r['total_realized_profit']:,.2f}
      - Unrealized: ${holders_r['total_unrealized_profit']:,.2f}
    Winners: {holders_r['profitable_count']} (sum: ${holders_r['sum_profitable_profit']:,.2f})
    Losers:  {holders_r['losing_count']} (sum: ${holders_r['sum_losing_loss']:,.2f})
    Win Rate: {holders_r['profitable_count']/(holders_r['profitable_count']+holders_r['losing_count'])*100 if (holders_r['profitable_count']+holders_r['losing_count']) > 0 else 0:.1f}%

  TRADERS (top-100 by profit):
    Total wallets analyzed: {traders_r['total']}
    Still holding: {traders_r['wallets_with_balance']} | Exited: {traders_r['wallets_exited']}
    Total P&L: ${traders_r['total_profit']:,.2f}
      - Realized:  ${traders_r['total_realized_profit']:,.2f}
      - Unrealized: ${traders_r['total_unrealized_profit']:,.2f}
    Winners: {traders_r['profitable_count']} (sum: ${traders_r['sum_profitable_profit']:,.2f})
    Losers:  {traders_r['losing_count']} (sum: ${traders_r['sum_losing_loss']:,.2f})
    Win Rate: {traders_r['profitable_count']/(traders_r['profitable_count']+traders_r['losing_count'])*100 if (traders_r['profitable_count']+traders_r['losing_count']) > 0 else 0:.1f}%
""")


if __name__ == "__main__":
    main()
