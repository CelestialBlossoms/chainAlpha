"""
Wallet P&L analysis: profitable wallets, losing wallets, exited wallets.

Usage:
    D:/software/anaconda/envs/py312/python.exe scripts/analyze_wallet_pnl.py <CA>
    D:/software/anaconda/envs/py312/python.exe scripts/analyze_wallet_pnl.py <CA> --limit 50
"""
import argparse, json, shutil, subprocess, sys

CHAIN = "sol"


def to_f(v, d=0.0):
    try:
        if v in (None, ""): return d
        return float(v)
    except: return d


def gmgn_prefix():
    exe = shutil.which("gmgn-cli") or shutil.which("gmgn-cli.cmd") or "gmgn-cli"
    if str(exe).lower().endswith(".ps1"):
        return ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", exe]
    return [exe]


def run_gmgn(args, timeout=60):
    cmd = [*gmgn_prefix(), *args, "--raw"]
    r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout)
    if r.returncode != 0: return None
    try: return json.loads(r.stdout)
    except: return None


def short_addr(w):
    return f"{w[:6]}...{w[-4:]}" if len(w) > 12 else w


def compact_usd(v):
    s = "-" if v < 0 else ""; v = abs(v)
    if v >= 1_000_000: return f"{s}${v/1_000_000:.2f}M"
    if v >= 1_000: return f"{s}${v/1_000:.1f}K"
    return f"{s}${v:,.0f}"


def fetch_traders(address, limit=50):
    all_t = {}
    for ob in ("profit", "buy_volume_cur", "sell_volume_cur", "unrealized_profit"):
        data = run_gmgn(["token", "traders", "--chain", CHAIN, "--address", address,
                         "--limit", str(limit), "--order-by", ob, "--direction", "desc"])
        if not isinstance(data, dict): continue
        for row in data.get("list", []):
            if not isinstance(row, dict): continue
            w = str(row.get("address") or "").strip()
            if w and w not in all_t: all_t[w] = row
    return list(all_t.values())


def fetch_holders(address, limit=50):
    data = run_gmgn(["token", "holders", "--chain", CHAIN, "--address", address,
                     "--limit", str(limit), "--order-by", "amount_percentage", "--direction", "desc"])
    if not isinstance(data, dict): return []
    return [r for r in data.get("list", []) if isinstance(r, dict)]


def analyze(address, limit=50):
    traders = fetch_traders(address, limit)
    holders = fetch_holders(address, limit)
    seen = set(); records = []
    for row in traders + holders:
        w = str(row.get("address") or "").strip()
        if not w or w in seen: continue
        seen.add(w)
        records.append({
            "wallet": w,
            "buy_vol": to_f(row.get("buy_volume_cur")),
            "sell_vol": to_f(row.get("sell_volume_cur")),
            "profit": to_f(row.get("profit")),
            "realized": to_f(row.get("realized_profit")),
            "unrealized": to_f(row.get("unrealized_profit")),
            "cost": to_f(row.get("total_cost") or row.get("history_bought_cost")),
            "hold_pct": to_f(row.get("amount_percentage")),
            "usd_value": to_f(row.get("usd_value")),
            "sell_pct": to_f(row.get("sell_amount_percentage")),
            "avg_cost": to_f(row.get("avg_cost")),
            "tags": row.get("maker_token_tags") or row.get("tags") or [],
        })

    profitable = sorted([r for r in records if r["profit"] > 0], key=lambda x: -x["profit"])
    losing = sorted([r for r in records if r["profit"] < 0], key=lambda x: x["profit"])
    exited_profit = sorted([r for r in records if r["sell_pct"] >= 0.95 and r["realized"] > 0], key=lambda x: -x["realized"])
    holding = sorted([r for r in records if r["unrealized"] > 0], key=lambda x: -x["unrealized"])

    return {
        "total": len(records), "profitable": len(profitable), "losing": len(losing),
        "exited_profit": len(exited_profit), "holding_profit": len(holding),
        "total_profit": sum(r["profit"] for r in records),
        "total_realized": sum(r["realized"] for r in records),
        "total_unrealized": sum(r["unrealized"] for r in records),
        "total_buy": sum(r["buy_vol"] for r in records),
        "total_sell": sum(r["sell_vol"] for r in records),
        "profitable_list": profitable[:12], "losing_list": losing[:10],
        "exited_list": exited_profit[:8], "holding_list": holding[:8],
    }


def print_report(r, addr):
    print(f"\n{'='*70}")
    print(f"  Wallet P&L: {addr[:24]}...")
    print(f"{'='*70}")
    print(f"  Wallets: {r['total']} | +{r['profitable']} | -{r['losing']} | Exit: {r['exited_profit']} | Holding: {r['holding_profit']}")
    print(f"  Total Profit: {compact_usd(r['total_profit'])} | Realized: {compact_usd(r['total_realized'])} | Unrealized: {compact_usd(r['total_unrealized'])}")
    print(f"  Total Buy: {compact_usd(r['total_buy'])} | Total Sell: {compact_usd(r['total_sell'])}")

    if r['profitable_list']:
        print(f"\n  [TOP 盈利]")
        print(f"  {'Wallet':<14} {'Profit':>10} {'P/L%':>8} {'Realized':>10} {'Unrealized':>10} {'Hold%':>7} {'Tags'}")
        print(f"  {'-'*70}")
        for x in r['profitable_list'][:10]:
            pnl = x['profit']/x['cost']*100 if x['cost'] > 0 else 0
            tags = ",".join(x['tags'][:2]) if x['tags'] else "-"
            print(f"  {short_addr(x['wallet']):<14} {compact_usd(x['profit']):>10} {pnl:>7.1f}% {compact_usd(x['realized']):>10} {compact_usd(x['unrealized']):>10} {x['hold_pct']*100:>6.2f}% {tags}")

    if r['losing_list']:
        print(f"\n  [TOP 亏损]")
        for x in r['losing_list'][:8]:
            pnl = x['profit']/x['cost']*100 if x['cost'] > 0 else 0
            print(f"  {short_addr(x['wallet']):<14} {compact_usd(x['profit']):>10} {pnl:>7.1f}% sold={x['sell_pct']*100:.0f}%")

    if r['exited_list']:
        print(f"\n  [已退出且盈利]")
        for x in r['exited_list'][:5]:
            print(f"  {short_addr(x['wallet']):<14} realized={compact_usd(x['realized'])}")

    if r['holding_list']:
        print(f"\n  [持仓浮盈]")
        for x in r['holding_list'][:5]:
            print(f"  {short_addr(x['wallet']):<14} unrealized={compact_usd(x['unrealized'])} hold={x['hold_pct']*100:.2f}%")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("ca"); p.add_argument("--limit", type=int, default=50)
    args = p.parse_args()
    r = analyze(args.ca, args.limit)
    print_report(r, args.ca)


if __name__ == "__main__":
    main()
