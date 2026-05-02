"""
AMM pool manipulation calculator — exact pump/dump cost analysis.

Usage:
    D:/software/anaconda/envs/py312/python.exe scripts/analyze_pool.py <CA>
    D:/software/anaconda/envs/py312/python.exe scripts/analyze_pool.py <CA> --control 0.5,1,3,5,10,20,30
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


def run_gmgn(args, timeout=30):
    cmd = [*gmgn_prefix(), *args, "--raw"]
    r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout)
    if r.returncode != 0: return None
    try: return json.loads(r.stdout)
    except: return None


def fetch_pool(address):
    info = run_gmgn(["token", "info", "--chain", CHAIN, "--address", address])
    if not info: return None
    pool = info.get("pool", {})
    supply = to_f(info.get("circulating_supply"))
    return {
        "symbol": info.get("symbol", "?"), "supply": supply,
        "price": to_f(info.get("price")),
        "mcap": to_f(info.get("price")) * supply,
        "liquidity": to_f(info.get("liquidity")),
        "exchange": pool.get("exchange", "?"),
        "quote_symbol": pool.get("quote_symbol", "?"),
        "base_reserve": to_f(pool.get("base_reserve")),
        "quote_reserve": to_f(pool.get("quote_reserve")),
    }


def compute(pool, control_pcts):
    X = pool["base_reserve"]; Y = pool["quote_reserve"]
    supply = pool["supply"]; k = X * Y
    sol_usd = 83

    results = []
    for pct in [-99, -95, -90, -80, -50, -30, -20, -10, -5, 5, 10, 20, 30, 50, 100, 200, 500]:
        r = 1 + pct / 100
        if r <= 0: continue
        if pct < 0:
            dx = X * (1/(r**0.5) - 1)  # tokens to sell
            sol_recv = Y - k/(X + dx)
            usd = sol_recv * sol_usd
            results.append({"type": "DUMP", "pct": pct, "tokens": dx, "pct_supply": dx/supply*100, "sol": sol_recv, "usd": usd})
        else:
            dx = X * (1 - 1/(r**0.5))  # tokens to buy
            sol_cost = k/(X - dx) - Y
            usd = sol_cost * sol_usd
            results.append({"type": "PUMP", "pct": pct, "tokens": dx, "pct_supply": dx/supply*100, "sol": sol_cost, "usd": usd})

    # Control scenarios
    ctrl_results = []
    for c_pct in control_pcts:
        ctrl_tokens = supply * c_pct / 100
        ctrl_worth = ctrl_tokens * pool["price"] * sol_usd
        dump_lvls = [f"{r['pct']}%" for r in results if r["type"] == "DUMP" and r["pct_supply"] <= c_pct]
        pump_lvls = [f"{r['pct']}%" for r in results if r["type"] == "PUMP" and r["usd"] <= ctrl_worth]
        ctrl_results.append({"ctrl_pct": c_pct, "tokens": ctrl_tokens, "worth_usd": ctrl_worth,
                             "can_dump": dump_lvls[:5], "can_pump": pump_lvls[:5]})

    return results, ctrl_results


def print_report(pool, results, ctrls):
    print(f"\n{'='*80}")
    print(f"  Pool Analysis: ${pool['symbol']} | {pool['exchange']} | {pool['quote_symbol']}")
    print(f"{'='*80}")
    print(f"  Base: {pool['base_reserve']:,.0f} tokens | Quote: {pool['quote_reserve']:.4f} SOL")
    print(f"  Price: {pool['price']:.10f} SOL | MCap: ${pool['mcap']:,.0f} | Liq: ${pool['liquidity']:,.0f}")
    sol_worth = pool['quote_reserve'] * 83
    pool_share = pool['base_reserve'] / pool['supply'] * 100
    print(f"  Pool SOL worth: ${sol_worth:,.0f} | Pool holds {pool_share:.1f}% of supply")

    print(f"\n  {'Target':<8} {'Tokens':>14} {'%Supply':>8} {'SOL':>10} {'USD':>10}")
    print(f"  {'-'*55}")
    for r in results:
        icon = "SELL" if r["type"] == "DUMP" else "BUY "
        print(f"  {r['pct']:>+5}% {icon} {r['tokens']:>14,.0f} {r['pct_supply']:>7.1f}% {r['sol']:>9.4f} ${r['usd']:>8,.0f}")

    if ctrls:
        print(f"\n  [Control Scenarios]")
        print(f"  {'Hold%':<8} {'Worth':>10} {'Can Dump':>30} {'Can Pump':>30}")
        for c in ctrls:
            print(f"  {c['ctrl_pct']:>5.1f}%  ${c['worth_usd']:>8,.0f}  {', '.join(c['can_dump']):>30}  {', '.join(c['can_pump']):>30}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("ca"); p.add_argument("--control", default="0.5,1,3,5,10,20,30")
    args = p.parse_args()
    pool = fetch_pool(args.ca)
    if not pool or pool["base_reserve"] <= 0: print("No pool data"); sys.exit(1)
    ctrls = [float(x.strip()) for x in args.control.split(",")]
    results, ctrl_results = compute(pool, ctrls)
    print_report(pool, results, ctrl_results)


if __name__ == "__main__":
    main()
