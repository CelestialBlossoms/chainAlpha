"""
Bottom push signal win rate analysis.
Strategy: blind buy at -40% DD from signal, trailing stop at +50% activation with -15% trail, hard stop -25%.
"""
import time
import requests
from collections import defaultdict
from db_client import DBClient

db = DBClient()

BINANCE_URL = "https://dquery.sintral.io/u-kline/v1/k-line/candles"
HEADERS = {"Accept-Encoding": "identity", "User-Agent": "binance-web3/1.1 (Skill)"}

VALID_TYPES = {"abnormal", "new_revival", "quiet_runup", "quiet_breakout", "watchlist_abnormal", "old_surge"}


def fetch_5m(address, limit=96):
    params = {"address": address, "platform": "solana", "interval": "5min", "limit": limit, "pm": "p"}
    try:
        resp = requests.get(BINANCE_URL, params=params, headers=HEADERS, timeout=10)
        if resp.status_code == 200:
            raw = resp.json().get("data", [])
            candles = []
            for item in raw or []:
                if not isinstance(item, list) or len(item) < 6:
                    continue
                ts = int(item[5] / 1000) if item[5] > 10**10 else int(item[5])
                candles.append({
                    "ts": ts, "open": float(item[0]), "high": float(item[1]),
                    "low": float(item[2]), "close": float(item[3]), "volume": float(item[4])
                })
            candles.sort(key=lambda c: c["ts"])
            return candles
    except Exception:
        pass
    return []


def simulate(candles, sig_price, sig_idx):
    """Simulate -40% entry, trailing stop, hard stop strategy."""
    if sig_idx < 0 or sig_idx >= len(candles):
        return {"traded": False, "reason": "bad_idx"}
    post = candles[sig_idx:]
    if len(post) < 5:
        return {"traded": False, "reason": "not_enough_data"}

    target = sig_price * 0.60  # -40% entry
    entry_idx = -1
    for j in range(len(post)):
        if post[j]["low"] <= target:
            entry_idx = j
            break
    if entry_idx < 0:
        return {"traded": False, "reason": "no_entry"}

    entry = target
    post_entry = post[entry_idx:]
    peak = entry
    hit50 = False
    exit_price = 0
    exit_reason = ""

    for c in post_entry:
        if c["high"] > peak:
            peak = c["high"]
        if not hit50 and c["high"] >= entry * 1.50:
            hit50 = True
        if hit50:
            if c["low"] <= peak * 0.85:
                exit_price = peak * 0.85
                exit_reason = "trailing_stop"
                break
        else:
            if c["low"] <= entry * 0.75:
                exit_price = entry * 0.75
                exit_reason = "hard_stop"
                break

    if exit_price == 0:
        exit_price = post_entry[-1]["close"]
        exit_reason = "eod_" + ("trail" if hit50 else "naked")

    pnl = (exit_price - entry) / entry
    return {
        "traded": True, "pnl": pnl, "reason": exit_reason,
        "peak": (peak - entry) / entry
    }


def get_signals(conn, date):
    cur = conn.cursor()
    cur.execute("""
        SELECT pr.address, pr.symbol, pr.signal_type, pr.event_ts,
               pr.current_mcap, pr.pool_mcap_ratio, pr.age_sec, pr.price_change_pct
        FROM bottom_top100_push_records pr
        WHERE pr.pushed_at::date = %s::date AND pr.chain = %s
        ORDER BY pr.event_ts
    """, (date, "sol"))
    return cur.fetchall()


def analyze_date(date):
    sigs = db.execute(lambda conn: get_signals(conn, date))
    print("=" * 70)
    print(f"  {date}: {len(sigs)} signals total")
    print("=" * 70)

    # Signal type breakdown
    by_type = defaultdict(list)
    for s in sigs:
        by_type[s[2]].append(s)
    for st in sorted(by_type.keys()):
        in_scope = "[OK]" if st in VALID_TYPES else "[SKIP]"
        print(f"  [{st}] x{len(by_type[st])} {in_scope}")

    # L1 filter
    filtered = [s for s in sigs if s[2] in VALID_TYPES]
    print(f"\n  L1 filter (valid types): {len(sigs)} -> {len(filtered)}")

    results = []
    skipped_reasons = defaultdict(int)

    for i, s in enumerate(filtered):
        addr, sym, stype, ets, mcap, pr, age_sec, price_pct = s
        mcap_v = float(mcap or 0)
        age_h = float(age_sec or 0) / 3600
        pr_v = float(pr or 0)

        # L2 quality filter
        if mcap_v < 50000 or mcap_v > 300000:
            skipped_reasons["L2_mcap"] += 1
            continue
        if age_h < 1 or age_h > 24:
            skipped_reasons["L2_age"] += 1
            continue
        if pr_v < 0.12:
            skipped_reasons["L2_pool_ratio"] += 1
            continue

        # Fetch K-line
        candles = fetch_5m(addr)
        if not candles or len(candles) < 10:
            skipped_reasons["no_klines"] += 1
            continue

        # Find signal candle
        sig_idx = 0
        for j, c in enumerate(candles):
            if abs(c["ts"] - int(ets)) < 300:
                sig_idx = j
                break

        sig_price = candles[sig_idx]["close"]
        if sig_price <= 0:
            skipped_reasons["bad_price"] += 1
            continue

        trade = simulate(candles, sig_price, sig_idx)
        trade["symbol"] = sym or (addr[:8] if addr else "?")
        trade["stype"] = stype
        trade["mcap"] = mcap_v
        results.append(trade)

        if (i + 1) % 10 == 0:
            print(f"  {date}: {i+1}/{len(filtered)} klines fetched...")
        time.sleep(0.25)

    print(f"\n  L2/K-line skips: {dict(skipped_reasons)}")

    traded = [r for r in results if r["traded"]]
    no_entry = [r for r in results if not r["traded"]]
    print(f"  Signals evaluated: {len(results)}")
    print(f"  No entry (never hit -40%): {len(no_entry)}")
    print(f"  TRADED: {len(traded)}")

    if not traded:
        print("  No trades to analyze.")
        return

    wins = [r for r in traded if r["pnl"] > 0]
    losses = [r for r in traded if r["pnl"] <= 0]
    wr = len(wins) / len(traded) * 100
    avg_pnl = sum(r["pnl"] for r in traded) / len(traded) * 100
    total_pnl = sum(r["pnl"] for r in traded) * 100
    avg_peak = sum(r["peak"] for r in traded) / len(traded) * 100

    print(f"\n  {'='*50}")
    print(f"  RESULTS for {date}")
    print(f"  {'='*50}")
    print(f"  Wins:       {len(wins)}")
    print(f"  Losses:     {len(losses)}")
    print(f"  Win Rate:   {wr:.1f}%")
    print(f"  Avg PnL:    {avg_pnl:+.1f}%")
    print(f"  Total PnL:  {total_pnl:+.1f}%")
    print(f"  Avg Peak:   {avg_peak:+.1f}%")

    # Exit breakdown
    print(f"\n  Exit breakdown:")
    exit_stats = defaultdict(lambda: {"count": 0, "pnl": 0.0})
    for r in traded:
        reason = r["reason"]
        exit_stats[reason]["count"] += 1
        exit_stats[reason]["pnl"] += r["pnl"]
    for reason, d in sorted(exit_stats.items()):
        avg = d["pnl"] / d["count"] * 100 if d["count"] else 0
        print(f"    {reason:<16} x{d['count']:<3}  avg_pnl={avg:>+7.1f}%")

    # By signal type
    print(f"\n  By signal type:")
    by_typ = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": []})
    for r in traded:
        t = r["stype"]
        if r["pnl"] > 0:
            by_typ[t]["wins"] += 1
        else:
            by_typ[t]["losses"] += 1
        by_typ[t]["pnl"].append(r["pnl"])
    for t, d in sorted(by_typ.items()):
        cnt_t = d["wins"] + d["losses"]
        wr_t = d["wins"] / cnt_t * 100 if cnt_t else 0
        avg_t = sum(d["pnl"]) / len(d["pnl"]) * 100 if d["pnl"] else 0
        print(f"    {t:<18} x{cnt_t:<3}  WR={wr_t:>5.1f}%  avg={avg_t:>+7.1f}%")

    # Trade details
    print(f"\n  Trade details (best -> worst):")
    print(f"  {'Symbol':<12} {'Type':<16} {'PnL':>8} {'Peak':>8} {'Exit':<16}")
    print(f"  {'-'*12} {'-'*16} {'-'*8} {'-'*8} {'-'*16}")
    for r in sorted(traded, key=lambda x: -x["pnl"]):
        print(f"  {r['symbol']:<12} {r['stype']:<16} {r['pnl']*100:>+7.1f}% {r['peak']*100:>+7.1f}% {r['reason']:<16}")

    # PnL distribution
    print(f"\n  PnL distribution:")
    buckets = [
        (-1.0, -0.25, "<= -25% (stopped out)"),
        (-0.25, -0.05, "-25% to -5%"),
        (-0.05, 0.05, "-5% to +5% (flat)"),
        (0.05, 0.25, "+5% to +25%"),
        (0.25, 0.50, "+25% to +50%"),
        (0.50, 1.00, "+50% to +100%"),
        (1.00, 2.00, "+100% to +200%"),
        (2.00, 999, "+200%+ (moon)"),
    ]
    for lo, hi, label in buckets:
        in_bucket = [r for r in traded if lo <= r["pnl"] < hi]
        if in_bucket:
            print(f"    {label:<30} x{len(in_bucket)}")


# Run analysis
if __name__ == "__main__":
    # Check which dates have data
    def get_available_dates(conn):
        cur = conn.cursor()
        cur.execute("""
            SELECT pushed_at::date as d, COUNT(*)
            FROM bottom_top100_push_records
            WHERE chain = 'sol'
            GROUP BY pushed_at::date
            ORDER BY d DESC
            LIMIT 5
        """)
        return cur.fetchall()

    dates = db.execute(get_available_dates)
    print("Available dates:")
    for d in dates:
        print(f"  {d[0]}: {d[1]} signals")
    print()

    # Use the 2 most recent dates with data
    target_dates = [str(d[0]) for d in dates[:2]]
    for date in target_dates:
        analyze_date(date)
        print()
