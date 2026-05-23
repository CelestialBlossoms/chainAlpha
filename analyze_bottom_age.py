"""Analyze token age vs win rate for bottom push signals at -40% entry."""
import time, requests
from collections import defaultdict
from datetime import datetime, timezone, timedelta

BINANCE_URL = "https://dquery.sintral.io/u-kline/v1/k-line/candles"
HEADERS = {"Accept-Encoding": "identity", "User-Agent": "binance-web3/1.1 (Skill)"}
BJT = timezone(timedelta(hours=8))

ENTRY_DD = 0.40
HARD_STOP = 0.75
TRAIL_ACTIVATE = 1.50
TRAIL_DRAWDOWN = 0.85


def fetch_5m(address, limit=60):
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
                candles.append({"ts": ts, "open": float(item[0]), "high": float(item[1]),
                                "low": float(item[2]), "close": float(item[3]), "volume": float(item[4])})
            candles.sort(key=lambda c: c["ts"])
            return candles
    except Exception:
        pass
    return []


def main():
    from db_client import DBClient
    db = DBClient()

    def q(conn):
        cur = conn.cursor()
        cur.execute("""
            SELECT pr.address, pr.symbol, pr.signal_type, pr.event_ts, pr.current_mcap,
                   pr.age_sec, pr.pool_mcap_ratio, pr.liquidity,
                   wt.token_created_at, wt.gmgn_created_at, wt.token_launch_at, wt.gmgn_open_at
            FROM bottom_top100_push_records pr
            LEFT JOIN bottom_watchlist_tokens wt ON wt.ca = pr.address
            WHERE pr.pushed_at::date='2026-05-22' AND pr.chain='sol'
            ORDER BY pr.event_ts
        """)
        return cur.fetchall()

    signals = db.execute(q)
    print(f"Total signals: {len(signals)}")
    print()

    results = []
    for i, sig in enumerate(signals):
        addr, sym, stype, event_ts, mcap, age_sec, pool_ratio, liq, tok_created, gmgn_created, tok_launch, gmgn_open = sig
        event_ts = int(event_ts)
        candles = fetch_5m(addr, limit=60)
        if not candles or len(candles) < 10:
            continue

        # Compute real age: signal time - token creation time
        created_ts = 0
        for ts_val in [gmgn_open, tok_launch, gmgn_created, tok_created]:
            if ts_val and int(ts_val) > 0:
                if int(ts_val) < int(time.time()) + 86400 * 365:
                    created_ts = int(ts_val)
                    break

        if created_ts <= 0 and age_sec and int(age_sec) > 0:
            created_ts = event_ts - int(age_sec)

        real_age_h = (event_ts - created_ts) / 3600 if created_ts > 0 else float(age_sec or 0) / 3600

        # Find signal candle
        sig_idx = 0
        for j, c in enumerate(candles):
            if abs(c["ts"] - event_ts) < 300:
                sig_idx = j
                break
        sig_price = candles[sig_idx]["close"]
        if sig_price <= 0:
            continue

        # Simulate -40% entry
        post = candles[sig_idx:]
        entry_target = sig_price * (1 - ENTRY_DD)
        entry_idx = -1
        for j in range(len(post)):
            if post[j]["low"] <= entry_target:
                entry_idx = j
                break

        if entry_idx < 0:
            results.append({"symbol": sym or addr[:8], "age_h": real_age_h, "mcap": float(mcap or 0),
                            "traded": False, "pnl": 0, "reason": "no_entry"})
            continue

        entry_price = entry_target
        post_entry = post[entry_idx:]
        peak = entry_price
        hit_50 = False
        exit_price = 0
        exit_reason = ""

        for c in post_entry:
            if c["high"] > peak:
                peak = c["high"]
            if not hit_50 and c["high"] >= entry_price * TRAIL_ACTIVATE:
                hit_50 = True
            if hit_50:
                if c["low"] <= peak * TRAIL_DRAWDOWN:
                    exit_price = peak * TRAIL_DRAWDOWN
                    exit_reason = "trailing_stop"
                    break
            else:
                if c["low"] <= entry_price * HARD_STOP:
                    exit_price = entry_price * HARD_STOP
                    exit_reason = "hard_stop"
                    break

        if exit_price == 0:
            exit_price = post_entry[-1]["close"]
            exit_reason = "end_of_data"
            if hit_50:
                exit_reason = "trailing_active_holding"

        pnl = (exit_price - entry_price) / entry_price if entry_price > 0 else 0
        results.append({"symbol": sym or addr[:8], "age_h": real_age_h, "mcap": float(mcap or 0),
                        "traded": True, "pnl": pnl, "reason": exit_reason,
                        "peak": (peak - entry_price) / entry_price})

        if (i + 1) % 10 == 0:
            print(f"  Progress: {i+1}/{len(signals)}...")
        time.sleep(0.25)

    # ---- Analysis ----
    traded = [r for r in results if r["traded"]]
    no_entry = [r for r in results if not r["traded"]]

    age_buckets = [
        (0, 1, "<1h"),
        (1, 3, "1-3h"),
        (3, 6, "3-6h"),
        (6, 12, "6-12h"),
        (12, 24, "12-24h"),
        (24, 48, "24-48h"),
        (48, 72, "48-72h"),
        (72, 168, "3-7天"),
        (168, 720, "7-30天"),
        (720, 99999, ">30天"),
    ]

    print()
    print("=" * 80)
    print(f"  代币年龄 vs 胜率 (-40%入场, {len(traded)}笔交易 / {len(results)}个信号)")
    print("=" * 80)
    print()
    print(f"  {'创建时间':<14} {'信号':>4} {'入场':>4} {'入场率':>6} {'胜':>3} {'胜率':>6} {'累计PnL':>8} {'均PnL':>8}")
    print(f"  {'-'*14} {'-'*4} {'-'*4} {'-'*6} {'-'*3} {'-'*6} {'-'*8} {'-'*8}")

    best_wr = 0
    best_label = ""
    best_pnl = -999

    for lo, hi, label in age_buckets:
        total_in = [r for r in results if lo <= r["age_h"] < hi]
        group = [r for r in traded if lo <= r["age_h"] < hi]
        if not total_in:
            continue
        entry_rate = len(group) / len(total_in)
        wins = [r for r in group if r["pnl"] > 0]
        wr = len(wins) / len(group) if group else 0
        total_pnl = sum(r["pnl"] for r in group) if group else 0
        avg_pnl = total_pnl / len(group) if group else 0

        marker = ""
        if wr > best_wr and len(group) >= 2:
            best_wr = wr
            best_label = label
        if total_pnl > best_pnl and len(group) >= 2:
            best_pnl = total_pnl

        print(f"  {label:<14} {len(total_in):>4} {len(group):>4} {entry_rate:>5.0%} {len(wins):>3} {wr:>5.0%} {total_pnl:>+7.1%} {avg_pnl:>+7.1%}")

    print()
    print(f"  胜率最高: {best_label} ({best_wr:.0%})")

    # Also show peak returns by age
    print()
    print(f"  {'创建时间':<14} {'均峰值':>8} {'最大峰值':>8} {'最大亏损':>8}")
    print(f"  {'-'*14} {'-'*8} {'-'*8} {'-'*8}")
    for lo, hi, label in age_buckets:
        group = [r for r in traded if lo <= r["age_h"] < hi]
        if not group:
            continue
        avg_peak = sum(r.get("peak", 0) for r in group) / len(group)
        max_peak = max(r.get("peak", 0) for r in group)
        max_loss = min(r["pnl"] for r in group)
        print(f"  {label:<14} {avg_peak:>+7.1%} {max_peak:>+7.1%} {max_loss:>+7.1%}")

    # Overall age stats
    ages = [r["age_h"] for r in results]
    print()
    print(f"  全信号年龄: 均值{sum(ages)/len(ages):.1f}h  中位{sorted(ages)[len(ages)//2]:.1f}h  "
          f"最小{min(ages):.1f}h  最大{max(ages):.1f}h")

    # Is 48h the cutoff?
    under_48 = [r for r in traded if r["age_h"] < 48]
    over_48 = [r for r in traded if r["age_h"] >= 48]
    if under_48 and over_48:
        wr_u = sum(1 for r in under_48 if r["pnl"] > 0) / len(under_48)
        wr_o = sum(1 for r in over_48 if r["pnl"] > 0) / len(over_48)
        pnl_u = sum(r["pnl"] for r in under_48)
        pnl_o = sum(r["pnl"] for r in over_48)
        print()
        print(f"  <48h:  {len(under_48)}笔  胜率{wr_u:.0%}  累计PnL={pnl_u:+.1%}  均PnL={pnl_u/len(under_48):+.1%}")
        print(f"  >=48h: {len(over_48)}笔  胜率{wr_o:.0%}  累计PnL={pnl_o:+.1%}  均PnL={pnl_o/len(over_48):+.1%}")

    print()


if __name__ == "__main__":
    main()
