#!/usr/bin/env python3
"""Compare 1m vs 5m K-line volume patterns for success vs failure."""
import sys, requests, time
from pathlib import Path
from collections import Counter
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from db_client import db_op

KLINE_URL = "https://dquery.sintral.io/u-kline/v1/k-line/candles"
H = {"Accept-Encoding": "identity", "User-Agent": "binance-web3/1.1 (Skill)"}

def fetch_kline(addr, et, resolution, bars=36):
    params = {"address": addr, "platform": "solana", "interval": resolution, "limit": bars, "pm": "p"}
    try:
        resp = requests.get(KLINE_URL, params=params, headers=H, timeout=20)
        candles = resp.json().get("data", [])
        if not candles or len(candles) < 6:
            return None
        sig_idx = len(candles) // 2
        step = int(resolution.replace("min", "")) * 60
        for j, c in enumerate(candles):
            ts = int(c[5] / 1000) if c[5] > 10**10 else int(c[5])
            if abs(ts - (et or 0)) < step * 2:
                sig_idx = j; break
        return candles, sig_idx
    except:
        return None


def main():
    def run(conn):
        cur = conn.cursor()
        cur.execute("""SELECT address, symbol, max_gain_pct, current_return_pct,
            entry_drawdown_pct, event_ts, current_mcap, result
            FROM bottom_push_performance
            WHERE analysis_date = '2026-05-19'
            ORDER BY event_ts""")
        rows = cur.fetchall()

        fm = lambda v: "${:.0f}".format(v)
        results = []

        for i, r in enumerate(rows):
            addr = r[0]; sym = r[1]; gain = r[2] or 0; cur = r[3] or 0
            dd = r[4] or 0; et = r[5]; mcap = r[6] or 0; result = r[7]

            v1m = fetch_kline(addr, et, "1min", bars=60)
            v5m = fetch_kline(addr, et, "5min", bars=24)

            if not v1m or not v5m:
                continue

            c1m, i1m = v1m; c5m, i5m = v5m

            # 1m volume: pre 3 bars vs post 3 bars
            pre1 = [float(c1m[j][4]) for j in range(max(0, i1m - 3), i1m)]
            post1 = [float(c1m[j][4]) for j in range(i1m + 1, min(len(c1m), i1m + 4))]
            pre1_avg = sum(pre1) / len(pre1) if pre1 else 0
            post1_avg = sum(post1) / len(post1) if post1 else 0
            r1 = post1_avg / pre1_avg if pre1_avg > 0 else 0

            # 5m volume: pre 3 bars vs post 3 bars
            pre5 = [float(c5m[j][4]) for j in range(max(0, i5m - 3), i5m)]
            post5 = [float(c5m[j][4]) for j in range(i5m + 1, min(len(c5m), i5m + 4))]
            pre5_avg = sum(pre5) / len(pre5) if pre5 else 0
            post5_avg = sum(post5) / len(post5) if post5 else 0
            r5 = post5_avg / pre5_avg if pre5_avg > 0 else 0

            # 1m K-line body (signal bar)
            sig1m = c1m[i1m]
            body1m = abs(float(sig1m[3]) - float(sig1m[0])) / float(sig1m[0]) * 100 if float(sig1m[0]) > 0 else 0
            sig5m = c5m[i5m]
            body5m = abs(float(sig5m[3]) - float(sig5m[0])) / float(sig5m[0]) * 100 if float(sig5m[0]) > 0 else 0

            # Wick ratio (upper wick / body)
            wick1m = 0
            if body1m > 0:
                h = float(sig1m[1]); l = float(sig1m[2])
                real_body_high = max(float(sig1m[3]), float(sig1m[0]))
                wick1m = (h - real_body_high) / body1m if body1m > 0 else 0

            wick5m = 0
            if body5m > 0:
                h = float(sig5m[1])
                real_body_high = max(float(sig5m[3]), float(sig5m[0]))
                wick5m = (h - real_body_high) / body5m if body5m > 0 else 0

            results.append({
                "sym": sym, "gain": gain, "cur": cur, "dd": dd,
                "pre1": pre1_avg, "post1": post1_avg, "r1": r1,
                "pre5": pre5_avg, "post5": post5_avg, "r5": r5,
                "body1m": body1m, "body5m": body5m,
                "wick1m": wick1m, "wick5m": wick5m,
                "result": result, "mcap": mcap,
            })

            if (i + 1) % 5 == 0:
                time.sleep(0.2)

        # Analysis by result
        for group in ["成功", "失败"]:
            items = [r for r in results if r["result"] == group]
            if not items:
                continue

            print("\n" + "=" * 90)
            print("  {} ({})".format(group, len(items)))
            print("=" * 90)
            print("{:<14} {:>6} {:>8} {:>8} {:>6} {:>8} {:>8} {:>6} {:>6} {:>6} {:>6}".format(
                "Symbol", "gain", "1m-pre", "1m-post", "1mR", "5m-pre", "5m-post", "5mR",
                "1mB%", "5mB%", "1mWk"))
            print("-" * 90)

            for r in items:
                r1s = "{:.1f}x".format(r["r1"]) if r["r1"] else "-"
                r5s = "{:.1f}x".format(r["r5"]) if r["r5"] else "-"
                print("{:<14} {:>+5.0f}% {:>8} {:>8} {:>6} {:>8} {:>8} {:>6} {:>5.1f}% {:>5.1f}% {:>5.1f}".format(
                    r["sym"], r["gain"],
                    fm(r["pre1"]), fm(r["post1"]), r1s,
                    fm(r["pre5"]), fm(r["post5"]), r5s,
                    r["body1m"], r["body5m"], r["wick1m"]))

            # Stats
            r1_vals = [r["r1"] for r in items if r["r1"] > 0]
            r5_vals = [r["r5"] for r in items if r["r5"] > 0]
            b1m = [r["body1m"] for r in items if r["body1m"] > 0]
            b5m = [r["body5m"] for r in items if r["body5m"] > 0]
            w1m = [r["wick1m"] for r in items if r["wick1m"] > 0]

            med = lambda arr: sorted(arr)[len(arr) // 2] if arr else 0
            avg = lambda arr: sum(arr) / len(arr) if arr else 0

            print("\n  1m量比: 中位{:.1f}x 均{:.1f}x | 放量>1.3x: {} 缩量<0.5x: {}".format(
                med(r1_vals), avg(r1_vals),
                sum(1 for r in items if r["r1"] > 1.3),
                sum(1 for r in items if 0 < r["r1"] < 0.5)))
            print("  5m量比: 中位{:.1f}x 均{:.1f}x | 放量>1.3x: {} 缩量<0.5x: {}".format(
                med(r5_vals), avg(r5_vals),
                sum(1 for r in items if r["r5"] > 1.3),
                sum(1 for r in items if 0 < r["r5"] < 0.5)))
            print("  1m实体: 中位{:.1f}% | 5m实体: 中位{:.1f}%".format(med(b1m), med(b5m)))
            print("  1m上影线/实体: 中位{:.1f}".format(med(w1m)))

            # Divergence: 1m UP but 5m DOWN
            div = sum(1 for r in items if r["r1"] > 1.3 and r["r5"] < 0.7)
            agree = sum(1 for r in items if r["r1"] > 1.3 and r["r5"] > 1.3)
            print("  1m放量+5m缩量(分歧): {} | 1m+5m同放量(共振): {}".format(div, agree))

    db_op(run)


if __name__ == "__main__":
    main()
