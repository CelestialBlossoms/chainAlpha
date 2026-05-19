#!/usr/bin/env python3
"""Compare healthy deep V vs dead cat bounce volume patterns."""
import sys, requests, time
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from db_client import db_op
from datetime import datetime, timezone, timedelta
tz = timezone(timedelta(hours=8))

KLINE_URL = "https://dquery.sintral.io/u-kline/v1/k-line/candles"
H = {"Accept-Encoding": "identity", "User-Agent": "binance-web3/1.1 (Skill)"}


def main():
    def run(conn):
        cur = conn.cursor()
        cur.execute("""
            SELECT address, symbol, max_gain_pct, current_return_pct,
                   entry_drawdown_pct, event_ts, current_mcap, result
            FROM bottom_push_performance
            WHERE analysis_date = '2026-05-18' AND entry_drawdown_pct < -10
            ORDER BY entry_drawdown_pct
        """)
        rows = cur.fetchall()

        fm = lambda v: "${:.0f}".format(v)

        vol_results = []
        print("=" * 95)
        print("  Deep V vs Dead Cat Bounce - Volume Analysis")
        print("=" * 95)
        print("{:>6} {:<14} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} {:>10}".format(
            "gain", "Symbol", "pre_v", "sig_v", "post1_v", "post2_v", "post3_v", "Trend", "Verdict"))
        print("-" * 95)

        for i, r in enumerate(rows):
            addr = r[0]; sym = r[1]; gain = r[2] or 0
            dd = r[4] or 0; et = r[5]; mcap = r[6] or 0; result = r[7]

            try:
                params = {"address": addr, "platform": "solana", "interval": "5min", "limit": 18, "pm": "p"}
                resp = requests.get(KLINE_URL, params=params, headers=H, timeout=20)
                candles = resp.json().get("data", [])
                if not candles or len(candles) < 6:
                    continue

                sig_idx = len(candles) // 2
                for j, c in enumerate(candles):
                    ts = int(c[5] / 1000) if c[5] > 10**10 else int(c[5])
                    if abs(ts - (et or 0)) < 600:
                        sig_idx = j
                        break

                pre_vols = [float(candles[j][4]) for j in range(max(0, sig_idx - 3), sig_idx)]
                pre_avg = sum(pre_vols) / len(pre_vols) if pre_vols else 0
                sig_vol = float(candles[sig_idx][4]) if sig_idx < len(candles) else 0
                post_vols = [float(candles[j][4]) for j in range(sig_idx + 1, min(len(candles), sig_idx + 4))]
                post_avg = sum(post_vols) / len(post_vols) if post_vols else 0

                if post_avg > pre_avg * 1.3:
                    trend = "UP"
                elif post_avg < pre_avg * 0.5:
                    trend = "DOWN"
                else:
                    trend = "FLAT"

                if result == "成功":
                    verdict = "Healthy V" if trend != "DOWN" else "Weak V"
                else:
                    verdict = "DCB" if trend == "DOWN" else "Weak DCB"

                p1 = post_vols[0] if len(post_vols) > 0 else 0
                p2 = post_vols[1] if len(post_vols) > 1 else 0
                p3 = post_vols[2] if len(post_vols) > 2 else 0

                print("{:>+5.0f}% ${:<13} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} {:>10}".format(
                    gain, sym, fm(pre_avg), fm(sig_vol), fm(p1), fm(p2), fm(p3), trend, verdict))

                vol_results.append({
                    "sym": sym, "gain": gain, "dd": dd,
                    "pre_avg": pre_avg, "post_avg": post_avg,
                    "trend": trend, "verdict": verdict, "result": result,
                })

            except Exception as e:
                print("{:>+5.0f}% ${:<13} ERR: {}".format(gain, sym, str(e)[:40]))

            if (i + 1) % 5 == 0:
                time.sleep(0.2)

        # Summary
        print("\n" + "=" * 60)
        print("  Summary")
        print("=" * 60)

        for label in ["Healthy V", "DCB", "Weak V", "Weak DCB"]:
            items = [r for r in vol_results if r["verdict"] == label]
            if items:
                print("\n  [{}] ({})".format(label, len(items)))
                for r in items:
                    print("    ${:<14s} gain={:+.0f}% dd={:+.0f}% pre_vol=${:.0f} post_vol=${:.0f} trend={}".format(
                        r["sym"], r["gain"], r["dd"], r["pre_avg"], r["post_avg"], r["trend"]))

        # Key metric
        succ_up = sum(1 for r in vol_results if r["result"] == "成功" and r["trend"] == "UP")
        fail_down = sum(1 for r in vol_results if r["result"] == "失败" and r["trend"] == "DOWN")
        print("\n  Volume UP + Success: {} | Volume DOWN + Failed: {}".format(succ_up, fail_down))

    db_op(run)


if __name__ == "__main__":
    main()
