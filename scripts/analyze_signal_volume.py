#!/usr/bin/env python3
"""Analyze 5m/15m volume at signal time for success vs failure."""
import sys, requests, time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import Counter

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from db_client import db_op

tz = timezone(timedelta(hours=8))
KLINE_URL = "https://dquery.sintral.io/u-kline/v1/k-line/candles"
H = {"Accept-Encoding": "identity", "User-Agent": "binance-web3/1.1 (Skill)"}


def analyze_volume_for_token(addr, et, resolution, bars=18):
    params = {"address": addr, "platform": "solana", "interval": resolution, "limit": bars, "pm": "p"}
    try:
        resp = requests.get(KLINE_URL, params=params, headers=H, timeout=20)
        candles = resp.json().get("data", [])
        if not candles:
            return None

        sig_idx = len(candles) // 2
        for j, c in enumerate(candles):
            ts = int(c[5] / 1000) if c[5] > 10**10 else int(c[5])
            if abs(ts - (et or 0)) < int(resolution.replace("min", "") or "5") * 60:
                sig_idx = j
                break

        pre = [float(candles[j][4]) for j in range(max(0, sig_idx - 3), sig_idx)]
        post = [float(candles[j][4]) for j in range(sig_idx + 1, min(len(candles), sig_idx + 4))]
        pre_avg = sum(pre) / len(pre) if pre else 0
        post_avg = sum(post) / len(post) if post else 0

        return {"pre_avg": pre_avg, "post_avg": post_avg}
    except:
        return None


def main():
    def run(conn):
        cur = conn.cursor()
        cur.execute("""
            SELECT address, symbol, max_gain_pct, current_return_pct,
                   current_mcap, signal_type, event_ts, result
            FROM bottom_push_performance
            WHERE analysis_date = '2026-05-18'
            ORDER BY event_ts
        """)
        rows = cur.fetchall()

        results = []
        for i, r in enumerate(rows):
            addr = r[0]; sym = r[1]; gain = r[2] or 0; cur = r[3] or 0
            mcap = r[4] or 0; et = r[6]; result = r[7]
            tstr = datetime.fromtimestamp(et, tz).strftime("%H:%M") if et else "?"

            vol5 = analyze_volume_for_token(addr, et, "5min")
            vol15 = analyze_volume_for_token(addr, et, "15min")

            delta5 = 0
            delta15 = 0
            if vol5 and vol5["pre_avg"] > 0:
                delta5 = (vol5["post_avg"] - vol5["pre_avg"]) / vol5["pre_avg"] * 100
            if vol15 and vol15["pre_avg"] > 0:
                delta15 = (vol15["post_avg"] - vol15["pre_avg"]) / vol15["pre_avg"] * 100

            results.append({
                "sym": sym, "t": tstr, "gain": gain, "cur": cur, "mcap": mcap,
                "v5_pre": vol5["pre_avg"] if vol5 else 0,
                "v5_post": vol5["post_avg"] if vol5 else 0,
                "v5_delta": delta5,
                "v15_pre": vol15["pre_avg"] if vol15 else 0,
                "v15_post": vol15["post_avg"] if vol15 else 0,
                "v15_delta": delta15,
                "result": result,
            })
            if (i + 1) % 5 == 0:
                time.sleep(0.2)

        fm = lambda v: "${:,.0f}".format(v)

        for group in ["成功", "失败"]:
            items = [x for x in results if x["result"] == group]
            if not items:
                continue

            print("\n" + "=" * 85)
            print("  {} ({})".format(group, len(items)))
            print("=" * 85)
            print("{:<6} {:<14} {:>10} {:>10} {:>8} {:>10} {:>10} {:>8} {:>6} {:>6}".format(
                "Time", "Symbol", "5m-PreVol", "5m-PostVol", "5m-Chg%",
                "15m-PreVol", "15m-PostVol", "15m-Chg%", "Gain", "Cur"))
            print("-" * 85)

            for r in items:
                v5d = "+{:.0f}%".format(r["v5_delta"]) if r["v5_delta"] > 0 else "{:.0f}%".format(r["v5_delta"])
                v15d = "+{:.0f}%".format(r["v15_delta"]) if r["v15_delta"] > 0 else "{:.0f}%".format(r["v15_delta"])
                print("{:<6} ${:<13} {:>10} {:>10} {:>8} {:>10} {:>10} {:>8} {:>+5.0f}% {:>+5.0f}%".format(
                    r["t"], r["sym"],
                    fm(r["v5_pre"]), fm(r["v5_post"]), v5d,
                    fm(r["v15_pre"]), fm(r["v15_post"]), v15d,
                    r["gain"], r["cur"]))

            # Volume change stats
            v5_deltas = [r["v5_delta"] for r in items]
            v15_deltas = [r["v15_delta"] for r in items]
            avg5 = sum(v5_deltas) / len(v5_deltas)
            avg15 = sum(v15_deltas) / len(v15_deltas)

            vol_up_5m = sum(1 for d in v5_deltas if d > 20)
            vol_down_5m = sum(1 for d in v5_deltas if d < -20)
            vol_up_15m = sum(1 for d in v15_deltas if d > 20)
            vol_down_15m = sum(1 for d in v15_deltas if d < -20)

            print("\n  5m量能变化: 均{:+.0f}% | 放量>20%: {}个 | 缩量<-20%: {}个".format(avg5, vol_up_5m, vol_down_5m))
            print("  15m量能变化: 均{:+.0f}% | 放量>20%: {}个 | 缩量<-20%: {}个".format(avg15, vol_up_15m, vol_down_15m))

    db_op(run)


if __name__ == "__main__":
    main()
