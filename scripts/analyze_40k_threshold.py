#!/usr/bin/env python3
"""Check if failed tokens briefly crossed $40K threshold then fell back."""
import sys, requests, time
from pathlib import Path
from datetime import datetime, timezone, timedelta

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from db_client import db_op
from config import GMGN_API_KEY

BINANCE_KLINE_URL = "https://dquery.sintral.io/u-kline/v1/k-line/candles"
HEADERS = {"Accept-Encoding": "identity", "User-Agent": "binance-web3/1.1 (Skill)"}
tz = timezone(timedelta(hours=8))


def fetch_kline(address, bars=48):
    params = {"address": address, "platform": "solana", "interval": "15min", "limit": bars, "pm": "m"}
    try:
        r = requests.get(BINANCE_KLINE_URL, params=params, headers=HEADERS, timeout=30)
        if r.ok:
            raw = (r.json().get("data") or [])
            return [{"ts": int(i[5] / 1000) if i[5] > 10**10 else int(i[5]),
                     "o": float(i[0]), "h": float(i[1]), "l": float(i[2]),
                     "c": float(i[3]), "v": float(i[4])} for i in raw if isinstance(i, list) and len(i) >= 6]
    except Exception as e:
        print(f"  K-line failed: {e}")
    return []


def main():
    def run(conn):
        cur = conn.cursor()
        cur.execute("""
            SELECT address, symbol, max_gain_pct, current_return_pct, current_mcap,
                   signal_type, event_ts, result
            FROM bottom_push_performance
            WHERE analysis_date = '2026-05-18'
            ORDER BY event_ts
        """)
        rows = cur.fetchall()

        failed = [r for r in rows if r[7] == "失败"]
        success = [r for r in rows if r[7] == "成功"]

        print("Today: {} success, {} failed".format(len(success), len(failed)))
        print()

        # Analyze failed tokens
        for label, tokens in [("失败", failed), ("成功", success)]:
            print("=" * 70)
            print("  {}组 K线分析".format(label))
            print("=" * 70)

            below_40k_before = 0
            crossed_40k = 0
            stayed_above = 0
            details = []

            for i, r in enumerate(tokens[:12]):  # sample first 12
                addr = r[0]; sym = r[1]; gain = r[2] or 0; cur_ret = r[3] or 0
                mcap = r[4] or 0; et = r[6]
                t = datetime.fromtimestamp(et, tz).strftime("%H:%M") if et else "?"

                candles = fetch_kline(addr, bars=48)
                if not candles:
                    continue

                # Find signal bar closest to event_ts
                sig_idx = 0
                for j, c in enumerate(candles):
                    if abs(c["ts"] - (et or 0)) < 900:
                        sig_idx = j
                        break

                # Pre-signal (before signal): how long above $40K?
                pre = candles[:sig_idx] if sig_idx > 0 else []
                post = candles[sig_idx:] if sig_idx < len(candles) else []

                pre_above_40k = [(c["h"] + c["l"]) / 2 >= 40000 for c in pre]
                post_above_40k = [(c["h"] + c["l"]) / 2 >= 40000 for c in post]

                pre_bars_above = sum(pre_above_40k) if pre else 0
                post_bars_above = sum(post_above_40k) if post else 0
                total_bars = len(pre) + len(post)

                # Peak mcap in K-line
                all_highs = [c["h"] for c in candles]
                peak_mcap = max(all_highs) if all_highs else 0
                signal_mcap = (candles[sig_idx]["h"] + candles[sig_idx]["l"]) / 2 if sig_idx < len(candles) else 0
                current_mcap = candles[-1]["c"] if candles else 0

                # Pattern classification
                if pre_bars_above == 0 and post_bars_above <= 2:
                    pattern = "擦边突破40K(刚过门槛)"
                elif pre_bars_above <= 2 and peak_mcap < 60000:
                    pattern = "短暂突破后回落"
                elif pre_bars_above > 5:
                    pattern = "长期在40K以上"
                else:
                    pattern = "其他"

                mcap_str = "${:.0f}K".format(peak_mcap/1e3) if peak_mcap >= 1e3 else "${:.0f}".format(peak_mcap)
                detail = {
                    "sym": sym, "t": t, "gain": gain, "cur": cur_ret, "mcap": mcap,
                    "peak_mcap": peak_mcap, "current_mcap": current_mcap,
                    "pre_above": pre_bars_above, "post_above": post_bars_above,
                    "total_bars": total_bars, "pattern": pattern,
                    "pre_ratio": pre_bars_above/max(1,len(pre))*100,
                }
                details.append(detail)

                print("  {t} ${sym:<14s} gain={gain:+.0f}% cur={cur:+.0f}% peak_mcap={pm} now=${cm:.0f} pre40K={pre}/{ptot}bars({pr:.0f}%) post40K={post} [{pat}]".format(
                    t=t, sym=sym, gain=gain, cur=cur_ret,
                    pm=mcap_str, cm=current_mcap,
                    pre=pre_bars_above, ptot=len(pre),
                    pr=pre_bars_above/max(1,len(pre))*100,
                    post=post_bars_above, pat=pattern))

                if i % 3 == 2:
                    time.sleep(0.2)

            # Summary
            pattern_count = {}
            for d in details:
                p = d["pattern"]
                pattern_count[p] = pattern_count.get(p, 0) + 1

            print()
            print("  模式分布:")
            for p, c in sorted(pattern_count.items(), key=lambda x: -x[1]):
                print("    {}: {}个".format(p, c))

            if details:
                avg_pre = sum(d["pre_ratio"] for d in details) / len(details)
                print("  平均信号前40K以上占比: {:.0f}%".format(avg_pre))

    db_op(run)


if __name__ == "__main__":
    main()
