#!/usr/bin/env python3
"""Full 300+ sample trading data analysis - success vs failure."""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from db_client import db_op
from collections import Counter

def main():
    def run(conn):
        cur = conn.cursor()
        cur.execute("""SELECT symbol, max_gain_pct, current_return_pct,
            entry_drawdown_pct, high_to_low_drawdown_pct, time_to_peak_min,
            signal_type, current_mcap, result, risk_tags
            FROM bottom_push_performance ORDER BY entry_drawdown_pct""")
        rows = cur.fetchall()

        # Column indices: 0:sym,1:gain,2:cur,3:dd_e,4:dd_h,5:peak,6:sig,7:mcap,8:result,9:tags
        succ = [r for r in rows if r[8] == "成功"]
        fail = [r for r in rows if r[8] == "失败"]
        med = lambda arr: sorted(arr)[len(arr)//2] if arr else 0
        avg = lambda arr: sum(arr)/len(arr) if arr else 0

        print("Total: {} (Success: {} Failed: {})".format(len(rows), len(succ), len(fail)))
        print()

        # ===== 1. DD vs SUCCESS RATE =====
        print("=" * 70)
        print("1. DD vs Success Rate and Expected Return")
        print("=" * 70)
        print("{:>14} {:>5} {:>5} {:>7} {:>8} {:>8} {:>8}".format(
            "DD range", "Succ", "Fail", "Rate", "SuccGain", "FailLoss", "Expected"))
        print("-" * 60)

        for lo, hi, lab in [(0,-5,"0~-5%"),(-5,-10,"-5~-10%"),(-10,-15,"-10~-15%"),
                            (-15,-20,"-15~-20%"),(-20,-25,"-20~-25%"),(-25,-30,"-25~-30%"),
                            (-30,-40,"-30~-40%"),(-40,-60,"-40~-60%"),(-99,-60,"<-60%")]:
            s = [r for r in succ if lo >= (r[3] or 0) > hi]
            f = [r for r in fail if lo >= (r[3] or 0) > hi]
            total = len(s) + len(f)
            if total < 3:
                continue
            rate = len(s) / total * 100
            avg_s = avg([r[1] or 0 for r in s]) if s else 0
            avg_f = avg([r[2] or 0 for r in f]) if f else 0
            ev = rate / 100 * avg_s + (1 - rate / 100) * avg_f
            print("{:>14} {:>5} {:>5} {:>6.0f}% {:>+7.0f}% {:>+7.0f}% {:>+7.0f}%".format(
                lab, len(s), len(f), rate, avg_s, avg_f, ev))

        # ===== 2. MCAP vs SUCCESS =====
        print()
        print("=" * 70)
        print("2. MCap vs Success Rate")
        print("=" * 70)
        for lo, hi, lab in [(0,50,"<$50K"),(50,80,"$50-80K"),(80,120,"$80-120K"),
                            (120,200,"$120-200K"),(200,500,"$200-500K"),(500,9999,">$500K")]:
            s = [r for r in succ if lo * 1000 <= (r[7] or 0) < hi * 1000]
            f = [r for r in fail if lo * 1000 <= (r[7] or 0) < hi * 1000]
            total = len(s) + len(f)
            if total < 3:
                continue
            rate = len(s) / total * 100
            avg_s = avg([r[1] or 0 for r in s]) if s else 0
            print("  {:>10s}: Succ{:>3} Fail{:>3} Rate{:.0f}% AvgGain{:+.0f}%".format(
                lab, len(s), len(f), rate, avg_s))

        # ===== 3. PEAK TIME =====
        print()
        print("=" * 70)
        print("3. Peak Time Distribution")
        print("=" * 70)
        for lo, hi, lab in [(0,5,"<=5min"),(5,30,"5-30m"),(30,60,"30-60m"),
                            (60,120,"1-2h"),(120,240,"2-4h"),(240,480,"4-8h"),(480,9999,">8h")]:
            s = sum(1 for r in succ if lo <= (r[5] or 0) < hi)
            f = sum(1 for r in fail if lo <= (r[5] or 0) < hi)
            total = s + f
            if total < 3:
                continue
            rate = s / total * 100
            bar = "#" * int(rate / 5) + "-" * (20 - int(rate / 5))
            print("  {:>8s}: Succ{:>3} Fail{:>3} Rate{:>5.0f}% |{}|".format(lab, s, f, rate, bar))

        # ===== 4. SIGNAL TYPE =====
        print()
        print("=" * 70)
        print("4. Signal Type Success Rate")
        print("=" * 70)
        for sig in ["abnormal", "new_revival", "quiet_runup", "quiet_breakout", "drop_40w", "drop_50w"]:
            s = sum(1 for r in succ if r[6] == sig)
            f = sum(1 for r in fail if r[6] == sig)
            total = s + f
            if total > 0:
                print("  {:>16s}: Succ{:>3} Fail{:>3} Rate{:.0f}%".format(sig, s, f, s / total * 100))

        # ===== 5. STOP LOSS ANALYSIS =====
        print()
        print("=" * 70)
        print("5. Stop Loss: What DD do failures reach?")
        print("=" * 70)
        for lo, hi, lab in [(0,-10,"0~-10%"),(-10,-20,"-10~-20%"),(-20,-30,"-20~-30%"),
                            (-30,-50,"-30~-50%"),(-99,-50,"<-50%")]:
            f_cnt = sum(1 for r in fail if lo >= (r[3] or 0) > hi)
            if f_cnt > 0:
                print("  {}: {} failures in this range".format(lab, f_cnt))

        print()
        print("Failure median DD: {:.0f}%".format(med([r[3] or 0 for r in fail])))
        print("Success median DD: {:.0f}%".format(med([r[3] or 0 for r in succ])))

        # ===== 6. RISK/REWARD SUMMARY =====
        print()
        print("=" * 70)
        print("6. Risk/Reward Summary (by entry DD)")
        print("=" * 70)
        print("{:>14} {:>8} {:>8} {:>8}".format("Entry DD", "SuccGain", "FailLoss", "RR"))
        print("-" * 45)
        for lo, hi, lab in [(-5,-10,"-5~-10%"),(-10,-15,"-10~-15%"),(-15,-20,"-15~-20%"),
                            (-20,-25,"-20~-25%"),(-25,-30,"-25~-30%")]:
            s_gains = [r[1] or 0 for r in succ if lo >= (r[3] or 0) > hi]
            f_losses = [r[2] or 0 for r in fail if lo >= (r[3] or 0) > hi]
            if s_gains and f_losses:
                avg_s = avg(s_gains)
                avg_f = abs(avg(f_losses))
                rr = avg_s / avg_f if avg_f > 0 else 0
                print("{:>14} {:>+7.0f}% {:>+7.0f}% {:>7.1f}:1".format(lab, avg_s, -avg(f_losses), rr))

    db_op(run)


if __name__ == "__main__":
    main()
