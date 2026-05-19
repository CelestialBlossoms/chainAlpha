#!/usr/bin/env python3
"""Analyze successful token patterns in detail."""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from db_client import db_op
from datetime import datetime, timezone, timedelta
from collections import Counter
tz = timezone(timedelta(hours=8))

def main():
    def run(conn):
        cur = conn.cursor()
        cur.execute("""SELECT symbol, max_gain_pct, current_return_pct, sig_pct,
            signal_type, event_ts, current_mcap, ath_mcap, volume_usd,
            entry_drawdown_pct, time_to_peak_min, risk_tags, result
            FROM bottom_push_performance WHERE analysis_date='2026-05-18'
            AND result='成功' ORDER BY max_gain_pct DESC""")
        rows = cur.fetchall()

        fm = lambda v: "${:.0f}K".format(v/1e3) if v>=1e3 else "${:.0f}".format(v)
        med = lambda arr: sorted(arr)[len(arr)//2] if arr else 0

        print("成功组 ({}):".format(len(rows)))
        print()

        gains = [r[1] or 0 for r in rows]      # max_gain_pct (col 1)
        curs = [r[2] or 0 for r in rows]       # current_return_pct (col 2)
        mcaps = [r[6] or 0 for r in rows]      # current_mcap (col 6)
        peaks = [r[10] or 0 for r in rows]     # time_to_peak_min (col 10)
        vols = [r[8] or 0 for r in rows]       # volume_usd (col 8)
        dds = [r[9] or 0 for r in rows]        # entry_drawdown_pct (col 9)

        print("=== 整体特征 ===")
        print("中位gain: {:.0f}% | 中位cur: {:+.0f}% | 中位mcap: {} | 中位peak: {:.0f}min | 中位vol: {}".format(
            med(gains), med(curs), fm(med(mcaps)), med(peaks),
            fm(med([v for v in vols if v > 0]))))

        # By pattern: dd = r[9], cur = r[2]
        pure = [r for r in rows if (r[9] or 0) > -5]
        vrev = [r for r in rows if (r[9] or 0) < -10 and (r[2] or 0) > 0]
        deep = [r for r in rows if (r[9] or 0) < -20]

        print("\n走势: 纯拉升{}个({:.0f}%) V反{}个({:.0f}%) 深V{}个({:.0f}%)".format(
            len(pure), len(pure)/max(len(rows),1)*100,
            len(vrev), len(vrev)/max(len(rows),1)*100,
            len(deep), len(deep)/max(len(rows),1)*100))

        # By signal type: r[2]
        sig_types = Counter(r[4] for r in rows)  # signal_type
        print("信号类型: {}".format(dict(sig_types)))

        # By time: r[3] is event_ts
        hours = Counter()
        for r in rows:
            et = r[5]  # event_ts
            try:
                h = datetime.fromtimestamp(int(et), tz).hour
                hours[h] += 1
            except: pass
        print("\n按时段:")
        for h in range(24):
            c = hours.get(h, 0)
            if c > 0:
                print("  {:02d}:00 {}".format(h, "#" * c))

        # Top performers: gain=r[8], mcap=r[5], ath=r[6], peak=r[14], dd=r[15], vol=r[17], sig=r[2]
        print("\n=== Top 10 涨幅 ===")
        print("{:>6} {:<14} {:>8} {:>8} {:>6} {:>6} {:>8} {:<14}".format(
            "gain", "Symbol", "mcap", "ATH/m", "peak", "dd", "vol", "type"))
        for r in rows[:10]:
            ath_r = (r[7] or 0) / max(1, r[6] or 0)  # ath_mcap / current_mcap
            print("{:>+5.0f}% ${:<13} {:>8} {:.1f}x {:>5.0f}m {:>+5.0f}% {:>8} {:<14}".format(
                r[1] or 0, r[0], fm(r[6] or 0), ath_r,
                r[10] or 0, r[9] or 0, fm(r[8] or 0), r[4] or ""))

        # MCap buckets success rate
        print("\n=== 市值分档 ===")
        for lo, hi, lab in [(0,50,"<$50K"),(50,100,"$50-100K"),(100,200,"$100-200K"),
                            (200,500,"$200-500K"),(500,9999,">$500K")]:
            bucket = [r for r in rows if lo*1000 <= (r[6] or 0) < hi*1000]  # current_mcap = r[6]
            print("  {}: {}个".format(lab, len(bucket)))

        # Peak time buckets
        print("\n=== 峰顶时间 ===")
        for lo, hi, lab in [(0,5,"瞬爆<=5m"),(5,30,"5-30m"),(30,120,"30-120m"),
                            (120,480,"2-8h"),(480,9999,">8h")]:
            cnt = sum(1 for r in rows if lo <= (r[10] or 0) < hi)
            print("  {}: {}个 ({:.0f}%)".format(lab, cnt, cnt/len(peaks)*100))

        # ATH ratio
        ath_ratios = [(r[7] or 0)/max(1, r[6] or 0) for r in rows]  # ath_mcap / current_mcap
        print("\n=== ATH空间 ===")
        for lo, hi, lab in [(0,1.5,"<1.5x(天花板)"),(1.5,3,"1.5-3x"),(3,10,"3-10x"),(10,999,">10x")]:
            cnt = sum(1 for a in ath_ratios if lo <= a < hi)
            print("  {}: {}个 ({:.0f}%)".format(lab, cnt, cnt/len(rows)*100))

        # Success by drawdown
        print("\n=== 回撤 vs 涨幅 ===")
        for lo, hi, lab in [(-5,0,"几乎不跌"),(-15,-5,"轻度回撤"),(-30,-15,"中度V反"),(-99,-30,"深度V反")]:
            bucket = [r for r in rows if lo >= (r[9] or 0) > hi]  # entry_drawdown_pct = r[9]
            if bucket:
                print("  {}: {}个 中位gain={:.0f}%".format(lab, len(bucket), med([r[1] or 0 for r in bucket])))

    db_op(run)

if __name__ == "__main__":
    main()
