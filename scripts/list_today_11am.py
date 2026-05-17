#!/usr/bin/env python3
"""List today's pushes from 11:00 onwards."""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from db_client import db_op
from datetime import datetime, timezone, timedelta
tz = timezone(timedelta(hours=8))

def main():
    def run(conn):
        cur = conn.cursor()
        cur.execute("""
            SELECT symbol, address, max_gain_pct, current_return_pct, sig_pct,
                   signal_type, event_ts, current_mcap, ath_mcap, volume_usd,
                   entry_drawdown_pct, high_to_low_drawdown_pct, time_to_peak_min,
                   candles, risk_tags, result, narrative_cat
            FROM bottom_push_performance
            WHERE analysis_date = '2026-05-17' AND event_ts >= EXTRACT(EPOCH FROM '2026-05-17 11:00:00+08:00'::timestamptz)::bigint
            ORDER BY event_ts
        """)
        rows = cur.fetchall()

        fm = lambda v: "${:.2f}M".format(v/1e6) if v>=1e6 else ("${:.0f}K".format(v/1e3) if v>=1e3 else "${:.0f}".format(v))
        fp = lambda v: "{:+.0f}%".format(v)

        print("今日11:00后的推送: {} 个".format(len(rows)))
        print()
        print("{:<6} {:<14} {:>6} {:>6} {:>8} {:>5} {:>5} {:>5} {:>5} {:>8} {:<16} {:<20} {:>4}".format(
            "时间","Symbol","gain","现在","mcap","ATH/m","sig%","dd_e","peak","vol","类型","标签","结果"))
        print("-"*125)

        succ = 0; fail = 0
        for r in rows:
            et = datetime.fromtimestamp(r[6], tz).strftime("%H:%M") if r[6] else "?"
            gain = float(r[2] or 0); cur = float(r[3] or 0); mcap = float(r[7] or 0)
            ath = float(r[8] or 0); sig = float(r[4] or 0); dd_e = float(r[10] or 0)
            peak = float(r[11] or 0); vol = float(r[9] or 0)
            sig_type = r[5] or ""
            tags_raw = r[14]
            tags = ",".join(tags_raw) if isinstance(tags_raw, list) else str(tags_raw or "")
            result = r[15] or ""
            if result == "成功": succ += 1
            else: fail += 1

            print("{:<6} ${:<13} {:>6} {:>6} {:>8} {:.1f}x {:>4.0f}% {:>+4.0f}% {:>4.0f}m {:>8} {:<16} {:<20} {:>4}".format(
                et, str(r[1]), fp(gain), fp(cur), fm(mcap),
                ath/max(1,mcap), sig, dd_e, peak, fm(vol),
                sig_type, tags, result))

        if succ + fail > 0:
            print("\n成功: {} | 失败: {} | 胜率: {:.0f}%".format(succ, fail, succ/(succ+fail)*100))

    db_op(run)

if __name__ == "__main__":
    main()
