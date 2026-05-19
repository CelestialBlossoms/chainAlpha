#!/usr/bin/env python3
"""DD breakdown for success tokens."""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from db_client import db_op

def main():
    def run(conn):
        cur = conn.cursor()
        cur.execute("SELECT symbol, max_gain_pct, current_return_pct, entry_drawdown_pct, time_to_peak_min FROM bottom_push_performance WHERE analysis_date=%s AND result=%s ORDER BY entry_drawdown_pct", ("2026-05-19", "成功"))
        rows = cur.fetchall()
        med = lambda arr: sorted(arr)[len(arr)//2] if arr else 0

        print("成功组全部({}) 回撤分布:".format(len(rows)))
        for lo, hi, lab in [(0, -3, "几乎不跌"), (-3, -5, "微跌"), (-5, -10, "轻度"), (-10, -20, "中度V"), (-20, -35, "深度V"), (-99, -35, "极端V")]:
            bucket = [r for r in rows if lo >= (r[3] or 0) > hi]
            if bucket:
                avg_g = sum(r[1] or 0 for r in bucket) / len(bucket)
                avg_c = sum(r[2] or 0 for r in bucket) / len(bucket)
                print("  {} (DD{:.0f}~{:.0f}%) {}个: 均涨{:+.0f}% 均现在{:+.0f}%".format(lab, lo, hi, len(bucket), avg_g, avg_c))
                for r in bucket:
                    print("    ${:<14s} dd={:+.0f}% gain={:+.0f}% cur={:+.0f}% peak={:.0f}m".format(r[0], r[3] or 0, r[1] or 0, r[2] or 0, r[4] or 0))
        print()
        print("中位DD: {:.0f}% | 中位gain: {:.0f}%".format(med([r[3] or 0 for r in rows]), med([r[1] or 0 for r in rows])))

    db_op(run)

if __name__ == "__main__":
    main()
