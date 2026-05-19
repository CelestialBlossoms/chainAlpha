#!/usr/bin/env python3
"""Analyze failed token losses."""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from db_client import db_op

def main():
    def run(conn):
        cur = conn.cursor()
        for d in ["2026-05-18", "2026-05-19"]:
            cur.execute("""SELECT symbol, max_gain_pct, current_return_pct,
                entry_drawdown_pct, high_to_low_drawdown_pct, current_mcap, risk_tags
                FROM bottom_push_performance
                WHERE analysis_date=%s AND result=%s ORDER BY current_return_pct""",
                (d, "失败"))
            rows = cur.fetchall()
            if not rows:
                continue
            avg_c = sum((r[2] or 0) for r in rows) / len(rows)
            avg_d = sum((r[3] or 0) for r in rows) / len(rows)
            fm = lambda v: "${:.0f}K".format(v/1e3) if v >= 1e3 else "${:.0f}".format(v)
            print("\n{} | {} failed | avg loss {:+.0f}% | avg dd {:+.0f}%".format(d, len(rows), avg_c, avg_d))
            print("{:<16} {:>6} {:>7} {:>7} {:>7} {:>8} {}".format("Symbol", "peak", "cur", "dd_e", "dd_h", "mcap", "tags"))
            print("-" * 80)
            for r in rows:
                tags = ",".join(r[6]) if isinstance(r[6], list) else str(r[6] or "")
                print("{:<16} {:>+5.0f}% {:>+6.0f}% {:>+6.0f}% {:>+6.0f}% {:>8} {}".format(
                    r[0], r[1] or 0, r[2] or 0, r[3] or 0, r[4] or 0, fm(r[5] or 0), tags[:50]))
    db_op(run)

if __name__ == "__main__":
    main()
