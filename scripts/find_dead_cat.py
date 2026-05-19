#!/usr/bin/env python3
"""Find dead cat bounce failures - pushed during brief uptick in downtrend."""
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
            SELECT address, symbol, max_gain_pct, current_return_pct, sig_pct,
                   signal_type, event_ts, current_mcap, ath_mcap, volume_usd,
                   entry_drawdown_pct, time_to_peak_min, candles, risk_tags, result
            FROM bottom_push_performance
            WHERE analysis_date = '2026-05-18' AND result = '失败'
            ORDER BY current_return_pct
        """)
        rows = cur.fetchall()
        print("Today failed: {}".format(len(rows)))
        print()

        fm = lambda v: "${:.0f}K".format(v/1e3) if v>=1e3 else "${:.0f}".format(v)
        fp = lambda v: "{:+.0f}%".format(v)

        for r in rows:
            sym = r[1]; gain = r[2] or 0; cur = r[3] or 0
            sig = r[4] or 0; stype = r[5]; et = r[6]
            mcap = r[7] or 0; ath = r[8] or 0; vol = r[9] or 0
            dd_e = r[10] or 0; peak = r[11] or 0
            tags = r[13] or ""

            t = datetime.fromtimestamp(et, tz).strftime("%H:%M") if et else "?"
            ath_r = ath/max(1, mcap)

            dcb = "[DCB]" if cur < -10 and sig > 15 else ""

            print("{t} ${sym:<14s} gain={fg} cur={fc} sig={fs} mcap={fm} ath={ath_r:.1f}x peak={peak:.0f}m dd={fd} vol={fv} {stype:<16s} {tags} {dcb}".format(
                t=t, sym=sym, fg=fp(gain), fc=fp(cur), fs=fp(sig),
                fm=fm(mcap), ath_r=ath_r, peak=peak, fd=fp(dd_e),
                fv=fm(vol), stype=stype, tags=tags, dcb=dcb))

    db_op(run)

if __name__ == "__main__":
    main()
