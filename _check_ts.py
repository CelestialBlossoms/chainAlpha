import sys
sys.path.insert(0, r'D:\github\chainAlpha')
from db_client import db_op

def check():
    def q(conn):
        cur = conn.cursor()
        cur.execute("""
            SELECT symbol, address, signal_type,
                   pushed_at,
                   to_timestamp(event_ts) as event_time,
                   event_ts,
                   extra->'pushed_at' as redis_pushed
            FROM bottom_top100_push_records
            WHERE pushed_at::date = '2026-05-27'
              AND extra->>'source_redis' = 'bottom:live_track'
            ORDER BY pushed_at DESC
            LIMIT 10
        """)
        print("pushed_at vs event_ts vs redis pushed_at:")
        print(f"  {'Symbol':<14} {'pushed_at':<22} {'event_time':<22} {'event_ts':>12} {'redis_ts':>12}")
        for r in cur.fetchall():
            print(f"  {r[0]:<14} {str(r[3]):<22} {str(r[4]):<22} {r[5] or 0:>12} {r[6] or 0:>12}")

    db_op(q)

check()
