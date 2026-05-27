import sys
sys.path.insert(0, r'D:\github\chainAlpha')
from db_client import db_op

def fix():
    def q(conn):
        cur = conn.cursor()
        cur.execute("""
            UPDATE bottom_top100_push_records
            SET pushed_at = to_timestamp(event_ts)
            WHERE pushed_at::date = '2026-05-27'
              AND extra->>'source_redis' = 'bottom:live_track'
        """)
        print(f"Fixed {cur.rowcount} records")

        # Verify
        cur.execute("""
            SELECT symbol,
                   pushed_at,
                   to_timestamp(event_ts) as event_time,
                   pushed_at = to_timestamp(event_ts) as matched
            FROM bottom_top100_push_records
            WHERE pushed_at::date = '2026-05-27'
              AND extra->>'source_redis' = 'bottom:live_track'
            ORDER BY pushed_at
            LIMIT 10
        """)
        print(f"\n  {'Symbol':<14} {'pushed_at':<22} {'event_time':<22} matched")
        for r in cur.fetchall():
            print(f"  {r[0]:<14} {str(r[1]):<22} {str(r[2]):<22} {r[3]}")

    db_op(q)

fix()
