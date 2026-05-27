"""Verify the trigger fix is working."""
import time
from db_client import DBClient

db = DBClient()

# Verify trigger includes review_ts
def check_trigger(conn):
    cur = conn.cursor()
    cur.execute("SELECT prosrc FROM pg_proc WHERE proname = %s", ("generate_bottom_strategy_trigger",))
    src = cur.fetchone()
    if src and "review_ts" in src[0]:
        print("OK: trigger now includes review_ts")
    else:
        print("WARN: review_ts not found in trigger function")

db.execute(check_trigger)

# Check latest push records
def latest(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT id, pushed_at, symbol, signal_type, status
        FROM bottom_top100_push_records
        ORDER BY id DESC
        LIMIT 10
    """)
    return cur.fetchall()

recs = db.execute(latest)
print("Latest 10 push records:")
for r in recs:
    print(f"  id={r[0]} pushed_at={r[1]} symbol={r[2]} type={r[3]} status={r[4]}")

# Check for records in the last hour
def recent(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT id, pushed_at, symbol, signal_type, status
        FROM bottom_top100_push_records
        WHERE pushed_at > now() - interval '1 hour'
        ORDER BY id DESC
    """)
    return cur.fetchall()

recs2 = db.execute(recent)
print(f"\nRecords in last hour: {len(recs2)}")
for r in recs2:
    print(f"  id={r[0]} pushed_at={r[1]} symbol={r[2]} type={r[3]} status={r[4]}")

# Check kline_reviews table
def kline_reviews_check(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) as total,
               COUNT(review_ts) as with_review_ts
        FROM bottom_top100_push_kline_reviews
    """)
    return cur.fetchone()

kr = db.execute(kline_reviews_check)
print(f"\nkline_reviews: total={kr[0]}, with review_ts={kr[1]}")
