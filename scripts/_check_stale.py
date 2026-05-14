import sys
sys.path.insert(0, ".")
from db_client import db_op

def _op(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT ca, symbol, current_mcap, last_mcap, last_seen_at,
               EXTRACT(EPOCH FROM now() - last_seen_at)/3600 AS hours_ago
        FROM bottom_watchlist_tokens
        WHERE ca IS NOT NULL AND current_mcap > 0
        ORDER BY last_seen_at ASC NULLS FIRST
        LIMIT 5
    """)
    for row in cur.fetchall():
        sym = row[1] or "?"
        print("  %s: mcap=$%.0f, last_seen=%s, hours_ago=%.1fh" % (sym, float(row[2] or 0), row[4], float(row[5] or 0)))

    cur.execute("""
        SELECT COUNT(*) FROM bottom_watchlist_tokens
        WHERE ca IS NOT NULL
          AND (last_seen_at IS NULL OR last_seen_at < now() - interval '24 hours')
    """)
    stale = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM bottom_watchlist_tokens WHERE ca IS NOT NULL")
    total = cur.fetchone()[0]
    print("  Stale (>24h): %d/%d tokens" % (stale, total))

db_op(_op)
