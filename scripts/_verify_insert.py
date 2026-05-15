import sys
sys.path.insert(0, ".")
from db_client import db_op

def _op(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT ca, symbol, current_mcap, last_mcap, peak_mcap, highest_mcap,
               last_pool_liquidity, last_seen_at
        FROM bottom_watchlist_tokens
        WHERE ca = %s
    """, ("F1ppSHedBsGGwEKH78JVgoqr4xkQHswtsGGLpgM7bCP2",))
    row = cur.fetchone()
    if row:
        print("  CA: %s" % row[0])
        print("  Symbol: %s" % row[1])
        print("  current_mcap: $%.0f" % float(row[2] or 0))
        print("  last_mcap: $%.0f" % float(row[3] or 0))
        print("  peak_mcap: $%.0f" % float(row[4] or 0))
        print("  highest_mcap: $%.0f" % float(row[5] or 0))
        print("  last_pool_liquidity: $%.0f" % float(row[6] or 0))
        print("  last_seen_at: %s" % row[7])
    else:
        print("NOT FOUND")

db_op(_op)
