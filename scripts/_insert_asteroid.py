import sys
sys.path.insert(0, ".")
from bottom_detection.bottom_watchlist_store import upsert_watchlist_token, ensure_watchlist_daily_mcap_columns

ca = "F1ppSHedBsGGwEKH78JVgoqr4xkQHswtsGGLpgM7bCP2"
price = 0.00025775521
supply = 999823450
mcap = price * supply
liq = 80866.17
created_ts = 1776397828

print("ASTEROID: mcap=$%.0f, liq=$%.0f, created_ts=%d" % (mcap, liq, created_ts))

ensure_watchlist_daily_mcap_columns()

upsert_watchlist_token(
    address=ca,
    created_ts=created_ts,
    mcap=mcap,
    source="manual_ca_query",
    auto_add_threshold=0,
)

# Also update liquidity
from db_client import db_op
def _op(conn):
    cur = conn.cursor()
    cur.execute("""
        UPDATE bottom_watchlist_tokens
        SET last_pool_liquidity = %s,
            last_pool_mcap_ratio = CASE WHEN %s > 0 THEN %s / %s ELSE 0 END,
            last_seen_at = now()
        WHERE ca = %s
    """, (liq, liq, mcap, liq, ca))
    print("  Updated liquidity: rows=%d" % cur.rowcount)
db_op(_op)

print("Done")
