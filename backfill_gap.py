"""Backfill missing push records from snapshots for May 26-27."""
import json
from db_client import DBClient

db = DBClient()

# 1. Daily summary
def count_by_date(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT
            COALESCE(pr.pushed_day, ss.snapshot_day) as day,
            COALESCE(pr.push_count, 0) as push_count,
            COALESCE(ss.snapshot_count, 0) as snapshot_count,
            COALESCE(ss.signaled_count, 0) as signaled_count
        FROM (
            SELECT pushed_at::date as pushed_day, COUNT(*) as push_count
            FROM bottom_top100_push_records WHERE chain = 'sol' GROUP BY pushed_at::date
        ) pr
        FULL OUTER JOIN (
            SELECT to_timestamp(snapshot_ts)::date as snapshot_day, COUNT(*) as snapshot_count,
                   SUM(CASE WHEN signal_type != 'watch' THEN 1 ELSE 0 END) as signaled_count
            FROM bottom_top100_snapshots WHERE chain = 'sol'
            GROUP BY to_timestamp(snapshot_ts)::date
        ) ss ON pr.pushed_day = ss.snapshot_day
        WHERE COALESCE(pr.pushed_day, ss.snapshot_day) >= '2026-05-25'
        ORDER BY day DESC
    """)
    return cur.fetchall()

days = db.execute(count_by_date)
print("=== Daily Summary ===")
print(f"{'Date':<14} {'Pushes':>8} {'Snaps':>8} {'Signaled':>10} {'Gap':>8}")
print("-" * 54)
for d in days:
    day, push_n, snap_n, sig_n = d
    gap = (sig_n or 0) - (push_n or 0)
    print(f"  {str(day):<12} {push_n or 0:>8} {snap_n or 0:>8} {sig_n or 0:>10} {gap:>8}")

# 2. Find unique (address, signal_type) combos in snapshots missing from push_records
def find_missing_combos(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT ss.address, ss.signal_type, MIN(ss.snapshot_ts) as first_ts,
               MIN(to_timestamp(ss.snapshot_ts)::date) as day,
               array_agg(DISTINCT to_timestamp(ss.snapshot_ts)::date ORDER BY to_timestamp(ss.snapshot_ts)::date) as days
        FROM bottom_top100_snapshots ss
        WHERE to_timestamp(ss.snapshot_ts)::date IN ('2026-05-26', '2026-05-27')
          AND ss.chain = 'sol'
          AND ss.signal_type NOT IN ('watch', '')
          AND NOT EXISTS (
              SELECT 1 FROM bottom_top100_push_records pr
              WHERE pr.address = ss.address
                AND pr.signal_type = ss.signal_type
                AND pr.chain = 'sol'
                AND pr.source = 'bottom_abnormal'
          )
        GROUP BY ss.address, ss.signal_type
        ORDER BY MIN(ss.snapshot_ts)
    """)
    return cur.fetchall()

missing = db.execute(find_missing_combos)
print(f"\n=== Missing {len(missing)} unique (address, signal_type) combos ===")

for addr, stype, first_ts, day, days in missing:
    print(f"  {addr[:10]} {stype:<18} first={day} days={days}")

# 3. For each missing combo, get the latest snapshot details to build a push record
print(f"\n=== Backfilling {len(missing)} missing records ===")
print("(These would have been stored if the trigger wasn't broken)")
print()

# We need to reconstruct the push record from the snapshot data
def get_best_snapshot(conn, address, signal_type):
    cur = conn.cursor()
    cur.execute("""
        SELECT id, summary, analysis, snapshot_ts, signal_type
        FROM bottom_top100_snapshots
        WHERE address = %s AND signal_type = %s AND chain = 'sol'
        ORDER BY id DESC
        LIMIT 1
    """, (address, signal_type))
    return cur.fetchone()

backfilled = 0
skipped = 0
errors = []
summary = []

for addr, stype, first_ts, day, days in missing:
    snap = db.execute(lambda conn: get_best_snapshot(conn, addr, stype))
    if not snap:
        errors.append(f"{addr[:10]} {stype}: no snapshot found")
        skipped += 1
        continue

    snap_id, sn_summary, sn_analysis, snap_ts, sig_type = snap

    # The sn_summary and sn_analysis are JSON columns
    summary_data = sn_summary if isinstance(sn_summary, dict) else {}
    analysis_data = sn_analysis if isinstance(sn_analysis, dict) else {}

    abnormal_rule = analysis_data.get("abnormal_rule") or ""
    trend_interval = analysis_data.get("trend_interval") or ""

    symbol = (summary_data.get("symbol") or
              analysis_data.get("symbol") or
              addr[:8])

    mcap = (float(analysis_data.get("current_mcap", 0)) or
            float(summary_data.get("mcap", 0)) or 0)
    first_mcap = float(analysis_data.get("first_signal_mcap", 0)) or 0
    price_change = (float(analysis_data.get("price_change_pct", 0)) or 0)
    pool_liq = (float(analysis_data.get("pool_total_liquidity", 0)) or
                float(summary_data.get("pool", {}).get("total_liquidity", 0)) or 0)
    pool_ratio = (float(analysis_data.get("pool_mcap_ratio", 0)) or
                  float(summary_data.get("pool", {}).get("liquidity_mcap_ratio", 0)) or 0)
    age_sec = int(float(analysis_data.get("age_sec", 0)) or 0)
    ath_mcap = float(analysis_data.get("ath_mcap", 0)) or 0
    liquidity = pool_liq

    extra = {
        "address": addr,
        "symbol": symbol,
        "signal_type": sig_type,
        "event_ts": snap_ts,
        "snapshot_id": snap_id,
        "current_mcap": mcap,
        "price_change_pct": price_change,
        "pool_total_liquidity": pool_liq,
        "pool_mcap_ratio": pool_ratio,
        "age_sec": age_sec,
        "ath_mcap": ath_mcap,
        "liquidity": liquidity,
        "abnormal_rule": analysis_data.get("abnormal_rule") or "",
        "trend_interval": analysis_data.get("trend_interval") or "",
        "backfilled": True,
    }

    # Build a minimal text representation
    text_parts = [
        f"🔔 底部异动 [{sig_type}]",
        f"${symbol}",
        f"市值: ${mcap:,.0f}",
        f"涨幅: {price_change:.1f}%",
        f"池子: ${pool_liq:,.0f}",
        f"年龄: {age_sec/3600:.1f}h",
    ]
    text = "\n".join(text_parts)

    # Insert directly (now that trigger is fixed)
    def do_insert(conn2):
        from psycopg2.extras import Json
        cur2 = conn2.cursor()
        cur2.execute("""
            INSERT INTO bottom_top100_push_records (
                event_ts, snapshot_id, chain, source, status, address, symbol,
                signal_type, abnormal_rule, trend_interval, current_mcap,
                first_signal_mcap, price_change_pct, ath_mcap, liquidity,
                pool_total_liquidity, pool_mcap_ratio, age_sec, text, extra
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (chain, source, address, signal_type) DO NOTHING
            RETURNING id
        """, (
            snap_ts, snap_id, 'sol', 'bottom_abnormal', 'backfilled',
            addr, symbol, sig_type, abnormal_rule, trend_interval,
            mcap, first_mcap, price_change, ath_mcap, liquidity,
            pool_liq, pool_ratio, age_sec, text, Json(extra)
        ))
        row = cur2.fetchone()
        return row[0] if row else None

    new_id = db.execute(do_insert)
    if new_id:
        print(f"  OK: id={new_id} {symbol} {sig_type} mcap=${mcap:,.0f}")
        backfilled += 1
    else:
        print(f"  SKIP: {symbol} {sig_type} (duplicate constraint)")
        skipped += 1

    summary.append({
        "symbol": symbol,
        "signal_type": sig_type,
        "day": str(day),
        "mcap": mcap,
        "id": new_id,
    })

print(f"\n=== Backfill Complete ===")
print(f"  Backfilled: {backfilled}")
print(f"  Skipped: {skipped}")
print(f"  Errors: {len(errors)}")
for e in errors:
    print(f"    {e}")
