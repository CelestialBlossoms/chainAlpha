#!/usr/bin/env python3
"""
Database access helpers for bottom_watchlist_tokens.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from db_client import db_op


def fetch_watchlist_records() -> list[dict[str, Any]]:
    ensure_watchlist_daily_mcap_columns()

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            SELECT ca, create_at, added_at, source, peak_mcap, last_mcap,
                   daily_mcap_date, token_created_at, ath_mcap, COALESCE(blacklisted, false),
                   last_pool_liquidity, last_pool_mcap_ratio, fee_sol, symbol
            FROM bottom_watchlist_tokens
            WHERE ca IS NOT NULL
            """
        )
        return [
            {
                "ca": row[0],
                "create_at": row[1],
                "added_at": row[2],
                "source": row[3],
                "peak_mcap": row[4],
                "last_mcap": row[5],
                "daily_mcap_date": row[6],
                "token_created_at": row[7] if len(row) > 7 else None,
                "ath_mcap": row[8] if len(row) > 8 else None,
                "blacklisted": bool(row[9]) if len(row) > 9 else False,
                "last_pool_liquidity": row[10] if len(row) > 10 else None,
                "last_pool_mcap_ratio": row[11] if len(row) > 11 else None,
                "fee_sol": row[12] if len(row) > 12 else None,
                "symbol": row[13] if len(row) > 13 else None,
            }
            for row in cur.fetchall()
        ]

    return db_op(_op)


def upsert_watchlist_token(
    address: str,
    created_ts: int,
    mcap: float,
    source: str = "auto_mcap_over_1m",
    auto_add_threshold: float = 1_000_000,
) -> None:
    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO bottom_watchlist_tokens (
                ca, create_at, added_at, last_seen_at, source, peak_mcap, last_mcap, note
            ) VALUES (
                %s, CASE WHEN %s > 0 THEN to_timestamp(%s) ELSE NULL END, now(), now(), %s, %s, %s, %s
            )
            ON CONFLICT (ca) DO UPDATE SET
                create_at = COALESCE(bottom_watchlist_tokens.create_at, EXCLUDED.create_at),
                last_seen_at = now(),
                source = COALESCE(bottom_watchlist_tokens.source, EXCLUDED.source),
                peak_mcap = GREATEST(COALESCE(bottom_watchlist_tokens.peak_mcap, 0), EXCLUDED.peak_mcap),
                last_mcap = EXCLUDED.last_mcap,
                ath_mcap = GREATEST(COALESCE(bottom_watchlist_tokens.ath_mcap, 0), EXCLUDED.peak_mcap),
                note = EXCLUDED.note
            """,
            (
                address,
                created_ts,
                created_ts,
                source,
                mcap,
                mcap,
                f"auto add when mcap >= ${auto_add_threshold:,.0f}",
            ),
        )

    db_op(_op)


def save_token_created_at(address: str, gmgn_created_ts: int, gmgn_ath_mcap: float = 0) -> None:
    """Save GMGN's creation_timestamp (always overwrite) and ATH MCap (take max) to the DB."""
    if gmgn_created_ts <= 0 and gmgn_ath_mcap <= 0:
        return
    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            "UPDATE bottom_watchlist_tokens SET token_created_at = %s, ath_mcap = GREATEST(COALESCE(ath_mcap, 0), %s) WHERE ca = %s",
            (gmgn_created_ts if gmgn_created_ts > 0 else None, gmgn_ath_mcap, address),
        )
    db_op(_op)


def fill_watchlist_token_created_at(address: str, gmgn_created_ts: int, gmgn_ath_mcap: float = 0) -> None:
    save_token_created_at(address, gmgn_created_ts, gmgn_ath_mcap)


def ensure_watchlist_daily_mcap_columns() -> None:
    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            ALTER TABLE bottom_watchlist_tokens
                ADD COLUMN IF NOT EXISTS symbol TEXT;
            ALTER TABLE bottom_watchlist_tokens
                ADD COLUMN IF NOT EXISTS fee_sol NUMERIC DEFAULT 0;
            ALTER TABLE bottom_watchlist_tokens
                ADD COLUMN IF NOT EXISTS token_created_at BIGINT DEFAULT 0;
            ALTER TABLE bottom_watchlist_tokens
                ADD COLUMN IF NOT EXISTS daily_mcap_date DATE;
            ALTER TABLE bottom_watchlist_tokens
                ADD COLUMN IF NOT EXISTS daily_mcap_threshold NUMERIC DEFAULT 1000000;
            ALTER TABLE bottom_watchlist_tokens
                ADD COLUMN IF NOT EXISTS daily_mcap_notified_date DATE;
            ALTER TABLE bottom_watchlist_tokens
                ADD COLUMN IF NOT EXISTS daily_mcap_notified_at TIMESTAMPTZ;
            ALTER TABLE bottom_watchlist_tokens
                ADD COLUMN IF NOT EXISTS ath_mcap NUMERIC DEFAULT 0;
            ALTER TABLE bottom_watchlist_tokens
                ADD COLUMN IF NOT EXISTS blacklisted BOOLEAN DEFAULT false;
            ALTER TABLE bottom_watchlist_tokens
                ADD COLUMN IF NOT EXISTS last_pool_liquidity NUMERIC DEFAULT 0;
            ALTER TABLE bottom_watchlist_tokens
                ADD COLUMN IF NOT EXISTS last_pool_mcap_ratio NUMERIC DEFAULT 0;
            """
        )

    db_op(_op)


def upsert_daily_mcap_watchlist_token(
    address: str,
    created_ts: int,
    mcap: float,
    fee_sol: float,
    symbol: str | None = None,
    threshold_mcap: float = 1_000_000,
    source: str = "auto_mcap_over_1m",
) -> None:
    ensure_watchlist_daily_mcap_columns()

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO bottom_watchlist_tokens (
                ca, create_at, added_at, last_seen_at, source, symbol,
                peak_mcap, last_mcap, fee_sol, daily_mcap_date,
                daily_mcap_threshold, note
            ) VALUES (
                %s, CASE WHEN %s > 0 THEN to_timestamp(%s) ELSE NULL END,
                now(), now(), %s, %s, %s, %s, %s, CURRENT_DATE, %s, %s
            )
            ON CONFLICT (ca) DO UPDATE SET
                create_at = COALESCE(bottom_watchlist_tokens.create_at, EXCLUDED.create_at),
                last_seen_at = now(),
                source = COALESCE(bottom_watchlist_tokens.source, EXCLUDED.source),
                symbol = COALESCE(EXCLUDED.symbol, bottom_watchlist_tokens.symbol),
                peak_mcap = GREATEST(COALESCE(bottom_watchlist_tokens.peak_mcap, 0), EXCLUDED.peak_mcap),
                last_mcap = EXCLUDED.last_mcap,
                ath_mcap = GREATEST(COALESCE(bottom_watchlist_tokens.ath_mcap, 0), EXCLUDED.peak_mcap),
                fee_sol = GREATEST(COALESCE(bottom_watchlist_tokens.fee_sol, 0), EXCLUDED.fee_sol),
                daily_mcap_date = CURRENT_DATE,
                daily_mcap_threshold = EXCLUDED.daily_mcap_threshold,
                note = EXCLUDED.note
            """,
            (
                address,
                created_ts,
                created_ts,
                source,
                symbol,
                mcap,
                mcap,
                fee_sol,
                threshold_mcap,
                f"daily auto record when mcap >= ${threshold_mcap:,.0f} and fee >= {fee_sol:.2f} SOL",
            ),
        )

    db_op(_op)


def daily_mcap_watchlist_needs_notify(address: str) -> bool:
    ensure_watchlist_daily_mcap_columns()

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1
            FROM bottom_watchlist_tokens
            WHERE ca = %s
              AND daily_mcap_date = CURRENT_DATE
              AND daily_mcap_notified_at IS NULL
            LIMIT 1
            """,
            (address,),
        )
        return cur.fetchone() is not None

    return bool(db_op(_op))


def mark_daily_mcap_watchlist_notified(address: str) -> None:
    ensure_watchlist_daily_mcap_columns()

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE bottom_watchlist_tokens
            SET daily_mcap_notified_date = CURRENT_DATE,
                daily_mcap_notified_at = now()
            WHERE ca = %s
            """,
            (address,),
        )

    db_op(_op)


def delete_watchlist_token(address: str) -> int:
    def _op(conn):
        cur = conn.cursor()
        cur.execute("DELETE FROM bottom_watchlist_tokens WHERE ca = %s", (address,))
        return cur.rowcount

    return int(db_op(_op) or 0)


def set_watchlist_blacklisted(address: str, blacklisted: bool = True) -> None:
    ensure_watchlist_daily_mcap_columns()

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO bottom_watchlist_tokens (
                ca, added_at, last_seen_at, source, blacklisted, note
            ) VALUES (
                %s, now(), now(), 'manual_blacklist', %s, %s
            )
            ON CONFLICT (ca) DO UPDATE SET
                last_seen_at = now(),
                blacklisted = EXCLUDED.blacklisted,
                note = CASE
                    WHEN EXCLUDED.blacklisted THEN 'manual blacklist'
                    ELSE bottom_watchlist_tokens.note
                END
            """,
            (address, blacklisted, "manual blacklist" if blacklisted else None),
        )
        print(f"blacklisted={blacklisted} for {address[:16]}... (upserted)")
    db_op(_op)


def is_watchlist_blacklisted(address: str) -> bool:
    ensure_watchlist_daily_mcap_columns()

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            "SELECT COALESCE(blacklisted, false) FROM bottom_watchlist_tokens WHERE ca = %s LIMIT 1",
            (address,),
        )
        row = cur.fetchone()
        return bool(row[0]) if row else False

    return bool(db_op(_op))


def clean_redis_stream_for_ca(address: str) -> int:
    """Remove all Redis Stream entries for a given CA address."""
    from redis_client import get_redis_client
    from tg_alert_stream import TG_ALERT_STREAM_KEY
    import json

    client = get_redis_client()
    if client is None:
        print("Redis not available")
        return 0

    deleted = 0
    try:
        start = "-"
        while True:
            rows = client.xrange(TG_ALERT_STREAM_KEY, min=start, count=500)
            if not rows:
                break
            last_id = rows[-1][0]
            for stream_id, fields in rows:
                ca = str(fields.get(b"ca", fields.get("ca", ""))).strip()
                extra_raw = fields.get(b"extra", fields.get("extra", "{}"))
                try:
                    extra = json.loads(extra_raw) if isinstance(extra_raw, (str, bytes)) else {}
                except (json.JSONDecodeError, TypeError):
                    extra = {}
                stream_ca = ca or extra.get("address", "")
                if stream_ca == address:
                    client.xdel(TG_ALERT_STREAM_KEY, stream_id)
                    deleted += 1
            start = f"({last_id.decode() if isinstance(last_id, bytes) else last_id}"
    except Exception as exc:
        print(f"Redis cleanup error: {exc}")

    # Push removal event so frontend drops this CA from memory
    from tg_alert_stream import publish_tg_alert
    publish_tg_alert(f"blacklisted {address[:16]}...", "blacklist_removal", status="removed",
                     ca=address, extra={"address": address})

    print(f"Cleaned {deleted} Redis Stream entries + pushed removal for {address[:16]}...")
    return deleted


def update_watchlist_seen(
    address: str,
    mcap: float,
    pool_liquidity: float = 0,
    pool_mcap_ratio: float = 0,
    fee_sol: float | None = None,
    symbol: str | None = None,
) -> None:
    ensure_watchlist_daily_mcap_columns()

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE bottom_watchlist_tokens
            SET last_seen_at = now(),
                peak_mcap = GREATEST(COALESCE(peak_mcap, 0), %s),
                last_mcap = %s,
                last_pool_liquidity = %s,
                last_pool_mcap_ratio = %s,
                fee_sol = GREATEST(COALESCE(fee_sol, 0), COALESCE(%s, 0)),
                symbol = COALESCE(%s, symbol)
            WHERE ca = %s
            """,
            (mcap, mcap, pool_liquidity, pool_mcap_ratio, fee_sol, symbol, address),
        )

    db_op(_op)


def fill_watchlist_create_at(address: str, created_ts: int) -> None:
    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE bottom_watchlist_tokens
            SET create_at = to_timestamp(%s)
            WHERE ca = %s AND create_at IS NULL
            """,
            (created_ts, address),
        )

    db_op(_op)
