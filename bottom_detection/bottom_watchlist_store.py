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
    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            SELECT ca, create_at, added_at, source, peak_mcap, last_mcap, daily_mcap_date
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
                ADD COLUMN IF NOT EXISTS daily_mcap_date DATE;
            ALTER TABLE bottom_watchlist_tokens
                ADD COLUMN IF NOT EXISTS daily_mcap_threshold NUMERIC DEFAULT 1000000;
            ALTER TABLE bottom_watchlist_tokens
                ADD COLUMN IF NOT EXISTS daily_mcap_notified_date DATE;
            ALTER TABLE bottom_watchlist_tokens
                ADD COLUMN IF NOT EXISTS daily_mcap_notified_at TIMESTAMPTZ;
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
                fee_sol = GREATEST(COALESCE(bottom_watchlist_tokens.fee_sol, 0), EXCLUDED.fee_sol),
                daily_mcap_notified_date = CASE
                    WHEN bottom_watchlist_tokens.daily_mcap_date = CURRENT_DATE
                    THEN bottom_watchlist_tokens.daily_mcap_notified_date
                    ELSE NULL
                END,
                daily_mcap_notified_at = CASE
                    WHEN bottom_watchlist_tokens.daily_mcap_date = CURRENT_DATE
                    THEN bottom_watchlist_tokens.daily_mcap_notified_at
                    ELSE NULL
                END,
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
              AND daily_mcap_notified_date IS DISTINCT FROM CURRENT_DATE
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


def update_watchlist_seen(address: str, mcap: float) -> None:
    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE bottom_watchlist_tokens
            SET last_seen_at = now(),
                peak_mcap = GREATEST(COALESCE(peak_mcap, 0), %s),
                last_mcap = %s
            WHERE ca = %s
            """,
            (mcap, mcap, address),
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
