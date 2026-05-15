"""Persistent records for Top100 abnormal push events."""

from __future__ import annotations

import time
from typing import Any

from psycopg2.extras import Json

from db_client import db_op

_TOP100_PUSH_RECORDS_TABLE_READY = False


def ensure_top100_push_records_table() -> None:
    global _TOP100_PUSH_RECORDS_TABLE_READY
    if _TOP100_PUSH_RECORDS_TABLE_READY:
        return

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS bottom_top100_push_records (
                id BIGSERIAL PRIMARY KEY,
                pushed_at TIMESTAMPTZ DEFAULT now(),
                event_ts BIGINT NOT NULL,
                chain TEXT NOT NULL DEFAULT 'sol',
                source TEXT NOT NULL DEFAULT 'bottom_abnormal',
                status TEXT NOT NULL DEFAULT 'frontend_update',
                address TEXT NOT NULL,
                symbol TEXT,
                signal_type TEXT,
                abnormal_rule TEXT,
                trend_interval TEXT,
                current_mcap NUMERIC DEFAULT 0,
                first_signal_mcap NUMERIC DEFAULT 0,
                first_signal_ts BIGINT DEFAULT 0,
                first_signal_change_pct NUMERIC DEFAULT 0,
                price_change_pct NUMERIC DEFAULT 0,
                max_abnormal_mcap NUMERIC DEFAULT 0,
                ath_mcap NUMERIC DEFAULT 0,
                pool_total_liquidity NUMERIC DEFAULT 0,
                pool_mcap_ratio NUMERIC DEFAULT 0,
                age_sec BIGINT DEFAULT 0,
                text TEXT,
                extra JSONB DEFAULT '{}'::jsonb
            );
            CREATE INDEX IF NOT EXISTS idx_bottom_top100_push_records_addr_ts
                ON bottom_top100_push_records(address, event_ts DESC);
            CREATE INDEX IF NOT EXISTS idx_bottom_top100_push_records_signal_ts
                ON bottom_top100_push_records(signal_type, event_ts DESC);
            CREATE INDEX IF NOT EXISTS idx_bottom_top100_push_records_pushed_at
                ON bottom_top100_push_records(pushed_at DESC);
            """
        )

    db_op(_op)
    _TOP100_PUSH_RECORDS_TABLE_READY = True


def _num(extra: dict[str, Any], key: str) -> float:
    try:
        value = extra.get(key)
        if value is None or value == "":
            return 0.0
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return 0.0


def _int(extra: dict[str, Any], key: str) -> int:
    try:
        value = extra.get(key)
        if value is None or value == "":
            return 0
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def record_top100_push(
    *,
    text: str,
    extra: dict[str, Any],
    status: str = "frontend_update",
    source: str = "bottom_abnormal",
    chain: str = "sol",
) -> None:
    address = str((extra or {}).get("address") or "").strip()
    if not address:
        return
    signal_type = str((extra or {}).get("signal_type") or "").strip()
    if not signal_type or signal_type == "watch":
        return
    event_ts = int(time.time())

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO bottom_top100_push_records (
                event_ts, chain, source, status, address, symbol, signal_type,
                abnormal_rule, trend_interval, current_mcap, first_signal_mcap,
                first_signal_ts, first_signal_change_pct, price_change_pct,
                max_abnormal_mcap, ath_mcap, pool_total_liquidity,
                pool_mcap_ratio, age_sec, text, extra
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s
            )
            """,
            (
                event_ts,
                chain,
                source,
                status,
                address,
                extra.get("symbol") or "",
                signal_type,
                extra.get("abnormal_rule") or "",
                extra.get("trend_interval") or "",
                _num(extra, "current_mcap"),
                _num(extra, "first_signal_mcap"),
                _int(extra, "first_signal_ts"),
                _num(extra, "first_signal_change_pct"),
                _num(extra, "price_change_pct") or _num(extra, "change_pct"),
                _num(extra, "max_abnormal_mcap"),
                _num(extra, "ath_mcap"),
                _num(extra, "pool_total_liquidity") or _num(extra, "pool_liquidity") or _num(extra, "liquidity"),
                _num(extra, "pool_mcap_ratio"),
                _int(extra, "age_sec"),
                text or "",
                Json(extra),
            ),
        )

    ensure_top100_push_records_table()
    db_op(_op)
