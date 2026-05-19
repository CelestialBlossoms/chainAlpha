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
                snapshot_id BIGINT,
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
                liquidity NUMERIC DEFAULT 0,
                pool_total_liquidity NUMERIC DEFAULT 0,
                pool_mcap_ratio NUMERIC DEFAULT 0,
                age_sec BIGINT DEFAULT 0,
                text TEXT,
                extra JSONB DEFAULT '{}'::jsonb
            );
            ALTER TABLE bottom_top100_push_records
                ADD COLUMN IF NOT EXISTS snapshot_id BIGINT;
            ALTER TABLE bottom_top100_push_records
                ADD COLUMN IF NOT EXISTS liquidity NUMERIC DEFAULT 0;
            CREATE INDEX IF NOT EXISTS idx_bottom_top100_push_records_addr_ts
                ON bottom_top100_push_records(address, event_ts DESC);
            CREATE INDEX IF NOT EXISTS idx_bottom_top100_push_records_signal_ts
                ON bottom_top100_push_records(signal_type, event_ts DESC);
            CREATE INDEX IF NOT EXISTS idx_bottom_top100_push_records_snapshot
                ON bottom_top100_push_records(snapshot_id);
            CREATE INDEX IF NOT EXISTS idx_bottom_top100_push_records_pushed_at
                ON bottom_top100_push_records(pushed_at DESC);
            DROP INDEX IF EXISTS uq_bottom_top100_push_records_ca;
            CREATE UNIQUE INDEX IF NOT EXISTS uq_bottom_top100_push_records_signal
                ON bottom_top100_push_records(chain, source, address, signal_type);

            COMMENT ON TABLE bottom_top100_push_records IS 'Top100异动首次推送记录表。每个CA在同一chain/source下只保留首次推送，后续检索明细由bottom_top100_snapshots记录';
            COMMENT ON COLUMN bottom_top100_push_records.id IS '推送记录自增ID';
            COMMENT ON COLUMN bottom_top100_push_records.pushed_at IS '数据库写入时间';
            COMMENT ON COLUMN bottom_top100_push_records.event_ts IS '推送发生时间，Unix秒';
            COMMENT ON COLUMN bottom_top100_push_records.snapshot_id IS '关联bottom_top100_snapshots.id，用于回查当时GMGN Top100持仓快照';
            COMMENT ON COLUMN bottom_top100_push_records.chain IS '链名称，当前主要为sol';
            COMMENT ON COLUMN bottom_top100_push_records.source IS '推送来源模块，例如bottom_abnormal';
            COMMENT ON COLUMN bottom_top100_push_records.status IS '推送状态，例如frontend_update';
            COMMENT ON COLUMN bottom_top100_push_records.address IS '代币CA，同一chain/source下唯一，只记录首次推送';
            COMMENT ON COLUMN bottom_top100_push_records.symbol IS '推送时识别到的代币符号';
            COMMENT ON COLUMN bottom_top100_push_records.signal_type IS '异动类型，例如abnormal、new_revival、drop_40w、quiet_runup、ema_golden_cross';
            COMMENT ON COLUMN bottom_top100_push_records.abnormal_rule IS '命中的异动规则或档位';
            COMMENT ON COLUMN bottom_top100_push_records.trend_interval IS '该代币来自的GMGN trending时间窗口，例如1m、5m、1h，可能为多个窗口合并';
            COMMENT ON COLUMN bottom_top100_push_records.current_mcap IS '推送当时市值，美元';
            COMMENT ON COLUMN bottom_top100_push_records.first_signal_mcap IS '该异动类型在当前基线窗口内首次异动市值，美元';
            COMMENT ON COLUMN bottom_top100_push_records.first_signal_ts IS '该异动类型在当前基线窗口内首次异动时间，Unix秒';
            COMMENT ON COLUMN bottom_top100_push_records.first_signal_change_pct IS '相对首次异动市值涨幅百分比';
            COMMENT ON COLUMN bottom_top100_push_records.price_change_pct IS '本次异动检测使用的价格或市值涨幅百分比';
            COMMENT ON COLUMN bottom_top100_push_records.max_abnormal_mcap IS '当前异动规则允许或记录的最高异常市值档位，美元';
            COMMENT ON COLUMN bottom_top100_push_records.ath_mcap IS 'GMGN或监控识别到的历史最高市值，美元';
            COMMENT ON COLUMN bottom_top100_push_records.liquidity IS '推送当时流动性，美元';
            COMMENT ON COLUMN bottom_top100_push_records.pool_total_liquidity IS '推送当时池子总流动性，美元，与liquidity保持兼容';
            COMMENT ON COLUMN bottom_top100_push_records.pool_mcap_ratio IS '池子流动性与市值比值';
            COMMENT ON COLUMN bottom_top100_push_records.age_sec IS '推送时代币年龄，秒';
            COMMENT ON COLUMN bottom_top100_push_records.text IS '推送给TG或插件前端的文本内容';
            COMMENT ON COLUMN bottom_top100_push_records.extra IS '推送时的完整结构化扩展数据JSON，不包含Top100 holders明细';
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
) -> bool:
    address = str((extra or {}).get("address") or "").strip()
    if not address:
        return False
    signal_type = str((extra or {}).get("signal_type") or "").strip()
    if not signal_type or signal_type == "watch":
        return False
    snapshot_id = _int(extra, "snapshot_id")
    liquidity = _num(extra, "liquidity") or _num(extra, "pool_total_liquidity") or _num(extra, "pool_liquidity")
    event_ts = _int(extra, "event_ts") or _int(extra, "signal_ts") or int(time.time())

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO bottom_top100_push_records (
                event_ts, snapshot_id, chain, source, status, address, symbol, signal_type,
                abnormal_rule, trend_interval, current_mcap, first_signal_mcap,
                first_signal_ts, first_signal_change_pct, price_change_pct,
                max_abnormal_mcap, ath_mcap, liquidity, pool_total_liquidity,
                pool_mcap_ratio, age_sec, text, extra
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (chain, source, address, signal_type) DO NOTHING
            RETURNING id
            """,
            (
                event_ts,
                snapshot_id or None,
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
                liquidity,
                liquidity,
                _num(extra, "pool_mcap_ratio"),
                _int(extra, "age_sec"),
                text or "",
                Json(extra),
            ),
        )
        return cur.fetchone() is not None

    ensure_top100_push_records_table()
    return bool(db_op(_op))


def top100_push_record_exists(
    address: str,
    *,
    source: str = "bottom_abnormal",
    chain: str = "sol",
) -> bool:
    address = str(address or "").strip()
    if not address:
        return False

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1
            FROM bottom_top100_push_records
            WHERE chain = %s AND source = %s AND address = %s
            LIMIT 1
            """,
            (chain, source, address),
        )
        return cur.fetchone() is not None

    ensure_top100_push_records_table()
    return bool(db_op(_op))


def top100_signal_push_record_exists(
    address: str,
    signal_type: str,
    *,
    source: str = "bottom_abnormal",
    chain: str = "sol",
) -> bool:
    address = str(address or "").strip()
    signal_type = str(signal_type or "").strip()
    if not address or not signal_type or signal_type == "watch":
        return False

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1
            FROM bottom_top100_push_records
            WHERE chain = %s
              AND source = %s
              AND address = %s
              AND signal_type = %s
            LIMIT 1
            """,
            (chain, source, address, signal_type),
        )
        return cur.fetchone() is not None

    ensure_top100_push_records_table()
    return bool(db_op(_op))
