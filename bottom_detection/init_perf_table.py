#!/usr/bin/env python3
"""Initialize bottom_push_performance table for persistent CA analysis storage."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from db_client import db_op


def init_table():
    def _op(conn):
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bottom_push_performance (
                address TEXT PRIMARY KEY,
                symbol TEXT,
                signal_type TEXT,
                event_ts BIGINT,
                event_time TEXT,
                current_mcap DOUBLE PRECISION DEFAULT 0,
                ath_mcap DOUBLE PRECISION DEFAULT 0,
                sig_pct DOUBLE PRECISION DEFAULT 0,
                max_gain_pct DOUBLE PRECISION DEFAULT 0,
                current_return_pct DOUBLE PRECISION DEFAULT 0,
                entry_price DOUBLE PRECISION DEFAULT 0,
                peak_price DOUBLE PRECISION DEFAULT 0,
                current_price DOUBLE PRECISION DEFAULT 0,
                peak_mcap DOUBLE PRECISION DEFAULT 0,
                time_to_peak_min DOUBLE PRECISION DEFAULT 0,
                entry_drawdown_pct DOUBLE PRECISION DEFAULT 0,
                high_to_low_drawdown_pct DOUBLE PRECISION DEFAULT 0,
                volume_usd DOUBLE PRECISION DEFAULT 0,
                candles INTEGER DEFAULT 0,
                binance_mcap DOUBLE PRECISION DEFAULT 0,
                binance_price DOUBLE PRECISION DEFAULT 0,
                binance_ok BOOLEAN DEFAULT false,
                narrative_desc TEXT,
                narrative_type TEXT,
                narrative_cat TEXT,
                risk_tags JSONB DEFAULT '[]'::jsonb,
                result TEXT,
                analysis_date DATE NOT NULL DEFAULT CURRENT_DATE,
                created_at TIMESTAMPTZ DEFAULT now(),
                updated_at TIMESTAMPTZ DEFAULT now()
            );
            CREATE INDEX IF NOT EXISTS idx_perf_symbol ON bottom_push_performance(symbol);
            CREATE INDEX IF NOT EXISTS idx_perf_date ON bottom_push_performance(analysis_date);
            CREATE INDEX IF NOT EXISTS idx_perf_result ON bottom_push_performance(result);
            CREATE INDEX IF NOT EXISTS idx_perf_gain ON bottom_push_performance(max_gain_pct DESC);
        """)
        print("Table bottom_push_performance initialized.")
    db_op(_op)


def upsert_performance(data: dict):
    """Insert or update a performance record. data keys must match column names."""
    def _op(conn):
        cur = conn.cursor()
        columns = [
            "address", "symbol", "signal_type", "first_push_ts", "first_push_time",
            "current_mcap", "ath_mcap", "ath_ratio", "sig_pct",
            "max_gain_pct", "current_return_pct",
            "entry_price", "peak_price", "current_price", "peak_mcap",
            "time_to_peak_min", "entry_drawdown_pct", "high_to_low_drawdown_pct",
            "volume_usd", "candles",
            "binance_mcap", "binance_price", "binance_ok",
            "narrative_desc", "narrative_type", "narrative_cat",
            "risk_tags", "result", "analysis_date",
        ]
        values = [data.get(c) for c in columns]
        set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c != "address")
        set_clause += ", updated_at = now()"

        cur.execute(f"""
            INSERT INTO bottom_push_performance ({", ".join(columns)})
            VALUES ({", ".join(["%s"] * len(columns))})
            ON CONFLICT (address) DO UPDATE SET {set_clause}
        """, values)
    db_op(_op)


if __name__ == "__main__":
    init_table()
