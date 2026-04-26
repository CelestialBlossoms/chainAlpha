import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from db_client import db_op


def init_bottom_accumulation_tables(conn):
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bottom_kline_cache (
            chain TEXT NOT NULL,
            address TEXT NOT NULL,
            resolution TEXT NOT NULL,
            bucket_ts BIGINT NOT NULL,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume NUMERIC,
            amount NUMERIC,
            raw JSONB,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (chain, address, resolution, bucket_ts)
        );
        CREATE INDEX IF NOT EXISTS idx_bottom_kline_addr_ts
            ON bottom_kline_cache(address, bucket_ts DESC);

        CREATE TABLE IF NOT EXISTS bottom_holder_snapshots (
            id BIGSERIAL PRIMARY KEY,
            chain TEXT NOT NULL,
            address TEXT NOT NULL,
            snapshot_ts BIGINT NOT NULL,
            holder_count INTEGER,
            non_pool_count INTEGER,
            top10_pct NUMERIC,
            top100_pct NUMERIC,
            non_pool_pct NUMERIC,
            buy_volume NUMERIC,
            sell_volume NUMERIC,
            netflow NUMERIC,
            raw_summary JSONB,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_bottom_holder_snap_addr_ts
            ON bottom_holder_snapshots(address, snapshot_ts DESC);

        CREATE TABLE IF NOT EXISTS bottom_holder_wallets (
            snapshot_id BIGINT NOT NULL REFERENCES bottom_holder_snapshots(id) ON DELETE CASCADE,
            address TEXT NOT NULL,
            wallet TEXT NOT NULL,
            amount_percentage NUMERIC,
            usd_value NUMERIC,
            buy_volume_cur NUMERIC,
            sell_volume_cur NUMERIC,
            netflow_usd NUMERIC,
            start_holding_at BIGINT,
            tags JSONB,
            raw JSONB,
            PRIMARY KEY (snapshot_id, wallet)
        );

        CREATE TABLE IF NOT EXISTS bottom_accumulation_signals (
            id BIGSERIAL PRIMARY KEY,
            chain TEXT NOT NULL,
            address TEXT NOT NULL,
            signal_type TEXT NOT NULL,
            window_minutes INTEGER NOT NULL,
            signal_bucket_ts BIGINT NOT NULL,
            spike_pct NUMERIC,
            base_range_pct NUMERIC,
            base_drift_pct NUMERIC,
            accumulation_score INTEGER,
            accumulation_pct_delta NUMERIC,
            distribution_pct_delta NUMERIC,
            rotation_score NUMERIC,
            netflow_usd NUMERIC,
            top_buyers JSONB,
            raw_analysis JSONB,
            notified BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (chain, address, signal_type, window_minutes, signal_bucket_ts)
        );
        CREATE INDEX IF NOT EXISTS idx_bottom_signals_created
            ON bottom_accumulation_signals(created_at DESC);
        """
    )
    print("Initialized bottom accumulation monitor tables")


if __name__ == "__main__":
    db_op(init_bottom_accumulation_tables)
