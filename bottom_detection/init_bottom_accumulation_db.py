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
        DROP TABLE IF EXISTS bottom_accumulation_signals;
        DROP TABLE IF EXISTS bottom_holder_wallets;
        DROP TABLE IF EXISTS bottom_holder_snapshots;
        DROP TABLE IF EXISTS bottom_holder_scan_runs;
        DROP TABLE IF EXISTS bottom_kline_cache;
        DROP TABLE IF EXISTS bottom_top100_snapshots;

        CREATE TABLE bottom_top100_snapshots (
            id BIGSERIAL PRIMARY KEY,
            scan_id TEXT NOT NULL,
            chain TEXT NOT NULL DEFAULT 'sol',
            trend_interval TEXT NOT NULL DEFAULT '1h',
            address TEXT NOT NULL,
            symbol TEXT,
            snapshot_ts BIGINT NOT NULL,
            signal_type TEXT,
            signal_score INTEGER DEFAULT 0,
            notified BOOLEAN DEFAULT FALSE,
            summary JSONB NOT NULL,
            holders JSONB NOT NULL,
            analysis JSONB,
            raw_token JSONB,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE INDEX idx_bottom_top100_addr_ts
            ON bottom_top100_snapshots(address, snapshot_ts DESC);
        CREATE INDEX idx_bottom_top100_scan
            ON bottom_top100_snapshots(scan_id);
        CREATE INDEX idx_bottom_top100_signal
            ON bottom_top100_snapshots(signal_type, signal_score DESC, created_at DESC);

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
        CREATE INDEX idx_bottom_top100_push_records_addr_ts
            ON bottom_top100_push_records(address, event_ts DESC);
        CREATE INDEX idx_bottom_top100_push_records_signal_ts
            ON bottom_top100_push_records(signal_type, event_ts DESC);
        CREATE INDEX idx_bottom_top100_push_records_pushed_at
            ON bottom_top100_push_records(pushed_at DESC);

        CREATE TABLE bottom_kline_cache (
            chain TEXT NOT NULL DEFAULT 'sol',
            address TEXT NOT NULL,
            resolution TEXT NOT NULL,
            ts BIGINT NOT NULL,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume NUMERIC,
            amount NUMERIC,
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (chain, address, resolution, ts)
        );

        CREATE INDEX idx_bottom_kline_cache_addr_res_ts
            ON bottom_kline_cache(address, resolution, ts);
        """
    )
    print("Initialized Top100 holder snapshot monitor and kline cache")


if __name__ == "__main__":
    db_op(init_bottom_accumulation_tables)
