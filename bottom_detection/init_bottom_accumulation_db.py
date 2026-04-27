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
        """
    )
    print("Initialized single-table Top100 holder snapshot monitor")


if __name__ == "__main__":
    db_op(init_bottom_accumulation_tables)
