import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from db_client import db_op


def init_bottom_watchlist_table(conn):
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bottom_watchlist_tokens (
            ca TEXT PRIMARY KEY,
            create_at TIMESTAMPTZ,
            added_at TIMESTAMPTZ DEFAULT now(),
            last_seen_at TIMESTAMPTZ,
            source TEXT DEFAULT 'auto_ath_mcap',
            peak_mcap NUMERIC DEFAULT 0,
            last_mcap NUMERIC DEFAULT 0,
            note TEXT
        );
        ALTER TABLE bottom_watchlist_tokens
            ADD COLUMN IF NOT EXISTS create_at TIMESTAMPTZ;
        ALTER TABLE bottom_watchlist_tokens
            ADD COLUMN IF NOT EXISTS added_at TIMESTAMPTZ DEFAULT now();
        ALTER TABLE bottom_watchlist_tokens
            ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ;
        ALTER TABLE bottom_watchlist_tokens
            ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'auto_ath_mcap';
        ALTER TABLE bottom_watchlist_tokens
            ADD COLUMN IF NOT EXISTS peak_mcap NUMERIC DEFAULT 0;
        ALTER TABLE bottom_watchlist_tokens
            ADD COLUMN IF NOT EXISTS last_mcap NUMERIC DEFAULT 0;
        ALTER TABLE bottom_watchlist_tokens
            ADD COLUMN IF NOT EXISTS note TEXT;
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
        ALTER TABLE bottom_watchlist_tokens
            ADD COLUMN IF NOT EXISTS narrative_desc TEXT;
        ALTER TABLE bottom_watchlist_tokens
            ADD COLUMN IF NOT EXISTS narrative_type TEXT;

        CREATE TABLE IF NOT EXISTS bottom_watchlist_delete_audit (
            id BIGSERIAL PRIMARY KEY,
            ca TEXT NOT NULL,
            deleted_at TIMESTAMPTZ DEFAULT now(),
            reason TEXT NOT NULL,
            source TEXT,
            symbol TEXT,
            peak_mcap NUMERIC DEFAULT 0,
            last_mcap NUMERIC DEFAULT 0,
            current_mcap NUMERIC DEFAULT 0,
            pool_liquidity NUMERIC DEFAULT 0,
            pool_mcap_ratio NUMERIC DEFAULT 0,
            daily_mcap_date DATE,
            blacklisted BOOLEAN DEFAULT false,
            note TEXT,
            metadata JSONB DEFAULT '{}'::jsonb
        );
        CREATE INDEX IF NOT EXISTS idx_bottom_watchlist_delete_audit_ca
            ON bottom_watchlist_delete_audit(ca);
        CREATE INDEX IF NOT EXISTS idx_bottom_watchlist_delete_audit_deleted_at
            ON bottom_watchlist_delete_audit(deleted_at DESC);
        """
    )
    print("Initialized bottom_watchlist_tokens and delete audit")


if __name__ == "__main__":
    db_op(init_bottom_watchlist_table)
