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
        """
    )
    print("Initialized bottom_watchlist_tokens")


if __name__ == "__main__":
    db_op(init_bottom_watchlist_table)
