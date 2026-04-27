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
            ca TEXT PRIMARY KEY
        );
        """
    )
    print("Initialized bottom_watchlist_tokens")


if __name__ == "__main__":
    db_op(init_bottom_watchlist_table)
