import os
from urllib.parse import urlparse

from env_loader import load_project_env


load_project_env()

# ---------------------------------------------------------------------------
# Database Configuration
# ---------------------------------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://localhost:5432/chainAlpha")


def parse_db_url(url: str) -> dict:
    url = url.replace("+asyncpg", "").replace("+psycopg2", "")
    parsed = urlparse(url)
    return {
        "host": parsed.hostname or "localhost",
        "port": parsed.port or 5432,
        "database": parsed.path.lstrip("/") or "chainAlpha",
        "user": parsed.username or "",
        "password": parsed.password or "",
    }


DB_CONFIG = parse_db_url(DATABASE_URL)

# ---------------------------------------------------------------------------
# API / Bot Configuration
# ---------------------------------------------------------------------------
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "")
GMGN_API_KEY = os.getenv("GMGN_API_KEY", "")

# ---------------------------------------------------------------------------
# Scanner Configuration
# ---------------------------------------------------------------------------
CHAINS = ["sol"]
SCAN_INTERVAL = 60

MILESTONES = {
    "1M": {"target": 1_000_000, "range": 0.2},
    "5M": {"target": 5_000_000, "range": 0.15},
}
