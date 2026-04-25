from urllib.parse import urlparse

# ---------------------------------------------------------------------------
# Database Configuration
# ---------------------------------------------------------------------------
DATABASE_URL = "postgresql://xf22610:1314zxcV1314@43.163.225.175:5432/chainAlpha"


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
TG_BOT_TOKEN = "8783449989:AAHNtioBE7LUx423YyEk6CB-sks_8i1HGnE"
TG_CHAT_ID = "5666543170"
GMGN_API_KEY = "gmgn_d6d3b278fffc6b5151ed2d36e659a804"

# ---------------------------------------------------------------------------
# Scanner Configuration
# ---------------------------------------------------------------------------
CHAINS = ["sol"]
SCAN_INTERVAL = 60

MILESTONES = {
    "1M": {"target": 1_000_000, "range": 0.2},
    "5M": {"target": 5_000_000, "range": 0.15},
}
