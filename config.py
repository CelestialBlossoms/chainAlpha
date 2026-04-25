import os
from urllib.parse import urlparse
from dotenv import load_dotenv

# 加载 .env 文件
load_dotenv()

# ---------------------------------------------------------------------------
# 1. 数据库配置 (PostgreSQL)
# ---------------------------------------------------------------------------
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://xf22610:1314zxcV1314@43.163.225.175:5432/chainAlpha",
)

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
# 2. Telegram 配置
# ---------------------------------------------------------------------------
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "你的_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "你的_CHAT_ID")

# ---------------------------------------------------------------------------
# 3. 扫描逻辑参数
# ---------------------------------------------------------------------------
CHAINS = ["sol"]
SCAN_INTERVAL = 60  # 每 60 秒扫描一次

# 市值检查点
MILESTONES = {
    "1M": {"target": 1_000_000, "range": 0.2},
    "5M": {"target": 5_000_000, "range": 0.15}
}
