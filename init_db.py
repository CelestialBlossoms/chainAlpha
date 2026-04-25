from db_client import db_op

def init_tables(conn):
    cur = conn.cursor()
    # 核心信号记录表
    cur.execute("""
        CREATE TABLE IF NOT EXISTS alpha_signals (
            id SERIAL PRIMARY KEY,
            address TEXT UNIQUE NOT NULL,
            chain TEXT NOT NULL,
            symbol TEXT,
            mcap_at_alert NUMERIC,
            milestone TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_alpha_address ON alpha_signals(address);
    """)
    print("数据库表结构已重构：保留 alpha_signals")

if __name__ == "__main__":
    db_op(init_tables)
