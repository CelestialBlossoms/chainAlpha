from db_client import db_op


def init_tables(conn):
    cur = conn.cursor()
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
        CREATE INDEX IF NOT EXISTS idx_alpha_signals_address
            ON alpha_signals(address);
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS alpha_token_candidates (
            id BIGSERIAL PRIMARY KEY,
            address TEXT UNIQUE NOT NULL,
            chain TEXT NOT NULL,
            symbol TEXT,
            trend_interval TEXT,
            mcap_at_alert NUMERIC,
            holder_count INTEGER,
            fee_sol NUMERIC,
            pool_label TEXT,
            pool_liquidity NUMERIC,
            token_created_ts BIGINT,
            token_created_time TEXT,
            verdict TEXT,
            control_ratio NUMERIC,
            associated_supply NUMERIC,
            associated_count INTEGER,
            cluster_size INTEGER,
            dump_progress NUMERIC,
            sold_supply_pct NUMERIC,
            is_dumping BOOLEAN,
            buys_5m INTEGER,
            sells_5m INTEGER,
            net_flow_5m NUMERIC,
            inflow_5m BOOLEAN,
            inflow_streak INTEGER,
            buy_score INTEGER,
            buy_reasons TEXT[],
            sm_count INTEGER,
            kol_count INTEGER,
            top10_rate NUMERIC,
            snipers INTEGER,
            rug_ratio TEXT,
            raw_stats JSONB,
            tg_chat_id TEXT,
            tg_message_id BIGINT,
            first_seen_at TIMESTAMPTZ DEFAULT NOW(),
            last_seen_at TIMESTAMPTZ DEFAULT NOW(),
            alert_count INTEGER DEFAULT 1
        );
        ALTER TABLE alpha_token_candidates
            ADD COLUMN IF NOT EXISTS tg_chat_id TEXT;
        ALTER TABLE alpha_token_candidates
            ADD COLUMN IF NOT EXISTS tg_message_id BIGINT;
        CREATE INDEX IF NOT EXISTS idx_alpha_candidates_score
            ON alpha_token_candidates(buy_score DESC);
        CREATE INDEX IF NOT EXISTS idx_alpha_candidates_last_seen
            ON alpha_token_candidates(last_seen_at DESC);
        CREATE INDEX IF NOT EXISTS idx_alpha_candidates_chain_interval
            ON alpha_token_candidates(chain, trend_interval);
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS alpha_push_events (
            id BIGSERIAL PRIMARY KEY,
            address TEXT NOT NULL,
            chain TEXT NOT NULL,
            symbol TEXT,
            source TEXT,
            trend_interval TEXT,
            alert_no INTEGER DEFAULT 1,
            repeat_alert BOOLEAN DEFAULT FALSE,
            repeat_alert_type TEXT,
            entry_mcap NUMERIC,
            entry_price NUMERIC,
            holder_count INTEGER,
            fee_sol NUMERIC,
            buy_score INTEGER,
            tg_chat_id TEXT,
            tg_message_id BIGINT,
            raw_stats JSONB,
            pushed_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_alpha_push_events_address
            ON alpha_push_events(address);
        CREATE INDEX IF NOT EXISTS idx_alpha_push_events_pushed_at
            ON alpha_push_events(pushed_at DESC);
        CREATE INDEX IF NOT EXISTS idx_alpha_push_events_address_alert_no
            ON alpha_push_events(address, alert_no);
        CREATE INDEX IF NOT EXISTS idx_alpha_push_events_source_interval
            ON alpha_push_events(source, trend_interval);
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS onchain_trading_guides (
            id BIGSERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            note TEXT NOT NULL,
            category TEXT,
            chain TEXT,
            token_address TEXT,
            source_url TEXT,
            tags TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
            metadata JSONB NOT NULL DEFAULT '{}'::JSONB,
            is_archived BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_onchain_trading_guides_created_at
            ON onchain_trading_guides(created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_onchain_trading_guides_category
            ON onchain_trading_guides(category);
        CREATE INDEX IF NOT EXISTS idx_onchain_trading_guides_chain
            ON onchain_trading_guides(chain);
        CREATE INDEX IF NOT EXISTS idx_onchain_trading_guides_tags
            ON onchain_trading_guides USING GIN(tags);
        COMMENT ON TABLE onchain_trading_guides IS '链上交易指南表：记录链上交易相关笔记、经验和规则';
        COMMENT ON COLUMN onchain_trading_guides.title IS '笔记标题';
        COMMENT ON COLUMN onchain_trading_guides.note IS '笔记正文';
        COMMENT ON COLUMN onchain_trading_guides.category IS '笔记分类，例如 risk、entry、exit、wallet、tool';
        COMMENT ON COLUMN onchain_trading_guides.chain IS '相关链，例如 sol、eth、bsc、base';
        COMMENT ON COLUMN onchain_trading_guides.token_address IS '相关代币或合约地址，可为空';
        COMMENT ON COLUMN onchain_trading_guides.source_url IS '来源链接，可为空';
        COMMENT ON COLUMN onchain_trading_guides.tags IS '标签列表';
        COMMENT ON COLUMN onchain_trading_guides.metadata IS '扩展结构化信息';
        COMMENT ON COLUMN onchain_trading_guides.is_archived IS '是否归档';
    """)
    print("Initialized alpha_signals, alpha_token_candidates, and onchain_trading_guides")


if __name__ == "__main__":
    db_op(init_tables)
