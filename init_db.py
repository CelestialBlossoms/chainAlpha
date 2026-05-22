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
        CREATE INDEX IF NOT EXISTS idx_alpha_push_events_interval_source_recent
            ON alpha_push_events(trend_interval, COALESCE(source, '1m'), pushed_at DESC, id DESC);
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
    cur.execute("""
        CREATE TABLE IF NOT EXISTS deep_alpha_kline_1m (
            chain TEXT NOT NULL DEFAULT 'sol',
            address TEXT NOT NULL,
            ts BIGINT NOT NULL,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume NUMERIC,
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (chain, address, ts)
        );
        CREATE INDEX IF NOT EXISTS idx_deep_alpha_kline_1m_addr_ts
            ON deep_alpha_kline_1m(address, ts);
        CREATE INDEX IF NOT EXISTS idx_deep_alpha_kline_1m_updated
            ON deep_alpha_kline_1m(updated_at DESC);

        CREATE TABLE IF NOT EXISTS deep_alpha_kline_5m (
            chain TEXT NOT NULL DEFAULT 'sol',
            address TEXT NOT NULL,
            ts BIGINT NOT NULL,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume NUMERIC,
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (chain, address, ts)
        );
        CREATE INDEX IF NOT EXISTS idx_deep_alpha_kline_5m_addr_ts
            ON deep_alpha_kline_5m(address, ts);
        CREATE INDEX IF NOT EXISTS idx_deep_alpha_kline_5m_updated
            ON deep_alpha_kline_5m(updated_at DESC);
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS alpha_abnormal_analysis (
            ca TEXT PRIMARY KEY,
            create_at TIMESTAMPTZ,
            added_at TIMESTAMPTZ DEFAULT now(),
            last_seen_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ DEFAULT now(),
            source TEXT DEFAULT 'auto_ath_mcap',
            peak_mcap NUMERIC DEFAULT 0,
            last_mcap NUMERIC DEFAULT 0,
            highest_mcap NUMERIC DEFAULT 0,
            current_mcap NUMERIC DEFAULT 0,
            gmgn_created_at BIGINT DEFAULT 0,
            gmgn_open_at BIGINT DEFAULT 0,
            note TEXT,
            remark TEXT,
            symbol TEXT,
            fee_sol NUMERIC DEFAULT 0,
            token_created_at BIGINT DEFAULT 0,
            token_launch_at BIGINT DEFAULT 0,
            daily_mcap_date DATE,
            daily_mcap_threshold NUMERIC DEFAULT 1000000,
            daily_mcap_notified_date DATE,
            daily_mcap_notified_at TIMESTAMPTZ,
            ath_mcap NUMERIC DEFAULT 0,
            blacklisted BOOLEAN DEFAULT false,
            last_pool_liquidity NUMERIC DEFAULT 0,
            last_pool_mcap_ratio NUMERIC DEFAULT 0,
            narrative_desc TEXT,
            narrative_type TEXT,
            narrative_category TEXT
        );
    """)
    print("Initialized alpha_signals, alpha_token_candidates, deep_alpha_kline_1m, deep_alpha_kline_5m, alpha_abnormal_analysis, and onchain_trading_guides")


if __name__ == "__main__":
    db_op(init_tables)
