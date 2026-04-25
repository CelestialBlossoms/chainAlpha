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
            first_seen_at TIMESTAMPTZ DEFAULT NOW(),
            last_seen_at TIMESTAMPTZ DEFAULT NOW(),
            alert_count INTEGER DEFAULT 1
        );
        CREATE INDEX IF NOT EXISTS idx_alpha_candidates_score
            ON alpha_token_candidates(buy_score DESC);
        CREATE INDEX IF NOT EXISTS idx_alpha_candidates_last_seen
            ON alpha_token_candidates(last_seen_at DESC);
        CREATE INDEX IF NOT EXISTS idx_alpha_candidates_chain_interval
            ON alpha_token_candidates(chain, trend_interval);
    """)
    print("Initialized alpha_signals and alpha_token_candidates")


if __name__ == "__main__":
    db_op(init_tables)
