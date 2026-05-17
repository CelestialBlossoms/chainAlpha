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
            updated_at TIMESTAMPTZ DEFAULT now(),
            source TEXT DEFAULT 'auto_ath_mcap',
            peak_mcap NUMERIC DEFAULT 0,
            last_mcap NUMERIC DEFAULT 0,
            highest_mcap NUMERIC DEFAULT 0,
            current_mcap NUMERIC DEFAULT 0,
            gmgn_created_at BIGINT DEFAULT 0,
            gmgn_open_at BIGINT DEFAULT 0,
            note TEXT,
            remark TEXT
        );
        ALTER TABLE bottom_watchlist_tokens
            ADD COLUMN IF NOT EXISTS create_at TIMESTAMPTZ;
        ALTER TABLE bottom_watchlist_tokens
            ADD COLUMN IF NOT EXISTS added_at TIMESTAMPTZ DEFAULT now();
        ALTER TABLE bottom_watchlist_tokens
            ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ;
        ALTER TABLE bottom_watchlist_tokens
            ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now();
        ALTER TABLE bottom_watchlist_tokens
            ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'auto_ath_mcap';
        ALTER TABLE bottom_watchlist_tokens
            ADD COLUMN IF NOT EXISTS peak_mcap NUMERIC DEFAULT 0;
        ALTER TABLE bottom_watchlist_tokens
            ADD COLUMN IF NOT EXISTS last_mcap NUMERIC DEFAULT 0;
        ALTER TABLE bottom_watchlist_tokens
            ADD COLUMN IF NOT EXISTS highest_mcap NUMERIC DEFAULT 0;
        ALTER TABLE bottom_watchlist_tokens
            ADD COLUMN IF NOT EXISTS current_mcap NUMERIC DEFAULT 0;
        ALTER TABLE bottom_watchlist_tokens
            ADD COLUMN IF NOT EXISTS gmgn_created_at BIGINT DEFAULT 0;
        ALTER TABLE bottom_watchlist_tokens
            ADD COLUMN IF NOT EXISTS gmgn_open_at BIGINT DEFAULT 0;
        ALTER TABLE bottom_watchlist_tokens
            ADD COLUMN IF NOT EXISTS note TEXT;
        ALTER TABLE bottom_watchlist_tokens
            ADD COLUMN IF NOT EXISTS remark TEXT;
        ALTER TABLE bottom_watchlist_tokens
            ADD COLUMN IF NOT EXISTS symbol TEXT;
        ALTER TABLE bottom_watchlist_tokens
            ADD COLUMN IF NOT EXISTS fee_sol NUMERIC DEFAULT 0;
        ALTER TABLE bottom_watchlist_tokens
            ADD COLUMN IF NOT EXISTS token_created_at BIGINT DEFAULT 0;
        ALTER TABLE bottom_watchlist_tokens
            ADD COLUMN IF NOT EXISTS token_launch_at BIGINT DEFAULT 0;
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
        ALTER TABLE bottom_watchlist_tokens
            ADD COLUMN IF NOT EXISTS remark TEXT;

        COMMENT ON TABLE bottom_watchlist_tokens IS '底部异动重点观察池。每个CA一行，保存当前观察状态，不保存每次异动历史';
        COMMENT ON COLUMN bottom_watchlist_tokens.ca IS '代币CA，主键';
        COMMENT ON COLUMN bottom_watchlist_tokens.create_at IS '代币创建时间，优先来自GMGN创建时间';
        COMMENT ON COLUMN bottom_watchlist_tokens.added_at IS '加入观察池时间';
        COMMENT ON COLUMN bottom_watchlist_tokens.last_seen_at IS '监控最近一次扫描到该CA的时间';
        COMMENT ON COLUMN bottom_watchlist_tokens.updated_at IS '观察池记录最近一次被业务流程更新的时间';
        COMMENT ON COLUMN bottom_watchlist_tokens.source IS '加入观察池来源，例如auto_mcap_over_1m、binance_web3、manual_blacklist';
        COMMENT ON COLUMN bottom_watchlist_tokens.peak_mcap IS '加入观察池或更新过程记录到的峰值市值，美元';
        COMMENT ON COLUMN bottom_watchlist_tokens.last_mcap IS '最近一次扫描写入的市值，美元';
        COMMENT ON COLUMN bottom_watchlist_tokens.highest_mcap IS '观察池生命周期内记录到的最高市值，美元';
        COMMENT ON COLUMN bottom_watchlist_tokens.current_mcap IS '当前用于前端展示和判断的最新市值，美元';
        COMMENT ON COLUMN bottom_watchlist_tokens.gmgn_created_at IS 'GMGN返回的代币创建时间，Unix秒';
        COMMENT ON COLUMN bottom_watchlist_tokens.gmgn_open_at IS 'GMGN返回的代币发射/开盘时间，优先来自open_timestamp，Unix秒';
        COMMENT ON COLUMN bottom_watchlist_tokens.note IS '系统写入的备注，例如自动加入原因';
        COMMENT ON COLUMN bottom_watchlist_tokens.remark IS '人工备注或补充说明';
        COMMENT ON COLUMN bottom_watchlist_tokens.symbol IS '代币符号';
        COMMENT ON COLUMN bottom_watchlist_tokens.fee_sol IS '创建或相关交易手续费SOL，用于1M标筛选';
        COMMENT ON COLUMN bottom_watchlist_tokens.token_created_at IS '代币创建时间，Unix秒';
        COMMENT ON COLUMN bottom_watchlist_tokens.token_launch_at IS '代币发射/开盘时间，池子迁移或open_timestamp，Unix秒';
        COMMENT ON COLUMN bottom_watchlist_tokens.daily_mcap_date IS '首次或最近记录达到每日1M市值条件的日期';
        COMMENT ON COLUMN bottom_watchlist_tokens.daily_mcap_threshold IS '每日市值里程碑阈值，默认1000000美元';
        COMMENT ON COLUMN bottom_watchlist_tokens.daily_mcap_notified_date IS '每日1M市值通知日期';
        COMMENT ON COLUMN bottom_watchlist_tokens.daily_mcap_notified_at IS '每日1M市值通知发送时间';
        COMMENT ON COLUMN bottom_watchlist_tokens.ath_mcap IS 'GMGN或监控识别到的历史最高市值，美元';
        COMMENT ON COLUMN bottom_watchlist_tokens.blacklisted IS '是否被黑名单过滤，true时扫描跳过';
        COMMENT ON COLUMN bottom_watchlist_tokens.last_pool_liquidity IS '最近一次可靠池子流动性，美元';
        COMMENT ON COLUMN bottom_watchlist_tokens.last_pool_mcap_ratio IS '最近一次可靠池子流动性与市值比值';
        COMMENT ON COLUMN bottom_watchlist_tokens.narrative_desc IS 'Binance Web3或其他来源识别到的叙事描述';
        COMMENT ON COLUMN bottom_watchlist_tokens.narrative_type IS '叙事分类或标签类型';

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
