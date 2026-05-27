import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from db_client import db_op


def init_bottom_accumulation_tables(conn):
    cur = conn.cursor()
    cur.execute(
        """
        DROP TABLE IF EXISTS bottom_accumulation_signals;
        DROP TABLE IF EXISTS bottom_holder_wallets;
        DROP TABLE IF EXISTS bottom_holder_snapshots;
        DROP TABLE IF EXISTS bottom_holder_scan_runs;
        DROP TABLE IF EXISTS bottom_kline_cache_1m;
        DROP TABLE IF EXISTS bottom_kline_cache;
        DROP TABLE IF EXISTS bottom_top100_snapshots;

        CREATE TABLE bottom_top100_snapshots (
            id BIGSERIAL PRIMARY KEY,
            scan_id TEXT NOT NULL,
            chain TEXT NOT NULL DEFAULT 'sol',
            trend_interval TEXT NOT NULL DEFAULT '1h',
            address TEXT NOT NULL,
            symbol TEXT,
            snapshot_ts BIGINT NOT NULL,
            signal_type TEXT,
            signal_score INTEGER DEFAULT 0,
            notified BOOLEAN DEFAULT FALSE,
            summary JSONB NOT NULL,
            holders JSONB NOT NULL,
            top_profit_traders JSONB NOT NULL DEFAULT '[]'::jsonb,
            top_loss_traders JSONB NOT NULL DEFAULT '[]'::jsonb,
            analysis JSONB,
            raw_token JSONB,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE INDEX idx_bottom_top100_addr_ts
            ON bottom_top100_snapshots(address, snapshot_ts DESC);
        CREATE INDEX idx_bottom_top100_scan
            ON bottom_top100_snapshots(scan_id);
        CREATE INDEX idx_bottom_top100_signal
            ON bottom_top100_snapshots(signal_type, signal_score DESC, created_at DESC);

        COMMENT ON TABLE bottom_top100_snapshots IS 'Top100持仓快照表。每次进入异动检测流程都会记录当时GMGN Top100持仓、摘要和分析结果';
        COMMENT ON COLUMN bottom_top100_snapshots.id IS '快照自增ID，可被bottom_top100_push_records.snapshot_id引用';
        COMMENT ON COLUMN bottom_top100_snapshots.scan_id IS '一次扫描批次ID';
        COMMENT ON COLUMN bottom_top100_snapshots.chain IS '链名称，当前主要为sol';
        COMMENT ON COLUMN bottom_top100_snapshots.trend_interval IS '扫描来源时间窗口，例如1m、5m、1h，watchlist来源可能沿用当前窗口';
        COMMENT ON COLUMN bottom_top100_snapshots.address IS '代币CA';
        COMMENT ON COLUMN bottom_top100_snapshots.symbol IS '快照时识别到的代币符号';
        COMMENT ON COLUMN bottom_top100_snapshots.snapshot_ts IS '快照采集时间，Unix秒';
        COMMENT ON COLUMN bottom_top100_snapshots.signal_type IS '本次快照分析出的信号类型，watch表示仅观察未推送';
        COMMENT ON COLUMN bottom_top100_snapshots.signal_score IS '本次信号评分';
        COMMENT ON COLUMN bottom_top100_snapshots.notified IS '历史兼容字段，当前推送状态以bottom_top100_push_records为准';
        COMMENT ON COLUMN bottom_top100_snapshots.summary IS '本次快照的市值、池子、Top10/20/50/100占比、买卖额等摘要JSON';
        COMMENT ON COLUMN bottom_top100_snapshots.holders IS '本次快照归一化后的GMGN Top100持仓明细JSON';
        COMMENT ON COLUMN bottom_top100_snapshots.top_profit_traders IS 'GMGN token traders snapshot ordered by realized profit desc';
        COMMENT ON COLUMN bottom_top100_snapshots.top_loss_traders IS 'GMGN token traders loss-candidate snapshot sorted locally by negative realized or unrealized PnL';
        COMMENT ON COLUMN bottom_top100_snapshots.analysis IS '本次异动检测分析结果JSON';
        COMMENT ON COLUMN bottom_top100_snapshots.raw_token IS '合并trending、watchlist、metadata后的原始代币数据JSON';
        COMMENT ON COLUMN bottom_top100_snapshots.created_at IS '数据库写入时间';

        CREATE TABLE IF NOT EXISTS bottom_top100_push_records (
            id BIGSERIAL PRIMARY KEY,
            pushed_at TIMESTAMPTZ DEFAULT now(),
            event_ts BIGINT NOT NULL,
            snapshot_id BIGINT,
            chain TEXT NOT NULL DEFAULT 'sol',
            source TEXT NOT NULL DEFAULT 'bottom_abnormal',
            status TEXT NOT NULL DEFAULT 'frontend_update',
            address TEXT NOT NULL,
            symbol TEXT,
            signal_type TEXT,
            abnormal_rule TEXT,
            trend_interval TEXT,
            current_mcap NUMERIC DEFAULT 0,
            first_signal_mcap NUMERIC DEFAULT 0,
            first_signal_ts BIGINT DEFAULT 0,
            first_signal_change_pct NUMERIC DEFAULT 0,
            price_change_pct NUMERIC DEFAULT 0,
            max_abnormal_mcap NUMERIC DEFAULT 0,
            ath_mcap NUMERIC DEFAULT 0,
            liquidity NUMERIC DEFAULT 0,
            pool_total_liquidity NUMERIC DEFAULT 0,
            pool_mcap_ratio NUMERIC DEFAULT 0,
            age_sec BIGINT DEFAULT 0,
            text TEXT,
            extra JSONB DEFAULT '{}'::jsonb
        );
        CREATE INDEX idx_bottom_top100_push_records_addr_ts
            ON bottom_top100_push_records(address, event_ts DESC);
        CREATE INDEX idx_bottom_top100_push_records_signal_ts
            ON bottom_top100_push_records(signal_type, event_ts DESC);
        CREATE INDEX idx_bottom_top100_push_records_snapshot
            ON bottom_top100_push_records(snapshot_id);
        CREATE INDEX idx_bottom_top100_push_records_pushed_at
            ON bottom_top100_push_records(pushed_at DESC);
        CREATE UNIQUE INDEX uq_bottom_top100_push_records_signal
            ON bottom_top100_push_records(chain, source, address, signal_type);

        COMMENT ON TABLE bottom_top100_push_records IS 'Top100异动首次推送记录表。每个CA在同一chain/source下只保留首次推送，后续检索明细由bottom_top100_snapshots记录';
        COMMENT ON COLUMN bottom_top100_push_records.id IS '推送记录自增ID';
        COMMENT ON COLUMN bottom_top100_push_records.pushed_at IS '数据库写入时间';
        COMMENT ON COLUMN bottom_top100_push_records.event_ts IS '推送发生时间，Unix秒';
        COMMENT ON COLUMN bottom_top100_push_records.snapshot_id IS '关联bottom_top100_snapshots.id，用于回查当时GMGN Top100持仓快照';
        COMMENT ON COLUMN bottom_top100_push_records.chain IS '链名称，当前主要为sol';
        COMMENT ON COLUMN bottom_top100_push_records.source IS '推送来源模块，例如bottom_abnormal';
        COMMENT ON COLUMN bottom_top100_push_records.status IS '推送状态，例如frontend_update';
        COMMENT ON COLUMN bottom_top100_push_records.address IS '代币CA，同一chain/source下唯一，只记录首次推送';
        COMMENT ON COLUMN bottom_top100_push_records.symbol IS '推送时识别到的代币符号';
        COMMENT ON COLUMN bottom_top100_push_records.signal_type IS '异动类型，例如abnormal、new_revival、drop_40w、quiet_runup、ema_golden_cross';
        COMMENT ON COLUMN bottom_top100_push_records.abnormal_rule IS '命中的异动规则或档位';
        COMMENT ON COLUMN bottom_top100_push_records.trend_interval IS '该代币来自的GMGN trending时间窗口，例如1m、5m、1h，可能为多个窗口合并';
        COMMENT ON COLUMN bottom_top100_push_records.current_mcap IS '推送当时市值，美元';
        COMMENT ON COLUMN bottom_top100_push_records.first_signal_mcap IS '该异动类型在当前基线窗口内首次异动市值，美元';
        COMMENT ON COLUMN bottom_top100_push_records.first_signal_ts IS '该异动类型在当前基线窗口内首次异动时间，Unix秒';
        COMMENT ON COLUMN bottom_top100_push_records.first_signal_change_pct IS '相对首次异动市值涨幅百分比';
        COMMENT ON COLUMN bottom_top100_push_records.price_change_pct IS '本次异动检测使用的价格或市值涨幅百分比';
        COMMENT ON COLUMN bottom_top100_push_records.max_abnormal_mcap IS '当前异动规则允许或记录的最高异常市值档位，美元';
        COMMENT ON COLUMN bottom_top100_push_records.ath_mcap IS 'GMGN或监控识别到的历史最高市值，美元';
        COMMENT ON COLUMN bottom_top100_push_records.liquidity IS '推送当时流动性，美元';
        COMMENT ON COLUMN bottom_top100_push_records.pool_total_liquidity IS '推送当时池子总流动性，美元，与liquidity保持兼容';
        COMMENT ON COLUMN bottom_top100_push_records.pool_mcap_ratio IS '池子流动性与市值比值';
        COMMENT ON COLUMN bottom_top100_push_records.age_sec IS '推送时代币年龄，秒';
        COMMENT ON COLUMN bottom_top100_push_records.text IS '推送给TG或插件前端的文本内容';
        COMMENT ON COLUMN bottom_top100_push_records.extra IS '推送时的完整结构化扩展数据JSON，不包含Top100 holders明细';

        CREATE TABLE bottom_kline_cache (
            chain TEXT NOT NULL DEFAULT 'sol',
            address TEXT NOT NULL,
            resolution TEXT NOT NULL,
            ts BIGINT NOT NULL,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume NUMERIC,
            amount NUMERIC,
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (chain, address, resolution, ts)
        );

        CREATE INDEX idx_bottom_kline_cache_addr_res_ts
            ON bottom_kline_cache(address, resolution, ts);

        CREATE TABLE bottom_kline_cache_1m (
            chain TEXT NOT NULL DEFAULT 'sol',
            address TEXT NOT NULL,
            resolution TEXT NOT NULL,
            ts BIGINT NOT NULL,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            volume NUMERIC,
            amount NUMERIC,
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (chain, address, resolution, ts)
        );

        CREATE INDEX idx_bottom_kline_cache_1m_addr_res_ts
            ON bottom_kline_cache_1m(address, resolution, ts);
        """
    )
    print("Initialized Top100 holder snapshot monitor and kline cache")


if __name__ == "__main__":
    db_op(init_bottom_accumulation_tables)
