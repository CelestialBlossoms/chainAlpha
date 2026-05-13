"""
SQLite database for storing token CA analysis results.

Tables:
  tokens            — token metadata, security, pool metrics
  token_holders     — per-wallet position & P&L data
  token_pnl_summary — aggregate P&L statistics (holders & traders)
  kline_candles     — raw OHLCV candles
  kline_analysis    — derived K-line metrics & phases
  cluster_analysis  — wallet cluster & bundle detection results
"""
import sqlite3
import json
from datetime import datetime, timezone

DB_PATH = None


def set_db_path(path: str):
    global DB_PATH
    DB_PATH = path


def _connect():
    if DB_PATH is None:
        raise RuntimeError("DB_PATH not set. Call set_db_path() first.")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = _connect()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS tokens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT NOT NULL UNIQUE,
            chain TEXT NOT NULL DEFAULT 'sol',
            symbol TEXT,
            name TEXT,
            price REAL DEFAULT 0,
            circulating_supply REAL DEFAULT 0,
            market_cap REAL DEFAULT 0,
            liquidity REAL DEFAULT 0,
            ath_price REAL DEFAULT 0,
            holder_count INTEGER DEFAULT 0,
            launchpad_platform TEXT,
            creation_timestamp INTEGER DEFAULT 0,
            -- creator
            creator_address TEXT,
            creator_status TEXT,
            cto_flag INTEGER DEFAULT 0,
            creator_prev_tokens INTEGER DEFAULT 0,
            creator_best_ath_mc REAL DEFAULT 0,
            -- social
            twitter_username TEXT,
            website TEXT,
            telegram TEXT,
            -- wallet tag stats
            smart_wallets INTEGER DEFAULT 0,
            kol_wallets INTEGER DEFAULT 0,
            sniper_wallets INTEGER DEFAULT 0,
            bundler_wallets INTEGER DEFAULT 0,
            rat_trader_wallets INTEGER DEFAULT 0,
            fresh_wallets INTEGER DEFAULT 0,
            -- security
            renounced_mint INTEGER DEFAULT 0,
            renounced_freeze INTEGER DEFAULT 0,
            buy_tax REAL DEFAULT 0,
            sell_tax REAL DEFAULT 0,
            burn_status TEXT,
            -- risk signals
            bot_degen_rate REAL DEFAULT 0,
            bundler_vol_rate REAL DEFAULT 0,
            rat_trader_rate REAL DEFAULT 0,
            entrapment_rate REAL DEFAULT 0,
            top10_concentration REAL DEFAULT 0,
            -- pool
            pool_exchange TEXT,
            pool_quote_symbol TEXT,
            pool_base_reserve REAL DEFAULT 0,
            pool_quote_reserve REAL DEFAULT 0,
            pool_base_reserve_value REAL DEFAULT 0,
            pool_quote_reserve_value REAL DEFAULT 0,
            pool_initial_liquidity REAL DEFAULT 0,
            pool_initial_base_reserve REAL DEFAULT 0,
            pool_initial_quote_reserve REAL DEFAULT 0,
            -- pool derived metrics
            lp_growth_pct REAL DEFAULT 0,
            base_change_pct REAL DEFAULT 0,
            quote_change_pct REAL DEFAULT 0,
            -- composite score
            composite_score REAL DEFAULT 0,
            -- timestamps
            fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS token_holders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token_address TEXT NOT NULL,
            wallet_address TEXT NOT NULL,
            holder_type TEXT NOT NULL DEFAULT 'holder',  -- holder | trader
            -- position
            amount_percentage REAL DEFAULT 0,
            balance REAL DEFAULT 0,
            usd_value REAL DEFAULT 0,
            -- P&L
            profit REAL DEFAULT 0,
            realized_profit REAL DEFAULT 0,
            unrealized_profit REAL DEFAULT 0,
            profit_change REAL DEFAULT 0,
            total_cost REAL DEFAULT 0,
            avg_cost REAL DEFAULT 0,
            avg_sold REAL DEFAULT 0,
            history_bought_cost REAL DEFAULT 0,
            history_sold_income REAL DEFAULT 0,
            buy_volume_cur REAL DEFAULT 0,
            sell_volume_cur REAL DEFAULT 0,
            netflow_usd REAL DEFAULT 0,
            -- trading behavior
            buy_count INTEGER DEFAULT 0,
            sell_count INTEGER DEFAULT 0,
            buy_tx_count INTEGER DEFAULT 0,
            sell_tx_count INTEGER DEFAULT 0,
            sell_amount_percentage REAL DEFAULT 0,
            hold_duration_seconds REAL DEFAULT 0,
            start_holding_at INTEGER DEFAULT 0,
            -- tags
            tags TEXT DEFAULT '[]',
            maker_token_tags TEXT DEFAULT '[]',
            addr_type INTEGER DEFAULT 0,  -- 2 = pool
            -- metadata
            twitter_username TEXT,
            twitter_name TEXT,
            fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(token_address, wallet_address, holder_type)
        );
        CREATE INDEX IF NOT EXISTS idx_holders_token ON token_holders(token_address, holder_type);
        CREATE INDEX IF NOT EXISTS idx_holders_wallet ON token_holders(wallet_address);

        CREATE TABLE IF NOT EXISTS token_pnl_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token_address TEXT NOT NULL,
            source_type TEXT NOT NULL,  -- holders | traders
            -- counts
            total_wallets INTEGER DEFAULT 0,
            pool_addresses INTEGER DEFAULT 0,
            wallets_with_balance INTEGER DEFAULT 0,
            wallets_exited INTEGER DEFAULT 0,
            -- aggregate P&L
            total_profit REAL DEFAULT 0,
            total_realized_profit REAL DEFAULT 0,
            total_unrealized_profit REAL DEFAULT 0,
            total_cost REAL DEFAULT 0,
            total_buy_vol REAL DEFAULT 0,
            total_sell_vol REAL DEFAULT 0,
            overall_roi_pct REAL DEFAULT 0,
            -- win/loss
            profitable_count INTEGER DEFAULT 0,
            losing_count INTEGER DEFAULT 0,
            breakeven_count INTEGER DEFAULT 0,
            sum_profitable_profit REAL DEFAULT 0,
            sum_losing_loss REAL DEFAULT 0,
            win_rate_pct REAL DEFAULT 0,
            profit_loss_ratio REAL DEFAULT 0,
            -- realized/unrealized breakdown
            realized_profitable_count INTEGER DEFAULT 0,
            realized_losing_count INTEGER DEFAULT 0,
            unrealized_profitable_count INTEGER DEFAULT 0,
            unrealized_losing_count INTEGER DEFAULT 0,
            -- distribution JSON
            distribution_json TEXT DEFAULT '{}',
            -- meta
            fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(token_address, source_type)
        );

        CREATE TABLE IF NOT EXISTS kline_candles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token_address TEXT NOT NULL,
            resolution TEXT NOT NULL DEFAULT '5m',
            candle_ts INTEGER NOT NULL,
            open REAL DEFAULT 0,
            high REAL DEFAULT 0,
            low REAL DEFAULT 0,
            close REAL DEFAULT 0,
            volume REAL DEFAULT 0,
            UNIQUE(token_address, resolution, candle_ts)
        );
        CREATE INDEX IF NOT EXISTS idx_kline_token ON kline_candles(token_address, resolution);

        CREATE TABLE IF NOT EXISTS kline_analysis (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token_address TEXT NOT NULL,
            resolution TEXT NOT NULL DEFAULT '5m',
            candle_count INTEGER DEFAULT 0,
            first_open REAL DEFAULT 0,
            ath REAL DEFAULT 0,
            ath_idx INTEGER DEFAULT 0,
            atl REAL DEFAULT 0,
            last_close REAL DEFAULT 0,
            total_change_pct REAL DEFAULT 0,
            ath_gain REAL DEFAULT 0,
            max_drawdown_pct REAL DEFAULT 0,
            total_volume REAL DEFAULT 0,
            avg_volume REAL DEFAULT 0,
            max_volume REAL DEFAULT 0,
            vol_ratio REAL DEFAULT 0,
            green_candles INTEGER DEFAULT 0,
            red_candles INTEGER DEFAULT 0,
            green_ratio REAL DEFAULT 0,
            sma20 REAL DEFAULT 0,
            sma50 REAL DEFAULT 0,
            resistance REAL DEFAULT 0,
            support REAL DEFAULT 0,
            trend TEXT,
            phases_json TEXT DEFAULT '[]',
            fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(token_address, resolution)
        );

        CREATE TABLE IF NOT EXISTS cluster_analysis (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token_address TEXT NOT NULL,
            -- cost tiers
            cost_tier_count INTEGER DEFAULT 0,
            cost_tier_mean REAL DEFAULT 0,
            cost_tier_deviation_pct REAL DEFAULT 0,
            cost_tiers_json TEXT DEFAULT '[]',
            -- position distribution
            position_dist_json TEXT DEFAULT '{}',
            position_narrow_band_clusters INTEGER DEFAULT 0,
            position_max_in_band INTEGER DEFAULT 0,
            -- trading behavior
            behavior_single_buy_count INTEGER DEFAULT 0,
            behavior_multi_buy_count INTEGER DEFAULT 0,
            behavior_never_sold_count INTEGER DEFAULT 0,
            behavior_has_sold_count INTEGER DEFAULT 0,
            behavior_single_buy_pct REAL DEFAULT 0,
            behavior_multi_buy_pct REAL DEFAULT 0,
            behavior_never_sold_pct REAL DEFAULT 0,
            -- bot vs human
            bot_wallet_count INTEGER DEFAULT 0,
            bot_avg_position REAL DEFAULT 0,
            bot_avg_cost REAL DEFAULT 0,
            bot_total_buy REAL DEFAULT 0,
            human_wallet_count INTEGER DEFAULT 0,
            human_avg_position REAL DEFAULT 0,
            human_avg_cost REAL DEFAULT 0,
            human_total_buy REAL DEFAULT 0,
            bot_buy_share_pct REAL DEFAULT 0,
            -- tag ecology JSON
            tag_ecology_json TEXT DEFAULT '{}',
            -- smart money in top100
            smart_money_in_top100 INTEGER DEFAULT 0,
            -- creation time clustering JSON
            creation_time_json TEXT DEFAULT '{}',
            -- bundle verdict
            bundle_score INTEGER DEFAULT 0,
            bundle_verdict TEXT,
            -- meta
            fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(token_address)
        );

        CREATE TABLE IF NOT EXISTS ingest_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token_address TEXT NOT NULL,
            chain TEXT NOT NULL DEFAULT 'sol',
            modules_fetched TEXT DEFAULT '',
            status TEXT DEFAULT 'pending',
            error_msg TEXT,
            started_at TEXT NOT NULL DEFAULT (datetime('now')),
            completed_at TEXT
        );
    """)
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Token CRUD
# ---------------------------------------------------------------------------

def upsert_token(addr: str, info: dict, security: dict):
    conn = _connect()
    dev = info.get("dev", {}) or {}
    stat = info.get("stat", {}) or {}
    wts = info.get("wallet_tags_stat", {}) or {}
    link = info.get("link", {}) or {}
    pool = info.get("pool", {}) or {}

    def _f(v, d=0.0):
        try:
            if v in (None, ""): return d
            if isinstance(v, (dict, list)): return d
            return float(v)
        except (ValueError, TypeError):
            return d

    price = _f(info.get("price"))
    supply = _f(info.get("circulating_supply"))
    mcap = price * supply
    liq = _f(info.get("liquidity"))

    # Pool derived metrics
    base_reserve = _f(pool.get("base_reserve"))
    quote_reserve = _f(pool.get("quote_reserve"))
    init_liq = _f(pool.get("initial_liquidity"))
    init_base = _f(pool.get("initial_base_reserve"))
    init_quote = _f(pool.get("initial_quote_reserve"))
    lp_growth = (liq - init_liq) / init_liq * 100 if init_liq > 0 else 0
    base_change = (base_reserve - init_base) / init_base * 100 if init_base > 0 else 0
    quote_change = (quote_reserve - init_quote) / init_quote * 100 if init_quote > 0 else 0

    conn.execute("""
        INSERT OR REPLACE INTO tokens (
            address, chain, symbol, name, price, circulating_supply, market_cap, liquidity,
            ath_price, holder_count, launchpad_platform, creation_timestamp,
            creator_address, creator_status, cto_flag, creator_prev_tokens, creator_best_ath_mc,
            twitter_username, website, telegram,
            smart_wallets, kol_wallets, sniper_wallets, bundler_wallets,
            rat_trader_wallets, fresh_wallets,
            renounced_mint, renounced_freeze, buy_tax, sell_tax, burn_status,
            bot_degen_rate, bundler_vol_rate, rat_trader_rate, entrapment_rate, top10_concentration,
            pool_exchange, pool_quote_symbol,
            pool_base_reserve, pool_quote_reserve, pool_base_reserve_value, pool_quote_reserve_value,
            pool_initial_liquidity, pool_initial_base_reserve, pool_initial_quote_reserve,
            lp_growth_pct, base_change_pct, quote_change_pct,
            fetched_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        addr, "sol",
        info.get("symbol"), info.get("name"),
        price, supply, mcap, liq,
        _f(info.get("ath_price")), info.get("holder_count", 0),
        info.get("launchpad_platform"), info.get("creation_timestamp", 0),
        dev.get("creator_address"), dev.get("creator_token_status"),
        int(dev.get("cto_flag", 0)), dev.get("creator_open_count", 0),
        _f((dev.get("ath_token_info") or {}).get("ath_mc", 0)),
        link.get("twitter_username"), link.get("website"), link.get("telegram"),
        wts.get("smart_wallets", 0), wts.get("renowned_wallets", 0),
        wts.get("sniper_wallets", 0), wts.get("bundler_wallets", 0),
        wts.get("rat_trader_wallets", 0), wts.get("fresh_wallets", 0),
        int(security.get("renounced_mint", False)), int(security.get("renounced_freeze_account", False)),
        _f(security.get("buy_tax", 0)), _f(security.get("sell_tax", 0)),
        security.get("burn_status"),
        _f(stat.get("top_bot_degen_percentage")), _f(stat.get("top_bundler_trader_percentage")),
        _f(stat.get("top_rat_trader_percentage")), _f(stat.get("top_entrapment_trader_percentage")),
        _f(stat.get("top_10_holder_rate")),
        pool.get("exchange"), pool.get("quote_symbol"),
        base_reserve, quote_reserve,
        _f(pool.get("base_reserve_value")), _f(pool.get("quote_reserve_value")),
        init_liq, init_base, init_quote,
        lp_growth, base_change, quote_change,
        datetime.now(timezone.utc).isoformat(),
    ))
    conn.commit()
    conn.close()


def upsert_holders(addr: str, holders: list, htype: str = "holder"):
    """Bulk upsert holders or traders."""
    conn = _connect()
    for h in holders:
        if not isinstance(h, dict):
            continue
        wallet = h.get("address", "")
        if not wallet:
            continue

        def _f(v, d=0.0):
            try:
                if v in (None, ""): return d
                return float(v)
            except (ValueError, TypeError):
                return d

        tags = json.dumps(h.get("tags") or [])
        mtags = json.dumps(h.get("maker_token_tags") or [])

        conn.execute("""
            INSERT OR REPLACE INTO token_holders (
                token_address, wallet_address, holder_type,
                amount_percentage, balance, usd_value,
                profit, realized_profit, unrealized_profit, profit_change,
                total_cost, avg_cost, avg_sold,
                history_bought_cost, history_sold_income,
                buy_volume_cur, sell_volume_cur, netflow_usd,
                buy_count, sell_count, buy_tx_count, sell_tx_count,
                sell_amount_percentage, hold_duration_seconds, start_holding_at,
                tags, maker_token_tags, addr_type,
                twitter_username, twitter_name,
                fetched_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            addr, wallet, htype,
            _f(h.get("amount_percentage")), _f(h.get("balance")), _f(h.get("usd_value")),
            _f(h.get("profit")), _f(h.get("realized_profit")), _f(h.get("unrealized_profit")),
            _f(h.get("profit_change")),
            _f(h.get("total_cost")), _f(h.get("avg_cost")), _f(h.get("avg_sold")),
            _f(h.get("history_bought_cost")), _f(h.get("history_sold_income")),
            _f(h.get("buy_volume_cur")), _f(h.get("sell_volume_cur")), _f(h.get("netflow_usd")),
            h.get("buy_count", 0) or h.get("buy", 0) or 0,
            h.get("sell_count", 0) or h.get("sell", 0) or 0,
            h.get("buy_tx_count", 0) or h.get("buy_tx_count_cur", 0) or 0,
            h.get("sell_tx_count", 0) or h.get("sell_tx_count_cur", 0) or 0,
            _f(h.get("sell_amount_percentage")), _f(h.get("hold_duration_seconds")),
            h.get("start_holding_at", 0),
            tags, mtags, h.get("addr_type", 0),
            h.get("twitter_username"), h.get("name"),
            datetime.now(timezone.utc).isoformat(),
        ))
    conn.commit()
    conn.close()


def upsert_pnl_summary(addr: str, stype: str, r: dict, distribution_buckets: list):
    conn = _connect()
    conn.execute("""
        INSERT OR REPLACE INTO token_pnl_summary (
            token_address, source_type,
            total_wallets, pool_addresses, wallets_with_balance, wallets_exited,
            total_profit, total_realized_profit, total_unrealized_profit,
            total_cost, total_buy_vol, total_sell_vol, overall_roi_pct,
            profitable_count, losing_count, breakeven_count,
            sum_profitable_profit, sum_losing_loss, win_rate_pct, profit_loss_ratio,
            realized_profitable_count, realized_losing_count,
            unrealized_profitable_count, unrealized_losing_count,
            distribution_json, fetched_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        addr, stype,
        r.get("total", 0), r.get("pool_addresses", 0),
        r.get("wallets_with_balance", 0), r.get("wallets_exited", 0),
        r.get("total_profit", 0), r.get("total_realized_profit", 0),
        r.get("total_unrealized_profit", 0),
        r.get("total_cost", 0), r.get("total_buy_vol", 0), r.get("total_sell_vol", 0),
        r.get("overall_roi_pct", 0),
        r.get("profitable_count", 0), r.get("losing_count", 0), r.get("breakeven_count", 0),
        r.get("sum_profitable_profit", 0), r.get("sum_losing_loss", 0),
        r.get("win_rate_pct", 0), r.get("profit_loss_ratio", 0),
        r.get("realized_profitable_count", 0), r.get("realized_losing_count", 0),
        r.get("unrealized_profitable_count", 0), r.get("unrealized_losing_count", 0),
        json.dumps(distribution_buckets),
        datetime.now(timezone.utc).isoformat(),
    ))
    conn.commit()
    conn.close()


def upsert_kline_candles(addr: str, resolution: str, candles: list):
    conn = _connect()
    for c in candles:
        conn.execute("""
            INSERT OR REPLACE INTO kline_candles (
                token_address, resolution, candle_ts,
                open, high, low, close, volume
            ) VALUES (?,?,?,?,?,?,?,?)
        """, (
            addr, resolution, c["ts"],
            c["open"], c["high"], c["low"], c["close"], c["volume"],
        ))
    conn.commit()
    conn.close()


def upsert_kline_analysis(addr: str, resolution: str, a: dict):
    conn = _connect()
    conn.execute("""
        INSERT OR REPLACE INTO kline_analysis (
            token_address, resolution, candle_count,
            first_open, ath, ath_idx, atl, last_close,
            total_change_pct, ath_gain, max_drawdown_pct,
            total_volume, avg_volume, max_volume, vol_ratio,
            green_candles, red_candles, green_ratio,
            sma20, sma50, resistance, support, trend,
            phases_json, fetched_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        addr, resolution, a.get("candle_count", 0),
        a.get("first_open", 0), a.get("ath", 0), a.get("ath_idx", 0),
        a.get("atl", 0), a.get("last_close", 0),
        a.get("total_change_pct", 0), a.get("ath_gain", 0),
        a.get("max_drawdown_pct", 0),
        a.get("total_volume", 0), a.get("avg_volume", 0),
        a.get("max_volume", 0), a.get("vol_ratio", 0),
        a.get("green_candles", 0), a.get("red_candles", 0),
        a.get("green_ratio", 0),
        a.get("sma20", 0), a.get("sma50", 0),
        a.get("resistance", 0), a.get("support", 0),
        a.get("trend", ""),
        json.dumps(a.get("phases", [])),
        datetime.now(timezone.utc).isoformat(),
    ))
    conn.commit()
    conn.close()


def upsert_cluster_analysis(addr: str, data: dict):
    conn = _connect()
    conn.execute("""
        INSERT OR REPLACE INTO cluster_analysis (
            token_address,
            cost_tier_count, cost_tier_mean, cost_tier_deviation_pct, cost_tiers_json,
            position_dist_json, position_narrow_band_clusters, position_max_in_band,
            behavior_single_buy_count, behavior_multi_buy_count,
            behavior_never_sold_count, behavior_has_sold_count,
            behavior_single_buy_pct, behavior_multi_buy_pct,
            behavior_never_sold_pct,
            bot_wallet_count, bot_avg_position, bot_avg_cost, bot_total_buy,
            human_wallet_count, human_avg_position, human_avg_cost, human_total_buy,
            bot_buy_share_pct,
            tag_ecology_json,
            smart_money_in_top100,
            creation_time_json,
            bundle_score, bundle_verdict,
            fetched_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        addr,
        data.get("cost_tier_count", 0), data.get("cost_tier_mean", 0),
        data.get("cost_tier_deviation_pct", 0), json.dumps(data.get("cost_tiers", [])),
        json.dumps(data.get("position_dist", {})),
        data.get("position_narrow_band_clusters", 0), data.get("position_max_in_band", 0),
        data.get("behavior_single_buy_count", 0), data.get("behavior_multi_buy_count", 0),
        data.get("behavior_never_sold_count", 0), data.get("behavior_has_sold_count", 0),
        data.get("behavior_single_buy_pct", 0), data.get("behavior_multi_buy_pct", 0),
        data.get("behavior_never_sold_pct", 0),
        data.get("bot_wallet_count", 0), data.get("bot_avg_position", 0),
        data.get("bot_avg_cost", 0), data.get("bot_total_buy", 0),
        data.get("human_wallet_count", 0), data.get("human_avg_position", 0),
        data.get("human_avg_cost", 0), data.get("human_total_buy", 0),
        data.get("bot_buy_share_pct", 0),
        json.dumps(data.get("tag_ecology", {})),
        data.get("smart_money_in_top100", 0),
        json.dumps(data.get("creation_time_clusters", {})),
        data.get("bundle_score", 0), data.get("bundle_verdict", ""),
        datetime.now(timezone.utc).isoformat(),
    ))
    conn.commit()
    conn.close()


def log_ingest(addr: str, chain: str, modules: str, status: str, error: str = None):
    conn = _connect()
    conn.execute("""
        INSERT INTO ingest_log (token_address, chain, modules_fetched, status, error_msg)
        VALUES (?,?,?,?,?)
    """, (addr, chain, modules, status, error))
    conn.commit()
    conn.close()


def update_ingest_log(addr: str, status: str, error: str = None):
    conn = _connect()
    if error:
        conn.execute("""
            UPDATE ingest_log SET status=?, error_msg=?, completed_at=datetime('now')
            WHERE token_address=? AND completed_at IS NULL
        """, (status, error, addr))
    else:
        conn.execute("""
            UPDATE ingest_log SET status=?, completed_at=datetime('now')
            WHERE token_address=? AND completed_at IS NULL
        """, (status, addr))
    conn.commit()
    conn.close()
