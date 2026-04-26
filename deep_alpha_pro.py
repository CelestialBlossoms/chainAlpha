import json
import subprocess
import time
import requests
from datetime import datetime
from collections import defaultdict
from psycopg2.extras import Json
from db_client import db_op
from config import TG_BOT_TOKEN, TG_CHAT_ID, CHAINS

# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------
CHECK_INTERVAL = 45 
TREND_INTERVALS = ["5m"]
MIN_MCAP_USD = 10_000
MIN_FEE_SOL = 1
DUMP_PROGRESS_THRESHOLD = 20
MIN_DUMP_ASSOCIATED_SUPPLY = 10
MIN_DUMP_SOLD_SUPPLY = 2
DEBUG_DEEP_LOG = False
MIN_CANDIDATE_CONTROL_RATIO = 10
MIN_CANDIDATE_CLUSTER_SIZE = 20
MIN_CANDIDATE_SM_COUNT = 1
MIN_CANDIDATE_HOLDER_COUNT = 500
MIN_BUY_SCORE = 20
MIN_INFLOW_STREAK = 2
MAX_DEV_BUY_USD = 500
MAX_DEV_HOLD_RATE = 0.30
MAX_MCAP_USD = 1_000_000
MIN_TOP_HOLDER_NETFLOW_USD = 5_000
MIN_FRONT_HOLDER_NETFLOW_USD = 2_000
NEW_WALLET_WINDOW_SEC = 3 * 24 * 60 * 60
WALLET_CREATION_CLUSTER_SEC = 5 * 24 * 60 * 60
KLINE_LOOKBACK_SEC = 2 * 60 * 60
NEW_TOKEN_MAX_AGE_SEC = 60 * 60
EARLY_TOKEN_MAX_AGE_SEC = 24 * 60 * 60
INFLOW_STATE = {}

def save_alpha_candidate(chain, interval, address, stats, tg_message_id=None):
    def _op(conn):
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO alpha_token_candidates (
                address, chain, symbol, trend_interval, mcap_at_alert,
                holder_count, fee_sol, pool_label, pool_liquidity,
                token_created_ts, token_created_time, verdict,
                control_ratio, associated_supply, associated_count,
                cluster_size, dump_progress, sold_supply_pct, is_dumping,
                buys_5m, sells_5m, net_flow_5m, inflow_5m, inflow_streak,
                buy_score, buy_reasons, sm_count, kol_count, top10_rate,
                snipers, rug_ratio, raw_stats, tg_chat_id, tg_message_id
            ) VALUES (
                %(address)s, %(chain)s, %(symbol)s, %(trend_interval)s, %(mcap_at_alert)s,
                %(holder_count)s, %(fee_sol)s, %(pool_label)s, %(pool_liquidity)s,
                %(token_created_ts)s, %(token_created_time)s, %(verdict)s,
                %(control_ratio)s, %(associated_supply)s, %(associated_count)s,
                %(cluster_size)s, %(dump_progress)s, %(sold_supply_pct)s, %(is_dumping)s,
                %(buys_5m)s, %(sells_5m)s, %(net_flow_5m)s, %(inflow_5m)s, %(inflow_streak)s,
                %(buy_score)s, %(buy_reasons)s, %(sm_count)s, %(kol_count)s, %(top10_rate)s,
                %(snipers)s, %(rug_ratio)s, %(raw_stats)s, %(tg_chat_id)s, %(tg_message_id)s
            )
            ON CONFLICT (address) DO UPDATE SET
                chain = EXCLUDED.chain,
                symbol = EXCLUDED.symbol,
                trend_interval = EXCLUDED.trend_interval,
                mcap_at_alert = EXCLUDED.mcap_at_alert,
                holder_count = EXCLUDED.holder_count,
                fee_sol = EXCLUDED.fee_sol,
                pool_label = EXCLUDED.pool_label,
                pool_liquidity = EXCLUDED.pool_liquidity,
                token_created_ts = EXCLUDED.token_created_ts,
                token_created_time = EXCLUDED.token_created_time,
                verdict = EXCLUDED.verdict,
                control_ratio = EXCLUDED.control_ratio,
                associated_supply = EXCLUDED.associated_supply,
                associated_count = EXCLUDED.associated_count,
                cluster_size = EXCLUDED.cluster_size,
                dump_progress = EXCLUDED.dump_progress,
                sold_supply_pct = EXCLUDED.sold_supply_pct,
                is_dumping = EXCLUDED.is_dumping,
                buys_5m = EXCLUDED.buys_5m,
                sells_5m = EXCLUDED.sells_5m,
                net_flow_5m = EXCLUDED.net_flow_5m,
                inflow_5m = EXCLUDED.inflow_5m,
                inflow_streak = EXCLUDED.inflow_streak,
                buy_score = EXCLUDED.buy_score,
                buy_reasons = EXCLUDED.buy_reasons,
                sm_count = EXCLUDED.sm_count,
                kol_count = EXCLUDED.kol_count,
                top10_rate = EXCLUDED.top10_rate,
                snipers = EXCLUDED.snipers,
                rug_ratio = EXCLUDED.rug_ratio,
                raw_stats = EXCLUDED.raw_stats,
                tg_chat_id = COALESCE(EXCLUDED.tg_chat_id, alpha_token_candidates.tg_chat_id),
                tg_message_id = COALESCE(EXCLUDED.tg_message_id, alpha_token_candidates.tg_message_id),
                last_seen_at = NOW(),
                alert_count = alpha_token_candidates.alert_count + 1
        """, {
            "address": address,
            "chain": chain,
            "symbol": stats.get("symbol"),
            "trend_interval": interval,
            "mcap_at_alert": stats.get("mcap"),
            "holder_count": stats.get("holder_count"),
            "fee_sol": stats.get("fee_sol"),
            "pool_label": stats.get("pool_label"),
            "pool_liquidity": stats.get("pool_liquidity"),
            "token_created_ts": int(safe_float(stats.get("created_at"))) if stats.get("created_at") else None,
            "token_created_time": stats.get("created_time"),
            "verdict": stats.get("verdict"),
            "control_ratio": stats.get("control_ratio"),
            "associated_supply": stats.get("associated_supply"),
            "associated_count": stats.get("associated_count"),
            "cluster_size": stats.get("cluster_size"),
            "dump_progress": stats.get("dump_progress"),
            "sold_supply_pct": stats.get("sold_supply_pct"),
            "is_dumping": stats.get("is_dumping"),
            "buys_5m": stats.get("buys_5m"),
            "sells_5m": stats.get("sells_5m"),
            "net_flow_5m": stats.get("net_flow_5m"),
            "inflow_5m": stats.get("inflow_5m"),
            "inflow_streak": stats.get("inflow_streak"),
            "buy_score": stats.get("buy_score"),
            "buy_reasons": stats.get("buy_reasons", []),
            "sm_count": stats.get("sm_count"),
            "kol_count": stats.get("kol_count"),
            "top10_rate": stats.get("top10_rate"),
            "snipers": stats.get("snipers"),
            "rug_ratio": str(stats.get("rug_ratio", "")),
            "raw_stats": Json(stats),
            "tg_chat_id": str(TG_CHAT_ID) if tg_message_id else None,
            "tg_message_id": tg_message_id,
        })
        cur.execute("""
            INSERT INTO alpha_signals (address, chain, symbol, mcap_at_alert, milestone)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (address) DO UPDATE SET
                symbol = EXCLUDED.symbol,
                mcap_at_alert = EXCLUDED.mcap_at_alert,
                milestone = EXCLUDED.milestone
        """, (address, chain, stats.get("symbol"), stats.get("mcap"), f"DeepControl_{interval}"))
    db_op(_op)

def run_command(cmd):
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, encoding='utf-8')
        if result.returncode != 0:
            err = (result.stderr or result.stdout or "").strip()
            print(f"Command failed ({result.returncode}): {cmd}")
            if err:
                print(err)
            return None
        return result.stdout
    except Exception as e:
        print(f"Command exception: {cmd} -> {e}")
        return None

def send_tg_alert(msg):
    if not TG_BOT_TOKEN or "你的" in TG_BOT_TOKEN: 
        print(f"--- TG ALERT ---\n{msg}\n----------------")
        return None
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, json={"chat_id": TG_CHAT_ID, "text": msg}, timeout=15)
        if not resp.ok:
            print(f"TG send failed: http={resp.status_code} body={resp.text[:200]}")
            return None
        payload = resp.json()
        if not payload.get("ok"):
            print(f"TG send failed: {payload}")
            return None
        return payload.get("result", {}).get("message_id")
    except Exception as e:
        print(f"TG send exception: {e}")
        return None

def edit_tg_alert(chat_id, message_id, msg):
    if not TG_BOT_TOKEN or "浣犵殑" in TG_BOT_TOKEN or not chat_id or not message_id:
        return False
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/editMessageText"
    try:
        resp = requests.post(
            url,
            json={
                "chat_id": chat_id,
                "message_id": message_id,
                "text": msg,
            },
            timeout=15,
        )
        if resp.ok and resp.json().get("ok"):
            return True
        print(f"TG edit failed: http={resp.status_code} body={resp.text[:200]}")
        return "message is not modified" in resp.text.lower()
    except Exception as e:
        print(f"TG edit exception: {e}")
        return False

def get_existing_tg_alert(address):
    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            "SELECT tg_chat_id, tg_message_id FROM alpha_token_candidates WHERE address=%s",
            (address,),
        )
        return cur.fetchone()
    row = db_op(_op)
    if not row:
        return None, None
    return row[0], row[1]

def candidate_exists(address):
    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM alpha_token_candidates WHERE address=%s",
            (address,),
        )
        return cur.fetchone() is not None
    return db_op(_op)

def upsert_tg_alert(address, msg):
    if candidate_exists(address):
        return None
    return send_tg_alert(msg)

def safe_float(value, default=0.0):
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default

def first_value(*sources, keys=()):
    for source in sources:
        if not isinstance(source, dict):
            continue
        for key in keys:
            value = source.get(key)
            if value not in (None, ""):
                return value
    return None

def first_float(*sources, keys=(), default=0.0):
    value = first_value(*sources, keys=keys)
    return safe_float(value, default)

def optional_float(*sources, keys=()):
    value = first_value(*sources, keys=keys)
    if value in (None, ""):
        return None
    return safe_float(value)

def nested_value(source, path):
    current = source
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
        if current in (None, ""):
            return None
    return current

def first_nested_float(*sources, paths=(), default=0.0):
    for source in sources:
        if not isinstance(source, dict):
            continue
        for path in paths:
            value = nested_value(source, path)
            if value not in (None, ""):
                return safe_float(value, default)
    return default

def first_nested_value(*sources, paths=()):
    for source in sources:
        if not isinstance(source, dict):
            continue
        for path in paths:
            value = nested_value(source, path)
            if value not in (None, ""):
                return value
    return None

def calc_mcap(*sources):
    mcap = first_float(
        *sources,
        keys=("market_cap", "usd_market_cap", "mcap", "fdv", "fully_diluted_valuation"),
    )
    if mcap > 0:
        return mcap
    info = sources[0] if sources and isinstance(sources[0], dict) else {}
    return safe_float(info.get("price")) * safe_float(
        first_value(info, keys=("circulating_supply", "total_supply", "supply"))
    )

def extract_fee_sol(*sources):
    return first_float(
        *sources,
        keys=(
            "fee_sol",
            "total_fee_sol",
            "fees_sol",
            "swap_fee_sol",
            "trade_fee_sol",
            "tx_fee_sol",
            "fee",
            "total_fee",
        ),
    )

def extract_pool_label(*sources):
    value = first_value(
        *sources,
        keys=(
            "pool",
            "pair",
            "amm",
            "exchange",
            "dex",
            "router",
        ),
    )
    if not value:
        return "未知", 0.0
    if isinstance(value, dict):
        exchange = value.get("exchange") or value.get("dex") or value.get("amm") or "未知"
        liquidity = safe_float(value.get("liquidity"))
        if liquidity > 0:
            return f"{exchange} | 流动性 ${liquidity:,.0f}", liquidity
        return f"{exchange}", liquidity
    return str(value), 0.0

def format_age(ts):
    ts = safe_float(ts)
    if ts <= 0:
        return "未知"
    if ts > 10_000_000_000:
        ts = ts / 1000
    delta = max(0, int(time.time() - ts))
    if delta < 3600:
        return f"{delta // 60}分钟前"
    if delta < 86400:
        return f"{delta // 3600}小时前"
    return f"{delta // 86400}天前"

def format_created_time(ts):
    raw_ts = safe_float(ts)
    if raw_ts <= 0:
        return "未知"
    if raw_ts > 10_000_000_000:
        raw_ts = raw_ts / 1000
    return f"{datetime.fromtimestamp(raw_ts).strftime('%Y-%m-%d %H:%M:%S')} ({format_age(ts)})"

def token_age_seconds(ts):
    raw_ts = safe_float(ts)
    if raw_ts <= 0:
        return None
    if raw_ts > 10_000_000_000:
        raw_ts = raw_ts / 1000
    return max(0, int(time.time() - raw_ts))

def token_age_type(age_seconds):
    if age_seconds is None:
        return "未知"
    if age_seconds <= NEW_TOKEN_MAX_AGE_SEC:
        return "新币"
    if age_seconds <= EARLY_TOKEN_MAX_AGE_SEC:
        return "早期币"
    return "老币"

def normalize_ratio(value):
    ratio = safe_float(value)
    if ratio > 1:
        ratio = ratio / 100
    return max(0.0, ratio)

def extract_dev_risk(info, sec, trend_row, holders_list):
    creator_address = first_nested_value(
        info,
        sec,
        trend_row,
        paths=(
            ("dev", "creator_address"),
            ("creator_address",),
            ("creator",),
            ("deployer",),
            ("owner",),
        ),
    )
    dev_team_hold_rate = first_nested_float(
        info,
        sec,
        trend_row,
        paths=(
            ("stat", "dev_team_hold_rate"),
            ("dev_team_hold_rate",),
            ("dev", "dev_team_hold_rate"),
        ),
    )
    creator_hold_rate = first_nested_float(
        info,
        sec,
        trend_row,
        paths=(
            ("stat", "creator_hold_rate"),
            ("creator_hold_rate",),
            ("creator_balance_rate",),
            ("dev", "creator_hold_rate"),
            ("dev", "creator_balance_rate"),
        ),
    )
    dev_buy_usd = first_nested_float(
        info,
        sec,
        trend_row,
        paths=(
            ("dev", "buy_volume_cur"),
            ("dev", "buy_volume_usd"),
            ("dev", "history_bought_cost"),
            ("creator_buy_volume",),
            ("creator_buy_usd",),
            ("creator_bought_cost",),
            ("dev_buy_volume",),
            ("dev_buy_usd",),
            ("dev_bought_cost",),
        ),
    )

    creator_address = str(creator_address or "").strip()
    if creator_address:
        for holder in holders_list:
            holder_address = str(holder.get("address") or holder.get("wallet_address") or "").strip()
            if holder_address != creator_address:
                continue
            creator_hold_rate = max(creator_hold_rate, normalize_ratio(holder.get("amount_percentage")))
            dev_buy_usd = max(
                dev_buy_usd,
                safe_float(holder.get("buy_volume_cur")),
                safe_float(holder.get("history_bought_cost")),
            )
            break

    dev_hold_rate = max(normalize_ratio(dev_team_hold_rate), normalize_ratio(creator_hold_rate))
    should_skip = dev_buy_usd > MAX_DEV_BUY_USD or dev_hold_rate > MAX_DEV_HOLD_RATE
    reasons = []
    if dev_buy_usd > MAX_DEV_BUY_USD:
        reasons.append(f"dev_buy=${dev_buy_usd:.0f}>{MAX_DEV_BUY_USD:.0f}")
    if dev_hold_rate > MAX_DEV_HOLD_RATE:
        reasons.append(f"dev_hold={dev_hold_rate * 100:.1f}%>{MAX_DEV_HOLD_RATE * 100:.0f}%")
    return {
        "creator_address": creator_address,
        "dev_buy_usd": dev_buy_usd,
        "dev_hold_rate": dev_hold_rate,
        "should_skip": should_skip,
        "reasons": reasons,
    }

def is_pool_holder(holder):
    return safe_float(holder.get("addr_type")) == 2 or "pool" in str(holder.get("exchange") or "").lower()

def short_addr(address):
    address = str(address or "")
    if len(address) <= 12:
        return address
    return f"{address[:6]}...{address[-4:]}"

def source_address(holder, key):
    value = holder.get(key)
    if not isinstance(value, dict):
        return ""
    address = str(value.get("address") or "").strip()
    holder_address = str(holder.get("address") or holder.get("wallet_address") or "").strip()
    if not address or address == holder_address:
        return ""
    return address

def holder_net_buy_usd(holder):
    buy_volume = safe_float(holder.get("buy_volume_cur"))
    sell_volume = safe_float(holder.get("sell_volume_cur"))
    if buy_volume > 0 or sell_volume > 0:
        return buy_volume - sell_volume
    raw_netflow = safe_float(holder.get("netflow_usd"))
    return -raw_netflow

def holder_created_ts(holder):
    ts = safe_float(holder.get("created_at"))
    if ts > 10_000_000_000:
        ts = ts / 1000
    return ts if ts > 0 else 0

def analyze_source_clusters(holders_list):
    clusters = defaultdict(list)
    for holder in holders_list:
        if is_pool_holder(holder):
            continue
        native_source = source_address(holder, "native_transfer")
        token_source = source_address(holder, "token_transfer_in")
        if native_source:
            clusters[("资金来源", native_source)].append(holder)
        if token_source:
            clusters[("Token来源", token_source)].append(holder)

    best = {
        "source_cluster_type": "无",
        "source_cluster_address": "",
        "source_cluster_size": 0,
        "source_cluster_supply": 0.0,
        "source_cluster_usd_value": 0.0,
        "source_cluster_amount": 0.0,
        "source_cluster_buy_volume": 0.0,
        "source_cluster_sell_volume": 0.0,
        "source_cluster_netflow": 0.0,
        "source_cluster_desc": "未发现同资金/Token来源",
    }
    for (source_type, address), wallets in clusters.items():
        if len(wallets) < 2:
            continue
        supply = sum(safe_float(w.get("amount_percentage")) * 100 for w in wallets)
        usd_value = sum(safe_float(w.get("usd_value")) for w in wallets)
        amount = sum(safe_float(w.get("amount_cur") or w.get("balance")) for w in wallets)
        buy_volume = sum(safe_float(w.get("buy_volume_cur")) for w in wallets)
        sell_volume = sum(safe_float(w.get("sell_volume_cur")) for w in wallets)
        netflow = sum(holder_net_buy_usd(w) for w in wallets)
        if supply <= best["source_cluster_supply"]:
            continue
        best = {
            "source_cluster_type": source_type,
            "source_cluster_address": address,
            "source_cluster_size": len(wallets),
            "source_cluster_supply": supply,
            "source_cluster_usd_value": usd_value,
            "source_cluster_amount": amount,
            "source_cluster_buy_volume": buy_volume,
            "source_cluster_sell_volume": sell_volume,
            "source_cluster_netflow": netflow,
            "source_cluster_desc": f"{source_type} {short_addr(address)} | {len(wallets)}个钱包 | 持仓{supply:.2f}%/${usd_value:,.0f}",
        }
    return best

def analyze_wallet_creation_clusters(holders_list):
    non_pool = [h for h in holders_list if not is_pool_holder(h)]
    now = time.time()
    new_wallets = [
        h for h in non_pool
        if h.get("is_new") or (holder_created_ts(h) > 0 and now - holder_created_ts(h) <= NEW_WALLET_WINDOW_SEC)
    ]
    created_wallets = sorted(
        [(holder_created_ts(holder), holder) for holder in non_pool if holder_created_ts(holder) > 0],
        key=lambda item: item[0],
    )
    best_wallets = []
    best_supply = 0.0
    best_start_ts = 0
    best_end_ts = 0
    for idx, (start_ts, _) in enumerate(created_wallets):
        end_ts = start_ts + WALLET_CREATION_CLUSTER_SEC
        wallets = [holder for created_ts, holder in created_wallets[idx:] if created_ts <= end_ts]
        if len(wallets) < 2:
            continue
        supply = sum(safe_float(w.get("amount_percentage")) * 100 for w in wallets)
        if supply > best_supply:
            best_wallets = wallets
            best_supply = supply
            best_start_ts = start_ts
            best_end_ts = max(holder_created_ts(w) for w in wallets)

    new_supply = sum(safe_float(w.get("amount_percentage")) * 100 for w in new_wallets)
    new_usd_value = sum(safe_float(w.get("usd_value")) for w in new_wallets)
    new_buy_volume = sum(safe_float(w.get("buy_volume_cur")) for w in new_wallets)
    new_sell_volume = sum(safe_float(w.get("sell_volume_cur")) for w in new_wallets)
    new_netflow = sum(holder_net_buy_usd(w) for w in new_wallets)
    cluster_buy = sum(safe_float(w.get("buy_volume_cur")) for w in best_wallets)
    cluster_sell = sum(safe_float(w.get("sell_volume_cur")) for w in best_wallets)
    cluster_netflow = sum(holder_net_buy_usd(w) for w in best_wallets)
    cluster_desc = "未发现同批创建钱包"
    if best_wallets:
        cluster_desc = (
            f"{datetime.fromtimestamp(best_start_ts).strftime('%m-%d')}~{datetime.fromtimestamp(best_end_ts).strftime('%m-%d')} | "
            f"{len(best_wallets)}个钱包 | 持仓{best_supply:.2f}%"
        )
    conspiracy_score = 0
    if len(new_wallets) >= 5 or new_supply >= 10:
        conspiracy_score += 25
    if len(best_wallets) >= 4 or best_supply >= 8:
        conspiracy_score += 30
    if cluster_netflow > 0:
        conspiracy_score += 10
    return {
        "new_wallet_count": len(new_wallets),
        "new_wallet_supply": new_supply,
        "new_wallet_usd_value": new_usd_value,
        "new_wallet_buy_volume": new_buy_volume,
        "new_wallet_sell_volume": new_sell_volume,
        "new_wallet_netflow": new_netflow,
        "wallet_creation_cluster_size": len(best_wallets),
        "wallet_creation_cluster_supply": best_supply,
        "wallet_creation_cluster_buy_volume": cluster_buy,
        "wallet_creation_cluster_sell_volume": cluster_sell,
        "wallet_creation_cluster_netflow": cluster_netflow,
        "wallet_creation_cluster_desc": cluster_desc,
        "conspiracy_wallet_score": min(conspiracy_score, 100),
    }

def parse_kline_rows(raw):
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except Exception:
        return []
    rows = data if isinstance(data, list) else data.get("list") or data.get("data", {}).get("list") or []
    candles = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        ts = safe_float(row.get("time") or row.get("timestamp") or row.get("t"))
        if ts > 10_000_000_000:
            ts = ts / 1000
        close = safe_float(row.get("close") or row.get("c"))
        if ts <= 0 or close <= 0:
            continue
        candles.append({
            "time": int(ts),
            "open": safe_float(row.get("open") or row.get("o"), close),
            "high": safe_float(row.get("high") or row.get("h"), close),
            "low": safe_float(row.get("low") or row.get("l"), close),
            "close": close,
            "volume": safe_float(row.get("volume") or row.get("v")),
            "amount": safe_float(row.get("amount") or row.get("a")),
        })
    return sorted(candles, key=lambda x: x["time"])

def fetch_5m_klines(chain, address, lookback_sec):
    end_ts = int(time.time())
    start_ts = end_ts - lookback_sec
    raw = run_command(
        f"gmgn-cli market kline --chain {chain} --address {address} "
        f"--resolution 5m --from {start_ts} --to {end_ts} --raw"
    )
    return parse_kline_rows(raw)

def max_drawdown(candles):
    peak = 0.0
    drawdown = 0.0
    for candle in candles:
        peak = max(peak, safe_float(candle.get("high")))
        low = safe_float(candle.get("low"))
        if peak > 0 and low > 0:
            drawdown = min(drawdown, (low - peak) / peak)
    return drawdown

def analyze_kline_health(chain, address, age_seconds):
    age_type = token_age_type(age_seconds)
    lookback_sec = 60 * 60 if age_type == "新币" else KLINE_LOOKBACK_SEC
    candles = fetch_5m_klines(chain, address, lookback_sec)
    if len(candles) < 3:
        return {
            "token_age_type": age_type,
            "kline_candle_count": len(candles),
            "kline_verdict": "K线不足",
            "kline_volume_ratio": 0,
            "kline_score": 0,
            "spike_high": 0,
            "retreat_low": 0,
            "current_price": 0,
            "spike_retreat_pct": 0,
            "recovery_from_low_pct": 0,
        }

    # ---- 冲高回落分析 ----
    # 找历史最高点
    highest_idx = 0
    highest_high = 0
    for i, c in enumerate(candles):
        h = safe_float(c.get("high"))
        if h > highest_high:
            highest_high = h
            highest_idx = i

    # 找最高点之后的最低点（冲高回落的低点）
    retreat_low = highest_high
    for c in candles[highest_idx:]:
        low = safe_float(c.get("low"))
        if low > 0 and low < retreat_low:
            retreat_low = low

    current_price = safe_float(candles[-1].get("close"))

    # 冲高回落幅度 = (回落低点 - 最高点) / 最高点 * 100
    spike_retreat_pct = (retreat_low - highest_high) / highest_high * 100 if highest_high > 0 else 0

    # 从低点到当前价的涨跌百分比
    recovery_from_low_pct = (current_price - retreat_low) / retreat_low * 100 if retreat_low > 0 else 0

    # ---- 量价分析 ----
    recent = candles[-6:] if len(candles) >= 6 else candles
    previous = candles[:-6] if len(candles) > 6 else candles
    recent_open = safe_float(recent[0].get("open"))
    recent_close = safe_float(recent[-1].get("close"))
    recent_change = (recent_close - recent_open) / recent_open if recent_open > 0 else 0
    recent_volume = sum(safe_float(c.get("volume")) for c in recent) / max(1, len(recent))
    previous_volume = sum(safe_float(c.get("volume")) for c in previous) / max(1, len(previous))
    volume_ratio = recent_volume / previous_volume if previous_volume > 0 else 0

    score = 50
    verdict = "价量震荡"
    if abs(recent_change) >= 0.25 and 0 < volume_ratio <= 0.75:
        verdict = "缩量大涨" if recent_change > 0 else "缩量大跌"
        score -= 25
    elif volume_ratio >= 1.8 and abs(recent_change) <= 0.08:
        verdict = "放量横盘-换筹"
        score += 5
    elif volume_ratio >= 1.3 and recent_change >= 0.10:
        verdict = "放量上涨-健康"
        score += 25
    elif volume_ratio >= 1.3 and recent_change <= -0.10:
        verdict = "放量下跌-出货压力"
        score -= 30
    elif recent_change >= 0.10:
        verdict = "温和上涨"
        score += 10
    elif recent_change <= -0.10:
        verdict = "缩量回落" if volume_ratio <= 0.9 else "走弱"
        score -= 15

    if spike_retreat_pct <= -50 and recovery_from_low_pct < 20:
        verdict = f"{verdict}/高位回落"
        score -= 20

    return {
        "token_age_type": age_type,
        "kline_candle_count": len(candles),
        "kline_verdict": verdict,
        "kline_volume_ratio": volume_ratio,
        "kline_score": max(0, min(100, score)),
        "kline_recent_change_pct": recent_change * 100,
        "spike_high": highest_high,
        "retreat_low": retreat_low,
        "current_price": current_price,
        "spike_retreat_pct": spike_retreat_pct,
        "recovery_from_low_pct": recovery_from_low_pct,
    }

def refine_kline_with_holder_flow(stats):
    verdict = str(stats.get("kline_verdict") or "")
    if "放量横盘" not in verdict:
        return
    has_absorption = (
        stats.get("front_holder_netflow", 0) >= MIN_FRONT_HOLDER_NETFLOW_USD
        or stats.get("holder_flow_netflow", 0) >= MIN_TOP_HOLDER_NETFLOW_USD
        or stats.get("accumulation_score", 0) >= 45
        or stats.get("low_sell_supply", 0) >= 5
    )
    has_distribution = (
        stats.get("front_holder_netflow", 0) <= -MIN_FRONT_HOLDER_NETFLOW_USD
        or stats.get("holder_flow_netflow", 0) <= -MIN_TOP_HOLDER_NETFLOW_USD
        or stats.get("distribution_score", 0) >= 45
    )
    if has_absorption and not has_distribution:
        stats["kline_verdict"] = "放量横盘-吸筹"
        stats["kline_score"] = min(100, stats.get("kline_score", 0) + 20)
    elif has_distribution and not has_absorption:
        stats["kline_verdict"] = "放量横盘-派发"
        stats["kline_score"] = max(0, stats.get("kline_score", 0) - 20)

def derive_market_structure(stats):
    kline = str(stats.get("kline_verdict") or "")
    front_flow = stats.get("front_holder_netflow", 0)
    top_flow = stats.get("holder_flow_netflow", 0)
    accumulation = stats.get("accumulation_score", 0)
    distribution = stats.get("distribution_score", 0)
    low_sell_supply = stats.get("low_sell_supply", 0)
    high_sell_supply = stats.get("high_sell_supply", 0)
    source_supply = stats.get("source_cluster_supply", 0)
    source_netflow = stats.get("source_cluster_netflow", 0)
    conspiracy = stats.get("conspiracy_wallet_score", 0)

    if "放量横盘-吸筹" in kline and accumulation >= 45 and front_flow >= 0:
        return {
            "market_structure": "横盘吸筹",
            "market_structure_score": 25,
            "market_structure_reason": "放量横盘且前排/Top100净流入，低卖出钱包支撑",
            "market_structure_risk": "低",
        }
    if "放量横盘" in kline and source_supply >= 8 and source_netflow > 0:
        return {
            "market_structure": "同源吸筹",
            "market_structure_score": 20,
            "market_structure_reason": "放量横盘叠加同源钱包净买入",
            "market_structure_risk": "中",
        }
    if "放量上涨-健康" in kline and accumulation >= 35 and distribution < 45:
        return {
            "market_structure": "健康上涨",
            "market_structure_score": 20,
            "market_structure_reason": "放量上涨且筹码未出现明显派发",
            "market_structure_risk": "低",
        }
    if ("缩量大涨" in kline or "缩量大跌" in kline) and (source_supply >= 8 or stats.get("control_ratio", 0) >= 30):
        return {
            "market_structure": "高控盘波动",
            "market_structure_score": -35,
            "market_structure_reason": "缩量大幅波动叠加筹码/同源集中，价格易被少量资金推动",
            "market_structure_risk": "高",
        }
    if "放量下跌" in kline and (distribution >= 45 or front_flow < 0 or top_flow < 0):
        return {
            "market_structure": "放量派发",
            "market_structure_score": -40,
            "market_structure_reason": "放量下跌叠加前排/Top100流出",
            "market_structure_risk": "高",
        }
    if "放量横盘-派发" in kline or (distribution >= 45 and high_sell_supply >= 5):
        return {
            "market_structure": "横盘派发",
            "market_structure_score": -35,
            "market_structure_reason": "价格横住但高卖出钱包和出货模型偏强",
            "market_structure_risk": "高",
        }
    if "拉高回落" in kline:
        return {
            "market_structure": "拉高回落",
            "market_structure_score": -30,
            "market_structure_reason": "当前价从高位明显回撤，追高风险大",
            "market_structure_risk": "高",
        }
    if conspiracy >= 50 and source_supply >= 5:
        return {
            "market_structure": "批量钱包控盘",
            "market_structure_score": -25,
            "market_structure_reason": "新/同批钱包风险高且同源持仓明显",
            "market_structure_risk": "高",
        }
    if accumulation >= 45 and low_sell_supply >= 5:
        return {
            "market_structure": "筹码吸筹",
            "market_structure_score": 15,
            "market_structure_reason": "Top100低卖出钱包和吸筹模型较强",
            "market_structure_risk": "中",
        }
    return {
        "market_structure": "观察",
        "market_structure_score": 0,
        "market_structure_reason": "量价和筹码方向尚未形成明确共振",
        "market_structure_risk": "中",
    }

def inflow_status_text(stats):
    streak = int(stats.get("inflow_streak", 0))
    if streak >= MIN_INFLOW_STREAK:
        return f"已确认({streak}轮)"
    if streak > 0:
        return f"未确认({streak}轮)"
    return "无"

def analyze_holder_flow(holders_list):
    non_pool = [h for h in holders_list if not is_pool_holder(h)]
    front = non_pool[:20]

    buy_volume = sum(safe_float(h.get("buy_volume_cur")) for h in non_pool)
    sell_volume = sum(safe_float(h.get("sell_volume_cur")) for h in non_pool)
    netflow = sum(holder_net_buy_usd(h) for h in non_pool)

    front_buy_volume = sum(safe_float(h.get("buy_volume_cur")) for h in front)
    front_sell_volume = sum(safe_float(h.get("sell_volume_cur")) for h in front)
    front_netflow = sum(holder_net_buy_usd(h) for h in front)

    net_buy_count = sum(1 for h in non_pool if holder_net_buy_usd(h) > 0)
    net_sell_count = sum(1 for h in non_pool if holder_net_buy_usd(h) < 0)
    front_net_buy_count = sum(1 for h in front if holder_net_buy_usd(h) > 0)
    front_net_sell_count = sum(1 for h in front if holder_net_buy_usd(h) < 0)

    low_sell_holders = [
        h for h in non_pool
        if safe_float(h.get("amount_percentage")) > 0
        and safe_float(h.get("buy_volume_cur")) > 0
        and normalize_ratio(h.get("sell_amount_percentage")) <= 0.30
    ]
    high_sell_holders = [
        h for h in non_pool
        if safe_float(h.get("sell_volume_cur")) > 0
        and normalize_ratio(h.get("sell_amount_percentage")) >= 0.50
    ]
    front_low_sell_holders = [h for h in front if h in low_sell_holders]
    front_high_sell_holders = [h for h in front if h in high_sell_holders]
    front_supply = sum(safe_float(h.get("amount_percentage")) * 100 for h in front)
    low_sell_supply = sum(safe_float(h.get("amount_percentage")) * 100 for h in low_sell_holders)
    high_sell_supply = sum(safe_float(h.get("amount_percentage")) * 100 for h in high_sell_holders)
    buy_tx_count = sum(safe_float(h.get("buy_tx_count_cur")) for h in non_pool)
    sell_tx_count = sum(safe_float(h.get("sell_tx_count_cur")) for h in non_pool)
    recent_cutoff = time.time() - 30 * 60
    recent_active_buy_count = sum(
        1 for h in non_pool
        if safe_float(h.get("last_active_timestamp")) >= recent_cutoff
        and holder_net_buy_usd(h) > 0
    )

    accumulation_score = 0
    distribution_score = 0
    if front_netflow >= MIN_FRONT_HOLDER_NETFLOW_USD:
        accumulation_score += 25
    if netflow >= MIN_TOP_HOLDER_NETFLOW_USD:
        accumulation_score += 20
    if front_net_buy_count >= front_net_sell_count and front_net_buy_count >= 3:
        accumulation_score += 15
    if len(front_low_sell_holders) >= 3 or low_sell_supply >= 5:
        accumulation_score += 20
    if buy_tx_count > sell_tx_count:
        accumulation_score += 10
    if recent_active_buy_count >= 3:
        accumulation_score += 10

    if front_netflow <= -MIN_FRONT_HOLDER_NETFLOW_USD:
        distribution_score += 30
    if netflow <= -MIN_TOP_HOLDER_NETFLOW_USD:
        distribution_score += 20
    if front_net_sell_count > front_net_buy_count and front_net_sell_count >= 3:
        distribution_score += 15
    if len(front_high_sell_holders) >= 3 or high_sell_supply >= 5:
        distribution_score += 25
    if sell_tx_count >= buy_tx_count and sell_tx_count > 0:
        distribution_score += 10

    if accumulation_score >= 45 and accumulation_score >= distribution_score + 15:
        verdict = "前排吸筹"
    elif distribution_score >= 45 and distribution_score >= accumulation_score + 15:
        verdict = "前排流出"
    elif netflow >= MIN_TOP_HOLDER_NETFLOW_USD and net_buy_count >= net_sell_count:
        verdict = "Top100吸筹"
    elif netflow <= -MIN_TOP_HOLDER_NETFLOW_USD and net_sell_count > net_buy_count:
        verdict = "Top100流出"
    else:
        verdict = "未确认"

    return {
        "holder_flow_verdict": verdict,
        "holder_flow_buy_volume": buy_volume,
        "holder_flow_sell_volume": sell_volume,
        "holder_flow_netflow": netflow,
        "holder_flow_net_buy_count": net_buy_count,
        "holder_flow_net_sell_count": net_sell_count,
        "front_holder_netflow": front_netflow,
        "front_holder_buy_volume": front_buy_volume,
        "front_holder_sell_volume": front_sell_volume,
        "front_holder_net_buy_count": front_net_buy_count,
        "front_holder_net_sell_count": front_net_sell_count,
        "front_holder_supply": front_supply,
        "low_sell_holder_count": len(low_sell_holders),
        "high_sell_holder_count": len(high_sell_holders),
        "front_low_sell_holder_count": len(front_low_sell_holders),
        "front_high_sell_holder_count": len(front_high_sell_holders),
        "low_sell_supply": low_sell_supply,
        "high_sell_supply": high_sell_supply,
        "holder_buy_tx_count": int(buy_tx_count),
        "holder_sell_tx_count": int(sell_tx_count),
        "recent_active_buy_count": recent_active_buy_count,
        "accumulation_score": min(accumulation_score, 100),
        "distribution_score": min(distribution_score, 100),
    }

def analyze_5m_flow(address, trend_row):
    buys = first_float(trend_row, keys=("buys", "buy_count", "buys_5m", "buy_count_5m"))
    sells = first_float(trend_row, keys=("sells", "sell_count", "sells_5m", "sell_count_5m"))
    buy_volume = optional_float(
        trend_row,
        keys=("buy_volume", "buy_volume_5m", "buy_volume_usd", "buy_volume_5m_usd", "volume_buy"),
    )
    sell_volume = optional_float(
        trend_row,
        keys=("sell_volume", "sell_volume_5m", "sell_volume_usd", "sell_volume_5m_usd", "volume_sell"),
    )
    net_buy = optional_float(
        trend_row,
        keys=("net_buy", "net_buy_5m", "net_buy_usd", "net_buy_volume", "net_buy_volume_5m"),
    )

    if net_buy is not None:
        inflow = net_buy > 0
        net_flow = net_buy
    elif buy_volume is not None and sell_volume is not None:
        net_flow = buy_volume - sell_volume
        inflow = net_flow > 0
    else:
        net_flow = buys - sells
        inflow = buys > sells

    previous = INFLOW_STATE.get(address, 0)
    streak = previous + 1 if inflow else 0
    INFLOW_STATE[address] = streak

    return {
        "buys_5m": int(buys),
        "sells_5m": int(sells),
        "net_flow_5m": net_flow,
        "inflow_5m": inflow,
        "inflow_streak": streak,
        "sustained_inflow": streak >= MIN_INFLOW_STREAK,
    }

def calc_buy_score(stats):
    score = 0
    reasons = []

    if stats["control_ratio"] >= MIN_CANDIDATE_CONTROL_RATIO:
        score += 20
        reasons.append(f"关联控盘{stats['control_ratio']:.1f}%")
    if stats["cluster_size"] >= MIN_CANDIDATE_CLUSTER_SIZE:
        score += 20
        reasons.append(f"同频集群{stats['cluster_size']}个")
    if stats.get("source_cluster_size", 0) >= 3:
        score += 20
        reasons.append(f"同源关联{stats['source_cluster_size']}个/{stats['source_cluster_supply']:.1f}%")
    elif stats.get("source_cluster_size", 0) >= 2 and stats.get("source_cluster_supply", 0) >= MIN_CANDIDATE_CONTROL_RATIO:
        score += 12
        reasons.append(f"同源持仓{stats['source_cluster_supply']:.1f}%")
    if stats["sm_count"] >= MIN_CANDIDATE_SM_COUNT:
        score += 25
        reasons.append(f"Smart Money {stats['sm_count']}")
    if stats["holder_count"] >= MIN_CANDIDATE_HOLDER_COUNT:
        score += 20
        reasons.append(f"持有人{stats['holder_count']}")
    if stats["sustained_inflow"]:
        score += 25
        reasons.append(f"5m连续流入{stats['inflow_streak']}轮")
    if stats.get("front_holder_netflow", 0) >= MIN_FRONT_HOLDER_NETFLOW_USD:
        score += 20
        reasons.append(f"前排吸筹${stats['front_holder_netflow']:,.0f}")
    elif stats.get("holder_flow_netflow", 0) >= MIN_TOP_HOLDER_NETFLOW_USD:
        score += 12
        reasons.append(f"Top100吸筹${stats['holder_flow_netflow']:,.0f}")
    if stats.get("front_holder_netflow", 0) <= -MIN_FRONT_HOLDER_NETFLOW_USD:
        score -= 25
        reasons.append(f"前排流出${abs(stats['front_holder_netflow']):,.0f}")
    elif stats.get("holder_flow_netflow", 0) <= -MIN_TOP_HOLDER_NETFLOW_USD:
        score -= 15
        reasons.append(f"Top100流出${abs(stats['holder_flow_netflow']):,.0f}")
    if stats.get("accumulation_score", 0) >= 45:
        score += 15
        reasons.append(f"吸筹模型{stats['accumulation_score']}")
    if stats.get("distribution_score", 0) >= 45:
        score -= 25
        reasons.append(f"出货模型{stats['distribution_score']}")
    if stats.get("conspiracy_wallet_score", 0) >= 50:
        score -= 20
        reasons.append(f"新/同批钱包风险{stats['conspiracy_wallet_score']}")
    elif stats.get("conspiracy_wallet_score", 0) >= 25:
        score -= 10
        reasons.append(f"疑似同批钱包{stats['wallet_creation_cluster_size']}个")
    if stats.get("kline_score", 0) >= 70:
        add_score = 5 if stats.get("token_age_type") == "老币" else 15
        score += add_score
        reasons.append(f"K线健康{stats['kline_score']}({stats['kline_verdict']})")
    elif "放量横盘-吸筹" in str(stats.get("kline_verdict") or ""):
        score += 12
        reasons.append("放量横盘吸筹")
    elif stats.get("kline_score", 0) > 0 and stats.get("kline_score", 0) <= 35:
        score -= 20
        reasons.append(f"K线弱{stats['kline_score']}({stats['kline_verdict']})")
    structure_score = stats.get("market_structure_score", 0)
    if structure_score:
        score += structure_score
        reasons.append(f"结构:{stats.get('market_structure')}({structure_score:+d})")

    return max(0, score), reasons

# ---------------------------------------------------------------------------
# 关联性与控盘砸盘深度分析
# ---------------------------------------------------------------------------
def analyze_control_and_dump(holders_list, debug=False):
    """
    通过前100钱包分析资金关联、控盘与砸盘
    """
    if not holders_list: return {}

    # 1. 聚类分析 (基于进场时间)
    time_clusters = defaultdict(list)
    total_supply_scanned = 0
    
    associated_supply = 0
    associated_count = 0
    sold_supply_from_clusters = 0
    sold_supply_pct = 0
    
    for h in holders_list:
        addr = h.get("address")
        supply_pct = safe_float(h.get("amount_percentage")) * 100
        raw_sell_pct = h.get("sell_amount_percentage", 0)
        sell_ratio = normalize_ratio(raw_sell_pct)
        buy_ts = safe_float(h.get("start_holding_at"))
        tags = h.get("maker_token_tags", [])
        
        # 核心关联逻辑 A：官方标记的捆绑包或老鼠仓
        is_labeled_associated = "bundler" in tags or "rat_trader" in tags
        
        # 核心关联逻辑 B：时间聚类 (5秒内进场视为疑似关联)
        # 将时间戳规整到 5 秒区间
        if buy_ts > 0:
            time_key = int(buy_ts) // 5 
            time_clusters[time_key].append(h)
        
        if is_labeled_associated:
            associated_supply += supply_pct
            associated_count += 1
            wallet_sold_supply = supply_pct * sell_ratio
            sold_supply_from_clusters += wallet_sold_supply
            sold_supply_pct += wallet_sold_supply
            if debug:
                print(
                    "    关联钱包 "
                    f"{str(addr)[:6]}...{str(addr)[-4:]} | "
                    f"标签={','.join(tags)} | 持仓={supply_pct:.2f}% | "
                    f"原始卖出={raw_sell_pct} | 归一化卖出={sell_ratio * 100:.2f}% | "
                    f"估算已卖供应={wallet_sold_supply:.4f}%"
                )

    # 找出最大的时间聚类 (疑似隐藏庄家)
    max_cluster_size = 0
    cluster_supply = 0
    for ts, hs in time_clusters.items():
        if len(hs) >= 3: # 超过或等于 3 个钱包在 5 秒内同步买入
            c_supply = sum(safe_float(x.get("amount_percentage")) * 100 for x in hs)
            if c_supply > cluster_supply:
                cluster_supply = c_supply
                max_cluster_size = len(hs)
    source_cluster = analyze_source_clusters(holders_list)

    # 综合评估
    # 控盘率 = 已标记关联 + 时间聚类关联 + 同资金/Token来源关联 (去重后的估算)
    control_ratio = max(associated_supply, cluster_supply, source_cluster.get("source_cluster_supply", 0))
    
    # 砸盘进度 = 关联钱包已卖出的比例
    # 粗略估算：如果关联钱包卖出比例 > 10% 则视为开始砸盘
    dump_progress = (sold_supply_from_clusters / associated_supply * 100) if associated_supply > 0 else 0
    is_dumping = (
        associated_supply >= MIN_DUMP_ASSOCIATED_SUPPLY
        and sold_supply_pct >= MIN_DUMP_SOLD_SUPPLY
        and dump_progress > DUMP_PROGRESS_THRESHOLD
    )
    if debug:
        print(
            "    砸盘判定: "
            f"关联标记钱包={associated_count}个 | "
            f"标记关联持仓={associated_supply:.2f}% | "
            f"估算已卖供应={sold_supply_pct:.4f}% | "
            f"最大同频集群={max_cluster_size}个/{cluster_supply:.2f}% | "
            f"加权卖出进度={dump_progress:.2f}% | "
            f"阈值=关联持仓>={MIN_DUMP_ASSOCIATED_SUPPLY}% 且 已卖供应>={MIN_DUMP_SOLD_SUPPLY}% 且 卖出>{DUMP_PROGRESS_THRESHOLD}% | "
            f"结果={'砸盘中' if is_dumping else '非砸盘'}"
        )

    return {
        "control_ratio": control_ratio,
        "cluster_size": max_cluster_size,
        "dump_progress": dump_progress,
        "sold_supply_pct": sold_supply_pct,
        "associated_count": associated_count,
        "associated_supply": associated_supply,
        "source_cluster_type": source_cluster.get("source_cluster_type", "无"),
        "source_cluster_address": source_cluster.get("source_cluster_address", ""),
        "source_cluster_size": source_cluster.get("source_cluster_size", 0),
        "source_cluster_supply": source_cluster.get("source_cluster_supply", 0),
        "source_cluster_usd_value": source_cluster.get("source_cluster_usd_value", 0),
        "source_cluster_amount": source_cluster.get("source_cluster_amount", 0),
        "source_cluster_buy_volume": source_cluster.get("source_cluster_buy_volume", 0),
        "source_cluster_sell_volume": source_cluster.get("source_cluster_sell_volume", 0),
        "source_cluster_netflow": source_cluster.get("source_cluster_netflow", 0),
        "source_cluster_desc": source_cluster.get("source_cluster_desc", "未发现同资金/Token来源"),
        "is_dumping": is_dumping,
        "verdict": "砸盘中" if is_dumping else ("高度控盘" if control_ratio > 40 else "筹码分散")
    }

def perform_deep_analysis(chain, address, trend_row=None):
    trend_row = trend_row or {}
    # 1. 获取基本信息
    info_raw = run_command(f"gmgn-cli token info --chain {chain} --address {address} --raw")
    if not info_raw: return None
    info = json.loads(info_raw)
    
    # 2. 获取安全数据
    sec_raw = run_command(f"gmgn-cli token security --chain {chain} --address {address} --raw")
    sec = json.loads(sec_raw) if sec_raw else {}
    
    # 3. 获取前100持币者
    holders_raw = run_command(f"gmgn-cli token holders --chain {chain} --address {address} --limit 100 --raw")
    holders_data = json.loads(holders_raw) if holders_raw else {"list": []}
    holders_list = holders_data.get("list", [])

    dev_risk = extract_dev_risk(info, sec, trend_row, holders_list)
    if dev_risk["should_skip"]:
        print(f"  [跳过] dev风险 {address}: {', '.join(dev_risk['reasons'])}")
        return None
    
    # 执行筹码关联分析
    ctrl = analyze_control_and_dump(holders_list, debug=DEBUG_DEEP_LOG)
    holder_flow = analyze_holder_flow(holders_list)
    wallet_creation = analyze_wallet_creation_clusters(holders_list)
    mcap = calc_mcap(info, trend_row)
    holder_count = first_float(
        info,
        sec,
        trend_row,
        keys=("holder_count", "holders_count", "holder_num", "holders", "holder"),
        default=len(holders_list),
    )
    fee_sol = extract_fee_sol(info, sec, trend_row)
    pool_label, pool_liquidity = extract_pool_label(info, trend_row, sec)
    created_at = first_value(
        info,
        trend_row,
        sec,
        keys=(
            "created_at",
            "creation_timestamp",
            "created_timestamp",
            "create_timestamp",
            "open_timestamp",
            "launch_timestamp",
            "pool_creation_timestamp",
            "pair_created_at",
        ),
    )
    age_seconds = token_age_seconds(created_at)
    kline_health = analyze_kline_health(chain, address, age_seconds)
    flow = analyze_5m_flow(address, trend_row)
    
    # 组装数据
    stats = {
        "symbol": info.get("symbol"),
        "mcap": mcap,
        "holder_count": int(holder_count),
        "fee_sol": fee_sol,
        "pool_label": pool_label,
        "pool_liquidity": pool_liquidity,
        "created_at": created_at,
        "created_age": format_age(created_at),
        "created_time": format_created_time(created_at),
        "token_age_type": kline_health.get("token_age_type", "未知"),
        "sm_count": info.get("wallet_tags_stat", {}).get("smart_wallets", 0),
        "kol_count": info.get("wallet_tags_stat", {}).get("renowned_wallets", 0),
        "top10_rate": safe_float(sec.get("top_10_holder_rate")) * 100,
        "snipers": sec.get("sniper_count", 0),
        "rug_ratio": sec.get("rug_ratio", "0"),
        "creator_address": dev_risk.get("creator_address"),
        "dev_buy_usd": dev_risk.get("dev_buy_usd", 0),
        "dev_hold_rate": dev_risk.get("dev_hold_rate", 0),
        "control_ratio": ctrl.get("control_ratio", 0),
        "dump_progress": ctrl.get("dump_progress", 0),
        "sold_supply_pct": ctrl.get("sold_supply_pct", 0),
        "associated_count": ctrl.get("associated_count", 0),
        "associated_supply": ctrl.get("associated_supply", 0),
        "cluster_size": ctrl.get("cluster_size", 0),
        "source_cluster_type": ctrl.get("source_cluster_type", "无"),
        "source_cluster_address": ctrl.get("source_cluster_address", ""),
        "source_cluster_size": ctrl.get("source_cluster_size", 0),
        "source_cluster_supply": ctrl.get("source_cluster_supply", 0),
        "source_cluster_usd_value": ctrl.get("source_cluster_usd_value", 0),
        "source_cluster_amount": ctrl.get("source_cluster_amount", 0),
        "source_cluster_buy_volume": ctrl.get("source_cluster_buy_volume", 0),
        "source_cluster_sell_volume": ctrl.get("source_cluster_sell_volume", 0),
        "source_cluster_netflow": ctrl.get("source_cluster_netflow", 0),
        "source_cluster_desc": ctrl.get("source_cluster_desc", "未发现同资金/Token来源"),
        "is_dumping": ctrl.get("is_dumping", False),
        "verdict": ctrl.get("verdict", "未知")
    }
    stats.update(flow)
    stats.update(holder_flow)
    stats.update(wallet_creation)
    stats.update(kline_health)
    refine_kline_with_holder_flow(stats)
    stats.update(derive_market_structure(stats))
    stats["inflow_status"] = inflow_status_text(stats)
    buy_score, buy_reasons = calc_buy_score(stats)
    stats["buy_score"] = buy_score
    stats["buy_reasons"] = buy_reasons
    return stats

# ---------------------------------------------------------------------------
# 扫描主循环
# ---------------------------------------------------------------------------
def scan_pro():
    for chain in CHAINS:
        for interval in TREND_INTERVALS:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 扫描 {chain} {interval} 筹码信号...")
            output = run_command(f"gmgn-cli market trending --chain {chain} --interval {interval} --limit 100 --raw")
            if not output:
                print(f"  No trending output for {chain} {interval}")
                continue
            
            try:
                data = json.loads(output)
                tokens = data.get("data", {}).get("rank", [])
                print(f"  共发现 {len(tokens)} 个代币")
                for t in tokens:
                    addr = t.get("address")
                    if not addr:
                        continue
                    trend_mcap = calc_mcap(t)
                    if trend_mcap > MAX_MCAP_USD:
                        continue
                    
                    s = perform_deep_analysis(chain, addr, t)
                    if not s: continue

                    if s["mcap"] < MIN_MCAP_USD:
                        continue
                    if s["mcap"] > MAX_MCAP_USD:
                        continue
                    if s["fee_sol"] < MIN_FEE_SOL:
                        continue
                    if s["is_dumping"]:
                        continue

                    
                    # 警报逻辑：硬过滤后，用可买分数聚合早期信号。
                    is_candidate = s["buy_score"] >= MIN_BUY_SCORE
                    if is_candidate:
                        print(
                            f"  [候选] ${s['symbol']} | CA={addr} | "
                            f"市值=${s['mcap']/1000:.1f}K | 持有人={s['holder_count']} | "
                            f"手续费={s['fee_sol']:.2f} SOL | 池={s['pool_label']} | 创建={s['created_time']} | "
                            f"类型={s['token_age_type']} | 量价={s['kline_verdict']} | 冲高回落={s['spike_retreat_pct']:.1f}% | 低点反弹={s['recovery_from_low_pct']:.1f}% | "
                            f"结构={s['market_structure']} | "
                            f"状态={s['verdict']} | 关联持仓={s['associated_supply']:.2f}% | "
                            f"同源={s['source_cluster_size']}个/{s['source_cluster_supply']:.2f}% | "
                            f"卖出进度={s['dump_progress']:.2f}% | "
                            f"前排净流={s['front_holder_netflow']:.0f}U | Top100净流={s['holder_flow_netflow']:.0f}U | "
                            f"5m买/卖={s['buys_5m']}/{s['sells_5m']} | 流入状态={s['inflow_status']} | "
                            f"可买分={s['buy_score']} | 理由={', '.join(s['buy_reasons'])}"
                        )

                        alert_icon = "🟡" if s["control_ratio"] > 50 else "🟢"
                        
                        msg = (
                            f"{alert_icon} *筹码关联性报警* | ${s['symbol']}\n"
                            f"市值: ${s['mcap']/1000:.1f}K | 持有人: {s['holder_count']} | 手续费: {s['fee_sol']:.2f} SOL\n"
                            f"流动性池: {s['pool_label']}\n"
                            f"创建时间: {s['created_time']} | 类型: {s['token_age_type']} | 状态: {s['verdict']}\n\n"
                            f"🧭 *市场结构*: {s['market_structure']} | 风险: {s['market_structure_risk']}\n"
                            f"- {s['market_structure_reason']}\n\n"
                            f"✅ *可买评分*: {s['buy_score']} 分\n"
                            f"- 理由: {', '.join(s['buy_reasons'])}\n"
                            f"- 5m买/卖: {s['buys_5m']}/{s['sells_5m']} | 流入状态: {s['inflow_status']}\n\n"
                            f"📈 *K线量价分析*\n"
                            f"- K线数: {s['kline_candle_count']} | 类型: {s['token_age_type']}\n"
                            f"- 量价判定: {s['kline_verdict']} | 尾段量能: {s['kline_volume_ratio']:.2f}x\n"
                            f"- 冲高回落: {s['spike_retreat_pct']:.1f}% (最高点 → 回落低点)\n"
                            f"- 低点反弹: {s['recovery_from_low_pct']:.1f}% (回落低点 → 当前价)\n\n"
                            f"🧬 *资金关联分析 (Top 100)*\n"
                            f"- 疑似关联总控盘: {s['control_ratio']:.1f}%\n"
                            f"- 最大同频率进场集群: {s['cluster_size']} 个钱包\n"
                            f"- 同资金/Token来源: {s['source_cluster_desc']}\n"
                            f"- 同源持仓: {s['source_cluster_supply']:.2f}% | ${s['source_cluster_usd_value']:,.0f} | Token数量 {s['source_cluster_amount']:,.0f}\n"
                            f"- 同源买卖: 买入 ${s['source_cluster_buy_volume']:,.0f} | 卖出 ${s['source_cluster_sell_volume']:,.0f} | 净流 ${s['source_cluster_netflow']:,.0f}\n"
                            f"- 新/同批钱包风险: {s['conspiracy_wallet_score']} | 新钱包 {s['new_wallet_count']}个/{s['new_wallet_supply']:.1f}% | 持仓 ${s['new_wallet_usd_value']:,.0f}\n"
                            f"- 新钱包买卖: 买入 ${s['new_wallet_buy_volume']:,.0f} | 卖出 ${s['new_wallet_sell_volume']:,.0f} | 净流 ${s['new_wallet_netflow']:,.0f}\n"
                            f"- 同批创建簇: {s['wallet_creation_cluster_desc']} | 净流 ${s['wallet_creation_cluster_netflow']:,.0f}\n"
                            f"- 庄家出货进度: {s['dump_progress']:.1f}%\n\n"
                            f"💸 *前排资金异动*\n"
                            f"- 结论: {s['holder_flow_verdict']}\n"
                            f"- 前20净流: ${s['front_holder_netflow']:,.0f} | 买入 ${s['front_holder_buy_volume']:,.0f} / 卖出 ${s['front_holder_sell_volume']:,.0f}\n"
                            f"- Top100净流: ${s['holder_flow_netflow']:,.0f} | 买入 ${s['holder_flow_buy_volume']:,.0f} / 卖出 ${s['holder_flow_sell_volume']:,.0f}\n"
                            f"- 净买/净卖钱包: Top100 {s['holder_flow_net_buy_count']}/{s['holder_flow_net_sell_count']} | 前20 {s['front_holder_net_buy_count']}/{s['front_holder_net_sell_count']}\n\n"
                            f"- 吸筹/出货模型: {s['accumulation_score']}/{s['distribution_score']}\n"
                            f"- 低卖出钱包: Top100 {s['low_sell_holder_count']}个/{s['low_sell_supply']:.1f}% | 前20 {s['front_low_sell_holder_count']}个\n"
                            f"- 高卖出钱包: Top100 {s['high_sell_holder_count']}个/{s['high_sell_supply']:.1f}% | 前20 {s['front_high_sell_holder_count']}个\n"
                            f"- 买/卖交易次数: {s['holder_buy_tx_count']}/{s['holder_sell_tx_count']} | 近30m活跃净买钱包: {s['recent_active_buy_count']}\n\n"
                            f"📊 *基础结构*\n"
                            f"- Top 10 持仓: {s['top10_rate']:.1f}%\n"
                            f"- 狙击手数量: {s['snipers']}\n"
                            f"- 风险分数: {s['rug_ratio']}\n\n"
                            f"👥 *共识强度*\n"
                            f"- Smart Money: {s['sm_count']} | KOL: {s['kol_count']}\n\n"
                            f"CA: `{addr}`\n"
                            f"[在 GMGN 查看关联图谱](https://gmgn.ai/{chain}/token/{addr})"
                        )
                        tg_message_id = upsert_tg_alert(addr, msg)
                        save_alpha_candidate(chain, interval, addr, s, tg_message_id=tg_message_id)
                    
            except Exception as e:
                print(f"Loop Error: {e}")
            time.sleep(2)

if __name__ == "__main__":
    print("深度关联分析机器人已启动...")
    while True:
        scan_pro()
        time.sleep(CHECK_INTERVAL)
