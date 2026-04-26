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
MAX_TOKEN_AGE_SEC = 2 * 24 * 60 * 60
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
            "pool_address",
            "pair_address",
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
        address = value.get("pooladdress") or value.get("pool_address") or value.get("address") or "未知"
        exchange = value.get("exchange") or value.get("dex") or value.get("amm") or "未知"
        liquidity = safe_float(value.get("liquidity"))
        if liquidity > 0:
            return f"{exchange} | {address} | 流动性 ${liquidity:,.0f}", liquidity
        return f"{exchange} | {address}", liquidity
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
    if stats["sm_count"] >= MIN_CANDIDATE_SM_COUNT:
        score += 25
        reasons.append(f"Smart Money {stats['sm_count']}")
    if stats["holder_count"] >= MIN_CANDIDATE_HOLDER_COUNT:
        score += 20
        reasons.append(f"持有人{stats['holder_count']}")
    if stats["sustained_inflow"]:
        score += 25
        reasons.append(f"5m连续流入{stats['inflow_streak']}轮")

    return score, reasons

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

    # 综合评估
    # 控盘率 = 已标记关联 + 时间聚类关联 (去重后的估算)
    control_ratio = max(associated_supply, cluster_supply)
    
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
    created_ts = safe_float(created_at)
    if created_ts > 10_000_000_000:
        created_ts = created_ts / 1000
    if created_ts > 0 and time.time() - created_ts > MAX_TOKEN_AGE_SEC:
        print(f"  [跳过] 创建超过2天 {address}: {format_created_time(created_at)}")
        return None
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
        "is_dumping": ctrl.get("is_dumping", False),
        "verdict": ctrl.get("verdict", "未知")
    }
    stats.update(flow)
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
                    
                    s = perform_deep_analysis(chain, addr, t)
                    if not s: continue

                    if s["mcap"] < MIN_MCAP_USD:
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
                            f"状态={s['verdict']} | 关联持仓={s['associated_supply']:.2f}% | "
                            f"卖出进度={s['dump_progress']:.2f}% | "
                            f"5m买/卖={s['buys_5m']}/{s['sells_5m']} | 连续流入={s['inflow_streak']} | "
                            f"可买分={s['buy_score']} | 理由={', '.join(s['buy_reasons'])}"
                        )

                        alert_icon = "🟡" if s["control_ratio"] > 50 else "🟢"
                        
                        msg = (
                            f"{alert_icon} *筹码关联性报警* | ${s['symbol']}\n"
                            f"市值: ${s['mcap']/1000:.1f}K | 持有人: {s['holder_count']} | 手续费: {s['fee_sol']:.2f} SOL\n"
                            f"流动性池: {s['pool_label']}\n"
                            f"创建时间: {s['created_time']} | 状态: {s['verdict']}\n\n"
                            f"✅ *可买评分*: {s['buy_score']} 分\n"
                            f"- 理由: {', '.join(s['buy_reasons'])}\n"
                            f"- 5m买/卖: {s['buys_5m']}/{s['sells_5m']} | 连续流入: {s['inflow_streak']}轮\n\n"
                            f"🧬 *资金关联分析 (Top 100)*\n"
                            f"- 疑似关联总控盘: {s['control_ratio']:.1f}%\n"
                            f"- 最大同频率进场集群: {s['cluster_size']} 个钱包\n"
                            f"- 庄家出货进度: {s['dump_progress']:.1f}%\n\n"
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
