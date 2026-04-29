import json
import os
import subprocess
import time
import requests
from datetime import datetime
from collections import defaultdict
from psycopg2.extras import Json
from db_client import db_op
from config import TG_BOT_TOKEN, TG_CHAT_ID, CHAINS
from redis_client import get_redis_client, redis_key
from tg_alert_stream import publish_tg_alert

# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------
CHECK_INTERVAL = 0
TREND_INTERVALS = ["1m"]
TREND_PLATFORMS = [item.strip() for item in os.getenv("DEEP_ALPHA_TREND_PLATFORMS", "Pump.fun").split(",") if item.strip()]
LOW_MCAP_STRICT_USD = 10_000
MID_MCAP_STRICT_USD = 20_000
LOW_MCAP_MIN_UP_PCT = 0.30
MID_MCAP_MIN_UP_PCT = 0.10
MIN_FEE_SOL = 1
HIGH_VOLUME_USD_THRESHOLD = 100_000
MIN_HIGH_VOLUME_FEE_SOL = 5
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
MAX_TOKEN_AGE_SEC = 24 * 60 * 60
MIN_TOP_HOLDER_NETFLOW_USD = 5_000
MIN_FRONT_HOLDER_NETFLOW_USD = 2_000
NEW_WALLET_WINDOW_SEC = 3 * 24 * 60 * 60
WALLET_CREATION_CLUSTER_SEC = 5 * 24 * 60 * 60
KLINE_LOOKBACK_SEC = 2 * 60 * 60
NEW_TOKEN_MAX_AGE_SEC = 60 * 60
EARLY_TOKEN_MAX_AGE_SEC = 24 * 60 * 60
INFLOW_STATE = {}
PRICE_OBSERVATION_STATE = {}
PRICE_OBSERVATION_ARCHIVE_STATE = {}
MIN_PRICE_OBSERVATION_SCANS = 3
PRICE_OBSERVATION_HISTORY_LIMIT = int(os.getenv("PRICE_OBSERVATION_HISTORY_LIMIT", "20"))
PRICE_OBSERVATION_BAND_LIMIT = int(os.getenv("PRICE_OBSERVATION_BAND_LIMIT", "6"))
FAST_PRICE_OBSERVATION_SCANS = 2
FAST_PRICE_UP_PCT = 0.15
MAX_PRICE_DROP_PCT = 0.30
MIN_REPEAT_PRICE_UP_PCT = 0.20
REBOUND_LOOKBACK_SCANS = int(os.getenv("REBOUND_LOOKBACK_SCANS", "12"))
MIN_REBOUND_DRAWDOWN_PCT = float(os.getenv("MIN_REBOUND_DRAWDOWN_PCT", "0.25"))
MIN_REBOUND_FROM_LOW_PCT = float(os.getenv("MIN_REBOUND_FROM_LOW_PCT", "0.20"))
SCAN_ROUND = 0
REDIS_KEY_PREFIX = os.getenv("PRICE_OBSERVATION_REDIS_PREFIX", "deep_alpha:price_observation")
DEFAULT_BUSINESS_REDIS_TTL_SEC = 4 * 60 * 60
REDIS_STATE_TTL_SEC = int(os.getenv("PRICE_OBSERVATION_REDIS_TTL_SEC", str(DEFAULT_BUSINESS_REDIS_TTL_SEC)))
ARCHIVE_REDIS_KEY_PREFIX = os.getenv("PRICE_OBSERVATION_ARCHIVE_REDIS_PREFIX", "deep_alpha:price_observation_archive")
ARCHIVE_REDIS_TTL_SEC = int(os.getenv("PRICE_OBSERVATION_ARCHIVE_REDIS_TTL_SEC", str(DEFAULT_BUSINESS_REDIS_TTL_SEC)))
PRICE_OBSERVATION_ARCHIVE_LIMIT = int(os.getenv("PRICE_OBSERVATION_ARCHIVE_LIMIT", "12"))
CHAIN_PRICE_DISPLAY_SCALE = float(os.getenv("CHAIN_PRICE_DISPLAY_SCALE", "1000000000"))
ALERT_REDIS_KEY_PREFIX = os.getenv("DEEP_ALPHA_ALERT_REDIS_PREFIX", "deep_alpha:alert_candidate")
ALERT_REDIS_TTL_SEC = int(os.getenv("DEEP_ALPHA_ALERT_REDIS_TTL_SEC", str(DEFAULT_BUSINESS_REDIS_TTL_SEC)))
ALERT_MISS_REDIS_KEY_PREFIX = os.getenv("DEEP_ALPHA_ALERT_MISS_REDIS_PREFIX", "deep_alpha:alert_candidate_miss")
ALERT_MISS_REDIS_TTL_SEC = int(os.getenv("DEEP_ALPHA_ALERT_MISS_REDIS_TTL_SEC", "300"))

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
    cache_candidate_snapshot(address, stats)

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

def shell_quote(value):
    value = str(value)
    return '"' + value.replace('"', '\\"') + '"'

def trend_platform_args():
    return " ".join(f"--platform {shell_quote(platform)}" for platform in TREND_PLATFORMS)

def send_tg_alert(msg):
    if not TG_BOT_TOKEN or "你的" in TG_BOT_TOKEN: 
        print(f"--- TG ALERT ---\n{msg}\n----------------")
        publish_tg_alert(msg, "deep_alpha", status="dry_run", chat_id=TG_CHAT_ID)
        return None
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, json={"chat_id": TG_CHAT_ID, "text": msg}, timeout=15)
        if not resp.ok:
            print(f"TG send failed: http={resp.status_code} body={resp.text[:200]}")
            publish_tg_alert(msg, "deep_alpha", status=f"failed_http_{resp.status_code}", chat_id=TG_CHAT_ID)
            return None
        payload = resp.json()
        if not payload.get("ok"):
            print(f"TG send failed: {payload}")
            publish_tg_alert(msg, "deep_alpha", status="failed_api", chat_id=TG_CHAT_ID, extra=payload)
            return None
        message_id = payload.get("result", {}).get("message_id")
        publish_tg_alert(msg, "deep_alpha", status="sent", chat_id=TG_CHAT_ID, message_id=message_id)
        return message_id
    except Exception as e:
        print(f"TG send exception: {e}")
        publish_tg_alert(msg, "deep_alpha", status="exception", chat_id=TG_CHAT_ID, extra={"error": str(e)})
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
    return get_candidate_snapshot(address) is not None

def get_candidate_snapshot(address):
    cached = load_candidate_snapshot_from_redis(address)
    if cached:
        return cached
    if load_candidate_miss_from_redis(address):
        return None

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            SELECT holder_count, mcap_at_alert, alert_count, raw_stats
            FROM alpha_token_candidates
            WHERE address=%s
            """,
            (address,),
        )
        return cur.fetchone()
    row = db_op(_op)
    if not row:
        cache_candidate_miss(address)
        return None
    raw_stats = row[3] if isinstance(row[3], dict) else {}
    snapshot = {
        "holder_count": int(row[0] or 0),
        "mcap": safe_float(row[1]),
        "alert_count": int(row[2] or 0),
        "price": safe_float(raw_stats.get("price")),
        "mcap_alert_history": raw_stats.get("mcap_alert_history") or [],
        "price_alert_history": raw_stats.get("price_alert_history") or [],
    }
    cache_candidate_snapshot(
        address,
        {
            "holder_count": snapshot["holder_count"],
            "mcap": snapshot["mcap"],
            "price": snapshot["price"],
            "alert_sequence_no": snapshot["alert_count"],
            "mcap_alert_history": snapshot["mcap_alert_history"],
            "price_alert_history": snapshot["price_alert_history"],
        },
    )
    return snapshot

_SNAPSHOT_NOT_PROVIDED = object()

def upsert_tg_alert(address, msg, allow_repeat=False, existing_candidate=_SNAPSHOT_NOT_PROVIDED):
    if existing_candidate is _SNAPSHOT_NOT_PROVIDED:
        existing_candidate = get_candidate_snapshot(address)
    if existing_candidate and not allow_repeat:
        return None
    return send_tg_alert(msg)

def format_mcap_short(value):
    value = safe_float(value)
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"{value / 1_000:.0f}K"
    return f"{value:.0f}"

def format_mcap_history(values):
    cleaned = [safe_float(value) for value in values if safe_float(value) > 0]
    return " -> ".join(format_mcap_short(value) for value in cleaned) if cleaned else "N/A"

def format_usd_short(value):
    value = safe_float(value)
    sign = "-" if value < 0 else ""
    value = abs(value)
    if value >= 1_000_000:
        return f"{sign}${value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"{sign}${value / 1_000:.1f}K"
    return f"{sign}${value:,.0f}"

def alert_candidate_redis_key(address):
    return redis_key(ALERT_REDIS_KEY_PREFIX, address)

def alert_candidate_miss_redis_key(address):
    return redis_key(ALERT_MISS_REDIS_KEY_PREFIX, address)

def normalize_candidate_snapshot(data):
    if not isinstance(data, dict):
        return None
    return {
        "holder_count": int(safe_float(data.get("holder_count"))),
        "mcap": safe_float(data.get("mcap")),
        "alert_count": int(safe_float(data.get("alert_count"))),
        "price": safe_float(data.get("price")),
        "mcap_alert_history": [
            safe_float(value)
            for value in (data.get("mcap_alert_history") or [])
            if safe_float(value) > 0
        ],
        "price_alert_history": [
            safe_float(value)
            for value in (data.get("price_alert_history") or [])
            if safe_float(value) > 0
        ],
    }

def load_candidate_snapshot_from_redis(address):
    client = get_redis_client()
    if client is None:
        return None
    try:
        raw = client.get(alert_candidate_redis_key(address))
        if not raw:
            return None
        return normalize_candidate_snapshot(json.loads(raw))
    except Exception as exc:
        print(f"  [Redis] 读取复推快照失败 {address[:8]}: {exc}")
        return None

def load_candidate_miss_from_redis(address):
    client = get_redis_client()
    if client is None:
        return False
    try:
        return bool(client.get(alert_candidate_miss_redis_key(address)))
    except Exception as exc:
        print(f"  [Redis] 读取未推送缓存失败 {address[:8]}: {exc}")
        return False

def cache_candidate_miss(address):
    client = get_redis_client()
    if client is None:
        return
    try:
        client.setex(alert_candidate_miss_redis_key(address), ALERT_MISS_REDIS_TTL_SEC, "1")
    except Exception as exc:
        print(f"  [Redis] 写入未推送缓存失败 {address[:8]}: {exc}")

def cache_candidate_snapshot(address, stats):
    client = get_redis_client()
    if client is None:
        return
    payload = {
        "holder_count": stats.get("holder_count"),
        "mcap": stats.get("mcap"),
        "alert_count": stats.get("alert_sequence_no") or stats.get("previous_alert_count", 0) + 1,
        "price": stats.get("price"),
        "mcap_alert_history": stats.get("mcap_alert_history") or [stats.get("mcap")],
        "price_alert_history": stats.get("price_alert_history") or [stats.get("price")],
        "updated_at": int(time.time()),
    }
    try:
        client.setex(alert_candidate_redis_key(address), ALERT_REDIS_TTL_SEC, json.dumps(payload, ensure_ascii=False))
        client.delete(alert_candidate_miss_redis_key(address))
    except Exception as exc:
        print(f"  [Redis] 写入复推快照失败 {address[:8]}: {exc}")

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

def token_observation_label(address, symbol=None):
    symbol = symbol or "UNKNOWN"
    short_addr = f"{address[:8]}..." if address else "noaddr"
    return f"${symbol}({short_addr})"

def redis_observation_key(address):
    return redis_key(REDIS_KEY_PREFIX, "token", address)

def redis_observation_archive_key(address):
    return redis_key(ARCHIVE_REDIS_KEY_PREFIX, "token", address)

def redis_scan_round_key():
    return redis_key(REDIS_KEY_PREFIX, "scan_round")

def reset_price_observation(address):
    PRICE_OBSERVATION_STATE.pop(address, None)
    client = get_redis_client()
    if client is None:
        return
    try:
        client.delete(redis_observation_key(address))
    except Exception as exc:
        print(f"  [Redis] 重置观察状态失败 {address[:8]}: {exc}")

def next_scan_round():
    global SCAN_ROUND
    client = get_redis_client()
    if client is not None:
        try:
            key = redis_scan_round_key()
            value = int(client.incr(key))
            client.expire(key, REDIS_STATE_TTL_SEC)
            return value
        except Exception as exc:
            print(f"  [Redis] scan_round 写入失败，使用内存计数: {exc}")
    SCAN_ROUND += 1
    return SCAN_ROUND

def load_price_observation_state(address):
    client = get_redis_client()
    if client is None:
        return PRICE_OBSERVATION_STATE.get(address)
    try:
        raw = client.get(redis_observation_key(address))
        return json.loads(raw) if raw else None
    except Exception as exc:
        print(f"  [Redis] 读取观察状态失败 {address[:8]}: {exc}")
        return PRICE_OBSERVATION_STATE.get(address)

def save_price_observation_state(address, state):
    PRICE_OBSERVATION_STATE[address] = state
    client = get_redis_client()
    if client is None:
        return
    try:
        client.setex(redis_observation_key(address), REDIS_STATE_TTL_SEC, json.dumps(state, ensure_ascii=False))
    except Exception as exc:
        print(f"  [Redis] 写入观察状态失败 {address[:8]}: {exc}")

def load_price_observation_archive(address):
    client = get_redis_client()
    if client is None:
        return PRICE_OBSERVATION_ARCHIVE_STATE.get(address, [])
    try:
        raw = client.get(redis_observation_archive_key(address))
        data = json.loads(raw) if raw else []
        return data if isinstance(data, list) else []
    except Exception as exc:
        print(f"  [Redis] 读取价格观察归档失败 {address[:8]}: {exc}")
        return PRICE_OBSERVATION_ARCHIVE_STATE.get(address, [])

def save_price_observation_archive(address, archive):
    archive = list(archive or [])[-PRICE_OBSERVATION_ARCHIVE_LIMIT:]
    PRICE_OBSERVATION_ARCHIVE_STATE[address] = archive
    client = get_redis_client()
    if client is None:
        return
    try:
        client.setex(redis_observation_archive_key(address), ARCHIVE_REDIS_TTL_SEC, json.dumps(archive, ensure_ascii=False))
    except Exception as exc:
        print(f"  [Redis] 写入价格观察归档失败 {address[:8]}: {exc}")

def compact_price_path(prices, limit=6):
    cleaned = [safe_float(value) for value in (prices or []) if safe_float(value) > 0]
    if not cleaned:
        return "N/A"
    trimmed = cleaned[-limit:]
    return " -> ".join(format_chain_price(value) for value in trimmed)

def observation_archive_entry(stats, price_observation):
    return {
        "ts": int(time.time()),
        "alert_no": int(stats.get("alert_sequence_no") or 1),
        "type": stats.get("repeat_alert_type") or ("复推" if stats.get("repeat_alert") else "首推"),
        "prices": price_observation.get("prices") or [],
        "band": price_observation.get("change_band_text") or "N/A",
        "last_change_pct": float(price_observation.get("change_pct") or 0) * 100,
    }

def format_price_observation_archive(archive, current_entry=None, limit=3):
    rows = list(archive or [])
    if current_entry:
        rows.append(current_entry)
    rows = rows[-limit:]
    if not rows:
        return ""
    lines = []
    for item in rows:
        lines.append(
            f"第{int(item.get('alert_no') or 0)}次/{item.get('type') or '观察'}: "
            f"{compact_price_path(item.get('prices'))} | 波段 {item.get('band') or 'N/A'}"
        )
    return "价格记录:\n" + "\n".join(lines) + "\n"

def update_price_observation(address, price, scan_round, symbol=None, holder_count=None):
    price = safe_float(price)
    holder_count = int(safe_float(holder_count))
    if price <= 0:
        return {
            "ready": False,
            "allowed": False,
            "reason": "price_missing",
            "count": 0,
            "change_pct": 0.0,
            "drop_pct": 0.0,
            "holder_count_delta": 0,
        }

    state = load_price_observation_state(address)
    if not state or int(state.get("last_round") or 0) != scan_round - 1:
        state = {
            "first_round": scan_round,
            "last_round": scan_round,
            "prices": [price],
            "holder_counts": [holder_count],
            "symbol": symbol,
        }
    else:
        prices = list(state.get("prices") or [])
        holder_counts = list(state.get("holder_counts") or [])
        prices.append(price)
        holder_counts.append(holder_count)
        state["prices"] = prices[-PRICE_OBSERVATION_HISTORY_LIMIT:]
        state["holder_counts"] = holder_counts[-PRICE_OBSERVATION_HISTORY_LIMIT:]
        state["last_round"] = scan_round
        state["symbol"] = symbol or state.get("symbol")

    save_price_observation_state(address, state)
    prices = list(state.get("prices") or [])
    holder_counts = list(state.get("holder_counts") or [])
    count = len(prices)
    recent_prices = prices[-MIN_PRICE_OBSERVATION_SCANS:]
    recent_holder_counts = holder_counts[-MIN_PRICE_OBSERVATION_SCANS:]
    observation_count = len(recent_prices)
    first_price = safe_float(recent_prices[0]) if recent_prices else 0.0
    previous_price = safe_float(prices[-2]) if count >= 2 else 0.0
    current_price = safe_float(prices[-1])
    change_pct = (current_price - previous_price) / previous_price if previous_price > 0 else 0.0
    drop_pct = -change_pct if change_pct < 0 else 0.0
    segment_changes = [
        (safe_float(prices[idx]) - safe_float(prices[idx - 1])) / safe_float(prices[idx - 1])
        for idx in range(1, len(prices))
        if safe_float(prices[idx - 1]) > 0
    ]
    band_changes = segment_changes[-PRICE_OBSERVATION_BAND_LIMIT:]
    change_band_text = " -> ".join(f"{value:+.1%}" for value in band_changes) if band_changes else "N/A"
    rebound_prices = [safe_float(value) for value in prices[-REBOUND_LOOKBACK_SCANS:] if safe_float(value) > 0]
    local_low_price = min(rebound_prices) if rebound_prices else 0.0
    rebound_from_low_pct = (current_price - local_low_price) / local_low_price if local_low_price > 0 else 0.0
    first_holder_count = int(safe_float(recent_holder_counts[0])) if recent_holder_counts else 0
    current_holder_count = int(safe_float(recent_holder_counts[-1])) if recent_holder_counts else 0
    holder_count_delta = current_holder_count - first_holder_count if first_holder_count > 0 and current_holder_count > 0 else 0
    continuous_up = observation_count >= FAST_PRICE_OBSERVATION_SCANS and all(
        recent_prices[idx] >= recent_prices[idx - 1] for idx in range(1, len(recent_prices))
    )
    fast_up = observation_count >= FAST_PRICE_OBSERVATION_SCANS and continuous_up and change_pct >= FAST_PRICE_UP_PCT
    not_large_drop = observation_count >= MIN_PRICE_OBSERVATION_SCANS and drop_pct <= MAX_PRICE_DROP_PCT
    ready = observation_count >= MIN_PRICE_OBSERVATION_SCANS or fast_up
    allowed = fast_up or (observation_count >= MIN_PRICE_OBSERVATION_SCANS and not_large_drop)
    if not ready:
        reason = f"observe_wait_{observation_count}/{MIN_PRICE_OBSERVATION_SCANS}"
    elif fast_up:
        reason = f"fast_up_{change_pct:.1%}"
    elif not_large_drop:
        reason = f"drop_ok_{drop_pct:.1%}"
    else:
        reason = f"drop_too_much_{drop_pct:.1%}"
    return {
        "ready": ready,
        "allowed": allowed,
        "reason": reason,
        "count": observation_count,
        "history_count": count,
        "change_pct": change_pct,
        "drop_pct": drop_pct,
        "first_price": first_price,
        "previous_price": previous_price,
        "current_price": current_price,
        "prices": prices,
        "segment_changes": segment_changes,
        "change_band_text": change_band_text,
        "local_low_price": local_low_price,
        "rebound_from_low_pct": rebound_from_low_pct,
        "holder_counts": holder_counts,
        "first_holder_count": first_holder_count,
        "current_holder_count": current_holder_count,
        "holder_count_delta": holder_count_delta,
        "continuous_up": continuous_up,
        "fast_up": fast_up,
    }

def mcap_price_observation_pass(mcap, price_observation):
    mcap = safe_float(mcap)
    change_pct = float(price_observation.get("change_pct") or 0)
    continuous_up = bool(price_observation.get("continuous_up"))
    fast_up = bool(price_observation.get("fast_up"))
    drop_pct = float(price_observation.get("drop_pct") or 0)
    if mcap < LOW_MCAP_STRICT_USD:
        return (
            (continuous_up and change_pct >= LOW_MCAP_MIN_UP_PCT) or fast_up,
            f"市值<{LOW_MCAP_STRICT_USD:,.0f}，需要连续上涨{LOW_MCAP_MIN_UP_PCT:.0%}，当前{change_pct:.1%}",
        )
    if mcap < MID_MCAP_STRICT_USD:
        return (
            (continuous_up and change_pct >= MID_MCAP_MIN_UP_PCT) or fast_up,
            f"市值{LOW_MCAP_STRICT_USD:,.0f}-{MID_MCAP_STRICT_USD:,.0f}，需要连续上涨{MID_MCAP_MIN_UP_PCT:.0%}，当前{change_pct:.1%}",
        )
    return (
        drop_pct <= MAX_PRICE_DROP_PCT,
        f"市值>={MID_MCAP_STRICT_USD:,.0f}，需要回撤不超{MAX_PRICE_DROP_PCT:.0%}，当前{drop_pct:.1%}",
    )

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

def extract_token_narrative(info, trend_row):
    value = first_nested_value(
        info,
        trend_row,
        paths=(
            ("link", "description"),
            ("description",),
            ("trans_name_zhcn",),
            ("name",),
            ("link", "twitter_username"),
            ("twitter_username",),
            ("link", "website"),
            ("website",),
            ("link", "telegram"),
            ("telegram",),
        ),
    )
    return str(value or "").strip()[:180]

def calc_mcap(*sources):
    price = first_float(*sources, keys=("price",))
    circulating_supply = first_float(*sources, keys=("circulating_supply",))
    if price > 0 and circulating_supply > 0:
        return price * circulating_supply
    return first_float(
        *sources,
        keys=("market_cap", "usd_market_cap", "mcap", "fdv", "fully_diluted_valuation"),
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

def extract_trade_volume_usd(*sources):
    volume = first_float(
        *sources,
        keys=(
            "volume",
            "volume_usd",
            "trade_volume",
            "trade_volume_usd",
            "swap_volume",
            "swap_volume_usd",
            "tx_volume",
            "tx_volume_usd",
            "total_volume",
            "total_volume_usd",
            "volume_1m",
            "volume_1m_usd",
            "volume_5m",
            "volume_5m_usd",
            "volume_h24",
            "volume_24h",
        ),
    )
    if volume > 0:
        return volume
    buy_volume = first_float(
        *sources,
        keys=("buy_volume", "buy_volume_usd", "buy_volume_1m", "buy_volume_1m_usd", "buy_volume_5m", "buy_volume_5m_usd"),
    )
    sell_volume = first_float(
        *sources,
        keys=("sell_volume", "sell_volume_usd", "sell_volume_1m", "sell_volume_1m_usd", "sell_volume_5m", "sell_volume_5m_usd"),
    )
    return buy_volume + sell_volume

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

def extract_dev_risk(info, trend_row, holders_list):
    creator_address = first_nested_value(
        info,
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
        trend_row,
        paths=(
            ("stat", "dev_team_hold_rate"),
            ("dev_team_hold_rate",),
            ("dev", "dev_team_hold_rate"),
        ),
    )
    creator_hold_rate = first_nested_float(
        info,
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
    dev_sell_usd = first_nested_float(
        info,
        trend_row,
        paths=(
            ("dev", "sell_volume_cur"),
            ("dev", "sell_volume_usd"),
            ("creator_sell_volume",),
            ("creator_sell_usd",),
            ("dev_sell_volume",),
            ("dev_sell_usd",),
        ),
    )
    dev_hold_value_usd = first_nested_float(
        info,
        trend_row,
        paths=(
            ("dev", "usd_value"),
            ("dev", "holding_value"),
            ("dev", "hold_value_usd"),
            ("creator_usd_value",),
            ("creator_hold_value_usd",),
            ("dev_hold_value_usd",),
        ),
    )
    dev_sell_amount_rate = first_nested_float(
        info,
        trend_row,
        paths=(
            ("dev", "sell_amount_percentage"),
            ("creator_sell_amount_percentage",),
            ("dev_sell_amount_percentage",),
        ),
    )
    creator_open_count = int(first_nested_float(
        info,
        trend_row,
        paths=(
            ("dev", "creator_open_count"),
            ("creator_open_count",),
        ),
    ))
    creator_created_count = int(first_nested_float(
        info,
        trend_row,
        paths=(
            ("dev", "creator_created_count"),
            ("creator_created_count",),
        ),
    ))
    creator_token_status = first_nested_value(
        info,
        trend_row,
        paths=(
            ("dev", "creator_token_status"),
            ("creator_token_status",),
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
            dev_sell_usd = max(dev_sell_usd, safe_float(holder.get("sell_volume_cur")))
            dev_hold_value_usd = max(dev_hold_value_usd, safe_float(holder.get("usd_value")))
            dev_sell_amount_rate = max(dev_sell_amount_rate, normalize_ratio(holder.get("sell_amount_percentage")))
            break

    dev_hold_rate = max(normalize_ratio(dev_team_hold_rate), normalize_ratio(creator_hold_rate))
    if dev_hold_value_usd <= 0:
        mcap = calc_mcap(trend_row, info)
        dev_hold_value_usd = mcap * dev_hold_rate if mcap > 0 and dev_hold_rate > 0 else 0
    should_skip = dev_buy_usd > MAX_DEV_BUY_USD or dev_hold_rate > MAX_DEV_HOLD_RATE
    reasons = []
    if dev_buy_usd > MAX_DEV_BUY_USD:
        reasons.append(f"dev_buy=${dev_buy_usd:.0f}>{MAX_DEV_BUY_USD:.0f}")
    if dev_hold_rate > MAX_DEV_HOLD_RATE:
        reasons.append(f"dev_hold={dev_hold_rate * 100:.1f}%>{MAX_DEV_HOLD_RATE * 100:.0f}%")
    return {
        "creator_address": creator_address,
        "dev_buy_usd": dev_buy_usd,
        "dev_sell_usd": dev_sell_usd,
        "dev_netflow_usd": dev_buy_usd - dev_sell_usd,
        "dev_hold_rate": dev_hold_rate,
        "dev_hold_value_usd": dev_hold_value_usd,
        "dev_sell_amount_rate": normalize_ratio(dev_sell_amount_rate),
        "creator_open_count": creator_open_count,
        "creator_created_count": creator_created_count,
        "creator_token_status": str(creator_token_status or ""),
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

def is_recent_new_wallet(holder, now=None):
    now = now or time.time()
    return holder.get("is_new") or (
        holder_created_ts(holder) > 0
        and now - holder_created_ts(holder) <= NEW_WALLET_WINDOW_SEC
    )

def build_creation_cluster(wallets):
    if not wallets:
        return {}
    start_ts = min(holder_created_ts(w) for w in wallets)
    end_ts = max(holder_created_ts(w) for w in wallets)
    supply = sum(safe_float(w.get("amount_percentage")) * 100 for w in wallets)
    buy_volume = sum(safe_float(w.get("buy_volume_cur")) for w in wallets)
    sell_volume = sum(safe_float(w.get("sell_volume_cur")) for w in wallets)
    netflow = sum(holder_net_buy_usd(w) for w in wallets)
    position_value = sum(holder_position_value_usd(w) for w in wallets)
    return {
        "wallets": wallets,
        "count": len(wallets),
        "supply": supply,
        "buy_volume": buy_volume,
        "sell_volume": sell_volume,
        "netflow": netflow,
        "position_value": position_value,
        "avg_cost": weighted_avg_cost(wallets),
        "median_cost": median_cost(wallets),
        "start_ts": start_ts,
        "end_ts": end_ts,
        "date_range": f"{datetime.fromtimestamp(start_ts).strftime('%m-%d')}~{datetime.fromtimestamp(end_ts).strftime('%m-%d')}",
    }


def find_creation_clusters(non_pool, min_wallets=2, max_clusters=5):
    created_wallets = sorted(
        [(holder_created_ts(holder), holder) for holder in non_pool if holder_created_ts(holder) > 0],
        key=lambda item: item[0],
    )
    candidates = []
    for idx, (start_ts, _) in enumerate(created_wallets):
        end_ts = start_ts + WALLET_CREATION_CLUSTER_SEC
        wallets = [holder for created_ts, holder in created_wallets[idx:] if created_ts <= end_ts]
        if len(wallets) < min_wallets:
            continue
        cluster = build_creation_cluster(wallets)
        cluster["wallet_set"] = {id(wallet) for wallet in wallets}
        candidates.append(cluster)

    selected = []
    used_wallet_ids = set()
    for cluster in sorted(candidates, key=lambda item: (item["supply"], item["count"]), reverse=True):
        wallet_set = cluster["wallet_set"]
        if wallet_set & used_wallet_ids:
            continue
        used_wallet_ids.update(wallet_set)
        cluster = dict(cluster)
        cluster.pop("wallet_set", None)
        selected.append(cluster)
        if len(selected) >= max_clusters:
            break
    selected.sort(key=lambda item: item["start_ts"])
    return selected


def find_best_creation_cluster(non_pool):
    clusters = find_creation_clusters(non_pool, min_wallets=2, max_clusters=1)
    if not clusters:
        return [], 0.0, 0, 0
    best = clusters[0]
    return best["wallets"], best["supply"], best["start_ts"], best["end_ts"]


def analyze_wallet_creation_clusters(holders_list):
    non_pool = [h for h in holders_list if not is_pool_holder(h)]
    now = time.time()
    new_wallets = [h for h in non_pool if is_recent_new_wallet(h, now=now)]
    creation_clusters = find_creation_clusters(non_pool, min_wallets=2, max_clusters=5)
    best_cluster = max(creation_clusters, key=lambda item: item["supply"], default={})
    best_wallets = best_cluster.get("wallets", [])
    best_supply = safe_float(best_cluster.get("supply"))

    new_supply = sum(safe_float(w.get("amount_percentage")) * 100 for w in new_wallets)
    new_usd_value = sum(safe_float(w.get("usd_value")) for w in new_wallets)
    new_buy_volume = sum(safe_float(w.get("buy_volume_cur")) for w in new_wallets)
    new_sell_volume = sum(safe_float(w.get("sell_volume_cur")) for w in new_wallets)
    new_netflow = sum(holder_net_buy_usd(w) for w in new_wallets)
    new_avg_cost = weighted_avg_cost(new_wallets)
    new_median_cost = median_cost(new_wallets)
    cluster_buy = sum(safe_float(w.get("buy_volume_cur")) for w in best_wallets)
    cluster_sell = sum(safe_float(w.get("sell_volume_cur")) for w in best_wallets)
    cluster_netflow = sum(holder_net_buy_usd(w) for w in best_wallets)
    cluster_avg_cost = weighted_avg_cost(best_wallets)
    cluster_median_cost = median_cost(best_wallets)
    cluster_desc = "no same-batch creation wallets"
    if creation_clusters:
        cluster_desc = "; ".join(
            f"{cluster['date_range']} | {cluster['count']} wallets | hold {cluster['supply']:.2f}%"
            for cluster in creation_clusters
        )
    conspiracy_score = 0
    if len(new_wallets) >= 5 or new_supply >= 10:
        conspiracy_score += 25
    if len(best_wallets) >= 4 or best_supply >= 8:
        conspiracy_score += 30
    if len(creation_clusters) >= 2:
        conspiracy_score += 15
    if cluster_netflow > 0:
        conspiracy_score += 10
    return {
        "new_wallet_count": len(new_wallets),
        "new_wallet_supply": new_supply,
        "new_wallet_usd_value": new_usd_value,
        "new_wallet_buy_volume": new_buy_volume,
        "new_wallet_sell_volume": new_sell_volume,
        "new_wallet_netflow": new_netflow,
        "new_wallet_avg_cost": new_avg_cost,
        "new_wallet_median_cost": new_median_cost,
        "wallet_creation_cluster_size": len(best_wallets),
        "wallet_creation_cluster_supply": best_supply,
        "wallet_creation_cluster_buy_volume": cluster_buy,
        "wallet_creation_cluster_sell_volume": cluster_sell,
        "wallet_creation_cluster_netflow": cluster_netflow,
        "wallet_creation_cluster_avg_cost": cluster_avg_cost,
        "wallet_creation_cluster_median_cost": cluster_median_cost,
        "wallet_creation_cluster_desc": cluster_desc,
        "wallet_creation_cluster_count": len(creation_clusters),
        "wallet_creation_clusters": [
            {key: value for key, value in cluster.items() if key != "wallets"}
            for cluster in creation_clusters
        ],
        "conspiracy_wallet_score": min(conspiracy_score, 100),
    }

# K-line analysis is disabled in deep_alpha_pro.
# This scanner is now for new-token chip/holder analysis only, so K-line fetching stays disabled here.
def parse_kline_rows(raw):
    return []

def fetch_5m_klines(chain, address, lookback_sec):
    return []

def max_drawdown(candles):
    return 0.0

def analyze_kline_health(chain, address, age_seconds):
    return {
        "token_age_type": token_age_type(age_seconds),
        "kline_verdict": "disabled",
        "kline_score": 0,
        "kline_recent_change_pct": 0,
        "spike_high": 0,
        "retreat_low": 0,
        "current_price": 0,
        "spike_retreat_pct": 0,
        "recovery_from_low_pct": 0,
    }

    return None

def derive_market_structure(stats):
    front_flow = stats.get("front_holder_netflow", 0)
    top_flow = stats.get("holder_flow_netflow", 0)
    accumulation = stats.get("accumulation_score", 0)
    distribution = stats.get("distribution_score", 0)
    low_sell_supply = stats.get("low_sell_supply", 0)
    high_sell_supply = stats.get("high_sell_supply", 0)
    source_supply = stats.get("source_cluster_supply", 0)
    source_netflow = stats.get("source_cluster_netflow", 0)
    conspiracy = stats.get("conspiracy_wallet_score", 0)
    if conspiracy >= 50 and source_supply >= 5:
        return {"market_structure": "batch_wallet_control", "market_structure_score": -25, "market_structure_reason": "same-batch wallet risk and same-source holding are both high", "market_structure_risk": "high"}
    if distribution >= 45 and (high_sell_supply >= 5 or front_flow < 0 or top_flow < 0):
        return {"market_structure": "distribution_pressure", "market_structure_score": -35, "market_structure_reason": "front/top100 holders show sell pressure", "market_structure_risk": "high"}
    if accumulation >= 45 and low_sell_supply >= 5:
        return {"market_structure": "holder_accumulation", "market_structure_score": 15, "market_structure_reason": "top holders show net inflow and low sell ratio", "market_structure_risk": "mid"}
    if source_supply >= 8 and source_netflow > 0:
        return {"market_structure": "same_source_accumulation", "market_structure_score": 10, "market_structure_reason": "same-source wallets have positive netflow", "market_structure_risk": "mid"}
    return {"market_structure": "watch", "market_structure_score": 0, "market_structure_reason": "holder flow has no clear accumulation/distribution edge", "market_structure_risk": "mid"}

def inflow_status_text(stats):
    streak = int(stats.get("inflow_streak", 0))
    if streak >= MIN_INFLOW_STREAK:
        return f"confirmed {streak} rounds"
    if streak > 0:
        return f"unconfirmed {streak} rounds"
    return "none"

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

def analyze_top10_holders(holders_list):
    non_pool = [h for h in holders_list if not is_pool_holder(h)]
    buckets = {}
    lines = []
    for size in (10, 100):
        wallets = non_pool[:size]
        supply = sum(safe_float(h.get("amount_percentage")) * 100 for h in wallets)
        buy_volume = sum(safe_float(h.get("buy_volume_cur")) for h in wallets)
        sell_volume = sum(safe_float(h.get("sell_volume_cur")) for h in wallets)
        profit = sum(safe_float(h.get("profit")) for h in wallets)
        profit_pct = (profit / buy_volume * 100) if buy_volume > 0 else 0
        sell_progress = wallet_sell_progress_pct(wallets)
        position_value = sum(holder_position_value_usd(h) for h in wallets)
        buy_tx = sum(safe_float(h.get("buy_tx_count_cur")) for h in wallets)
        sell_tx = sum(safe_float(h.get("sell_tx_count_cur")) for h in wallets)
        netflow = sum(holder_net_buy_usd(h) for h in wallets)
        buckets[size] = {
            "supply": supply,
            "buy_volume": buy_volume,
            "sell_volume": sell_volume,
            "profit": profit,
            "profit_pct": profit_pct,
            "sell_progress": sell_progress,
            "position_value": position_value,
            "buy_tx": int(buy_tx),
            "sell_tx": int(sell_tx),
            "netflow": netflow,
        }
    lines = [
        (
            f"Top{size}: 持仓{buckets[size]['supply']:.1f}% | "
            f"${buckets[size]['position_value']:,.0f} | "
            f"盈利{buckets[size]['profit_pct']:+.1f}% | "
            f"卖出进度{buckets[size]['sell_progress']:.1f}% | "
            f"次数{buckets[size]['buy_tx']}/{buckets[size]['sell_tx']}"
        )
        for size in (10, 100)
    ]
    top10 = buckets[10]
    return {
        "rank_bucket_stats": buckets,
        "rank_bucket_desc": "\n".join(lines),
        "top10_holder_supply": top10["supply"],
        "top10_holder_buy_volume": top10["buy_volume"],
        "top10_holder_sell_volume": top10["sell_volume"],
        "top10_holder_netflow": top10["netflow"],
    }

def holder_tags(holder):
    tags = set()
    raw_tags = holder.get("tags") or []
    maker_tags = holder.get("maker_token_tags") or []
    if isinstance(raw_tags, list):
        tags.update(str(tag) for tag in raw_tags)
    if isinstance(maker_tags, list):
        tags.update(str(tag) for tag in maker_tags)
    if is_recent_new_wallet(holder):
        tags.add("fresh_wallet")
    return tags

def weighted_avg_cost(holders):
    total_weight = 0.0
    total_cost = 0.0
    for holder in holders:
        avg_cost = safe_float(holder.get("avg_cost"))
        weight = safe_float(holder.get("amount_percentage"))
        if avg_cost <= 0 or weight <= 0:
            continue
        total_weight += weight
        total_cost += avg_cost * weight
    return total_cost / total_weight if total_weight > 0 else 0.0

def median_cost(holders):
    costs = sorted(
        safe_float(holder.get("avg_cost"))
        for holder in holders
        if safe_float(holder.get("avg_cost")) > 0
    )
    if not costs:
        return 0.0
    mid = len(costs) // 2
    if len(costs) % 2:
        return costs[mid]
    return (costs[mid - 1] + costs[mid]) / 2

def format_chain_price(value):
    scaled = safe_float(value) * CHAIN_PRICE_DISPLAY_SCALE
    def trunc_1(num):
        return int(num * 10) / 10
    def trim(num, decimals=1):
        text = f"{num:.{decimals}f}"
        return text.rstrip("0").rstrip(".")
    if scaled <= 0:
        return "0"
    if scaled >= 10_000:
        return f"{trim(trunc_1(scaled / 10_000))}W"
    if scaled >= 1_000:
        return f"{trim(trunc_1(scaled / 1_000))}k"
    if scaled >= 1:
        return trim(scaled)
    return trim(scaled, 4)

def format_pnl_pct(current_price, avg_cost):
    if safe_float(avg_cost) <= 0:
        return "0.0%"
    pnl_pct = (safe_float(current_price) / safe_float(avg_cost) - 1) * 100
    return f"{pnl_pct:+.1f}%"

def holder_position_value_usd(holder):
    return safe_float(holder.get("usd_value"))

def wallet_sell_progress_pct(wallets):
    supply = sum(safe_float(holder.get("amount_percentage")) * 100 for holder in wallets)
    if supply <= 0:
        return 0.0
    sold_supply = sum(
        safe_float(holder.get("amount_percentage")) * 100 * normalize_ratio(holder.get("sell_amount_percentage"))
        for holder in wallets
    )
    return sold_supply / supply * 100

def analyze_holder_tags_and_costs(holders_list, current_price):
    non_pool = [h for h in holders_list if not is_pool_holder(h)]
    tag_defs = [
        ("smart_degen", "聪明钱"),
        ("renowned", "KOL"),
        ("sniper", "狙击手"),
        ("bundler", "捆绑"),
        ("rat_trader", "老鼠仓"),
        ("fresh_wallet", "新钱包"),
        ("dex_bot", "交易机器人"),
        ("bluechip_owner", "蓝筹持有人"),
    ]
    tag_stats = {}
    tag_lines = []
    for tag_key, label in tag_defs:
        wallets = [h for h in non_pool if tag_key in holder_tags(h)]
        supply = sum(safe_float(h.get("amount_percentage")) * 100 for h in wallets)
        buy_volume = sum(safe_float(h.get("buy_volume_cur")) for h in wallets)
        sell_volume = sum(safe_float(h.get("sell_volume_cur")) for h in wallets)
        netflow = sum(holder_net_buy_usd(h) for h in wallets)
        profit = sum(safe_float(h.get("profit")) for h in wallets)
        profit_pct = (profit / buy_volume * 100) if buy_volume > 0 else 0
        position_value = sum(holder_position_value_usd(h) for h in wallets)
        sell_progress = wallet_sell_progress_pct(wallets)
        avg_cost = weighted_avg_cost(wallets)
        mid_cost = median_cost(wallets)
        tag_stats[tag_key] = {
            "label": label,
            "count": len(wallets),
            "supply": supply,
            "buy_volume": buy_volume,
            "sell_volume": sell_volume,
            "netflow": netflow,
            "profit": profit,
            "profit_pct": profit_pct,
            "position_value": position_value,
            "sell_progress": sell_progress,
            "avg_cost": avg_cost,
            "median_cost": mid_cost,
        }
        if wallets:
            tag_lines.append(
                f"{label}{len(wallets)}个 持仓{supply:.1f}%/${position_value:,.0f} 盈利{profit_pct:+.1f}% 卖出进度{sell_progress:.1f}%"
            )

    creation_clusters = find_creation_clusters(non_pool, min_wallets=2, max_clusters=5)
    if creation_clusters:
        tag_stats["same_creation_clusters"] = [
            {key: value for key, value in cluster.items() if key != "wallets"}
            for cluster in creation_clusters
        ]
        cluster_wallets = []
        for cluster in creation_clusters:
            cluster_wallets.extend(cluster.get("wallets", []))
        cluster_count = len(cluster_wallets)
        cluster_supply = sum(safe_float(cluster.get("supply")) for cluster in creation_clusters)
        cluster_position_value = sum(safe_float(cluster.get("position_value")) for cluster in creation_clusters)
        cluster_buy_volume = sum(safe_float(cluster.get("buy_volume")) for cluster in creation_clusters)
        cluster_sell_volume = sum(safe_float(cluster.get("sell_volume")) for cluster in creation_clusters)
        cluster_netflow = sum(safe_float(cluster.get("netflow")) for cluster in creation_clusters)
        cluster_sell_progress = wallet_sell_progress_pct(cluster_wallets)
        cluster_avg_cost = weighted_avg_cost(cluster_wallets)
        cluster_median_cost = median_cost(cluster_wallets)
        cost_values = [safe_float(holder.get("avg_cost")) for holder in cluster_wallets if safe_float(holder.get("avg_cost")) > 0]
        cost_range = (
            f"{format_chain_price(min(cost_values))}-{format_chain_price(max(cost_values))}"
            if cost_values
            else "0"
        )
        tag_stats["same_creation_cluster_summary"] = {
            "cluster_count": len(creation_clusters),
            "wallet_count": cluster_count,
            "supply": cluster_supply,
            "position_value": cluster_position_value,
            "buy_volume": cluster_buy_volume,
            "sell_volume": cluster_sell_volume,
            "netflow": cluster_netflow,
            "sell_progress": cluster_sell_progress,
            "avg_cost": cluster_avg_cost,
            "median_cost": cluster_median_cost,
            "cost_range": cost_range,
        }
        tag_lines.append(
            f"同批创建簇汇总{len(creation_clusters)}簇/{cluster_count}个 持仓{cluster_supply:.1f}%/${cluster_position_value:,.0f} 盈利{format_pnl_pct(current_price, cluster_avg_cost)} 卖出进度{cluster_sell_progress:.1f}%"
        )

    top20 = non_pool[:20]
    top50 = non_pool[:50]
    top20_cost = weighted_avg_cost(top20)
    top50_cost = weighted_avg_cost(top50)
    top100_cost = weighted_avg_cost(non_pool)

    cost_bands = [
        ("深度盈利", 0.0, 0.5),
        ("盈利区", 0.5, 0.8),
        ("成本区", 0.8, 1.2),
        ("套牢区", 1.2, 2.0),
        ("高位接盘", 2.0, float("inf")),
    ]
    band_stats = []
    for label, low, high in cost_bands:
        wallets = []
        for holder in non_pool:
            avg_cost = safe_float(holder.get("avg_cost"))
            if current_price <= 0 or avg_cost <= 0:
                continue
            ratio = avg_cost / current_price
            if low <= ratio < high:
                wallets.append(holder)
        supply = sum(safe_float(h.get("amount_percentage")) * 100 for h in wallets)
        buy_volume = sum(safe_float(h.get("buy_volume_cur")) for h in wallets)
        sell_volume = sum(safe_float(h.get("sell_volume_cur")) for h in wallets)
        avg_cost = weighted_avg_cost(wallets)
        mid_cost = median_cost(wallets)
        price_low = current_price * low if current_price > 0 else 0
        price_high = current_price * high if current_price > 0 and high != float("inf") else 0
        if high == float("inf"):
            price_range = f">{format_chain_price(price_low)}"
        else:
            price_range = f"{format_chain_price(price_low)}-{format_chain_price(price_high)}"
        pnl_pct = (current_price / avg_cost - 1) * 100 if avg_cost > 0 else 0
        pnl_text = "盈利" if pnl_pct >= 0 else "亏损"
        band_stats.append({
            "label": label,
            "count": len(wallets),
            "supply": supply,
            "buy_volume": buy_volume,
            "sell_volume": sell_volume,
            "netflow": sum(holder_net_buy_usd(h) for h in wallets),
            "avg_cost": avg_cost,
            "median_cost": mid_cost,
            "price_range": price_range,
            "pnl_pct": pnl_pct,
            "pnl_text": pnl_text,
        })
    dominant_band = max(band_stats, key=lambda item: item["supply"], default={"label": "未知", "supply": 0, "count": 0})
    band_desc = "\n".join(
        f"{item['label']}({item['price_range']}) 持仓{item['supply']:.1f}%/{item['count']}个 "
        f"均{format_chain_price(item['avg_cost'])} 中{format_chain_price(item['median_cost'])} "
        f"{item['pnl_text']}{item['pnl_pct']:+.1f}%"
        for item in band_stats
        if item["count"] > 0
    ) or "无有效成本数据"

    return {
        "holder_tag_stats": tag_stats,
        "holder_tag_desc": "\n".join(tag_lines) if tag_lines else "未发现重点标签钱包",
        "top20_avg_cost": top20_cost,
        "top50_avg_cost": top50_cost,
        "top100_avg_cost": top100_cost,
        "top20_cost_ratio": (current_price / top20_cost) if top20_cost > 0 else 0,
        "top50_cost_ratio": (current_price / top50_cost) if top50_cost > 0 else 0,
        "top100_cost_ratio": (current_price / top100_cost) if top100_cost > 0 else 0,
        "cost_band_stats": band_stats,
        "cost_band_desc": band_desc,
        "dominant_cost_band": dominant_band["label"],
        "dominant_cost_band_supply": dominant_band["supply"],
        "dominant_cost_band_count": dominant_band["count"],
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
    non_pool = [h for h in holders_list if not is_pool_holder(h)]

    # 1. 聚类分析 (基于进场时间)
    time_clusters = defaultdict(list)
    total_supply_scanned = 0
    
    associated_supply = 0
    associated_count = 0
    sold_supply_from_clusters = 0
    sold_supply_pct = 0
    
    for h in non_pool:
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

def perform_deep_analysis(chain, address, trend_row=None, enforce_dev_risk=True):
    trend_row = trend_row or {}
    # 1. 获取基本信息
    info_raw = run_command(f"gmgn-cli token info --chain {chain} --address {address} --raw")
    if not info_raw: return None
    info = json.loads(info_raw)
    
    # 2. 获取前100持币者
    holders_raw = run_command(f"gmgn-cli token holders --chain {chain} --address {address} --limit 100 --raw")
    holders_data = json.loads(holders_raw) if holders_raw else {"list": []}
    holders_list = holders_data.get("list", [])
    non_pool_holder_count = sum(1 for holder in holders_list if not is_pool_holder(holder))

    dev_risk = extract_dev_risk(info, trend_row, holders_list)
    if enforce_dev_risk and dev_risk["should_skip"]:
        print(f"  [跳过] dev风险 {address}: {', '.join(dev_risk['reasons'])}")
        return None
    
    # 执行筹码关联分析
    ctrl = analyze_control_and_dump(holders_list, debug=DEBUG_DEEP_LOG)
    holder_flow = analyze_holder_flow(holders_list)
    top10_holders = analyze_top10_holders(holders_list)
    current_price = safe_float(info.get("price") or trend_row.get("price"))
    holder_tags_costs = analyze_holder_tags_and_costs(holders_list, current_price)
    wallet_creation = analyze_wallet_creation_clusters(holders_list)
    narrative = extract_token_narrative(info, trend_row)
    trend_mcap = calc_mcap(trend_row)
    info_mcap = calc_mcap(info)
    mcap = calc_mcap(trend_row, info)
    if first_float(trend_row, info, keys=("price",)) > 0 and first_float(trend_row, info, keys=("circulating_supply",)) > 0:
        mcap_source = "price_x_circulating_supply"
    else:
        mcap_source = "fallback_returned_field"
    holder_count = first_float(
        info,
        trend_row,
        keys=("holder_count", "holders_count", "holder_num", "holders", "holder"),
        default=non_pool_holder_count,
    )
    fee_sol = extract_fee_sol(info, trend_row)
    trade_volume_usd = extract_trade_volume_usd(trend_row, info)
    pool_label, pool_liquidity = extract_pool_label(info, trend_row)
    created_at = first_value(
        info,
        trend_row,
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
    token_type = token_age_type(age_seconds)
    flow = analyze_5m_flow(address, trend_row)
    
    # 组装数据
    stats = {
        "symbol": info.get("symbol"),
        "name": info.get("name") or trend_row.get("name"),
        "narrative": narrative,
        "mcap": mcap,
        "trend_mcap": trend_mcap,
        "info_mcap": info_mcap,
        "mcap_source": mcap_source,
        "circulating_supply": first_float(trend_row, info, keys=("circulating_supply",)),
        "holder_count": int(holder_count),
        "fee_sol": fee_sol,
        "trade_volume_usd": trade_volume_usd,
        "pool_label": pool_label,
        "pool_liquidity": pool_liquidity,
        "price": current_price,
        "created_at": created_at,
        "created_age": format_age(created_at),
        "created_time": format_created_time(created_at),
        "token_age_type": token_type,
        "sm_count": info.get("wallet_tags_stat", {}).get("smart_wallets", 0),
        "kol_count": info.get("wallet_tags_stat", {}).get("renowned_wallets", 0),
        "top10_rate": first_float(info, trend_row, keys=("top_10_holder_rate", "top10_holder_rate", "top10_rate")) * 100,
        "snipers": int(first_float(info, trend_row, keys=("sniper_count", "snipers"), default=0)),
        "rug_ratio": first_value(info, trend_row, keys=("rug_ratio", "risk_score", "risk_level")) or "0",
        "creator_address": dev_risk.get("creator_address"),
        "dev_buy_usd": dev_risk.get("dev_buy_usd", 0),
        "dev_sell_usd": dev_risk.get("dev_sell_usd", 0),
        "dev_netflow_usd": dev_risk.get("dev_netflow_usd", 0),
        "dev_hold_rate": dev_risk.get("dev_hold_rate", 0),
        "dev_hold_value_usd": dev_risk.get("dev_hold_value_usd", 0),
        "dev_sell_amount_rate": dev_risk.get("dev_sell_amount_rate", 0),
        "creator_open_count": dev_risk.get("creator_open_count", 0),
        "creator_created_count": dev_risk.get("creator_created_count", 0),
        "creator_token_status": dev_risk.get("creator_token_status", ""),
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
    stats.update(top10_holders)
    stats.update(holder_tags_costs)
    stats.update(wallet_creation)
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
            scan_round = next_scan_round()
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 扫描 {chain} {interval} 筹码信号...")
            output = run_command(
                f"gmgn-cli market trending --chain {chain} --interval {interval} "
                f"--limit 100 {trend_platform_args()} --raw"
            )
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
                    existing_candidate = get_candidate_snapshot(addr)
                    trend_price = first_float(t, keys=("price",))
                    trend_holder_count = first_float(
                        t,
                        keys=("holder_count", "holders_count", "holder_num", "holders", "holder"),
                    )
                    price_observation = update_price_observation(
                        addr,
                        trend_price,
                        scan_round,
                        symbol=t.get("symbol") or t.get("name"),
                        holder_count=trend_holder_count,
                    )
                    if not price_observation["ready"]:
                        print(
                            f"  [观察] {token_observation_label(addr, t.get('symbol'))} "
                            f"{price_observation['count']}/{MIN_PRICE_OBSERVATION_SCANS} "
                            f"price={trend_price:.12f} holders={int(trend_holder_count)}"
                        )
                        continue
                    if not price_observation["allowed"]:
                        print(
                            f"  [跳过] 三次价格观察失败 {token_observation_label(addr, t.get('symbol'))}: "
                            f"上次={price_observation.get('previous_price', 0):.12f}, 现价={price_observation['current_price']:.12f}, "
                            f"跌幅={price_observation['drop_pct']:.1%}>{MAX_PRICE_DROP_PCT:.0%}"
                        )
                        continue
                    s = perform_deep_analysis(chain, addr, t)
                    if not s: continue
                    s["price_observation_count"] = price_observation["count"]
                    s["price_observation_change_pct"] = price_observation["change_pct"] * 100
                    s["price_observation_drop_pct"] = price_observation["drop_pct"] * 100
                    s["price_observation_reason"] = price_observation["reason"]
                    s["price_observation_change_band_text"] = price_observation.get("change_band_text", "N/A")

                    mcap_ok, mcap_observation_reason = mcap_price_observation_pass(s["mcap"], price_observation)
                    if not mcap_ok:
                        print(f"  [跳过] 市值分层价格观察不通过 ${s['symbol']} {addr}: {mcap_observation_reason}")
                        continue
                    s["mcap_observation_reason"] = mcap_observation_reason
                    if s["mcap"] > MAX_MCAP_USD:
                        continue
                    age_seconds = token_age_seconds(s.get("created_at"))
                    if age_seconds is not None and age_seconds > MAX_TOKEN_AGE_SEC:
                        continue
                    if s["fee_sol"] < MIN_FEE_SOL:
                        print(f"  [跳过] 手续费过低 ${s['symbol']} {addr}: {s['fee_sol']:.2f} SOL<{MIN_FEE_SOL:.2f} SOL")
                        continue
                    if s["trade_volume_usd"] >= HIGH_VOLUME_USD_THRESHOLD and s["fee_sol"] < MIN_HIGH_VOLUME_FEE_SOL:
                        print(
                            f"  [跳过] 高交易量低手续费 ${s['symbol']} {addr}: "
                            f"volume=${s['trade_volume_usd']:,.0f}>={HIGH_VOLUME_USD_THRESHOLD:,.0f}, "
                            f"fee={s['fee_sol']:.2f} SOL<{MIN_HIGH_VOLUME_FEE_SOL:.2f} SOL"
                        )
                        continue
                    if s["is_dumping"]:
                        continue
                    if existing_candidate:
                        if not price_observation.get("continuous_up"):
                            prices_text = " -> ".join(f"{safe_float(price):.12g}" for price in price_observation.get("prices", []))
                            print(
                                f"  [观察跳过] 已推送代币复推价格未连续上涨 ${t.get('symbol') or 'UNKNOWN'} {addr}: "
                                f"{prices_text}"
                            )
                            continue
                        previous_price_history = [
                            safe_float(value)
                            for value in (existing_candidate.get("price_alert_history") or [])
                            if safe_float(value) > 0
                        ]
                        previous_price = previous_price_history[-1] if previous_price_history else safe_float(existing_candidate.get("price"))
                        current_price = safe_float(s.get("price") or price_observation.get("current_price"))
                        local_low_price = safe_float(price_observation.get("local_low_price"))
                        rebound_from_low_pct = float(price_observation.get("rebound_from_low_pct") or 0)
                        drawdown_from_alert_pct = (
                            (previous_price - local_low_price) / previous_price
                            if previous_price > 0 and local_low_price > 0 and local_low_price < previous_price
                            else 0.0
                        )
                        breakout_repeat = (
                            previous_price > 0
                            and current_price > previous_price
                            and float(price_observation.get("change_pct") or 0) >= MIN_REPEAT_PRICE_UP_PCT
                        )
                        rebound_repeat = (
                            previous_price > 0
                            and current_price <= previous_price
                            and drawdown_from_alert_pct >= MIN_REBOUND_DRAWDOWN_PCT
                            and rebound_from_low_pct >= MIN_REBOUND_FROM_LOW_PCT
                        )
                        if not breakout_repeat and not rebound_repeat:
                            print(
                                f"  [观察跳过] 已推送代币复推条件不足 ${s['symbol']} {addr}: "
                                f"current={current_price:.12g}, previous_alert={previous_price:.12g}, "
                                f"last_change={float(price_observation.get('change_pct') or 0):.1%}, "
                                f"drawdown={drawdown_from_alert_pct:.1%}, rebound={rebound_from_low_pct:.1%}"
                            )
                            continue
                        previous_holders = int(existing_candidate.get("holder_count") or 0)
                        observation_holder_delta = int(price_observation.get("holder_count_delta") or 0)
                        db_holder_delta = int(s["holder_count"]) - previous_holders
                        if observation_holder_delta <= 0:
                            print(
                                f"  [观察跳过] 已推送代币三次观察持有人未上升 ${s['symbol']} {addr}: "
                                f"{price_observation.get('current_holder_count', 0)} <= {price_observation.get('first_holder_count', 0)} "
                                f"(上次推送库内持有人={previous_holders}, 当前深度查询={s['holder_count']})"
                            )
                            continue
                        s["repeat_alert"] = True
                        s["previous_holder_count"] = previous_holders
                        s["holder_count_delta"] = observation_holder_delta
                        s["db_holder_count_delta"] = db_holder_delta
                        s["observation_first_holder_count"] = price_observation.get("first_holder_count", 0)
                        s["observation_current_holder_count"] = price_observation.get("current_holder_count", 0)
                        s["previous_alert_count"] = existing_candidate.get("alert_count", 0)
                        s["repeat_alert_type"] = "突破复推" if breakout_repeat else "回撤反弹复推"
                        s["rebound_from_low_pct"] = rebound_from_low_pct * 100
                        s["drawdown_from_alert_pct"] = drawdown_from_alert_pct * 100
                        s["local_low_price"] = local_low_price
                    else:
                        s["repeat_alert"] = False
                        s["previous_holder_count"] = 0
                        s["holder_count_delta"] = 0
                        s["db_holder_count_delta"] = 0
                        s["repeat_alert_type"] = "首推"
                        s["rebound_from_low_pct"] = 0
                        s["drawdown_from_alert_pct"] = 0
                        s["local_low_price"] = 0
                    previous_mcap_history = []
                    if existing_candidate:
                        previous_mcap_history = [
                            safe_float(value)
                            for value in (existing_candidate.get("mcap_alert_history") or [])
                            if safe_float(value) > 0
                        ]
                        if not previous_mcap_history and existing_candidate.get("mcap"):
                            previous_mcap_history = [safe_float(existing_candidate.get("mcap"))]
                    s["mcap_alert_history"] = [*previous_mcap_history, safe_float(s["mcap"])]
                    s["mcap_alert_history_text"] = format_mcap_history(s["mcap_alert_history"])
                    previous_price_history = []
                    if existing_candidate:
                        previous_price_history = [
                            safe_float(value)
                            for value in (existing_candidate.get("price_alert_history") or [])
                            if safe_float(value) > 0
                        ]
                        if not previous_price_history and existing_candidate.get("price"):
                            previous_price_history = [safe_float(existing_candidate.get("price"))]
                    s["price_alert_history"] = [*previous_price_history, safe_float(s.get("price"))]
                    s["alert_sequence_no"] = int((existing_candidate or {}).get("alert_count") or 0) + 1

                    
                    # 警报逻辑：硬过滤后，用可买分数聚合早期信号。
                    is_candidate = s["buy_score"] >= MIN_BUY_SCORE
                    if is_candidate:
                        print(
                            f"  [候选] ${s['symbol']} | CA={addr} | "
                            f"市值=${s['mcap']/1000:.1f}K | 持有人={s['holder_count']} | "
                            f"手续费={s['fee_sol']:.2f} SOL | 池={s['pool_label']} | 创建={s['created_time']} | "
                            f"结构={s['market_structure']} | "
                            f"状态={s['verdict']} | 关联持仓={s['associated_supply']:.2f}% | "
                            f"同源={s['source_cluster_size']}个/{s['source_cluster_supply']:.2f}% | "
                            f"卖出进度={s['dump_progress']:.2f}% | "
                            f"前排净流={s['front_holder_netflow']:.0f}U | Top100净流={s['holder_flow_netflow']:.0f}U | "
                            f"观察={s['price_observation_count']}次/{s['price_observation_change_pct']:+.1f}% | "
                            f"5m买/卖={s['buys_5m']}/{s['sells_5m']} | "
                        )

                        alert_icon = "🟡" if s["control_ratio"] > 50 else "🟢"
                        alert_title = "筹码关联性追踪报警" if s.get("repeat_alert") else "筹码关联性报警"
                        repeat_line = (
                            f"复推次数: 第{s.get('alert_sequence_no', 1)}次 | 市值路径: {s.get('mcap_alert_history_text', 'N/A')}\n"
                            f"持有人变化: +{s['holder_count_delta']} "
                            f"({s.get('observation_first_holder_count', 0)} -> {s.get('observation_current_holder_count', 0)}) | "
                            f"库内对比 {s['db_holder_count_delta']:+d}\n"
                            if s.get("repeat_alert")
                            else ""
                        )
                        if s.get("repeat_alert"):
                            repeat_line += f"复推类型: {s.get('repeat_alert_type', '复推')}"
                            if s.get("repeat_alert_type") == "回撤反弹复推":
                                repeat_line += (
                                    f" | 回撤{s.get('drawdown_from_alert_pct', 0):.1f}%"
                                    f" | 低点反弹{s.get('rebound_from_low_pct', 0):.1f}%"
                                )
                            repeat_line += "\n"
                        price_archive = load_price_observation_archive(addr)
                        current_price_archive_entry = observation_archive_entry(s, price_observation)
                        s["price_observation_archive_text"] = format_price_observation_archive(
                            price_archive,
                            current_entry=current_price_archive_entry,
                        )
                        
                        # Build Smart Money / KOL holding details from tag stats
                        sm_stats = s.get('holder_tag_stats', {}).get('smart_degen', {})
                        kol_stats = s.get('holder_tag_stats', {}).get('renowned', {})
                        sm_detail = (
                            f"聪明钱{sm_stats['count']}个 持仓{sm_stats['supply']:.1f}%/${sm_stats['position_value']:,.0f} "
                            f"盈利{sm_stats.get('profit_pct', 0):+.1f}% 卖出进度{sm_stats.get('sell_progress', 0):.1f}%"
                        ) if sm_stats.get('count', 0) > 0 else "聪明钱0个"
                        kol_detail = (
                            f"KOL{kol_stats['count']}个 持仓{kol_stats['supply']:.1f}%/${kol_stats['position_value']:,.0f} "
                            f"盈利{kol_stats.get('profit_pct', 0):+.1f}% 卖出进度{kol_stats.get('sell_progress', 0):.1f}%"
                        ) if kol_stats.get('count', 0) > 0 else "KOL0个"

                        dev_address = short_addr(s.get("creator_address")) if s.get("creator_address") else "未知"
                        dev_detail = (
                            f"- Dev: {dev_address} | 持仓{s.get('dev_hold_rate', 0) * 100:.2f}% "
                            f"{format_usd_short(s.get('dev_hold_value_usd'))} | "
                            f"买{format_usd_short(s.get('dev_buy_usd'))} 卖{format_usd_short(s.get('dev_sell_usd'))} "
                            f"净{format_usd_short(s.get('dev_netflow_usd'))} | "
                            f"已卖{s.get('dev_sell_amount_rate', 0) * 100:.1f}%"
                        )
                        narrative_line = f"叙事: {s['narrative']}\n" if s.get("narrative") else ""

                        msg = (
                            f"{alert_icon} *{alert_title}* | ${s['symbol']}\n"
                            f"市值: ${s['mcap']/1000:.1f}K | 持有人: {s['holder_count']} | 手续费: {s['fee_sol']:.2f} SOL\n"
                            f"价格变化: {s['price_observation_change_pct']:+.1f}% | 波段 {s.get('price_observation_change_band_text', 'N/A')} | 回撤 {s['price_observation_drop_pct']:.1f}%\n"
                            f"{s.get('price_observation_archive_text', '')}"
                            f"{repeat_line}"
                            f"{narrative_line}"
                            f"流动性池: {s['pool_label']}\n"
                            f"创建时间: {s['created_time']} | 类型: {s['token_age_type']} | 状态: {s['verdict']}\n\n"
                            f"🏷️ *标签钱包分析*\n"
                            f"{sm_detail}\n"
                            f"{kol_detail}\n"
                            f"{s['holder_tag_desc']}\n\n"
                            f"📊 *基础结构*\n"
                            f"{s['rank_bucket_desc']}\n"
                            f"\n"
                            f"*Dev数据*\n"
                            f"{dev_detail}\n\n"
                            f"CA: `{addr}`\n"
                            f"[在 GMGN 查看关联图谱](https://gmgn.ai/{chain}/token/{addr})"
                        )
                        tg_message_id = upsert_tg_alert(
                            addr,
                            msg,
                            allow_repeat=s.get("repeat_alert", False),
                            existing_candidate=existing_candidate,
                        )
                        save_alpha_candidate(chain, interval, addr, s, tg_message_id=tg_message_id)
                        save_price_observation_archive(addr, [*price_archive, current_price_archive_entry])
                        reset_price_observation(addr)
                    
            except Exception as e:
                print(f"Loop Error: {e}")
            time.sleep(2)

if __name__ == "__main__":
    print("深度关联分析机器人已启动...")
    while True:
        scan_pro()
        if CHECK_INTERVAL > 0:
            time.sleep(CHECK_INTERVAL)
