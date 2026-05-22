import json
import os
import re
import time
from datetime import datetime
from pathlib import Path

import requests

from config import GMGN_API_KEY, TG_BOT_TOKEN, TG_CHAT_ID
from tg_alert_stream import publish_tg_alert
from deep_alpha.deep_alpha_pro import (
    CHAINS,
    format_chain_price,
    format_pnl_pct,
    perform_deep_analysis,
)
from bottom_detection import bottom_accumulation_monitor as bottom_monitor


POLL_TIMEOUT = 25
POLL_INTERVAL = 1
REQUEST_TIMEOUT = 30
TG_MESSAGE_LIMIT = 3900
ADDRESS_RE = re.compile(r"\b[1-9A-HJ-NP-Za-km-z]{32,50}\b")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-v4-pro")
DEEPSEEK_TIMEOUT = int(os.getenv("DEEPSEEK_TIMEOUT", "45"))
DEEPSEEK_MAX_HISTORY = int(os.getenv("DEEPSEEK_MAX_HISTORY", "100"))
DEEPSEEK_KLINE_CANDLES_PER_SNAPSHOT = int(os.getenv("DEEPSEEK_KLINE_CANDLES_PER_SNAPSHOT", "12"))
PHASE_UP_PCT = float(os.getenv("DEEPSEEK_PHASE_UP_PCT", "0.12"))
PHASE_DOWN_PCT = float(os.getenv("DEEPSEEK_PHASE_DOWN_PCT", "-0.12"))
CURRENT_NODE_MIN_AGE_SEC = int(os.getenv("TG_CA_CURRENT_NODE_MIN_AGE_SEC", "900"))
DEEPSEEK_ENABLED = os.getenv("DEEPSEEK_ENABLED", "1") != "0"
DEEPSEEK_THINKING = os.getenv("DEEPSEEK_THINKING", "enabled")
DEEPSEEK_REASONING_EFFORT = os.getenv("DEEPSEEK_REASONING_EFFORT", "high")
PACKAGE_WALLET_MAP_PATH = os.getenv("PACKAGE_WALLET_MAP_PATH", "gmgn_outputs/package_wallet_map.json")
PACKAGE_WALLET_MAP_CACHE = None
BUNDLE_SIMILAR_HOLD_TOLERANCE_PCT = float(os.getenv("TG_CA_BUNDLE_SIMILAR_HOLD_TOLERANCE_PCT", "0.001"))
BUNDLE_SIMILAR_TIME_WINDOW_SEC = int(os.getenv("TG_CA_BUNDLE_SIMILAR_TIME_WINDOW_SEC", "600"))
BUNDLE_SIMILAR_MIN_WALLETS = int(os.getenv("TG_CA_BUNDLE_SIMILAR_MIN_WALLETS", "3"))
WALLET_CREATION_CLUSTER_SEC = int(os.getenv("TG_CA_WALLET_CREATION_CLUSTER_SEC", str(5 * 24 * 3600)))
WALLET_CREATION_CLUSTER_MIN_WALLETS = int(os.getenv("TG_CA_WALLET_CREATION_CLUSTER_MIN_WALLETS", "2"))


def allowed_chat_ids():
    configured = os.getenv("TG_CA_QUERY_ALLOWED_CHATS") or str(TG_CHAT_ID or "")
    return {item.strip() for item in configured.split(",") if item.strip()}


def tg_api(method, payload):
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/{method}"
    resp = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
    if not resp.ok:
        if method == "sendMessage":
            publish_tg_alert(
                str(payload.get("text") or ""),
                "tg_ca_chip",
                status=f"failed_http_{resp.status_code}",
                chat_id=payload.get("chat_id"),
            )
        raise RuntimeError(f"Telegram {method} failed: http={resp.status_code} {resp.text[:300]}")
    data = resp.json()
    if not data.get("ok"):
        if method == "sendMessage":
            publish_tg_alert(
                str(payload.get("text") or ""),
                "tg_ca_chip",
                status="failed_api",
                chat_id=payload.get("chat_id"),
                extra=data,
            )
        raise RuntimeError(f"Telegram {method} failed: {data}")
    result = data.get("result")
    if method == "sendMessage":
        publish_tg_alert(
            str(payload.get("text") or ""),
            "tg_ca_chip",
            status="sent",
            chat_id=payload.get("chat_id"),
            message_id=(result or {}).get("message_id") if isinstance(result, dict) else None,
        )
    return result


def send_message(chat_id, text, reply_to_message_id=None):
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    if reply_to_message_id:
        payload["reply_to_message_id"] = reply_to_message_id
    return tg_api("sendMessage", payload)


def split_long_text(text, limit=TG_MESSAGE_LIMIT):
    text = str(text or "")
    if len(text) <= limit:
        return [text]
    chunks = []
    current = ""
    for line in text.splitlines(keepends=True):
        if len(line) > limit:
            if current:
                chunks.append(current.rstrip())
                current = ""
            for start in range(0, len(line), limit):
                chunks.append(line[start:start + limit].rstrip())
            continue
        if len(current) + len(line) > limit:
            chunks.append(current.rstrip())
            current = line
        else:
            current += line
    if current:
        chunks.append(current.rstrip())
    return chunks


def send_long_message(chat_id, text, reply_to_message_id=None):
    chunks = split_long_text(text)
    if len(chunks) == 1:
        send_message(chat_id, chunks[0], reply_to_message_id)
        return
    total = len(chunks)
    for index, chunk in enumerate(chunks, start=1):
        prefix = f"({index}/{total})\n"
        send_message(chat_id, prefix + chunk, reply_to_message_id if index == 1 else None)


def extract_addresses(text):
    seen = set()
    addresses = []
    for match in ADDRESS_RE.findall(text or ""):
        if match not in seen:
            seen.add(match)
            addresses.append(match)
    return addresses


def compact_money(value):
    value = float(value or 0)
    sign = "-" if value < 0 else ""
    value = abs(value)
    if value >= 1_000_000:
        return f"{sign}${value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"{sign}${value / 1_000:.1f}K"
    return f"{sign}${value:,.0f}"


def profit_pct_text(profit, buy_volume):
    buy = float(buy_volume or 0)
    pct = (float(profit or 0) / buy * 100) if buy > 0 else 0.0
    return f"{pct:+.1f}%"


def cost_text(value):
    cost = float(value or 0)
    return format_chain_price(cost) if cost > 0 else "-"


def round_float(value, digits=4):
    try:
        return round(float(value or 0), digits)
    except (TypeError, ValueError):
        return 0


def compact_holder(holder, include_wallet=True):
    item = {
        "rank": holder.get("rank"),
        "hold_pct": round_float(holder.get("hold_pct"), 6),
        "usd_value": round_float(holder.get("usd_value"), 2),
        "buy": round_float(holder.get("buy_volume"), 2),
        "sell": round_float(holder.get("sell_volume"), 2),
        "net": round_float(holder.get("netflow"), 2),
        "profit": round_float(holder.get("profit"), 2),
        "avg_cost": holder.get("avg_cost"),
        "buy_count": holder.get("buy_count"),
        "sell_count": holder.get("sell_count"),
        "tags": holder.get("tags") or [],
    }
    if include_wallet:
        item["wallet"] = holder.get("wallet")
    return item


def load_package_wallet_map():
    global PACKAGE_WALLET_MAP_CACHE
    if PACKAGE_WALLET_MAP_CACHE is not None:
        return PACKAGE_WALLET_MAP_CACHE
    path = Path(PACKAGE_WALLET_MAP_PATH)
    if not path.exists():
        PACKAGE_WALLET_MAP_CACHE = {}
        return PACKAGE_WALLET_MAP_CACHE
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"package wallet map load failed: {exc}")
        PACKAGE_WALLET_MAP_CACHE = {}
        return PACKAGE_WALLET_MAP_CACHE
    if not isinstance(data, dict):
        PACKAGE_WALLET_MAP_CACHE = {}
        return PACKAGE_WALLET_MAP_CACHE
    normalized = {}
    for address, meta in data.items():
        if not isinstance(meta, dict):
            continue
        groups = meta.get("groups") or []
        if not isinstance(groups, list):
            groups = [str(groups)]
        normalized[str(address).strip()] = {
            "name": str(meta.get("name") or ""),
            "groups": [str(group) for group in groups if str(group).strip()],
        }
    PACKAGE_WALLET_MAP_CACHE = normalized
    return PACKAGE_WALLET_MAP_CACHE


def compact_candle(candle):
    return {
        "ts": candle.get("ts"),
        "o": candle.get("open"),
        "h": candle.get("high"),
        "l": candle.get("low"),
        "c": candle.get("close"),
        "v": round_float(candle.get("volume"), 2),
        "a": round_float(candle.get("amount"), 2),
    }


def compact_kline_summary(summary):
    summary = summary or {}
    return {
        "resolution": summary.get("resolution"),
        "count": summary.get("count"),
        "from_ts": summary.get("from_ts"),
        "to_ts": summary.get("to_ts"),
        "open": summary.get("open"),
        "close": summary.get("close"),
        "change_pct": round_float(summary.get("change_pct"), 2),
        "high": summary.get("high"),
        "low": summary.get("low"),
        "volume_usd": round_float(summary.get("volume_usd"), 2),
        "last_volume_usd": round_float(summary.get("last_volume_usd"), 2),
    }


def frame_price(summary):
    summary = summary or {}
    kline = summary.get("kline") or {}
    return float(summary.get("price") or kline.get("close") or 0)


def phase_name(price_change_pct):
    if price_change_pct >= PHASE_UP_PCT:
        return "up"
    if price_change_pct <= PHASE_DOWN_PCT:
        return "down"
    return "sideways"


def phase_text(name):
    return {
        "up": "上涨",
        "down": "下跌",
        "sideways": "横盘",
    }.get(name, name or "未知")


def holder_map(holders):
    return {str(holder.get("wallet") or "").strip(): holder for holder in holders if holder.get("wallet")}


def wallet_delta(prev_holder, cur_holder):
    prev_holder = prev_holder or {}
    cur_holder = cur_holder or {}
    return {
        "wallet": cur_holder.get("wallet") or prev_holder.get("wallet"),
        "first_rank": prev_holder.get("rank", 0),
        "last_rank": cur_holder.get("rank", 0),
        "rank_delta": int(float(prev_holder.get("rank") or 0)) - int(float(cur_holder.get("rank") or 0)),
        "hold_delta": round_float(float(cur_holder.get("hold_pct") or 0) - float(prev_holder.get("hold_pct") or 0), 6),
        "buy_delta": round_float(float(cur_holder.get("buy_volume") or 0) - float(prev_holder.get("buy_volume") or 0), 2),
        "sell_delta": round_float(float(cur_holder.get("sell_volume") or 0) - float(prev_holder.get("sell_volume") or 0), 2),
        "netflow_delta": round_float(
            (float(cur_holder.get("buy_volume") or 0) - float(prev_holder.get("buy_volume") or 0))
            - (float(cur_holder.get("sell_volume") or 0) - float(prev_holder.get("sell_volume") or 0)),
            2,
        ),
        "profit_delta": round_float(float(cur_holder.get("profit") or 0) - float(prev_holder.get("profit") or 0), 2),
        "last_hold_pct": round_float(cur_holder.get("hold_pct"), 6),
        "avg_cost": cur_holder.get("avg_cost") or prev_holder.get("avg_cost"),
        "tags": cur_holder.get("tags") or prev_holder.get("tags") or [],
    }


def merge_wallet_phase_delta(bucket, delta):
    wallet = delta.get("wallet")
    if not wallet:
        return
    item = bucket.setdefault(
        wallet,
        {
            "wallet": wallet,
            "first_rank": delta.get("first_rank", 0),
            "last_rank": delta.get("last_rank", 0),
            "hold_delta": 0.0,
            "buy_delta": 0.0,
            "sell_delta": 0.0,
            "netflow_delta": 0.0,
            "profit_delta": 0.0,
            "last_hold_pct": 0.0,
            "avg_cost": delta.get("avg_cost"),
            "tags": delta.get("tags") or [],
            "steps": 0,
        },
    )
    item["last_rank"] = delta.get("last_rank", item["last_rank"])
    item["hold_delta"] += float(delta.get("hold_delta") or 0)
    item["buy_delta"] += float(delta.get("buy_delta") or 0)
    item["sell_delta"] += float(delta.get("sell_delta") or 0)
    item["netflow_delta"] += float(delta.get("netflow_delta") or 0)
    item["profit_delta"] += float(delta.get("profit_delta") or 0)
    item["last_hold_pct"] = delta.get("last_hold_pct", item["last_hold_pct"])
    item["avg_cost"] = delta.get("avg_cost") or item.get("avg_cost")
    item["tags"] = delta.get("tags") or item.get("tags") or []
    item["steps"] += 1


def top_wallets(bucket, mode, limit=8):
    items = list(bucket.values())
    for item in items:
        item["hold_delta"] = round_float(item.get("hold_delta"), 6)
        item["buy_delta"] = round_float(item.get("buy_delta"), 2)
        item["sell_delta"] = round_float(item.get("sell_delta"), 2)
        item["netflow_delta"] = round_float(item.get("netflow_delta"), 2)
        item["profit_delta"] = round_float(item.get("profit_delta"), 2)
    if mode == "buy":
        items.sort(key=lambda item: (item["hold_delta"], item["netflow_delta"], item["buy_delta"]), reverse=True)
    else:
        items.sort(key=lambda item: (item["hold_delta"], -item["sell_delta"], item["netflow_delta"]))
    return items[:limit]


def analyze_phase_wallet_flows(current_summary, current_holders, history):
    frames = []
    for snap in reversed(history or []):
        summary = snap.get("summary") or {}
        frames.append(
            {
                "ts": snap.get("snapshot_ts"),
                "summary": summary,
                "holders": snap.get("holders") or [],
                "price": frame_price(summary),
                "top10_pct": float(summary.get("top10_pct") or 0),
                "top20_pct": float(summary.get("top20_pct") or 0),
                "top50_pct": float(summary.get("top50_pct") or 0),
                "top100_pct": float(summary.get("top100_pct") or 0),
            }
        )
    frames.append(
        {
            "ts": int(time.time()),
            "summary": current_summary or {},
            "holders": current_holders,
            "price": frame_price(current_summary or {}),
            "top10_pct": float((current_summary or {}).get("top10_pct") or 0),
            "top20_pct": float((current_summary or {}).get("top20_pct") or 0),
            "top50_pct": float((current_summary or {}).get("top50_pct") or 0),
            "top100_pct": float((current_summary or {}).get("top100_pct") or 0),
        }
    )
    frames = [frame for frame in frames if frame["holders"]]
    current_node = {}
    if len(frames) >= 2:
        cur = frames[-1]
        cur_ts = int(cur.get("ts") or time.time())
        prev_candidates = [
            frame for frame in frames[:-1]
            if int(frame.get("ts") or 0) > 0 and cur_ts - int(frame.get("ts") or 0) >= CURRENT_NODE_MIN_AGE_SEC
        ]
        prev = prev_candidates[-1] if prev_candidates else {}
    if len(frames) >= 2 and prev:
        prev_price = float(prev.get("price") or 0)
        cur_price = float(cur.get("price") or 0)
        change_pct = ((cur_price - prev_price) / prev_price) if prev_price > 0 and cur_price > 0 else 0.0
        phase = phase_name(change_pct)
        prev_map = holder_map(prev["holders"])
        cur_map = holder_map(cur["holders"])
        wallet_bucket = {}
        for wallet in set(prev_map) | set(cur_map):
            merge_wallet_phase_delta(wallet_bucket, wallet_delta(prev_map.get(wallet), cur_map.get(wallet)))
        current_node = {
            "phase": phase,
            "phase_text": phase_text(phase),
            "from_ts": prev.get("ts") or 0,
            "to_ts": cur.get("ts") or 0,
            "price_from": prev_price,
            "price_to": cur_price,
            "price_change_pct": round_float(change_pct * 100, 2),
            "top100_hold_delta": round_float(float(cur.get("top100_pct") or 0) - float(prev.get("top100_pct") or 0), 6),
            "top20_hold_delta": round_float(float(cur.get("top20_pct") or 0) - float(prev.get("top20_pct") or 0), 6),
            "buyers_or_accumulators": top_wallets(wallet_bucket, "buy", limit=8),
            "sellers_or_distributors": top_wallets(wallet_bucket, "sell", limit=8),
        }
    phases = {
        "up": {"intervals": 0, "wallets": {}, "top100_delta": 0.0, "netflow": 0.0, "price_changes": [], "first_ts": 0, "last_ts": 0},
        "down": {"intervals": 0, "wallets": {}, "top100_delta": 0.0, "netflow": 0.0, "price_changes": [], "first_ts": 0, "last_ts": 0},
        "sideways": {"intervals": 0, "wallets": {}, "top100_delta": 0.0, "netflow": 0.0, "price_changes": [], "first_ts": 0, "last_ts": 0},
    }
    for prev, cur in zip(frames, frames[1:]):
        prev_price = float(prev.get("price") or 0)
        cur_price = float(cur.get("price") or 0)
        if prev_price <= 0 or cur_price <= 0:
            change_pct = 0.0
        else:
            change_pct = (cur_price - prev_price) / prev_price
        phase = phase_name(change_pct)
        phase_bucket = phases[phase]
        phase_bucket["intervals"] += 1
        phase_bucket["first_ts"] = phase_bucket["first_ts"] or prev.get("ts") or 0
        phase_bucket["last_ts"] = cur.get("ts") or phase_bucket["last_ts"]
        phase_bucket["top100_delta"] += float(cur.get("top100_pct") or 0) - float(prev.get("top100_pct") or 0)
        phase_bucket["price_changes"].append(change_pct)
        prev_map = holder_map(prev["holders"])
        cur_map = holder_map(cur["holders"])
        for wallet in set(prev_map) | set(cur_map):
            delta = wallet_delta(prev_map.get(wallet), cur_map.get(wallet))
            phase_bucket["netflow"] += delta["netflow_delta"]
            merge_wallet_phase_delta(phase_bucket["wallets"], delta)

    def phase_summary(name, bucket):
        changes = bucket["price_changes"]
        avg_change = sum(changes) / len(changes) if changes else 0.0
        wallets = bucket["wallets"]
        return {
            "phase": name,
            "intervals": bucket["intervals"],
            "avg_price_change_pct": round_float(avg_change * 100, 2),
            "first_ts": bucket.get("first_ts") or 0,
            "last_ts": bucket.get("last_ts") or 0,
            "top100_hold_delta": round_float(bucket["top100_delta"], 6),
            "netflow_delta": round_float(bucket["netflow"], 2),
            "buyers_or_accumulators": top_wallets(wallets, "buy", limit=8),
            "sellers_or_distributors": top_wallets(wallets, "sell", limit=8),
        }

    prices = [frame["price"] for frame in frames if frame["price"] > 0]
    first = frames[0] if frames else {}
    last = frames[-1] if frames else {}
    return {
        "frame_count": len(frames),
        "price_change_total_pct": round_float(((prices[-1] - prices[0]) / prices[0] * 100) if len(prices) >= 2 else 0, 2),
        "price_high": max(prices) if prices else 0,
        "price_low": min(prices) if prices else 0,
        "current_price": prices[-1] if prices else 0,
        "top100_hold_change": {
            "first": round_float(first.get("top100_pct"), 6),
            "last": round_float(last.get("top100_pct"), 6),
            "delta": round_float(float(last.get("top100_pct") or 0) - float(first.get("top100_pct") or 0), 6),
        },
        "top20_hold_change": {
            "first": round_float(first.get("top20_pct"), 6),
            "last": round_float(last.get("top20_pct"), 6),
            "delta": round_float(float(last.get("top20_pct") or 0) - float(first.get("top20_pct") or 0), 6),
        },
        "current_node": current_node,
        "phases": {
            "up": phase_summary("up", phases["up"]),
            "down": phase_summary("down", phases["down"]),
            "sideways": phase_summary("sideways", phases["sideways"]),
        },
    }


def clean_holder_tag_desc(desc):
    desc = str(desc or "未发现重点标签钱包")
    replacements = {
        "same_batch#": "同批创建簇#",
        " wallets/": "个/",
        " wallet/": "个/",
        "hold $": "持仓$",
        " buy $": " 买$",
        " sell $": " 卖$",
        " net $": " 净$",
        " avg ": " 均",
        " med ": " 中",
        "smart ": "聪明钱 ",
        "fresh ": "新钱包 ",
        "bundler ": "捆绑 ",
        "sniper ": "狙击手 ",
        "rat ": "老鼠仓 ",
        "bot ": "交易机器人 ",
        "bluechip ": "蓝筹持有人 ",
    }
    for old, new in replacements.items():
        desc = desc.replace(old, new)
    cleaned = []
    for line in desc.splitlines():
        line = re.sub(r"\s+买(?:\$?[-+\d,.KMBkmb万亿]+)?", "", line)
        line = re.sub(r"\s+卖(?:\$?[-+\d,.KMBkmb万亿]+)?", "", line)
        line = re.sub(r"\s+净\$?[-+\d,.KMBkmb万亿]+", "", line)
        line = re.sub(r"\s+均[^\s|，。]+", "", line)
        line = re.sub(r"\s+中[^\s|，。]+", "", line)
        cleaned.append(line.strip())
    return "\n".join(line for line in cleaned if line)


def raw_wallet_address(row):
    return str(row.get("address") or row.get("wallet_address") or row.get("account_address") or "").strip()


# ---------------------------------------------------------------------------
# K-line pattern & volume analysis (golden dog patterns)
# ---------------------------------------------------------------------------

KLINE_PATTERN_LOOKBACK_DAYS = 90
KLINE_NEW_TOKEN_AGE_SEC = 3 * 24 * 3600
KLINE_MID_TOKEN_AGE_SEC = 14 * 24 * 3600


def pick_kline_resolution(age_seconds):
    if age_seconds <= 0:
        return "1h"
    if age_seconds <= KLINE_NEW_TOKEN_AGE_SEC:
        return "1h"
    if age_seconds <= KLINE_MID_TOKEN_AGE_SEC:
        return "4h"
    return "1d"


def fetch_kline_for_pattern(address, age_seconds):
    resolution = pick_kline_resolution(age_seconds)
    end = int(time.time())
    start = end - KLINE_PATTERN_LOOKBACK_DAYS * 24 * 3600
    data = bottom_monitor.run_gmgn(
        [
            "market", "kline", "--chain", "sol", "--address", address,
            "--resolution", resolution,
            "--from", str(start), "--to", str(end),
        ],
        timeout=90,
    )
    if not data:
        return None, resolution, []
    rows = data.get("list") or data.get("data", {}).get("list") or []
    candles = []
    for row in (rows if isinstance(rows, list) else []):
        if not isinstance(row, dict):
            continue
        raw_ts = int(float(str(row.get("time") or row.get("timestamp") or row.get("t") or 0)))
        ts = raw_ts // 1000 if raw_ts > 10_000_000_000 else raw_ts
        close = float(row.get("close") or row.get("c") or 0)
        if ts <= 0 or close <= 0:
            continue
        candles.append({
            "ts": ts,
            "open": float(row.get("open") or row.get("o") or close),
            "high": float(row.get("high") or row.get("h") or close),
            "low": float(row.get("low") or row.get("l") or close),
            "close": close,
            "volume": float(row.get("volume") or row.get("v") or 0),
        })
    candles.sort(key=lambda c: c["ts"])
    return candles, resolution, rows


def analyze_kline_volume_pattern(address, token_info):
    """Analyze K-line for golden dog patterns: N-type, flash_pump, etc."""
    age_seconds = max(0, int(time.time()) - int(float(str(token_info.get("created_at") or 0))))
    candles, resolution, _ = fetch_kline_for_pattern(address, age_seconds)

    if not candles or len(candles) < 6:
        return {"error": f"K线数据不足 ({len(candles)}根{resolution}蜡烛)", "resolution": resolution}

    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    volumes = [c["volume"] for c in candles]

    first = closes[0]
    peak = max(highs)
    peak_i = highs.index(peak)
    low = min(lows)
    low_i = lows.index(low)
    cur = closes[-1]
    avg_vol = sum(volumes) / len(volumes) if volumes else 1
    max_vol = max(volumes) if volumes else 0
    max_vol_i = volumes.index(max_vol) if max_vol > 0 else 0
    vol_at_peak = volumes[peak_i] if peak_i < len(volumes) else 0

    # Classify pattern type
    is_n_type = low_i < peak_i and first > 0 and low > 0 and first / low >= 4
    gain_from_low = (peak - low) / low * 100 if low > 0 else 0
    total_gain = (peak - first) / first * 100 if first > 0 else 0
    drawdown = (peak - cur) / peak * 100 if peak > 0 else 0
    max_drawdown = (peak - min(lows[peak_i:])) / peak * 100 if peak_i < len(lows) and peak > 0 else 0
    volume_ratio = max_vol / avg_vol if avg_vol > 0 else 0
    late_vol = sum(volumes[-max(1, len(volumes)//4):]) / max(1, len(volumes)//4)
    vol_trend = (late_vol - avg_vol) / avg_vol if avg_vol > 0 else 0

    # Breakout bars
    hour_changes = []
    for i in range(1, len(closes)):
        chg = (closes[i] - closes[i-1]) / closes[i-1] * 100 if closes[i-1] > 0 else 0
        hour_changes.append(chg)
    breakout_bars = sum(1 for chg in hour_changes if chg >= 30)
    green_ratio = sum(1 for c in candles if c["close"] > c["open"]) / len(candles)

    # Pattern classification
    if is_n_type:
        first_drop = (first - low) / first * 100
        if gain_from_low > 5000:
            pattern = "N型反转(超级)"
        elif gain_from_low > 500:
            pattern = "N型反转"
        else:
            pattern = "N型弱反转"
        pattern_desc = f"第1波顶→砸盘{first_drop:.0f}%→CTO拉{gain_from_low:,.0f}%"
    elif total_gain > 500 and breakout_bars >= 3 and max_vol_i >= peak_i - 2 and max_vol_i <= peak_i + 2:
        pattern = "classic_pump_dump"
        pattern_desc = f"积累→突破{breakout_bars}根放量阳线→顶点(量比{volume_ratio:.1f}x)→派发"
    elif total_gain > 200 and breakout_bars >= 3:
        pattern = "flash_pump" if peak_i <= 6 else "volatile_climb"
        pattern_desc = f"{peak_i}根{resolution}内急拉{total_gain:+.0f}%, {breakout_bars}根突破阳线"
    elif max_drawdown > 70 and total_gain < 50:
        pattern = "rug_pull"
        pattern_desc = f"峰值后回撤{max_drawdown:.0f}%, 已归零"
    elif abs(total_gain) < 30:
        pattern = "sideways_chop"
        pattern_desc = f"横盘{total_gain:+.0f}%, 无明显方向"
    else:
        pattern = "complex"
        pattern_desc = f"混合走势, 涨幅{total_gain:+.0f}%, 回撤{drawdown:.0f}%"

    return {
        "resolution": resolution,
        "candle_count": len(candles),
        "token_age_days": round(age_seconds / 86400, 1),
        "first_price": first,
        "lowest_price": low,
        "peak_price": peak,
        "current_price": cur,
        "total_gain_pct": round(total_gain, 1),
        "gain_from_low_pct": round(gain_from_low, 1) if is_n_type else None,
        "drawdown_pct": round(drawdown, 1),
        "max_drawdown_pct": round(max_drawdown, 1),
        "volume_ratio": round(volume_ratio, 1),
        "vol_trend": round(vol_trend, 2),
        "green_ratio": round(green_ratio, 2),
        "breakout_bars": breakout_bars,
        "peak_bar_index": peak_i,
        "low_bar_index": low_i,
        "max_vol_bar_index": max_vol_i,
        "is_n_type": is_n_type,
        "pattern": pattern,
        "pattern_desc": pattern_desc,
        "_candles": candles,  # pass to volume divergence analysis
    }


def analyze_volume_divergence(kline_result):
    """Analyze volume-price relationship from K-line candles.
    Returns structured stats for TG message integration."""
    candles = kline_result.get("_candles") or []
    if len(candles) < 3:
        return {"error": "candles insufficient", "text": ""}

    closes = [c["close"] for c in candles]
    volumes = [c["volume"] for c in candles]
    avg_vol = sum(volumes) / len(volumes) if volumes else 1

    events = {"absorption": 0, "exhaustion": 0, "climax_up": 0, "climax_down": 0, "equal_down": 0}
    climax_details = []

    for i in range(1, len(candles)):
        prev, cur = candles[i - 1], candles[i]
        price_chg = (cur["close"] - prev["close"]) / prev["close"] * 100
        vol_chg = (cur["volume"] - prev["volume"]) / prev["volume"] * 100 if prev["volume"] > 0 else 0
        vol_ratio = cur["volume"] / avg_vol if avg_vol > 0 else 1

        # 缩量下跌: vol down >30% + price down
        if vol_chg <= -30 and price_chg < 0 and abs(price_chg) > 0.5:
            events["exhaustion"] += 1

        # 放量滞跌: vol up >50% + price down <5% (absorption)
        if vol_chg >= 50 and -5 <= price_chg < 0:
            events["absorption"] += 1

        # 量能顶点
        if vol_ratio >= 4.0:
            if price_chg > 5:
                events["climax_up"] += 1
                climax_details.append(f"第{i}根放量拉升 +{price_chg:.0f}% (量比{vol_ratio:.1f}x)")
            elif price_chg < -5:
                events["climax_down"] += 1
                climax_details.append(f"第{i}根放量砸盘 {price_chg:.0f}% (量比{vol_ratio:.1f}x)")

        # 等量等幅下跌
        if i >= 2:
            prev2 = candles[i - 2]
            prev_price_chg_val = (prev["close"] - prev2["close"]) / prev2["close"] * 100
            vol_similar = abs(vol_chg) <= 25
            price_similar = abs(price_chg - prev_price_chg_val) <= 3
            if vol_similar and price_similar and price_chg < 0 and prev_price_chg_val < 0:
                events["equal_down"] += 1

    # Volume asymmetry
    up_vols = [candles[i]["volume"] for i in range(1, len(candles)) if candles[i]["close"] > candles[i-1]["close"]]
    down_vols = [candles[i]["volume"] for i in range(1, len(candles)) if candles[i]["close"] < candles[i-1]["close"]]
    avg_up = sum(up_vols) / len(up_vols) if up_vols else 0
    avg_down = sum(down_vols) / len(down_vols) if down_vols else 0
    asymmetry = (avg_up - avg_down) / avg_down if avg_down > 0 else 0

    # Absorption score
    score = events["absorption"] * 3 + events["exhaustion"] * 2
    if asymmetry > 0.3:
        score += 2
    elif asymmetry < -0.3:
        score -= 2

    if score >= 6:
        phase = "底部吸筹 — 多次放量滞跌+缩量下跌衰竭，多头量能主导"
    elif score >= 3:
        phase = "疑似底部积累 — 有吸筹信号但未完全确认"
    elif events["climax_down"] >= 2 and asymmetry < -0.3:
        phase = "派发砸盘 — 多次放量下跌+空头量能主导"
    elif events["climax_up"] >= 1 and score >= 3:
        phase = "拉升后换手洗盘 — 量能顶点后出现吸筹信号"
    elif events["climax_up"] >= 1:
        phase = "突破拉升 — 放量突破但需关注后续能否持续"
    else:
        phase = "量价信号不明确 — 缩量横盘或死币状态"

    asymmetry_desc = (
        "多头主导(上涨放量>下跌)" if asymmetry > 0.3
        else "空头主导(下跌放量>上涨)" if asymmetry < -0.3
        else "多空均衡"
    )

    text = (
        f"量价关系分析\n"
        f"- 量能不对称: {asymmetry_desc} (上涨均量{avg_up:.0f} vs 下跌均量{avg_down:.0f}, 不对称度{asymmetry:+.2f})\n"
        f"- 缩量下跌衰竭: {events['exhaustion']}次 | 放量滞跌吸筹: {events['absorption']}次 | 等量等幅下跌: {events['equal_down']}次\n"
        f"- 放量拉升: {events['climax_up']}次 | 放量砸盘: {events['climax_down']}次\n"
        f"- 吸筹评分: {score}分 → {phase}\n"
    )
    if climax_details:
        text += f"- 关键量能事件: {' | '.join(climax_details[-4:])}\n"

    return {"error": None, "text": text, "score": score, "phase": phase, "asymmetry": round(asymmetry, 2)}


def kline_volume_analysis_text(kline_result):
    if not kline_result:
        return ""
    if kline_result.get("error"):
        return f"K线量价分析\n- {kline_result['error']}\n"

    r = kline_result
    vol_status = "放量" if r["vol_trend"] > 0.3 else ("缩量" if r["vol_trend"] < -0.3 else "量能平稳")
    vol_climax_info = (
        f"最大量柱在第{r['max_vol_bar_index']}根(量比{r['volume_ratio']:.1f}x)"
        if r["volume_ratio"] > 2 else "无量能异常"
    )

    n_type_extra = ""
    if r["is_n_type"]:
        n_type_extra = (
            f"\n- N型细节: 起点价{r['first_price']:.10f} → 砸盘底{r['lowest_price']:.10f} "
            f"→ CTO新高{r['peak_price']:.10f} (底部涨幅{r['gain_from_low_pct']:,.0f}%)"
        )

    # Volume-price divergence analysis
    vol_div = analyze_volume_divergence(kline_result)
    vol_div_text = vol_div.get("text", "") if vol_div and not vol_div.get("error") else ""

    return (
        f"K线量价分析 (周期: {r['resolution']}, 代币年龄: {r['token_age_days']}天)\n"
        f"- 走势分类: {r['pattern']}\n"
        f"- 走势描述: {r['pattern_desc']}\n"
        f"- 蜡烛数: {r['candle_count']}根 | 阳线比: {r['green_ratio']:.0%} | 突破阳线: {r['breakout_bars']}根\n"
        f"- 起点价: {r['first_price']:.10f} → 最低: {r['lowest_price']:.10f} → 最高: {r['peak_price']:.10f} → 现价: {r['current_price']:.10f}\n"
        f"- 总涨幅: {r['total_gain_pct']:+.1f}% | 峰值回撤: {r['drawdown_pct']:.1f}% (最大: {r['max_drawdown_pct']:.1f}%)\n"
        f"- 量能: 均量比{r['volume_ratio']:.1f}x | {vol_climax_info} | 趋势: {vol_status} ({r['vol_trend']:+.2f})"
        f"{n_type_extra}\n\n"
        f"{vol_div_text}"
    )


def short_addr(value):
    value = str(value or "")
    return f"{value[:6]}...{value[-4:]}" if len(value) > 12 else value


def raw_tags(row):
    tags = []
    for key in ("maker_token_tags", "tags"):
        value = row.get(key)
        if isinstance(value, list):
            tags.extend(str(item) for item in value if item)
        elif value:
            tags.append(str(value))
    tag_v2 = row.get("wallet_tag_v2")
    if tag_v2:
        tags.append(str(tag_v2))
    seen = set()
    result = []
    for tag in tags:
        if tag not in seen:
            seen.add(tag)
            result.append(tag)
    return result


def transfer_source(row, key):
    data = row.get(key)
    if not isinstance(data, dict):
        return ""
    for source_key in ("from_address", "src_address", "source", "address", "from"):
        value = str(data.get(source_key) or "").strip()
        if value:
            return value
    return ""


def transfer_token_name(row, key):
    data = row.get(key)
    if not isinstance(data, dict):
        return ""
    return str(data.get("name") or data.get("symbol") or data.get("type") or "").strip()


def normalize_raw_wallet(row, rank_no=0, role="holder"):
    wallet = raw_wallet_address(row)
    if not wallet or bottom_monitor.is_pool_holder(row):
        return None
    buy = float(row.get("buy_volume_cur") or 0)
    sell = float(row.get("sell_volume_cur") or 0)
    net = float(row.get("netflow_usd") or (buy - sell))
    return {
        "wallet": wallet,
        "rank": rank_no,
        "roles": {role},
        "hold_pct": float(row.get("amount_percentage") or 0),
        "usd_value": float(row.get("usd_value") or 0),
        "buy_volume": buy,
        "sell_volume": sell,
        "netflow": net,
        "buy_count": int(float(row.get("buy_tx_count_cur") or 0)),
        "sell_count": int(float(row.get("sell_tx_count_cur") or 0)),
        "profit": float(row.get("profit") or 0),
        "realized_profit": float(row.get("realized_profit") or 0),
        "unrealized_profit": float(row.get("unrealized_profit") or 0),
        "avg_cost": float(row.get("avg_cost") or row.get("cost_cur") or row.get("cost") or 0),
        "avg_sold": float(row.get("avg_sold") or 0),
        "sell_amount_pct": float(row.get("sell_amount_percentage") or 0),
        "start_holding_at": int(float(row.get("start_holding_at") or 0)),
        "end_holding_at": int(float(row.get("end_holding_at") or 0)),
        "last_active_at": int(float(row.get("last_active_timestamp") or 0)),
        "created_at": int(float(row.get("created_at") or 0)),
        "tags": raw_tags(row),
        "native_source": transfer_source(row, "native_transfer"),
        "token_source": transfer_source(row, "token_transfer_in") or transfer_source(row, "token_transfer"),
        "token_source_name": transfer_token_name(row, "token_transfer_in") or transfer_token_name(row, "token_transfer"),
        "transfer_in_count": int(float(row.get("transfer_in_count") or 0)),
        "transfer_out_count": int(float(row.get("transfer_out_count") or 0)),
    }


def fetch_token_traders(address, chain="sol", limit=100):
    rows = []
    seen = set()
    for order_by in ("sell_volume_cur", "buy_volume_cur"):
        data = bottom_monitor.run_gmgn(
            [
                "token",
                "traders",
                "--chain",
                chain,
                "--address",
                address,
                "--limit",
                str(limit),
                "--order-by",
                order_by,
                "--direction",
                "desc",
            ],
            timeout=90,
        )
        if not isinstance(data, dict):
            continue
        traders = data.get("list") or data.get("data", {}).get("list") or []
        for row in traders if isinstance(traders, list) else []:
            wallet = raw_wallet_address(row)
            if not wallet or wallet in seen:
                continue
            seen.add(wallet)
            rows.append(row)
    return rows


TRADER_SCENARIOS = [
    ("smart_buy", "smart_degen", "buy_volume_cur", "精明资金高买入"),
    ("smart_sell", "smart_degen", "sell_volume_cur", "聪明资金高卖出"),
    ("kol_active", "renowned", "amount_percentage", "仍有持仓KOL"),
    ("smart_profit", "smart_degen", "profit", "聪明资金盈利"),
    ("sniper_hold", "sniper", "amount_percentage", "狙击手坚守"),
    ("smart_unrealized", "smart_degen", "unrealized_profit", "聪明资金未实现收益"),
    ("kol_profit", "renowned", "profit", "KOL已获利"),
]


def fetch_token_trader_scenarios(address, chain="sol", limit=20):
    scenarios = {}
    for key, tag, order_by, label in TRADER_SCENARIOS:
        data = bottom_monitor.run_gmgn(
            [
                "token",
                "traders",
                "--chain",
                chain,
                "--address",
                address,
                "--limit",
                str(limit),
                "--tag",
                tag,
                "--order-by",
                order_by,
                "--direction",
                "desc",
            ],
            timeout=90,
        )
        rows = []
        if isinstance(data, dict):
            traders = data.get("list") or data.get("data", {}).get("list") or []
            for rank_no, row in enumerate(traders if isinstance(traders, list) else [], start=1):
                item = normalize_raw_wallet(row, rank_no, "trader")
                if item:
                    rows.append(item)
        scenarios[key] = {
            "tag": tag,
            "order_by": order_by,
            "label": label,
            "wallets": rows,
        }
    return scenarios


def merge_wallet_record(target, item):
    wallet = item.get("wallet")
    existing = target.get(wallet)
    if not existing:
        target[wallet] = item
        return
    existing["roles"].update(item.get("roles") or set())
    for key in ("hold_pct", "usd_value", "buy_volume", "sell_volume", "buy_count", "sell_count", "profit", "realized_profit", "unrealized_profit", "sell_amount_pct"):
        existing[key] = max(float(existing.get(key) or 0), float(item.get(key) or 0))
    existing["netflow"] = existing.get("buy_volume", 0) - existing.get("sell_volume", 0)
    for key in ("native_source", "token_source", "token_source_name"):
        if not existing.get(key) and item.get(key):
            existing[key] = item.get(key)
    existing["tags"] = sorted(set(existing.get("tags") or []) | set(item.get("tags") or []))
    if not existing.get("rank") or (item.get("rank") and item.get("rank") < existing.get("rank")):
        existing["rank"] = item.get("rank")
    for key in ("start_holding_at", "end_holding_at", "last_active_at", "created_at"):
        if not existing.get(key) and item.get(key):
            existing[key] = item.get(key)


def group_wallet_sources(records, source_key):
    groups = {}
    for row in records:
        source = str(row.get(source_key) or "").strip()
        if not source:
            continue
        item = groups.setdefault(
            source,
            {
                "source": source,
                "wallet_count": 0,
                "hold_pct": 0.0,
                "usd_value": 0.0,
                "buy_volume": 0.0,
                "sell_volume": 0.0,
                "netflow": 0.0,
                "profit": 0.0,
                "wallets": [],
                "token_name": row.get("token_source_name") if source_key == "token_source" else "",
            },
        )
        item["wallet_count"] += 1
        item["hold_pct"] += float(row.get("hold_pct") or 0)
        item["usd_value"] += float(row.get("usd_value") or 0)
        item["buy_volume"] += float(row.get("buy_volume") or 0)
        item["sell_volume"] += float(row.get("sell_volume") or 0)
        item["netflow"] += float(row.get("netflow") or 0)
        item["profit"] += float(row.get("profit") or 0)
        if len(item["wallets"]) < 4:
            item["wallets"].append(row.get("wallet"))
    return sorted(groups.values(), key=lambda x: (x["hold_pct"], x["wallet_count"], x["netflow"]), reverse=True)


def weighted_avg_cost(records):
    weighted = 0.0
    weight = 0.0
    for item in records:
        cost = float(item.get("avg_cost") or 0)
        hold = float(item.get("hold_pct") or 0)
        if cost > 0 and hold > 0:
            weighted += cost * hold
            weight += hold
    return weighted / weight if weight > 0 else 0.0


def median_cost(records):
    costs = sorted(float(item.get("avg_cost") or 0) for item in records if float(item.get("avg_cost") or 0) > 0)
    if not costs:
        return 0.0
    mid = len(costs) // 2
    if len(costs) % 2:
        return costs[mid]
    return (costs[mid - 1] + costs[mid]) / 2


def summarize_package_wallet_matches(records):
    wallet_map = load_package_wallet_map()
    if not wallet_map:
        return {}
    matched = []
    for item in records:
        meta = wallet_map.get(item.get("wallet"))
        if not meta:
            continue
        row = dict(item)
        row["package_name"] = meta.get("name") or ""
        row["package_groups"] = meta.get("groups") or []
        matched.append(row)
    if not matched:
        return {"count": 0, "groups": [], "wallets": []}

    def summarize(items, group_name="all"):
        return {
            "group": group_name,
            "count": len(items),
            "hold_pct": sum(float(item.get("hold_pct") or 0) for item in items),
            "usd_value": sum(float(item.get("usd_value") or 0) for item in items),
            "buy_volume": sum(float(item.get("buy_volume") or 0) for item in items),
            "sell_volume": sum(float(item.get("sell_volume") or 0) for item in items),
            "netflow": sum(float(item.get("netflow") or 0) for item in items),
            "profit": sum(float(item.get("profit") or 0) for item in items),
            "avg_cost": weighted_avg_cost(items),
            "median_cost": median_cost(items),
        }

    grouped = {}
    for item in matched:
        groups = item.get("package_groups") or ["未分组"]
        for group in groups:
            grouped.setdefault(group or "未分组", []).append(item)

    group_summaries = [summarize(items, group) for group, items in grouped.items()]
    group_summaries.sort(key=lambda row: (row["hold_pct"], row["usd_value"], row["profit"]), reverse=True)
    wallet_rows = []
    for item in sorted(matched, key=lambda row: (float(row.get("hold_pct") or 0), float(row.get("usd_value") or 0)), reverse=True)[:8]:
        wallet_rows.append(
            {
                "wallet": item.get("wallet"),
                "name": item.get("package_name") or "",
                "groups": item.get("package_groups") or [],
                "hold_pct": float(item.get("hold_pct") or 0),
                "usd_value": float(item.get("usd_value") or 0),
                "buy_volume": float(item.get("buy_volume") or 0),
                "profit": float(item.get("profit") or 0),
                "avg_cost": float(item.get("avg_cost") or 0),
                "roles": sorted(item.get("roles") or []),
            }
        )
    return {
        **summarize(matched, "all"),
        "groups": group_summaries,
        "wallets": wallet_rows,
    }


def analyze_traders_and_sources(raw_holders, raw_traders):
    holder_records = []
    for rank_no, row in enumerate(raw_holders or [], start=1):
        item = normalize_raw_wallet(row, rank_no, "top100")
        if item:
            holder_records.append(item)

    trader_records = []
    for rank_no, row in enumerate(raw_traders or [], start=1):
        item = normalize_raw_wallet(row, rank_no, "trader")
        if item:
            trader_records.append(item)

    merged = {}
    for item in holder_records + trader_records:
        merge_wallet_record(merged, item)
    records = list(merged.values())
    top100_wallets = {item["wallet"] for item in holder_records}
    traders_in_top100 = [item for item in trader_records if item["wallet"] in top100_wallets]
    traders_out_top100 = [item for item in trader_records if item["wallet"] not in top100_wallets]
    top_buyers = sorted(records, key=lambda x: (x["buy_volume"], x["hold_pct"]), reverse=True)[:5]
    top_sellers = sorted(records, key=lambda x: (x["sell_volume"], x["sell_amount_pct"]), reverse=True)[:5]
    exited_sellers = [
        item for item in trader_records
        if item["wallet"] not in top100_wallets and (item.get("end_holding_at") or item.get("sell_volume", 0) > 0)
    ]
    exited_sellers = sorted(exited_sellers, key=lambda x: (x["sell_volume"], x["sell_amount_pct"]), reverse=True)[:5]
    tagged = [item for item in records if item.get("tags")]
    native_sources = group_wallet_sources(records, "native_source")[:3]
    token_sources = group_wallet_sources(records, "token_source")[:3]
    package_wallets = summarize_package_wallet_matches(records)

    def public_wallets(items):
        result = []
        for item in items:
            row = dict(item)
            row["roles"] = sorted(row.get("roles") or [])
            result.append(row)
        return result

    return {
        "holder_count": len(holder_records),
        "trader_count": len(trader_records),
        "merged_wallet_count": len(records),
        "traders_in_top100": len(traders_in_top100),
        "traders_out_top100": len(traders_out_top100),
        "top100_buy": sum(item["buy_volume"] for item in holder_records),
        "top100_sell": sum(item["sell_volume"] for item in holder_records),
        "trader_buy": sum(item["buy_volume"] for item in trader_records),
        "trader_sell": sum(item["sell_volume"] for item in trader_records),
        "trader_net": sum(item["netflow"] for item in trader_records),
        "tagged_wallet_count": len(tagged),
        "tagged_hold_pct": sum(item["hold_pct"] for item in tagged),
        "top_buyers": public_wallets(top_buyers),
        "top_sellers": public_wallets(top_sellers),
        "exited_sellers": public_wallets(exited_sellers),
        "native_sources": native_sources,
        "token_sources": token_sources,
        "package_wallets": package_wallets,
    }


def summarize_trader_scenarios(scenarios):
    result = {}
    for key, data in (scenarios or {}).items():
        wallets = data.get("wallets") or []
        result[key] = {
            "label": data.get("label") or key,
            "count": len(wallets),
            "hold_pct": sum(float(item.get("hold_pct") or 0) for item in wallets),
            "buy_volume": sum(float(item.get("buy_volume") or 0) for item in wallets),
            "sell_volume": sum(float(item.get("sell_volume") or 0) for item in wallets),
            "profit": sum(float(item.get("profit") or 0) for item in wallets),
            "unrealized_profit": sum(float(item.get("unrealized_profit") or 0) for item in wallets),
            "avg_cost": weighted_avg_cost(wallets),
            "wallets": wallets[:5],
        }
    return result


def trader_scenario_conclusion_text(summary):
    if not summary:
        return "交易员结论\n- 暂无 token traders 标签数据"

    smart_buy = summary.get("smart_buy") or {}
    smart_sell = summary.get("smart_sell") or {}
    smart_profit = summary.get("smart_profit") or {}
    smart_unrealized = summary.get("smart_unrealized") or {}
    kol_active = summary.get("kol_active") or {}
    kol_profit = summary.get("kol_profit") or {}
    sniper_hold = summary.get("sniper_hold") or {}

    smart_buy_usd = float(smart_buy.get("buy_volume") or 0)
    smart_sell_usd = float(smart_sell.get("sell_volume") or 0)
    smart_net = smart_buy_usd - smart_sell_usd
    smart_hold = float(smart_buy.get("hold_pct") or 0) + float(smart_profit.get("hold_pct") or 0)
    profit_pressure = float(smart_profit.get("profit") or 0) + float(kol_profit.get("profit") or 0)
    profit_buy = float(smart_profit.get("buy_volume") or 0) + float(kol_profit.get("buy_volume") or 0)
    profit_pressure_pct = (profit_pressure / profit_buy * 100) if profit_buy > 0 else 0.0
    unrealized = float(smart_unrealized.get("unrealized_profit") or 0)
    sniper_hold_pct = float(sniper_hold.get("hold_pct") or 0)

    if smart_sell_usd > smart_buy_usd * 1.2 and profit_pressure > 0:
        conclusion = "聪明资金卖出强于买入，且已有盈利钱包，偏出货观察。"
    elif smart_buy_usd > smart_sell_usd * 1.2 and smart_hold > 0:
        conclusion = "聪明资金买入强于卖出，且仍有持仓，偏吸筹观察。"
    elif unrealized > 0 and sniper_hold_pct > 0.01:
        conclusion = "聪明资金有未实现收益，狙击手仍有持仓，注意利润兑现。"
    else:
        conclusion = "买卖和盈利信号不极端，暂按观察处理。"

    return (
        "交易员结论\n"
        f"- 结论: {conclusion}\n"
        f"- 盈利压力: 聪明资金+KOL综合盈利{profit_pressure_pct:+.1f}% | 未实现收益{compact_money(unrealized)}\n"
        f"- 精明买入: {int(smart_buy.get('count') or 0)}个 | 持仓{float(smart_buy.get('hold_pct') or 0):.2%} | 买入{compact_money(smart_buy_usd)}\n"
        f"- 精明卖出: {int(smart_sell.get('count') or 0)}个 | 持仓{float(smart_sell.get('hold_pct') or 0):.2%} | 卖出{compact_money(smart_sell_usd)} | 净{compact_money(smart_net)}\n"
        f"- 精明盈利: {int(smart_profit.get('count') or 0)}个 | 持仓{float(smart_profit.get('hold_pct') or 0):.2%} | 盈利{profit_pct_text(smart_profit.get('profit'), smart_profit.get('buy_volume'))} | 成本{cost_text(smart_profit.get('avg_cost'))}\n"
        f"- 未实现收益: {int(smart_unrealized.get('count') or 0)}个 | 持仓{float(smart_unrealized.get('hold_pct') or 0):.2%} | 未实现{compact_money(unrealized)} | 成本{cost_text(smart_unrealized.get('avg_cost'))}\n"
        f"- KOL活跃/盈利: 活跃{int(kol_active.get('count') or 0)}个 | 已获利{int(kol_profit.get('count') or 0)}个 | 持仓{float(kol_profit.get('hold_pct') or 0):.2%} | 盈利{profit_pct_text(kol_profit.get('profit'), kol_profit.get('buy_volume'))} | 成本{cost_text(kol_profit.get('avg_cost'))}\n"
        f"- 狙击手坚守: {int(sniper_hold.get('count') or 0)}个 | 持仓{float(sniper_hold.get('hold_pct') or 0):.2%} | 成本{cost_text(sniper_hold.get('avg_cost'))}"
    )


def analyze_snapshot_change(current_holders, history, summary):
    previous_holders = (history[0].get("holders") if history else []) or []
    holder_change = (
        bottom_monitor.compare_holder_sets(current_holders, previous_holders)
        if current_holders and previous_holders
        else {
            "accumulation_pct_delta": 0.0,
            "distribution_pct_delta": 0.0,
            "new_holder_pct": 0.0,
            "exited_holder_pct": 0.0,
            "netflow_usd": 0.0,
        }
    )
    previous_summary = (history[0].get("summary") if history else {}) or {}
    return {
        "summary": summary or {},
        "previous_summary": previous_summary,
        "accumulation_pct_delta": holder_change["accumulation_pct_delta"],
        "distribution_pct_delta": holder_change["distribution_pct_delta"],
        "new_holder_pct": holder_change["new_holder_pct"],
        "exited_holder_pct": holder_change["exited_holder_pct"],
        "netflow_usd": holder_change["netflow_usd"],
        "top10_hold_delta": float((summary or {}).get("top10_pct") or 0) - float(previous_summary.get("top10_pct") or 0),
        "top20_hold_delta": float((summary or {}).get("top20_pct") or 0) - float(previous_summary.get("top20_pct") or 0),
        "top100_hold_delta": float((summary or {}).get("top100_pct") or 0) - float(previous_summary.get("top100_pct") or 0),
    }


def load_bottom_snapshot_analysis(address, chain="sol", limit=100, stats=None):
    raw_holders = bottom_monitor.fetch_top100_holders(address)
    if not raw_holders:
        return None

    token = {
        "address": address,
        "symbol": (stats or {}).get("symbol"),
        "market_cap": (stats or {}).get("mcap"),
        "price": (stats or {}).get("price"),
        "circulating_supply": (stats or {}).get("circulating_supply"),
        "liquidity": (stats or {}).get("pool_liquidity"),
        "created_at": (stats or {}).get("created_at"),
        "fee_sol": (stats or {}).get("fee_sol"),
    }
    kline_resolution = bottom_monitor.token_kline_resolution(token)
    candles = bottom_monitor.fetch_kline(address, kline_resolution, token)
    summary, holders = bottom_monitor.build_snapshot_json(token, raw_holders, candles, kline_resolution)
    raw_traders = fetch_token_traders(address, chain=chain, limit=100)
    trader_scenarios = fetch_token_trader_scenarios(address, chain=chain, limit=20)
    history = bottom_monitor.recent_snapshots(address, limit=limit)
    analysis = analyze_snapshot_change(holders, history, summary)
    analysis["snapshot_count"] = len(history)
    analysis["current_holder_count"] = len(holders)
    analysis["current_snapshot_ts"] = int(time.time())
    analysis["latest_history_snapshot_ts"] = history[0].get("snapshot_ts") if history else None
    analysis["current_kline"] = {
        "summary": compact_kline_summary(summary.get("kline")),
        "candles": [compact_candle(candle) for candle in (summary.get("kline_candles") or [])[-24:]],
    }
    analysis["current_top_holders"] = [compact_holder(holder) for holder in holders[:100]]
    analysis["trader_source_analysis"] = analyze_traders_and_sources(raw_holders, raw_traders)
    analysis["trader_scenario_analysis"] = summarize_trader_scenarios(trader_scenarios)
    analysis["phase_wallet_flows"] = analyze_phase_wallet_flows(summary, holders, history[:DEEPSEEK_MAX_HISTORY])
    previous_holders = (history[0].get("holders") if history else []) or []
    analysis["profit_track"] = {
        "current_top100_profit": sum(float(holder.get("profit") or 0) for holder in holders[:100]),
        "previous_top100_profit": sum(float(holder.get("profit") or 0) for holder in previous_holders[:100]),
        "current_top100_buy": sum(float(holder.get("buy_volume") or 0) for holder in holders[:100]),
        "current_top20_profit": sum(float(holder.get("profit") or 0) for holder in holders[:20]),
        "previous_top20_profit": sum(float(holder.get("profit") or 0) for holder in previous_holders[:20]),
        "current_top20_buy": sum(float(holder.get("buy_volume") or 0) for holder in holders[:20]),
    }
    return analysis


def fmt_pct(value):
    return f"{float(value or 0):.2%}"


def wallet_brief(items, limit=3, sign="+"):
    parts = []
    for item in (items or [])[:limit]:
        wallet = str(item.get("wallet") or "")
        short = f"{wallet[:6]}...{wallet[-4:]}" if len(wallet) > 12 else wallet
        tags = item.get("tags") or []
        tags_text = ",".join(str(tag) for tag in tags[:2]) if isinstance(tags, list) else str(tags)
        suffix = f" {tags_text}" if tags_text else ""
        hold_delta = float(item.get("hold_delta") or 0)
        netflow = float(item.get("netflow_delta") or 0)
        profit = float(item.get("profit_delta") or 0)
        parts.append(f"{short} {hold_delta:+.2%} 净{compact_money(netflow)} 盈亏{compact_money(profit)}{suffix}")
    return " | ".join(parts) if parts else "证据不足"


def fmt_ts(ts):
    ts = int(float(ts or 0))
    return datetime.fromtimestamp(ts).strftime("%m-%d %H:%M") if ts > 0 else "未知"


def phase_time_text(phase):
    if not phase or not phase.get("intervals"):
        return "无对应阶段"
    return f"{fmt_ts(phase.get('first_ts'))}~{fmt_ts(phase.get('last_ts'))}"


def wallet_flow_totals(items, mode):
    items = items or []
    if mode == "buy":
        selected = [item for item in items if float(item.get("hold_delta") or 0) > 0]
        hold = sum(float(item.get("hold_delta") or 0) for item in selected)
    else:
        selected = [item for item in items if float(item.get("hold_delta") or 0) < 0]
        hold = sum(abs(float(item.get("hold_delta") or 0)) for item in selected)
    return {
        "count": len(selected),
        "hold": hold,
        "buy": sum(float(item.get("buy_delta") or 0) for item in selected),
        "sell": sum(float(item.get("sell_delta") or 0) for item in selected),
        "net": sum(float(item.get("netflow_delta") or 0) for item in selected),
        "profit": sum(float(item.get("profit_delta") or 0) for item in selected),
    }


def wallet_flow_summary(label, phase_label, phase, items, mode, limit=3):
    totals = wallet_flow_totals(items, mode)
    return (
        f"- {label}: {phase_label}阶段({phase_time_text(phase)})，"
        f"{totals['count']}个钱包，持仓变化{totals['hold']:.2%}，"
        f"买入{compact_money(totals['buy'])}，卖出{compact_money(totals['sell'])}，"
        f"净流{compact_money(totals['net'])}，盈亏{compact_money(totals['profit'])}。\n"
        f"  重点钱包: {wallet_brief(items, limit=limit)}"
    )


def current_node_text(node):
    if not node:
        return f"- 当前节点: 没有找到距离当前至少{CURRENT_NODE_MIN_AGE_SEC // 60}分钟的历史快照，暂不判断。"
    buyers = node.get("buyers_or_accumulators") or []
    sellers = node.get("sellers_or_distributors") or []
    buyer_totals = wallet_flow_totals(buyers, "buy")
    seller_totals = wallet_flow_totals(sellers, "sell")
    return (
        f"- 当前节点: {node.get('phase_text')}状态({fmt_ts(node.get('from_ts'))}~{fmt_ts(node.get('to_ts'))})，"
        f"价格{node.get('price_from')}->{node.get('price_to')}({float(node.get('price_change_pct') or 0):+.2f}%)，"
        f"Top100变化{float(node.get('top100_hold_delta') or 0):+.2%}，Top20变化{float(node.get('top20_hold_delta') or 0):+.2%}。\n"
        f"  当前增持: {buyer_totals['count']}个钱包，持仓+{buyer_totals['hold']:.2%}，净流{compact_money(buyer_totals['net'])}；{wallet_brief(buyers, limit=3)}。\n"
        f"  当前卖出: {seller_totals['count']}个钱包，减持{seller_totals['hold']:.2%}，净流{compact_money(seller_totals['net'])}；{wallet_brief(sellers, limit=3)}。"
    )


def local_stage_conclusion(phase_flows):
    phases = (phase_flows or {}).get("phases") or {}
    down = phases.get("down") or {}
    sideways = phases.get("sideways") or {}
    up = phases.get("up") or {}
    total_change = float((phase_flows or {}).get("price_change_total_pct") or 0)
    top100_delta = float(((phase_flows or {}).get("top100_hold_change") or {}).get("delta") or 0)
    down_sell = sum(abs(float(item.get("hold_delta") or 0)) for item in down.get("sellers_or_distributors") or [])
    side_buy = sum(float(item.get("hold_delta") or 0) for item in sideways.get("buyers_or_accumulators") or [])
    up_buy = sum(float(item.get("hold_delta") or 0) for item in up.get("buyers_or_accumulators") or [])
    if total_change <= -30 and down_sell >= 0.03:
        return "下跌出货/砸盘风险"
    if side_buy >= 0.03 and top100_delta > 0:
        return "横盘吸筹/换筹"
    if up_buy >= 0.02 and total_change > 0:
        return "拉升吸筹"
    if top100_delta >= 0.05:
        return "筹码增持观察"
    return "观察"


def local_phase_rule_text(analysis):
    phase_flows = (analysis or {}).get("phase_wallet_flows") or {}
    phases = phase_flows.get("phases") or {}
    up = phases.get("up") or {}
    down = phases.get("down") or {}
    sideways = phases.get("sideways") or {}
    top100 = phase_flows.get("top100_hold_change") or {}
    top20 = phase_flows.get("top20_hold_change") or {}
    current_node = phase_flows.get("current_node") or {}
    current_kline = (analysis or {}).get("current_kline") or {}
    kline_summary = current_kline.get("summary") or {}
    stage = local_stage_conclusion(phase_flows)
    return (
        f"本地规则阶段分析\n"
        f"- 阶段结论: {stage}。\n"
        f"- K线证据: 总涨跌{phase_flows.get('price_change_total_pct', 0):.2f}% | "
        f"现价{phase_flows.get('current_price', 0)} | 高{phase_flows.get('price_high', 0)} | 低{phase_flows.get('price_low', 0)}。\n"
        f"- Top100持仓变化: {fmt_pct(top100.get('first'))}->{fmt_pct(top100.get('last'))} "
        f"({float(top100.get('delta') or 0):+.2%}) | Top20 {fmt_pct(top20.get('first'))}->{fmt_pct(top20.get('last'))} "
        f"({float(top20.get('delta') or 0):+.2%})。\n"
        f"{wallet_flow_summary('吸筹钱包', '横盘', sideways, sideways.get('buyers_or_accumulators'), 'buy')}。\n"
        f"{wallet_flow_summary('上涨买入', '上涨', up, up.get('buyers_or_accumulators'), 'buy', limit=2)}。\n"
        f"{wallet_flow_summary('出货钱包', '下跌', down, down.get('sellers_or_distributors'), 'sell')}。\n"
        f"- 换筹证据: 横盘Top100变化{float(sideways.get('top100_hold_delta') or 0):+.2%} | "
        f"横盘净流{compact_money(sideways.get('netflow_delta'))} | 下跌净流{compact_money(down.get('netflow_delta'))} | "
        f"时间段{phase_time_text(sideways)}，与K线横盘阶段对应。\n"
        f"{current_node_text(current_node)}"
    )


def trader_wallet_brief(items, mode="buy", limit=3):
    parts = []
    for item in (items or [])[:limit]:
        tags = item.get("tags") or []
        tags_text = ",".join(str(tag) for tag in tags[:2]) if isinstance(tags, list) else str(tags)
        suffix = f" {tags_text}" if tags_text else ""
        hold = float(item.get("hold_pct") or 0)
        profit = float(item.get("profit") or 0)
        parts.append(
            f"{short_addr(item.get('wallet'))} 持仓{hold:.2%} "
            f"盈利{profit_pct_text(profit, item.get('buy_volume'))} "
            f"成本{cost_text(item.get('avg_cost'))}{suffix}"
        )
    return " | ".join(parts) if parts else "暂无明显钱包"


def source_group_brief(groups, label):
    parts = []
    for group in (groups or [])[:3]:
        token_name = f"/{group.get('token_name')}" if group.get("token_name") else ""
        wallets = ",".join(short_addr(wallet) for wallet in (group.get("wallets") or [])[:2])
        parts.append(
            f"{short_addr(group.get('source'))}{token_name} "
            f"{int(group.get('wallet_count') or 0)}个/持{float(group.get('hold_pct') or 0):.2%}/"
            f"买{compact_money(group.get('buy_volume'))}/卖{compact_money(group.get('sell_volume'))}/"
            f"净{compact_money(group.get('netflow'))}"
            f"({wallets})"
        )
    return f"- {label}: " + (" | ".join(parts) if parts else "暂无明显同源簇")


def package_wallet_group_brief(groups):
    parts = []
    for group in (groups or [])[:4]:
        parts.append(
            f"{group.get('group')} {int(group.get('count') or 0)}个/"
            f"持仓{float(group.get('hold_pct') or 0):.2%}/"
            f"盈利{profit_pct_text(group.get('profit'), group.get('buy_volume'))}"
        )
    return " | ".join(parts) if parts else "暂无分组命中"


def package_wallet_brief(wallets):
    parts = []
    for item in (wallets or [])[:5]:
        groups = ",".join(item.get("groups") or [])
        name = item.get("name") or groups or short_addr(item.get("wallet"))
        parts.append(
            f"{name} "
            f"持仓{float(item.get('hold_pct') or 0):.2%} "
            f"盈利{profit_pct_text(item.get('profit'), item.get('buy_volume'))}"
        )
    return " | ".join(parts) if parts else "暂无重点命中钱包"


def package_wallet_text(analysis):
    data = ((analysis or {}).get("trader_source_analysis") or {}).get("package_wallets") or {}
    if not data or int(data.get("count") or 0) <= 0:
        return "钱包命中分析\n- 暂无 package_wallet_map.json 命中钱包"
    return (
        "钱包命中分析\n"
        f"- 命中汇总: {int(data.get('count') or 0)}个 | 持仓{float(data.get('hold_pct') or 0):.2%} | "
        f"持仓金额{compact_money(data.get('usd_value'))} | 盈利{profit_pct_text(data.get('profit'), data.get('buy_volume'))}。\n"
        f"- 分组聚合: {package_wallet_group_brief(data.get('groups'))}。\n"
        f"- 重点钱包: {package_wallet_brief(data.get('wallets'))}。"
    )


def trader_source_text(analysis):
    data = (analysis or {}).get("trader_source_analysis") or {}
    if not data:
        return ""
    return (
        "Traders + 来源聚合分析\n"
        f"- 数据覆盖: Top100 {data.get('holder_count', 0)}个 | Traders {data.get('trader_count', 0)}个 | "
        f"合并钱包 {data.get('merged_wallet_count', 0)}个 | Traders仍在Top100 {data.get('traders_in_top100', 0)}个 | "
        f"已离开Top100 {data.get('traders_out_top100', 0)}个。\n"
        f"- Top100买卖: 买{compact_money(data.get('top100_buy'))} | 卖{compact_money(data.get('top100_sell'))}；"
        f"Traders买卖: 买{compact_money(data.get('trader_buy'))} | 卖{compact_money(data.get('trader_sell'))} | "
        f"净{compact_money(data.get('trader_net'))}。\n"
        f"- 标签钱包聚合: {data.get('tagged_wallet_count', 0)}个 | 持仓{float(data.get('tagged_hold_pct') or 0):.2%}。\n"
        f"- 主要买入钱包: {trader_wallet_brief(data.get('top_buyers'), 'buy')}。\n"
        f"- 主要卖出钱包: {trader_wallet_brief(data.get('top_sellers'), 'sell')}。\n"
        f"- 已退出/不在Top100卖出: {trader_wallet_brief(data.get('exited_sellers'), 'sell')}。\n"
        f"{source_group_brief(data.get('native_sources'), 'SOL资金来源簇')}。\n"
        f"{source_group_brief(data.get('token_sources'), 'Token转入来源簇')}。\n"
        f"{package_wallet_text(analysis)}"
    )


def bottom_profit_wallet_text(analysis):
    current = (analysis or {}).get("current_top_holders") or []
    data = (analysis or {}).get("trader_source_analysis") or {}
    profitable = [item for item in current if float(item.get("profit") or 0) > 0]
    profitable.sort(key=lambda item: (float(item.get("profit") or 0), float(item.get("hold_pct") or 0)), reverse=True)
    total_hold = sum(float(item.get("hold_pct") or 0) for item in profitable)
    total_profit = sum(float(item.get("profit") or 0) for item in profitable)
    total_buy = sum(float(item.get("buy") or 0) for item in profitable)
    total_profit_pct = (total_profit / total_buy * 100) if total_buy > 0 else 0.0
    top_profit = " | ".join(
        f"{short_addr(item.get('wallet'))} 持仓{float(item.get('hold_pct') or 0):.2%} "
        f"盈利{profit_pct_text(item.get('profit'), item.get('buy'))} "
        f"成本{cost_text(item.get('avg_cost'))}"
        for item in profitable[:3]
    ) or "暂无盈利钱包"
    sellers = data.get("top_sellers") or []
    exited = data.get("exited_sellers") or []
    seller_count = len(sellers) + len(exited)
    seller_hold = sum(float(item.get("hold_pct") or 0) for item in sellers + exited)
    if total_hold >= 0.30 and total_profit_pct >= 20 and seller_count >= 3:
        conclusion = "盈利钱包持仓高且已有卖出钱包，底部存在兑现/出货压力。"
    elif total_hold >= 0.30 and total_profit_pct >= 20:
        conclusion = "盈利钱包持仓高但卖出证据不强，偏继续观察兑现动作。"
    elif seller_count >= 3 and seller_hold <= 0.01:
        conclusion = "卖出观察钱包多但当前持仓低，偏已兑现离场。"
    else:
        conclusion = "盈利聚合不极端，暂按观察处理。"
    return (
        "底部盈利钱包聚合\n"
        f"- 结论: {conclusion}\n"
        f"- 盈利钱包: {len(profitable)}个 | 持仓{total_hold:.2%} | 盈利{profit_pct_text(total_profit, total_buy)}。\n"
        f"- 主要盈利钱包: {top_profit}。\n"
        f"- 当前卖出观察: {trader_wallet_brief(sellers, 'sell')}。\n"
        f"- 已退出卖出观察: {trader_wallet_brief(exited, 'sell')}。"
    )


def bottom_chip_history_text(analysis):
    if not analysis:
        return "最近100次筹码轨迹\n- 暂无 bottom_top100_snapshots 历史数据"
    current_ts = analysis.get("current_snapshot_ts")
    history_ts = analysis.get("latest_history_snapshot_ts")
    current_time = datetime.fromtimestamp(current_ts).strftime("%Y-%m-%d %H:%M:%S") if current_ts else "未知"
    history_time = datetime.fromtimestamp(history_ts).strftime("%Y-%m-%d %H:%M:%S") if history_ts else "暂无"
    top100 = ((analysis or {}).get("phase_wallet_flows") or {}).get("top100_hold_change") or {}
    top20 = ((analysis or {}).get("phase_wallet_flows") or {}).get("top20_hold_change") or {}
    profit = (analysis or {}).get("profit_track") or {}
    top100_profit_delta = float(profit.get("current_top100_profit") or 0) - float(profit.get("previous_top100_profit") or 0)
    top20_profit_delta = float(profit.get("current_top20_profit") or 0) - float(profit.get("previous_top20_profit") or 0)
    return (
        f"实时Top100 + 最近100次筹码轨迹\n"
        f"- 当前查询: {current_time} | 当前Top100: {analysis.get('current_holder_count', 0)}个 | 历史快照: {analysis.get('snapshot_count', 0)}条\n"
        f"- 最近历史: {history_time}\n"
        f"- Top100持仓变化: {fmt_pct(top100.get('first'))}->{fmt_pct(top100.get('last'))} ({float(top100.get('delta') or 0):+.2%}) | "
        f"盈利变化: {profit_pct_text(top100_profit_delta, profit.get('current_top100_buy'))}\n"
        f"- Top20持仓变化: {fmt_pct(top20.get('first'))}->{fmt_pct(top20.get('last'))} ({float(top20.get('delta') or 0):+.2%}) | "
        f"盈利变化: {profit_pct_text(top20_profit_delta, profit.get('current_top20_buy'))}\n\n"
        f"{trader_scenario_conclusion_text((analysis or {}).get('trader_scenario_analysis'))}"
    )


def build_deepseek_payload(chain, address, stats, bottom_analysis):
    return {
        "chain": chain,
        "address": address,
        "token": {
            "symbol": stats.get("symbol"),
            "mcap": stats.get("mcap"),
            "price": stats.get("price"),
            "holder_count": stats.get("holder_count"),
            "fee_sol": stats.get("fee_sol"),
            "pool_label": stats.get("pool_label"),
            "pool_liquidity": stats.get("pool_liquidity"),
            "created_time": stats.get("created_time"),
            "verdict": stats.get("verdict"),
            "control_ratio": stats.get("control_ratio"),
            "associated_supply": stats.get("associated_supply"),
            "dump_progress": stats.get("dump_progress"),
        },
        "data_note": "Local code aggregated historical Top100 wallet flows by K-line phase to reduce token usage. No local final conclusion is included.",
        "current_kline": (bottom_analysis or {}).get("current_kline") or {},
        "current_top_holders": ((bottom_analysis or {}).get("current_top_holders") or [])[:30],
        "phase_wallet_flows": (bottom_analysis or {}).get("phase_wallet_flows") or {},
        "trader_source_analysis": (bottom_analysis or {}).get("trader_source_analysis") or {},
    }


def call_deepseek_chip_analysis(chain, address, stats, bottom_analysis):
    if not DEEPSEEK_ENABLED or not DEEPSEEK_API_KEY or not bottom_analysis:
        return ""
    payload = build_deepseek_payload(chain, address, stats, bottom_analysis)
    prompt = (
        "你是链上筹码操纵分析助手。输入已经由本地代码按K线阶段聚合，包含当前K线、当前前排持仓、"
        "上涨/下跌/横盘阶段的钱包买卖和持仓变化摘要。没有本地最终结论。"
        "请基于这些聚合数据判断：1 当前处于拉升、下跌、横盘吸筹、派发还是洗盘阶段；"
        "2 上涨阶段哪些钱包买入推动；3 下跌阶段哪些钱包卖出或砸盘；4 横盘阶段哪些钱包吸筹；"
        "5 Top100/Top20持仓占比如何变化；6 当前更偏吸筹、换筹、出货还是观察。6，当前阶段是那些钱包主导导致的"
        "请用中文输出，必须包含：阶段结论、K线证据、Top100持仓变化、吸筹钱包、出货钱包、换筹证据、后续观察条件。"
        "不要给出买入建议，不要编造输入里没有的数据。\n\n"
        f"数据JSON:\n{json.dumps(payload, ensure_ascii=False, separators=(',', ':'))}"
    )
    body = {
        "model": DEEPSEEK_MODEL,
        "messages": [
            {"role": "system", "content": "你是严谨的链上数据分析师，只根据给定JSON做判断。"},
            {"role": "user", "content": prompt},
        ],
        "stream": False,
        "reasoning_effort": DEEPSEEK_REASONING_EFFORT,
        "thinking": {"type": DEEPSEEK_THINKING},
    }
    try:
        resp = requests.post(
            f"{DEEPSEEK_BASE_URL.rstrip('/')}/chat/completions",
            headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"},
            json=body,
            timeout=DEEPSEEK_TIMEOUT,
        )
        if not resp.ok:
            return f"DeepSeek分析失败: http={resp.status_code} {resp.text[:160]}"
        data = resp.json()
        return str(data.get("choices", [{}])[0].get("message", {}).get("content") or "").strip()
    except Exception as exc:
        return f"DeepSeek分析异常: {exc}"


def deepseek_chip_text(text):
    if not text:
        return ""
    return f"DeepSeek筹码结论\n{text}\n\n"


def estimate_token_from_holders(address, raw_holders, candles=None):
    price = 0.0
    supply = 0.0
    for row in raw_holders or []:
        if bottom_monitor.is_pool_holder(row):
            continue
        amount = float(row.get("amount_cur") or row.get("balance") or 0)
        pct = float(row.get("amount_percentage") or 0)
        usd = float(row.get("usd_value") or 0)
        if price <= 0 and amount > 0 and usd > 0:
            price = usd / amount
        if supply <= 0 and amount > 0 and pct > 0:
            supply = amount / pct
        if price > 0 and supply > 0:
            break
    if price <= 0 and candles:
        price = float((candles[-1] or {}).get("close") or 0)
    return {
        "address": address,
        "price": price,
        "circulating_supply": supply,
        "market_cap": price * supply if price > 0 and supply > 0 else 0,
    }


def build_token_from_info(address, info=None, pool_data=None):
    info = info or {}
    token = {
        "address": address,
        "symbol": info.get("symbol"),
        "name": info.get("name"),
        "market_cap": info.get("market_cap") or info.get("mcap"),
        "price": info.get("price"),
        "circulating_supply": info.get("circulating_supply") or info.get("total_supply"),
        "total_supply": info.get("total_supply"),
        "liquidity": info.get("liquidity"),
        "created_at": info.get("creation_timestamp") or info.get("open_timestamp"),
        "creation_timestamp": info.get("creation_timestamp"),
        "open_timestamp": info.get("open_timestamp"),
        "fee_sol": info.get("total_fee") or info.get("trade_fee"),
        "total_fee": info.get("total_fee"),
        "trade_fee": info.get("trade_fee"),
        "launchpad": info.get("launchpad"),
        "launchpad_platform": info.get("launchpad_platform"),
        "biggest_pool_address": info.get("biggest_pool_address"),
        "_gmgn_info": info,
        "_gmgn_pool": pool_data if pool_data is not None else {},
    }
    ath_mcap = bottom_monitor.current_token_ath_mcap(info)
    if ath_mcap > 0:
        token["ath_mcap"] = ath_mcap
    return token


def merge_token_estimates(primary, fallback):
    merged = dict(fallback or {})
    for key, value in (primary or {}).items():
        if value not in (None, "", 0):
            merged[key] = value
        elif key not in merged:
            merged[key] = value
    for key in ("_gmgn_info", "_gmgn_pool", "_gmgn_security"):
        if key in (primary or {}):
            merged[key] = primary.get(key)
    return merged


def aggregate_tag_stats(holders):
    groups = {
        "smart_degen": "聪明钱",
        "renowned": "KOL",
        "bundler": "捆绑",
        "sniper": "狙击手",
        "rat_trader": "老鼠仓",
        "fresh_wallet": "新钱包",
    }
    result = {}
    for key, label in groups.items():
        rows = []
        for item in holders or []:
            if bottom_monitor.is_pool_holder(item):
                continue
            tags = set(raw_tags(item))
            if key in tags:
                rows.append(item)
        buy = sum(float(item.get("buy_volume") or item.get("buy_volume_cur") or 0) for item in rows)
        sell = sum(float(item.get("sell_volume") or item.get("sell_volume_cur") or 0) for item in rows)
        profit = sum(float(item.get("profit") or 0) for item in rows)
        hold = sum(float(item.get("hold_pct") or item.get("amount_percentage") or 0) for item in rows)
        usd = sum(float(item.get("usd_value") or 0) for item in rows)
        result[key] = {
            "label": label,
            "count": len(rows),
            "hold_pct": hold,
            "usd_value": usd,
            "buy": buy,
            "sell": sell,
            "net": buy - sell,
            "profit": profit,
            "profit_pct": (profit / buy * 100) if buy > 0 else 0.0,
        }
    return result


def similar_hold_bundle_clusters(holders):
    """Find likely bundled holder groups by near-equal position size and near-equal entry time."""
    candidates = []
    for item in holders or []:
        hold = float(item.get("hold_pct") or item.get("amount_percentage") or 0)
        start_ts = int(float(item.get("start_holding_at") or 0))
        wallet = item.get("wallet") or raw_wallet_address(item)
        if not wallet or hold <= 0 or start_ts <= 0:
            continue
        candidates.append(
            {
                "wallet": wallet,
                "rank": item.get("rank"),
                "hold_pct": hold,
                "usd_value": float(item.get("usd_value") or 0),
                "buy": float(item.get("buy_volume") or item.get("buy_volume_cur") or item.get("buy") or 0),
                "sell": float(item.get("sell_volume") or item.get("sell_volume_cur") or item.get("sell") or 0),
                "profit": float(item.get("profit") or 0),
                "avg_cost": float(item.get("avg_cost") or 0),
                "start_holding_at": start_ts,
                "tags": item.get("tags") or [],
            }
        )
    candidates.sort(key=lambda row: row["hold_pct"])

    clusters = []
    seen_wallet_sets = set()
    max_hold_span = BUNDLE_SIMILAR_HOLD_TOLERANCE_PCT * 2
    for idx, base in enumerate(candidates):
        hold_group = [
            row for row in candidates[idx:]
            if row["hold_pct"] - base["hold_pct"] <= max_hold_span
        ]
        if len(hold_group) < BUNDLE_SIMILAR_MIN_WALLETS:
            continue
        hold_group.sort(key=lambda row: row["start_holding_at"])
        for time_idx, time_base in enumerate(hold_group):
            time_group = [
                row for row in hold_group[time_idx:]
                if row["start_holding_at"] - time_base["start_holding_at"] <= BUNDLE_SIMILAR_TIME_WINDOW_SEC
            ]
            if len(time_group) < BUNDLE_SIMILAR_MIN_WALLETS:
                continue
            wallets_key = tuple(sorted(row["wallet"] for row in time_group))
            if wallets_key in seen_wallet_sets:
                continue
            seen_wallet_sets.add(wallets_key)
            hold_values = [row["hold_pct"] for row in time_group]
            start_values = [row["start_holding_at"] for row in time_group]
            total_buy = sum(row["buy"] for row in time_group)
            total_profit = sum(row["profit"] for row in time_group)
            clusters.append(
                {
                    "wallet_count": len(time_group),
                    "hold_pct": sum(hold_values),
                    "usd_value": sum(row["usd_value"] for row in time_group),
                    "avg_hold_pct": sum(hold_values) / len(hold_values),
                    "min_hold_pct": min(hold_values),
                    "max_hold_pct": max(hold_values),
                    "hold_span_pct": max(hold_values) - min(hold_values),
                    "start_ts": min(start_values),
                    "end_ts": max(start_values),
                    "time_span_sec": max(start_values) - min(start_values),
                    "buy": total_buy,
                    "sell": sum(row["sell"] for row in time_group),
                    "profit": total_profit,
                    "profit_pct": (total_profit / total_buy * 100) if total_buy > 0 else 0.0,
                    "wallets": sorted(
                        time_group,
                        key=lambda row: (row["hold_pct"], row["usd_value"]),
                        reverse=True,
                    )[:6],
                }
            )
    clusters.sort(key=lambda row: (row["hold_pct"], row["wallet_count"], -row["time_span_sec"]), reverse=True)
    return clusters[:6]


def similar_hold_bundle_text(clusters):
    if not clusters:
        return "- 暂无相近持仓+相近买入时间的疑似捆绑聚合"
    lines = []
    for idx, row in enumerate(clusters[:4], start=1):
        start_text = datetime.fromtimestamp(int(row.get("start_ts") or 0)).strftime("%H:%M:%S") if row.get("start_ts") else "-"
        end_text = datetime.fromtimestamp(int(row.get("end_ts") or 0)).strftime("%H:%M:%S") if row.get("end_ts") else "-"
        wallets = " ".join(short_addr(item.get("wallet")) for item in (row.get("wallets") or [])[:4])
        lines.append(
            f"- 簇{idx}: {int(row.get('wallet_count') or 0)}个 | 持仓{float(row.get('hold_pct') or 0):.2%}/"
            f"{compact_money(row.get('usd_value'))} | 单仓{float(row.get('min_hold_pct') or 0):.2%}-"
            f"{float(row.get('max_hold_pct') or 0):.2%} | 时间{start_text}-{end_text} "
            f"({int(row.get('time_span_sec') or 0)}s) | 盈利{float(row.get('profit_pct') or 0):+.1f}% | {wallets}"
        )
    return "\n".join(lines)


def wallet_creation_clusters(holders):
    candidates = []
    for item in holders or []:
        created_ts = int(float(item.get("created_at") or 0))
        wallet = item.get("wallet") or raw_wallet_address(item)
        if not wallet or created_ts <= 0:
            continue
        candidates.append(
            {
                "wallet": wallet,
                "rank": item.get("rank"),
                "created_at": created_ts,
                "hold_pct": float(item.get("hold_pct") or item.get("amount_percentage") or 0),
                "usd_value": float(item.get("usd_value") or 0),
                "buy": float(item.get("buy_volume") or item.get("buy_volume_cur") or item.get("buy") or 0),
                "sell": float(item.get("sell_volume") or item.get("sell_volume_cur") or item.get("sell") or 0),
                "profit": float(item.get("profit") or 0),
                "avg_cost": float(item.get("avg_cost") or 0),
                "tags": item.get("tags") or [],
            }
        )
    candidates.sort(key=lambda row: row["created_at"])
    clusters = []
    used_wallets = set()
    for idx, base in enumerate(candidates):
        rows = [
            row for row in candidates[idx:]
            if row["created_at"] - base["created_at"] <= WALLET_CREATION_CLUSTER_SEC
        ]
        rows = [row for row in rows if row["wallet"] not in used_wallets]
        if len(rows) < WALLET_CREATION_CLUSTER_MIN_WALLETS:
            continue
        total_buy = sum(row["buy"] for row in rows)
        total_profit = sum(row["profit"] for row in rows)
        cluster = {
            "wallet_count": len(rows),
            "hold_pct": sum(row["hold_pct"] for row in rows),
            "usd_value": sum(row["usd_value"] for row in rows),
            "buy": total_buy,
            "sell": sum(row["sell"] for row in rows),
            "net": sum(row["buy"] - row["sell"] for row in rows),
            "profit": total_profit,
            "profit_pct": (total_profit / total_buy * 100) if total_buy > 0 else 0.0,
            "start_ts": min(row["created_at"] for row in rows),
            "end_ts": max(row["created_at"] for row in rows),
            "time_span_sec": max(row["created_at"] for row in rows) - min(row["created_at"] for row in rows),
            "wallets": sorted(rows, key=lambda row: (row["hold_pct"], row["usd_value"]), reverse=True)[:6],
        }
        clusters.append(cluster)
        used_wallets.update(row["wallet"] for row in rows)
    clusters.sort(key=lambda row: (row["hold_pct"], row["wallet_count"]), reverse=True)
    return clusters[:5]


def wallet_creation_cluster_text(clusters):
    if not clusters:
        return "- 暂无同批创建钱包簇"
    lines = []
    for idx, row in enumerate(clusters[:5], start=1):
        start_text = datetime.fromtimestamp(int(row.get("start_ts") or 0)).strftime("%m-%d %H:%M") if row.get("start_ts") else "-"
        end_text = datetime.fromtimestamp(int(row.get("end_ts") or 0)).strftime("%m-%d %H:%M") if row.get("end_ts") else "-"
        span_hours = float(row.get("time_span_sec") or 0) / 3600
        wallets = " ".join(short_addr(item.get("wallet")) for item in (row.get("wallets") or [])[:4])
        lines.append(
            f"- 簇{idx}: {int(row.get('wallet_count') or 0)}个 | 持仓{float(row.get('hold_pct') or 0):.2%}/"
            f"{compact_money(row.get('usd_value'))} | 创建{start_text}-{end_text} "
            f"({span_hours:.1f}h) | 买{compact_money(row.get('buy'))} 卖{compact_money(row.get('sell'))} "
            f"| 净{compact_money(row.get('net'))} | 盈利{float(row.get('profit_pct') or 0):+.1f}% | {wallets}"
        )
    return "\n".join(lines)


def light_wallet_lines(holders, limit=8):
    rows = sorted(
        holders or [],
        key=lambda item: (float(item.get("hold_pct") or 0), float(item.get("usd_value") or 0)),
        reverse=True,
    )[:limit]
    lines = []
    for item in rows:
        tags = ",".join(str(tag) for tag in (item.get("tags") or [])[:2])
        suffix = f" {tags}" if tags else ""
        buy = item.get("buy_volume", item.get("buy"))
        sell = item.get("sell_volume", item.get("sell"))
        lines.append(
            f"- {short_addr(item.get('wallet'))} 持仓{float(item.get('hold_pct') or 0):.2%}/"
            f"{compact_money(item.get('usd_value'))} 买{compact_money(buy)} "
            f"卖{compact_money(sell)} 盈利{profit_pct_text(item.get('profit'), buy)}{suffix}"
        )
    return "\n".join(lines) if lines else "- 暂无有效持仓钱包"


def analyze_bottom_chip_sell(raw_holders, holders, summary, kline_summary):
    price = float((summary or {}).get("price") or (kline_summary or {}).get("close") or 0)
    open_price = float((kline_summary or {}).get("open") or 0)
    close_price = float((kline_summary or {}).get("close") or price or 0)
    high_price = float((kline_summary or {}).get("high") or close_price or 0)
    low_price = float((kline_summary or {}).get("low") or 0)
    change_pct = float((kline_summary or {}).get("change_pct") or 0)

    holder_by_wallet = {str(item.get("wallet") or ""): item for item in holders or []}
    candidate_rows = []
    seller_rows = []
    for rank_no, row in enumerate(raw_holders or [], start=1):
        if bottom_monitor.is_pool_holder(row):
            continue
        wallet = raw_wallet_address(row)
        if not wallet:
            continue
        buy = float(row.get("buy_volume_cur") or row.get("buy_volume") or 0)
        sell = float(row.get("sell_volume_cur") or row.get("sell_volume") or 0)
        sell_amount_pct = float(row.get("sell_amount_percentage") or row.get("sell_amount_pct") or 0)
        avg_cost = float(row.get("avg_cost") or row.get("cost_cur") or row.get("cost") or 0)
        normalized = holder_by_wallet.get(wallet) or {}
        hold_pct = float(normalized.get("hold_pct") or row.get("amount_percentage") or 0)
        usd_value = float(normalized.get("usd_value") or row.get("usd_value") or 0)
        if sell_amount_pct <= 0 and buy > 0 and sell > 0:
            sell_amount_pct = min(sell / buy, 1.0)
        item = {
            "wallet": wallet,
            "rank": rank_no,
            "hold_pct": hold_pct,
            "usd_value": usd_value,
            "buy": buy,
            "sell": sell,
            "net": buy - sell,
            "profit": float(row.get("profit") or 0),
            "realized_profit": float(row.get("realized_profit") or 0),
            "unrealized_profit": float(row.get("unrealized_profit") or 0),
            "avg_cost": avg_cost,
            "sell_amount_pct": max(0.0, min(sell_amount_pct, 1.0)),
            "tags": raw_tags(row),
        }
        if avg_cost > 0:
            candidate_rows.append(item)
        if item["sell_amount_pct"] >= 0.8:
            seller_rows.append(item)

    candidate_rows.sort(key=lambda item: item["avg_cost"])
    cost_thresholds = []
    if low_price > 0:
        cost_thresholds.append(low_price * 1.25)
    if price > 0:
        cost_thresholds.append(price * 0.55)
    if candidate_rows:
        percentile_index = max(0, min(len(candidate_rows) - 1, int(len(candidate_rows) * 0.25) - 1))
        cost_thresholds.append(candidate_rows[percentile_index]["avg_cost"])
    bottom_cost_threshold = max(cost_thresholds) if cost_thresholds else 0.0
    bottom_rows = [
        item for item in candidate_rows
        if bottom_cost_threshold > 0 and item["avg_cost"] <= bottom_cost_threshold
    ]
    threshold_source = "cost_threshold"
    if not bottom_rows and candidate_rows:
        fallback_count = max(3, min(15, max(1, len(candidate_rows) // 5)))
        bottom_rows = candidate_rows[:fallback_count]
        bottom_cost_threshold = bottom_rows[-1]["avg_cost"]
        threshold_source = "lowest_cost_fallback"

    bottom_seller_rows = [item for item in bottom_rows if item["sell_amount_pct"] >= 0.8]
    target_rows = bottom_rows
    total_buy = sum(item["buy"] for item in target_rows)
    total_sell = sum(item["sell"] for item in target_rows)
    total_profit = sum(item["profit"] for item in target_rows)
    total_realized_profit = sum(item["realized_profit"] for item in target_rows)
    total_unrealized_profit = sum(item["unrealized_profit"] for item in target_rows)
    total_hold_pct = sum(item["hold_pct"] for item in target_rows)
    total_usd_value = sum(item["usd_value"] for item in target_rows)
    avg_cost = weighted_avg_cost([
        {"avg_cost": item["avg_cost"], "hold_pct": max(item["hold_pct"], item["buy"])}
        for item in target_rows
    ])
    weighted_sell_progress = 0.0
    weight = 0.0
    for item in target_rows:
        item_weight = max(item["buy"], item["usd_value"], item["hold_pct"])
        weighted_sell_progress += item["sell_amount_pct"] * item_weight
        weight += item_weight
    sell_progress = weighted_sell_progress / weight if weight > 0 else 0.0
    price_near_high = high_price > 0 and close_price >= high_price * 0.95
    price_not_down = close_price >= open_price if open_price > 0 and close_price > 0 else change_pct >= 0
    takeover = sell_progress >= 0.8 and (price_not_down or price_near_high)
    if not target_rows:
        conclusion = "暂无可识别的底部成本筹码钱包。"
    elif takeover:
        conclusion = "底部筹码整体卖出进度接近80%，且价格未明显下跌或接近区间高位，显示卖压有承接。"
    elif sell_progress >= 0.5:
        conclusion = "底部筹码已有较高卖出进度，需观察剩余持仓与后续承接。"
    else:
        conclusion = "底部筹码卖出进度不高，主要观察剩余持仓和盈利兑现压力。"

    target_rows.sort(key=lambda item: (item["sell_amount_pct"], item["sell"], item["profit"]), reverse=True)
    return {
        "bottom_wallet_count": len(bottom_rows),
        "seller_count": len(seller_rows),
        "bottom_seller_count": len(bottom_seller_rows),
        "used_bottom_cost_filter": bool(bottom_rows),
        "threshold_source": threshold_source,
        "bottom_cost_threshold": bottom_cost_threshold,
        "hold_pct": total_hold_pct,
        "usd_value": total_usd_value,
        "buy": total_buy,
        "sell": total_sell,
        "net": total_buy - total_sell,
        "profit": total_profit,
        "realized_profit": total_realized_profit,
        "unrealized_profit": total_unrealized_profit,
        "profit_pct": (total_profit / total_buy * 100) if total_buy > 0 else 0.0,
        "avg_cost": avg_cost,
        "sell_progress": sell_progress,
        "price_not_down": price_not_down,
        "price_near_high": price_near_high,
        "takeover": takeover,
        "price_change_pct": change_pct,
        "open": open_price,
        "close": close_price,
        "high": high_price,
        "low": low_price,
        "conclusion": conclusion,
        "wallets": target_rows[:5],
    }


def bottom_chip_sell_text(data):
    if not data:
        return "- 暂无底部筹码卖出分析"
    lines = [
        f"- 结论: {data.get('conclusion')}",
        f"- 底部筹码钱包: {int(data.get('bottom_wallet_count') or 0)}个 | 高卖出{int(data.get('bottom_seller_count') or 0)}个 | "
        f"持仓百分比{float(data.get('hold_pct') or 0):.2%}",
        f"- 卖出进度: {float(data.get('sell_progress') or 0):.1%} | "
        f"剩余持仓{compact_money(data.get('usd_value'))} | 成本{cost_text(data.get('avg_cost'))}",
        f"- 盈利情况: 总盈利{compact_money(data.get('profit'))} ({float(data.get('profit_pct') or 0):+.1f}%) | "
        f"已实现{compact_money(data.get('realized_profit'))} | 未实现{compact_money(data.get('unrealized_profit'))}",
        f"- 买/卖: {compact_money(data.get('buy'))}/{compact_money(data.get('sell'))} | 净{compact_money(data.get('net'))}",
        f"- 价格承接: 涨跌{float(data.get('price_change_pct') or 0):+.2f}% | "
        f"开{format_chain_price(data.get('open'))} 收{format_chain_price(data.get('close'))} "
        f"高{format_chain_price(data.get('high'))} 低{format_chain_price(data.get('low'))}",
    ]
    wallet_parts = []
    for item in (data.get("wallets") or [])[:4]:
        tags = ",".join(str(tag) for tag in (item.get("tags") or [])[:2])
        suffix = f"/{tags}" if tags else ""
        wallet_parts.append(
            f"{short_addr(item.get('wallet'))}{suffix} 持仓{float(item.get('hold_pct') or 0):.2%} "
            f"已卖{float(item.get('sell_amount_pct') or 0):.1%} 成本{cost_text(item.get('avg_cost'))}"
        )
    if wallet_parts:
        lines.append("- 主要钱包: " + " | ".join(wallet_parts))
    return "\n".join(lines)


def build_light_ca_analysis(address, chain="sol", limit=100):
    raw_holders = bottom_monitor.fetch_top100_holders(address)
    if not raw_holders:
        return None

    token = estimate_token_from_holders(address, raw_holders)
    candles = bottom_monitor.fetch_kline(address, "5m", token)
    token = estimate_token_from_holders(address, raw_holders, candles)
    summary, holders = bottom_monitor.build_snapshot_json(token, raw_holders, candles, "5m")
    history = bottom_monitor.recent_snapshots(address, limit=limit)
    analysis = analyze_snapshot_change(holders, history, summary)
    phase_flows = analyze_phase_wallet_flows(summary, holders, history[:DEEPSEEK_MAX_HISTORY])
    previous_holders = (history[0].get("holders") if history else []) or []
    profit = {
        "current_top100_profit": sum(float(holder.get("profit") or 0) for holder in holders[:100]),
        "previous_top100_profit": sum(float(holder.get("profit") or 0) for holder in previous_holders[:100]),
        "current_top100_buy": sum(float(holder.get("buy_volume") or 0) for holder in holders[:100]),
        "current_top20_profit": sum(float(holder.get("profit") or 0) for holder in holders[:20]),
        "previous_top20_profit": sum(float(holder.get("profit") or 0) for holder in previous_holders[:20]),
        "current_top20_buy": sum(float(holder.get("buy_volume") or 0) for holder in holders[:20]),
    }
    analysis.update(
        {
            "signal_type": "tg_ca_query",
            "score": 0,
            "snapshot_count": len(history),
            "current_holder_count": len(holders),
            "current_snapshot_ts": int(time.time()),
            "latest_history_snapshot_ts": history[0].get("snapshot_ts") if history else None,
            "current_kline": {
                "summary": compact_kline_summary(summary.get("kline")),
                "candles": [compact_candle(candle) for candle in (candles or [])[-24:]],
            },
            "current_top_holders": [compact_holder(holder) for holder in holders[:100]],
            "phase_wallet_flows": phase_flows,
            "profit_track": profit,
            "tag_stats": aggregate_tag_stats(raw_holders),
            "bundle_similarity_clusters": similar_hold_bundle_clusters(holders),
            "wallet_creation_clusters": wallet_creation_clusters(holders),
            "bottom_chip_sell": analyze_bottom_chip_sell(raw_holders, holders, summary, summary.get("kline")),
        }
    )
    try:
        snapshot_id = bottom_monitor.save_snapshot(f"tg_ca_{int(time.time())}", token, summary, holders, analysis)
        analysis["snapshot_id"] = snapshot_id
    except Exception as exc:
        analysis["snapshot_save_error"] = str(exc)
    return analysis


def light_tag_stats_text(tag_stats):
    lines = []
    for key in ("smart_degen", "renowned", "bundler", "sniper", "rat_trader", "fresh_wallet"):
        row = (tag_stats or {}).get(key) or {}
        if int(row.get("count") or 0) <= 0:
            continue
        lines.append(
            f"- {row.get('label')}: {int(row.get('count') or 0)}个 | 持仓{float(row.get('hold_pct') or 0):.2%}/"
            f"{compact_money(row.get('usd_value'))} | 买{compact_money(row.get('buy'))} "
            f"卖{compact_money(row.get('sell'))} | 净{compact_money(row.get('net'))} | "
            f"盈利{float(row.get('profit_pct') or 0):+.1f}%"
        )
    return "\n".join(lines) if lines else "- 暂无重点标签钱包"


def build_light_ca_message(chain, address, analysis):
    summary = (analysis or {}).get("summary") or {}
    kline = ((analysis or {}).get("current_kline") or {}).get("summary") or {}
    top100 = ((analysis or {}).get("phase_wallet_flows") or {}).get("top100_hold_change") or {}
    top20 = ((analysis or {}).get("phase_wallet_flows") or {}).get("top20_hold_change") or {}
    profit = (analysis or {}).get("profit_track") or {}
    current_ts = analysis.get("current_snapshot_ts")
    history_ts = analysis.get("latest_history_snapshot_ts")
    current_time = datetime.fromtimestamp(current_ts).strftime("%Y-%m-%d %H:%M:%S") if current_ts else "未知"
    history_time = datetime.fromtimestamp(history_ts).strftime("%Y-%m-%d %H:%M:%S") if history_ts else "暂无"
    top100_profit_delta = float(profit.get("current_top100_profit") or 0) - float(profit.get("previous_top100_profit") or 0)
    top20_profit_delta = float(profit.get("current_top20_profit") or 0) - float(profit.get("previous_top20_profit") or 0)
    return (
        f"CA 筹码查询\n"
        f"CA: {address}\n"
        f"链: {chain} | 查询时间: {current_time}\n\n"
        f"基础数据\n"
        f"- 市值: {compact_money(summary.get('mcap'))} | Top100有效钱包: {analysis.get('current_holder_count', 0)} | 历史快照: {analysis.get('snapshot_count', 0)}\n"
        f"- Top10: {float(summary.get('top10_pct') or 0):.2%} | Top20: {float(summary.get('top20_pct') or 0):.2%} | Top100: {float(summary.get('top100_pct') or 0):.2%}\n"
        f"- Top100买/卖: {compact_money(summary.get('buy_volume'))}/{compact_money(summary.get('sell_volume'))} | 净流 {compact_money(summary.get('netflow'))}\n\n"
        f"5M K线\n"
        f"- K线数: {int(kline.get('count') or 0)} | 涨跌: {float(kline.get('change_pct') or 0):+.2f}% | 成交量: {compact_money(kline.get('volume_usd'))}\n"
        f"- 开盘: {format_chain_price(kline.get('open'))} | 收盘: {format_chain_price(kline.get('close'))} | 高: {format_chain_price(kline.get('high'))} | 低: {format_chain_price(kline.get('low'))}\n\n"
        f"数据库历史对比\n"
        f"- 最近历史: {history_time}\n"
        f"- Top100持仓变化: {fmt_pct(top100.get('first'))}->{fmt_pct(top100.get('last'))} ({float(top100.get('delta') or 0):+.2%}) | 盈利变化 {profit_pct_text(top100_profit_delta, profit.get('current_top100_buy'))}\n"
        f"- Top20持仓变化: {fmt_pct(top20.get('first'))}->{fmt_pct(top20.get('last'))} ({float(top20.get('delta') or 0):+.2%}) | 盈利变化 {profit_pct_text(top20_profit_delta, profit.get('current_top20_buy'))}\n"
        f"- 本次Top100变化: 增持{analysis.get('accumulation_pct_delta', 0):.2%} | 减持{analysis.get('distribution_pct_delta', 0):.2%} | 新进{analysis.get('new_holder_pct', 0):.2%} | 退出{analysis.get('exited_holder_pct', 0):.2%} | 净流{compact_money(analysis.get('netflow_usd'))}\n\n"
        f"标签钱包聚合\n"
        f"{light_tag_stats_text(analysis.get('tag_stats'))}\n\n"
        f"相似持仓捆绑聚合\n"
        f"{similar_hold_bundle_text(analysis.get('bundle_similarity_clusters'))}\n\n"
        f"同批创建钱包簇\n"
        f"{wallet_creation_cluster_text(analysis.get('wallet_creation_clusters'))}\n\n"
        f"底部筹码卖出观察\n"
        f"{bottom_chip_sell_text(analysis.get('bottom_chip_sell'))}\n\n"
        f"Top持仓钱包\n"
        f"GMGN: https://gmgn.ai/{chain}/token/{address}"
    )


def build_chip_alert_message(chain, address, stats, bottom_analysis=None, deepseek_analysis="", kline_analysis_text=""):
    holder_tag_desc = clean_holder_tag_desc(stats.get("holder_tag_desc"))
    sm_stats = stats.get("holder_tag_stats", {}).get("smart_degen", {})
    kol_stats = stats.get("holder_tag_stats", {}).get("renowned", {})
    sm_detail = (
        f"聪明钱{sm_stats['count']}个 持仓{sm_stats['supply']:.1f}%/${sm_stats['position_value']:,.0f} "
        f"盈利{sm_stats.get('profit_pct', 0):+.1f}% 卖出进度{sm_stats.get('sell_progress', 0):.1f}%"
    ) if sm_stats.get("count", 0) > 0 else "聪明钱0个"
    kol_detail = (
        f"KOL{kol_stats['count']}个 持仓{kol_stats['supply']:.1f}%/${kol_stats['position_value']:,.0f} "
        f"盈利{kol_stats.get('profit_pct', 0):+.1f}% 卖出进度{kol_stats.get('sell_progress', 0):.1f}%"
    ) if kol_stats.get("count", 0) > 0 else "KOL0个"
    dev_address = short_addr(stats.get("creator_address")) if stats.get("creator_address") else "未知"
    dev_detail = (
        f"- Dev: {dev_address} | 持仓{stats.get('dev_hold_rate', 0) * 100:.2f}% "
        f"{compact_money(stats.get('dev_hold_value_usd'))} | "
        f"买{compact_money(stats.get('dev_buy_usd'))} 卖{compact_money(stats.get('dev_sell_usd'))} "
        f"净{compact_money(stats.get('dev_netflow_usd'))} | "
        f"已卖{stats.get('dev_sell_amount_rate', 0) * 100:.1f}%"
    )

    return (
        f"${stats.get('symbol') or 'UNKNOWN'}\n"
        f"CA: {address}\n"
        f"链: {chain} | 查询时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        f"基础信息\n"
        f"- 市值: {compact_money(stats.get('mcap'))} | 持有人: {stats.get('holder_count', 0)} | 手续费: {stats.get('fee_sol', 0):.2f} SOL\n"
        f"- 流动性池: {stats.get('pool_label')} | 流动性: {compact_money(stats.get('pool_liquidity'))}\n"
        f"- 创建时间: {stats.get('created_time')} | 类型: {stats.get('token_age_type')} | 状态: {stats.get('verdict')}\n"
        f"- 交易量: {compact_money(stats.get('trade_volume_usd'))} | 买税: {stats.get('buy_tax_pct', 0):.2f}% | 卖税: {stats.get('sell_tax_pct', 0):.2f}%\n\n"
        f"标签钱包分析\n"
        f"{sm_detail}\n"
        f"{kol_detail}\n"
        f"{holder_tag_desc}\n\n"
        f"基础结构\n"
        f"{stats.get('rank_bucket_desc')}\n"
        f"- 捆绑持仓: {stats.get('associated_supply', 0):.1f}% | 钱包 {stats.get('associated_count', 0)}个 | 卖出进度 {stats.get('dump_progress', 0):.1f}%\n\n"
        f"Dev数据\n"
        f"{dev_detail}\n\n"
        f"{bottom_chip_history_text(bottom_analysis)}\n\n"
        f"{kline_analysis_text}"
        f"{deepseek_chip_text(deepseek_analysis)}"
        f"GMGN: https://gmgn.ai/{chain}/token/{address}"
    )

def analyze_and_reply(chat_id, message_id, address, chain="sol"):
    send_message(chat_id, f"收到 CA，开始轻量查询：Top100 holders + 5M K线 + 数据库历史快照...\n{address}", message_id)
    analysis = build_light_ca_analysis(address, chain=chain, limit=100)
    if not analysis:
        send_message(chat_id, f"查询失败或 GMGN 未返回 Top100 holders：\n{address}", message_id)
        return
    msg = "\n".join(
        line for line in build_light_ca_message(chain, address, analysis).splitlines()
        if not line.startswith("Top")
    )
    send_long_message(chat_id, msg, message_id)
    return
    send_message(chat_id, f"收到 CA，开始查询 GMGN Top100 筹码关联数据...\n{address}", message_id)
    stats = perform_deep_analysis(chain, address, {}, enforce_dev_risk=False)
    if not stats:
        send_message(chat_id, f"查询失败或该 CA 被风控条件跳过：\n{address}", message_id)
        return

    bottom_analysis = load_bottom_snapshot_analysis(address, chain=chain, limit=100, stats=stats)
    # DeepSeek analysis is disabled for now to avoid API usage.
    # deepseek_analysis = call_deepseek_chip_analysis(chain, address, stats, bottom_analysis)
    deepseek_analysis = ""

    # K-line volume & pattern analysis (golden dog patterns)
    kline_analysis = analyze_kline_volume_pattern(address, stats)
    kline_text = kline_volume_analysis_text(kline_analysis)

    msg = build_chip_alert_message(
        chain,
        address,
        stats,
        bottom_analysis=bottom_analysis,
        deepseek_analysis=deepseek_analysis,
        kline_analysis_text=kline_text,
    )
    send_long_message(chat_id, msg, message_id)
    return

    send_message(chat_id, msg[:3900] + "\n\n内容过长，已截断。", message_id)


def handle_update(update, allowed_ids):
    message = update.get("message") or update.get("edited_message") or {}
    text = message.get("text") or ""
    chat = message.get("chat") or {}
    chat_id = str(chat.get("id") or "")
    message_id = message.get("message_id")

    if not chat_id or not text:
        return
    if allowed_ids and chat_id not in allowed_ids:
        send_message(chat_id, "当前 chat 未授权使用 CA 筹码查询。", message_id)
        return

    addresses = extract_addresses(text)
    if not addresses:
        send_message(chat_id, "没有识别到 CA，请直接发送 Solana 代币地址。", message_id)
        return

    chain = CHAINS[0] if CHAINS else "sol"
    for address in addresses[:3]:
        try:
            analyze_and_reply(chat_id, message_id, address, chain=chain)
        except Exception as exc:
            send_message(chat_id, f"分析失败：{address}\n{exc}", message_id)


def main():
    if not TG_BOT_TOKEN:
        raise RuntimeError("TG_BOT_TOKEN 未配置")
    if GMGN_API_KEY:
        os.environ.setdefault("GMGN_API_KEY", GMGN_API_KEY)

    allowed_ids = allowed_chat_ids()
    offset = None
    print("TG CA 筹码关联查询机器人已启动...")
    print(f"Allowed chats: {', '.join(sorted(allowed_ids)) if allowed_ids else 'all'}")

    while True:
        try:
            payload = {"timeout": POLL_TIMEOUT}
            if offset is not None:
                payload["offset"] = offset
            updates = tg_api("getUpdates", payload) or []
            for update in updates:
                offset = update["update_id"] + 1
                handle_update(update, allowed_ids)
        except KeyboardInterrupt:
            break
        except Exception as exc:
            print(f"Polling error: {exc}")
            time.sleep(5)
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
