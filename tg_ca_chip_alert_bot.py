import json
import os
import re
import time
from datetime import datetime
from pathlib import Path

import requests

from config import GMGN_API_KEY, TG_BOT_TOKEN, TG_CHAT_ID
from tg_alert_stream import publish_tg_alert
from deep_alpha_pro import (
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
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "sk-b9fa593d50ce4e469d7645f530de2623")
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
    if value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"${value / 1_000:.1f}K"
    return f"${value:,.0f}"


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
    return desc


def raw_wallet_address(row):
    return str(row.get("address") or row.get("wallet_address") or row.get("account_address") or "").strip()


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
    candles = bottom_monitor.fetch_kline(address, kline_resolution)
    summary, holders = bottom_monitor.build_snapshot_json(token, raw_holders, candles, kline_resolution)
    raw_traders = fetch_token_traders(address, chain=chain, limit=100)
    history = bottom_monitor.recent_snapshots(address, limit=limit)
    analysis = bottom_monitor.analyze_snapshot_change(holders, history, summary)
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
    analysis["phase_wallet_flows"] = analyze_phase_wallet_flows(summary, holders, history[:DEEPSEEK_MAX_HISTORY])
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
        metric = f"卖{compact_money(item.get('sell_volume'))}" if mode == "sell" else f"买{compact_money(item.get('buy_volume'))}"
        hold = float(item.get("hold_pct") or 0)
        profit = float(item.get("profit") or 0)
        parts.append(f"{short_addr(item.get('wallet'))} {metric} 持{hold:.2%} 盈亏{compact_money(profit)}{suffix}")
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
            f"持{float(group.get('hold_pct') or 0):.2%}/"
            f"盈亏{compact_money(group.get('profit'))}/"
            f"均{format_chain_price(group.get('avg_cost'))}/中{format_chain_price(group.get('median_cost'))}"
        )
    return " | ".join(parts) if parts else "暂无分组命中"


def package_wallet_brief(wallets):
    parts = []
    for item in (wallets or [])[:5]:
        groups = ",".join(item.get("groups") or [])
        name = item.get("name") or short_addr(item.get("wallet"))
        group_text = f"/{groups}" if groups else ""
        parts.append(
            f"{name}{group_text} {short_addr(item.get('wallet'))} "
            f"持{float(item.get('hold_pct') or 0):.2%} "
            f"盈亏{compact_money(item.get('profit'))} "
            f"成本{format_chain_price(item.get('avg_cost'))}"
        )
    return " | ".join(parts) if parts else "暂无重点命中钱包"


def package_wallet_text(analysis):
    data = ((analysis or {}).get("trader_source_analysis") or {}).get("package_wallets") or {}
    if not data or int(data.get("count") or 0) <= 0:
        return "自定义钱包命中分析\n- 暂无 package_wallet_map.json 命中钱包"
    return (
        "自定义钱包命中分析\n"
        f"- 命中汇总: {int(data.get('count') or 0)}个 | 持仓{float(data.get('hold_pct') or 0):.2%} | "
        f"持仓金额{compact_money(data.get('usd_value'))} | 买{compact_money(data.get('buy_volume'))} | "
        f"卖{compact_money(data.get('sell_volume'))} | 净{compact_money(data.get('netflow'))} | "
        f"盈亏{compact_money(data.get('profit'))} | 均{format_chain_price(data.get('avg_cost'))} | "
        f"中{format_chain_price(data.get('median_cost'))}。\n"
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


def bottom_chip_history_text(analysis):
    if not analysis:
        return "最近100次筹码轨迹\n- 暂无 bottom_top100_snapshots 历史数据"
    current_ts = analysis.get("current_snapshot_ts")
    history_ts = analysis.get("latest_history_snapshot_ts")
    current_time = datetime.fromtimestamp(current_ts).strftime("%Y-%m-%d %H:%M:%S") if current_ts else "未知"
    history_time = datetime.fromtimestamp(history_ts).strftime("%Y-%m-%d %H:%M:%S") if history_ts else "暂无"
    return (
        f"实时Top100 + 最近100次筹码轨迹\n"
        f"- 当前查询: {current_time} | 当前Top100: {analysis.get('current_holder_count', 0)}个 | 历史快照: {analysis.get('snapshot_count', 0)}条\n"
        f"- 最近历史: {history_time}\n"
        f"{local_phase_rule_text(analysis)}\n\n"
        f"{trader_source_text(analysis)}"
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


def build_chip_alert_message(chain, address, stats, bottom_analysis=None, deepseek_analysis=""):
    reasons = ", ".join(stats.get("buy_reasons") or []) or "无明显加分项"
    holder_tag_desc = clean_holder_tag_desc(stats.get("holder_tag_desc"))

    return (
        f"${stats.get('symbol') or 'UNKNOWN'}\n"
        f"CA: {address}\n"
        f"链: {chain} | 查询时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        f"基础信息\n"
        f"- 市值: {compact_money(stats.get('mcap'))} | 持有人: {stats.get('holder_count', 0)} | 手续费: {stats.get('fee_sol', 0):.2f} SOL\n"
        f"- 流动性池: {stats.get('pool_label')} | 流动性: {compact_money(stats.get('pool_liquidity'))}\n"
        f"- 创建时间: {stats.get('created_time')} | 类型: {stats.get('token_age_type')} | 状态: {stats.get('verdict')}\n"
        f"- Smart Money: {stats.get('sm_count', 0)} | KOL: {stats.get('kol_count', 0)} | 狙击手: {stats.get('snipers', 0)}\n\n"
        f"市场结构\n"
        f"- {stats.get('market_structure')}\n"
        f"- {stats.get('market_structure_reason')}\n"
        f"- 可买评分: {stats.get('buy_score', 0)} | 理由: {reasons}\n"
        f"- 5m买/卖: {stats.get('buys_5m', 0)}/{stats.get('sells_5m', 0)}\n\n"
        f"资金关联分析 Top100\n"
        f"- 疑似关联控盘: {stats.get('control_ratio', 0):.1f}%\n"
        f"- 同资金/Token来源: {stats.get('source_cluster_desc')}\n"
        f"- 同源持仓: {stats.get('source_cluster_supply', 0):.2f}% | {compact_money(stats.get('source_cluster_usd_value'))} | Token {stats.get('source_cluster_amount', 0):,.0f}\n"
        f"- 同源买卖: 买入 {compact_money(stats.get('source_cluster_buy_volume'))} | 卖出 {compact_money(stats.get('source_cluster_sell_volume'))} | 净流 {compact_money(stats.get('source_cluster_netflow'))}\n"
        f"- 庄家出货进度: {stats.get('dump_progress', 0):.1f}%\n\n"
        f"标签钱包分析\n"
        f"{holder_tag_desc}\n\n"
        f"成本线分析\n"
        f"- 链上价(x1e9): {format_chain_price(stats.get('price'))}\n"
        f"- Top20成本: {format_chain_price(stats.get('top20_avg_cost'))} | 盈亏 {format_pnl_pct(stats.get('price'), stats.get('top20_avg_cost'))}\n"
        f"- Top50成本: {format_chain_price(stats.get('top50_avg_cost'))} | 盈亏 {format_pnl_pct(stats.get('price'), stats.get('top50_avg_cost'))}\n"
        f"- Top100成本: {format_chain_price(stats.get('top100_avg_cost'))} | 盈亏 {format_pnl_pct(stats.get('price'), stats.get('top100_avg_cost'))}\n"
        f"- 主成本区: {stats.get('dominant_cost_band')} {stats.get('dominant_cost_band_count', 0)}个/{stats.get('dominant_cost_band_supply', 0):.1f}%\n"
        f"- 区间分布:\n{stats.get('cost_band_desc')}\n\n"
        f"基础结构\n"
        f"{stats.get('rank_bucket_desc')}\n"
        f"- 捆绑持仓: {stats.get('associated_supply', 0):.1f}% | 钱包 {stats.get('associated_count', 0)}个 | 卖出进度 {stats.get('dump_progress', 0):.1f}%\n\n"
        f"{bottom_chip_history_text(bottom_analysis)}\n\n"
        f"{deepseek_chip_text(deepseek_analysis)}"
        f"GMGN: https://gmgn.ai/{chain}/token/{address}"
    )

def analyze_and_reply(chat_id, message_id, address, chain="sol"):
    send_message(chat_id, f"收到 CA，开始查询 GMGN Top100 筹码关联数据...\n{address}", message_id)
    stats = perform_deep_analysis(chain, address, {}, enforce_dev_risk=False)
    if not stats:
        send_message(chat_id, f"查询失败或该 CA 被风控条件跳过：\n{address}", message_id)
        return

    bottom_analysis = load_bottom_snapshot_analysis(address, chain=chain, limit=100, stats=stats)
    # DeepSeek analysis is disabled for now to avoid API usage.
    # deepseek_analysis = call_deepseek_chip_analysis(chain, address, stats, bottom_analysis)
    deepseek_analysis = ""
    msg = build_chip_alert_message(
        chain,
        address,
        stats,
        bottom_analysis=bottom_analysis,
        deepseek_analysis=deepseek_analysis,
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
