import json
import os
import re
import time
from datetime import datetime

import requests

from config import GMGN_API_KEY, TG_BOT_TOKEN, TG_CHAT_ID
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
ADDRESS_RE = re.compile(r"\b[1-9A-HJ-NP-Za-km-z]{32,50}\b")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "sk-b9fa593d50ce4e469d7645f530de2623")
DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-v4-pro")
DEEPSEEK_TIMEOUT = int(os.getenv("DEEPSEEK_TIMEOUT", "45"))
DEEPSEEK_MAX_HISTORY = int(os.getenv("DEEPSEEK_MAX_HISTORY", "100"))
DEEPSEEK_KLINE_CANDLES_PER_SNAPSHOT = int(os.getenv("DEEPSEEK_KLINE_CANDLES_PER_SNAPSHOT", "12"))
PHASE_UP_PCT = float(os.getenv("DEEPSEEK_PHASE_UP_PCT", "0.12"))
PHASE_DOWN_PCT = float(os.getenv("DEEPSEEK_PHASE_DOWN_PCT", "-0.12"))
DEEPSEEK_ENABLED = os.getenv("DEEPSEEK_ENABLED", "1") != "0"
DEEPSEEK_THINKING = os.getenv("DEEPSEEK_THINKING", "enabled")
DEEPSEEK_REASONING_EFFORT = os.getenv("DEEPSEEK_REASONING_EFFORT", "high")


def allowed_chat_ids():
    configured = os.getenv("TG_CA_QUERY_ALLOWED_CHATS") or str(TG_CHAT_ID or "")
    return {item.strip() for item in configured.split(",") if item.strip()}


def tg_api(method, payload):
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/{method}"
    resp = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
    if not resp.ok:
        raise RuntimeError(f"Telegram {method} failed: http={resp.status_code} {resp.text[:300]}")
    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram {method} failed: {data}")
    return data.get("result")


def send_message(chat_id, text, reply_to_message_id=None):
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    if reply_to_message_id:
        payload["reply_to_message_id"] = reply_to_message_id
    return tg_api("sendMessage", payload)


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
    phases = {
        "up": {"intervals": 0, "wallets": {}, "top100_delta": 0.0, "netflow": 0.0, "price_changes": []},
        "down": {"intervals": 0, "wallets": {}, "top100_delta": 0.0, "netflow": 0.0, "price_changes": []},
        "sideways": {"intervals": 0, "wallets": {}, "top100_delta": 0.0, "netflow": 0.0, "price_changes": []},
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


def load_bottom_snapshot_analysis(address, chain="sol", limit=100, stats=None):
    raw_holders = bottom_monitor.fetch_top100_holders(address)
    if not raw_holders:
        return None

    token = {
        "address": address,
        "symbol": (stats or {}).get("symbol"),
        "market_cap": (stats or {}).get("mcap"),
        "price": (stats or {}).get("price"),
        "liquidity": (stats or {}).get("pool_liquidity"),
        "created_at": (stats or {}).get("created_at"),
        "fee_sol": (stats or {}).get("fee_sol"),
    }
    kline_resolution = bottom_monitor.token_kline_resolution(token)
    candles = bottom_monitor.fetch_kline(address, kline_resolution)
    summary, holders = bottom_monitor.build_snapshot_json(token, raw_holders, candles, kline_resolution)
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
        parts.append(f"{short} {hold_delta:+.2%} 净{compact_money(netflow)}{suffix}")
    return " | ".join(parts) if parts else "证据不足"


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
    current_kline = (analysis or {}).get("current_kline") or {}
    kline_summary = current_kline.get("summary") or {}
    stage = local_stage_conclusion(phase_flows)
    risks = []
    if down.get("sellers_or_distributors"):
        risks.append(f"下跌阶段出货钱包 {wallet_brief(down.get('sellers_or_distributors'), limit=2, sign='-')}")
    if float(down.get("top100_hold_delta") or 0) < 0:
        risks.append(f"下跌阶段Top100减持{fmt_pct(abs(float(down.get('top100_hold_delta') or 0)))}")
    if float(kline_summary.get("change_pct") or 0) < -30:
        risks.append(f"当前K线跌幅{kline_summary.get('change_pct'):.1f}%")
    if not risks:
        risks.append("未见明确砸盘证据")
    return (
        f"本地规则阶段分析\n"
        f"- 阶段结论: {stage}\n"
        f"- K线证据: 总涨跌{phase_flows.get('price_change_total_pct', 0):.2f}% | "
        f"现价{phase_flows.get('current_price', 0)} | 高{phase_flows.get('price_high', 0)} | 低{phase_flows.get('price_low', 0)}\n"
        f"- Top100持仓变化: {fmt_pct(top100.get('first'))}->{fmt_pct(top100.get('last'))} "
        f"({float(top100.get('delta') or 0):+.2%}) | Top20 {fmt_pct(top20.get('first'))}->{fmt_pct(top20.get('last'))} "
        f"({float(top20.get('delta') or 0):+.2%})\n"
        f"- 吸筹钱包: 横盘 {wallet_brief(sideways.get('buyers_or_accumulators'))} | 上涨 {wallet_brief(up.get('buyers_or_accumulators'), limit=2)}\n"
        f"- 出货钱包: 下跌 {wallet_brief(down.get('sellers_or_distributors'), sign='-')}\n"
        f"- 换筹证据: 横盘Top100变化{float(sideways.get('top100_hold_delta') or 0):+.2%} | "
        f"横盘净流{compact_money(sideways.get('netflow_delta'))} | 下跌净流{compact_money(down.get('netflow_delta'))}\n"
        f"- 风险点: {'；'.join(risks)}"
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
        f"{local_phase_rule_text(analysis)}"
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
        "请用中文输出，必须包含：阶段结论、K线证据、Top100持仓变化、吸筹钱包、出货钱包、换筹证据、风险点、后续观察条件。"
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
    icon = "高风险" if stats.get("is_dumping") else "筹码报警"
    holder_tag_desc = clean_holder_tag_desc(stats.get("holder_tag_desc"))

    return (
        f"{icon} | ${stats.get('symbol') or 'UNKNOWN'}\n"
        f"CA: {address}\n"
        f"链: {chain} | 查询时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        f"基础信息\n"
        f"- 市值: {compact_money(stats.get('mcap'))} | 持有人: {stats.get('holder_count', 0)} | 手续费: {stats.get('fee_sol', 0):.2f} SOL\n"
        f"- 流动性池: {stats.get('pool_label')} | 流动性: {compact_money(stats.get('pool_liquidity'))}\n"
        f"- 创建时间: {stats.get('created_time')} | 类型: {stats.get('token_age_type')} | 状态: {stats.get('verdict')}\n"
        f"- Smart Money: {stats.get('sm_count', 0)} | KOL: {stats.get('kol_count', 0)} | 狙击手: {stats.get('snipers', 0)} | 风险分数: {stats.get('rug_ratio')}\n\n"
        f"市场结构\n"
        f"- {stats.get('market_structure')} | 风险: {stats.get('market_structure_risk')}\n"
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
    deepseek_analysis = call_deepseek_chip_analysis(chain, address, stats, bottom_analysis)
    msg = build_chip_alert_message(
        chain,
        address,
        stats,
        bottom_analysis=bottom_analysis,
        deepseek_analysis=deepseek_analysis,
    )
    if len(msg) <= 4000:
        send_message(chat_id, msg, message_id)
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
