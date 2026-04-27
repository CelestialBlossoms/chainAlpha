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
    summary, holders = bottom_monitor.build_snapshot_json(token, raw_holders)
    history = bottom_monitor.recent_snapshots(address, limit=limit)
    analysis = bottom_monitor.analyze_snapshot_change(holders, history, summary)
    analysis["snapshot_count"] = len(history)
    analysis["current_holder_count"] = len(holders)
    analysis["current_snapshot_ts"] = int(time.time())
    analysis["latest_history_snapshot_ts"] = history[0].get("snapshot_ts") if history else None
    analysis["current_top_holders"] = [compact_holder(holder) for holder in holders[:100]]
    analysis["history_top100_holders"] = [
        {
            "snapshot_ts": snap.get("snapshot_ts"),
            "holders": [compact_holder(holder) for holder in (snap.get("holders") or [])[:100]],
        }
        for snap in history[:DEEPSEEK_MAX_HISTORY]
    ]
    return analysis


def bottom_chip_history_text(analysis):
    if not analysis:
        return "最近100次筹码轨迹\n- 暂无 bottom_top100_snapshots 历史数据"
    current_ts = analysis.get("current_snapshot_ts")
    history_ts = analysis.get("latest_history_snapshot_ts")
    current_time = datetime.fromtimestamp(current_ts).strftime("%Y-%m-%d %H:%M:%S") if current_ts else "未知"
    history_time = datetime.fromtimestamp(history_ts).strftime("%Y-%m-%d %H:%M:%S") if history_ts else "暂无"
    reasons = ", ".join(analysis.get("reasons") or []) or "无"
    return (
        f"实时Top100 + 最近100次筹码轨迹\n"
        f"- 当前查询: {current_time} | 当前Top100: {analysis.get('current_holder_count', 0)}个 | 历史快照: {analysis.get('snapshot_count', 0)}条\n"
        f"- 最近历史: {history_time}\n"
        f"- 类型: {bottom_monitor.signal_type_text(analysis.get('signal_type'))} | 分数: {analysis.get('score', 0)}\n"
        f"- 窗口增持: {analysis.get('window_accumulation_pct_delta', 0):.2%} | "
        f"窗口减持: {analysis.get('window_distribution_pct_delta', 0):.2%} | "
        f"窗口净买入: {compact_money(analysis.get('window_netflow_usd'))}\n"
        f"- 本轮增持: {analysis.get('accumulation_pct_delta', 0):.2%} | "
        f"本轮减持: {analysis.get('distribution_pct_delta', 0):.2%} | "
        f"换筹比: {analysis.get('rotation_score', 0):.2f}\n"
        f"{bottom_monitor.wallet_behavior_text(analysis)}\n"
        f"- 理由: {reasons}"
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
        "data_note": "Only raw compressed Top100 holder snapshots are provided. No local signal conclusion, score, reasons, or precomputed wallet behavior labels are included.",
        "current_top100_holders": (bottom_analysis or {}).get("current_top_holders") or [],
        "history_top100_snapshots": (bottom_analysis or {}).get("history_top100_holders") or [],
    }


def call_deepseek_chip_analysis(chain, address, stats, bottom_analysis):
    if not DEEPSEEK_ENABLED or not DEEPSEEK_API_KEY or not bottom_analysis:
        return ""
    payload = build_deepseek_payload(chain, address, stats, bottom_analysis)
    prompt = (
        "你是链上筹码操纵分析助手。输入只包含当前实时Top100持仓和最近历史Top100持仓快照，"
        "没有本地预计算结论。请你自己基于钱包持仓占比、买入/卖出、净流、排名变化、成本、标签和进出Top100情况，"
        "识别该CA最近是否存在吸筹、换筹、出货、早期钱包持续卖出或砸盘风险。"
        "请用中文输出，限制在10行内。必须包含：总体结论、吸筹钱包、出货钱包、换筹证据、风险点、后续观察条件。"
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
