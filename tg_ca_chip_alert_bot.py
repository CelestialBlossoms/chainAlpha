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


POLL_TIMEOUT = 25
POLL_INTERVAL = 1
REQUEST_TIMEOUT = 30
ADDRESS_RE = re.compile(r"\b[1-9A-HJ-NP-Za-km-z]{32,50}\b")


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


def build_chip_alert_message(chain, address, stats):
    reasons = ", ".join(stats.get("buy_reasons") or []) or "无明显加分项"
    icon = "高风险" if stats.get("is_dumping") else "筹码报警"

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
        f"- 可买评分: {stats.get('buy_score', 0)} | 理由: {reasons}\n\n"
        f"K线量价\n"
        f"- 判定: {stats.get('kline_verdict')} | 尾段量能: {stats.get('kline_volume_ratio', 0):.2f}x | K线数: {stats.get('kline_candle_count', 0)}\n"
        f"- 冲高回落: {stats.get('spike_retreat_pct', 0):.1f}% | 低点反弹: {stats.get('recovery_from_low_pct', 0):.1f}%\n"
        f"- 5m买/卖: {stats.get('buys_5m', 0)}/{stats.get('sells_5m', 0)}\n\n"
        f"资金关联分析 Top100\n"
        f"- 疑似关联控盘: {stats.get('control_ratio', 0):.1f}%\n"
        f"- 同资金/Token来源: {stats.get('source_cluster_desc')}\n"
        f"- 同源持仓: {stats.get('source_cluster_supply', 0):.2f}% | {compact_money(stats.get('source_cluster_usd_value'))} | Token {stats.get('source_cluster_amount', 0):,.0f}\n"
        f"- 同源买卖: 买入 {compact_money(stats.get('source_cluster_buy_volume'))} | 卖出 {compact_money(stats.get('source_cluster_sell_volume'))} | 净流 {compact_money(stats.get('source_cluster_netflow'))}\n"
        f"- 庄家出货进度: {stats.get('dump_progress', 0):.1f}%\n\n"
        f"标签钱包分析\n"
        f"{stats.get('holder_tag_desc')}\n\n"
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
        f"GMGN: https://gmgn.ai/{chain}/token/{address}"
    )


def analyze_and_reply(chat_id, message_id, address, chain="sol"):
    send_message(chat_id, f"收到 CA，开始查询 GMGN Top100 筹码关联数据...\n{address}", message_id)
    stats = perform_deep_analysis(chain, address, {}, enforce_dev_risk=False)
    if not stats:
        send_message(chat_id, f"查询失败或该 CA 被风控条件跳过：\n{address}", message_id)
        return

    msg = build_chip_alert_message(chain, address, stats)
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
