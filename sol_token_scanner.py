#!/usr/bin/env python3
"""
Solana token scanner using gmgn-cli.

Pipeline:
1. Discover every scan from GMGN market ranking/trending data.
2. Track survivors across scans for holder/liquidity/pressure changes.
3. Deep-analyze survivors with token info.
4. Notify Telegram with CA, market cap, age, narrative, KOL, top holders, score.

Environment:
    TG_BOT_TOKEN     Telegram bot token, optional unless --notify is used.
    TG_CHAT_ID       Telegram chat id, optional unless --notify is used.

Usage:
    python sol_token_scanner.py --once
    python sol_token_scanner.py --watch --notify
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


CHAIN = "sol"
DEFAULT_INTERVAL_SEC = 30
DEFAULT_LIMIT = 100
STATE_PATH = Path("sol_token_scanner_state.json")
DISCOVERY_INTERVALS = ("1m", "5m")

# Stage 1: fast discovery filters.
MIN_HOLDERS = 70
MAX_AGE_SEC = 6 * 60 * 60
MIN_LIQUIDITY_USD = 20_000
MIN_MARKET_CAP_USD = 30_000
MAX_MARKET_CAP_USD = 8_000_000
MAX_TOP10_HOLDER_RATE = 0.28
MAX_DEV_TEAM_HOLD_RATE = 0.03
MAX_WASH_TRADING = 0

# Stage 2: survivor tracking filters.
MIN_SCANS_BEFORE_DEEP = 2
MIN_HOLDER_GROWTH_PCT = -0.05
MAX_LIQUIDITY_DROP_PCT = -0.30
MIN_BUY_PRESSURE = 0.50
MAX_BUNDLER_RATE = 0.35
MAX_BOT_DEGEN_RATE = 0.25

# Stage 3: deep-analysis filters.
MAX_RUG_RATIO = 0.25
MAX_ENTRAPMENT_RATIO = 0.25
MAX_SOCIAL_DUPLICATES = 5
MIN_SCORE_TO_ALERT = 70
ALERT_COOLDOWN_SEC = 6 * 60 * 60

# Top-holder chip movement analysis.
TOP_HOLDER_LIMIT = 100
MIN_TOP100_NETFLOW_USD = 20_000
MIN_TOP100_NET_BUY_COUNT = 8
MIN_TOP100_ACCUMULATION_RATIO = 0.58


def now_ts() -> int:
    return int(time.time())


def to_float(value: Any, default: float = 0.0) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return default
    return default


def to_int(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(float(value))
        except ValueError:
            return default
    return default


def pct(value: float) -> str:
    return f"{value * 100:.2f}%"


def usd(value: float) -> str:
    if value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"${value / 1_000:.1f}K"
    return f"${value:.0f}"


def age_text(creation_ts: int) -> str:
    if not creation_ts:
        return "unknown"
    seconds = max(0, now_ts() - creation_ts)
    if seconds < 3600:
        return f"{seconds // 60}m"
    if seconds < 86400:
        return f"{seconds / 3600:.1f}h"
    return f"{seconds / 86400:.1f}d"


def find_gmgn_cli() -> str | None:
    candidates = [
        shutil.which("gmgn-cli.cmd"),
        shutil.which("gmgn-cli"),
        str(Path.home() / "AppData" / "Roaming" / "npm" / "gmgn-cli.cmd"),
    ]
    for candidate in candidates:
        if candidate and Path(candidate).exists():
            return candidate
    return None


def run_gmgn(args: list[str], timeout: int = 30) -> dict[str, Any] | None:
    gmgn_cli = find_gmgn_cli()
    if not gmgn_cli:
        print("gmgn-cli not found. Install it with: npm install -g gmgn-cli", file=sys.stderr)
        return None
    cmd = [gmgn_cli, *args, "--raw"]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
        )
    except FileNotFoundError:
        print(f"gmgn-cli executable not found: {gmgn_cli}", file=sys.stderr)
        return None
    except subprocess.TimeoutExpired:
        print(f"gmgn-cli timeout: {' '.join(args)}", file=sys.stderr)
        return None

    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        print(f"gmgn-cli failed: {stderr[:300]}", file=sys.stderr)
        return None

    raw = (result.stdout or "").strip()
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        print(f"Invalid JSON from gmgn-cli: {raw[:300]}", file=sys.stderr)
        return None
    return data if isinstance(data, dict) else None


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"tokens": {}, "alerts": {}}
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return {"tokens": {}, "alerts": {}}
    data.setdefault("tokens", {})
    data.setdefault("alerts", {})
    return data


def save_state(path: Path, state: dict[str, Any]) -> None:
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2, sort_keys=True)
    tmp.replace(path)


def fetch_rank_interval(interval: str, limit: int) -> list[dict[str, Any]]:
    data = run_gmgn(
        [
            "market",
            "trending",
            "--chain",
            CHAIN,
            "--interval",
            interval,
            "--order-by",
            "volume",
            "--direction",
            "desc",
            "--filter",
            "renounced",
            "--filter",
            "frozen",
            "--filter",
            "not_wash_trading",
            "--limit",
            str(limit),
        ]
    )
    if not data:
        return []
    rank = data.get("data", {}).get("rank", [])
    if not isinstance(rank, list):
        return []
    for token in rank:
        if isinstance(token, dict):
            token.setdefault("discovery_intervals", [])
            token["discovery_intervals"].append(interval)
    return rank


def fetch_rank(limit: int, intervals: tuple[str, ...] = DISCOVERY_INTERVALS) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for interval in intervals:
        for token in fetch_rank_interval(interval, limit):
            address = str(token.get("address") or "")
            if not address:
                continue
            if address not in merged:
                merged[address] = token
                continue
            existing = merged[address]
            existing_intervals = set(existing.get("discovery_intervals") or [])
            existing_intervals.update(token.get("discovery_intervals") or [])
            existing["discovery_intervals"] = sorted(existing_intervals)
            if to_float(token.get("volume")) > to_float(existing.get("volume")):
                for key, value in token.items():
                    if key != "discovery_intervals":
                        existing[key] = value
    return list(merged.values())


def fetch_token_info(address: str) -> dict[str, Any] | None:
    return run_gmgn(["token", "info", "--chain", CHAIN, "--address", address])


def fetch_top_holders(address: str) -> list[dict[str, Any]]:
    data = run_gmgn(
        [
            "token",
            "holders",
            "--chain",
            CHAIN,
            "--address",
            address,
            "--limit",
            str(TOP_HOLDER_LIMIT),
        ],
        timeout=45,
    )
    if not data:
        return []
    holders = data.get("list", [])
    return holders if isinstance(holders, list) else []


@dataclass
class TokenView:
    address: str
    symbol: str
    name: str
    market_cap: float
    price: float
    liquidity: float
    volume: float
    swaps: int
    buys: int
    sells: int
    holder_count: int
    creation_ts: int
    top10_holder_rate: float
    dev_team_hold_rate: float
    rug_ratio: float
    entrapment_ratio: float
    bundler_rate: float
    bot_degen_rate: float
    smart_degen_count: int
    renowned_count: int
    image_dup: int
    twitter_dup: int
    telegram_dup: int
    website_dup: int
    is_wash_trading: bool
    narrative: str
    discovery_intervals: tuple[str, ...]


def normalize_token(raw: dict[str, Any]) -> TokenView:
    buys = to_int(raw.get("buys"))
    sells = to_int(raw.get("sells"))
    symbol = str(raw.get("symbol") or "?")[:24]
    name = str(raw.get("name") or "")[:80]
    narrative = (
        raw.get("trans_name_zhcn")
        or raw.get("twitter_username")
        or raw.get("website")
        or raw.get("telegram")
        or name
        or symbol
    )
    return TokenView(
        address=str(raw.get("address") or ""),
        symbol=symbol,
        name=name,
        market_cap=to_float(raw.get("market_cap") or raw.get("usd_market_cap")),
        price=to_float(raw.get("price")),
        liquidity=to_float(raw.get("liquidity")),
        volume=to_float(raw.get("volume") or raw.get("volume_24h")),
        swaps=to_int(raw.get("swaps"), buys + sells),
        buys=buys,
        sells=sells,
        holder_count=to_int(raw.get("holder_count")),
        creation_ts=to_int(raw.get("creation_timestamp") or raw.get("open_timestamp")),
        top10_holder_rate=to_float(raw.get("top_10_holder_rate")),
        dev_team_hold_rate=to_float(raw.get("dev_team_hold_rate")),
        rug_ratio=to_float(raw.get("rug_ratio")),
        entrapment_ratio=to_float(raw.get("entrapment_ratio")),
        bundler_rate=to_float(raw.get("bundler_rate")),
        bot_degen_rate=to_float(raw.get("bot_degen_rate")),
        smart_degen_count=to_int(raw.get("smart_degen_count")),
        renowned_count=to_int(raw.get("renowned_count")),
        image_dup=to_int(raw.get("image_dup")),
        twitter_dup=to_int(raw.get("twitter_dup")),
        telegram_dup=to_int(raw.get("telegram_dup")),
        website_dup=to_int(raw.get("website_dup")),
        is_wash_trading=bool(raw.get("is_wash_trading")),
        narrative=str(narrative)[:120],
        discovery_intervals=tuple(raw.get("discovery_intervals") or ()),
    )


def pass_discovery(t: TokenView) -> tuple[bool, str]:
    age = now_ts() - t.creation_ts if t.creation_ts else 10**9
    checks = [
        (t.address != "", "missing address"),
        (t.holder_count >= MIN_HOLDERS, f"holders {t.holder_count} < {MIN_HOLDERS}"),
        (age <= MAX_AGE_SEC, f"age {age_text(t.creation_ts)} > {MAX_AGE_SEC // 3600}h"),
        (t.liquidity >= MIN_LIQUIDITY_USD, f"liquidity {usd(t.liquidity)} too low"),
        (t.market_cap >= MIN_MARKET_CAP_USD, f"mcap {usd(t.market_cap)} too low"),
        (t.market_cap <= MAX_MARKET_CAP_USD, f"mcap {usd(t.market_cap)} too high"),
        (t.top10_holder_rate <= MAX_TOP10_HOLDER_RATE, f"top10 {pct(t.top10_holder_rate)} too high"),
        (t.dev_team_hold_rate <= MAX_DEV_TEAM_HOLD_RATE, f"dev hold {pct(t.dev_team_hold_rate)} too high"),
        (not t.is_wash_trading or MAX_WASH_TRADING, "wash trading"),
    ]
    for ok, reason in checks:
        if not ok:
            return False, reason
    return True, "ok"


def update_tracking(state: dict[str, Any], t: TokenView) -> dict[str, Any]:
    tokens = state.setdefault("tokens", {})
    item = tokens.setdefault(
        t.address,
        {
            "first_seen": now_ts(),
            "scan_count": 0,
            "first_holder_count": t.holder_count,
            "first_liquidity": t.liquidity,
            "first_market_cap": t.market_cap,
            "max_market_cap": t.market_cap,
        },
    )
    item["scan_count"] = int(item.get("scan_count", 0)) + 1
    item["last_seen"] = now_ts()
    item["symbol"] = t.symbol
    item["name"] = t.name
    item["address"] = t.address
    item["market_cap"] = t.market_cap
    item["price"] = t.price
    item["liquidity"] = t.liquidity
    item["volume"] = t.volume
    item["holder_count"] = t.holder_count
    item["top10_holder_rate"] = t.top10_holder_rate
    item["bundler_rate"] = t.bundler_rate
    item["bot_degen_rate"] = t.bot_degen_rate
    item["buy_pressure"] = buy_pressure(t)
    item["discovery_intervals"] = list(t.discovery_intervals)
    item["max_market_cap"] = max(to_float(item.get("max_market_cap")), t.market_cap)
    return item


def buy_pressure(t: TokenView) -> float:
    total = t.buys + t.sells
    if total <= 0:
        return 0.0
    return t.buys / total


def growth_pct(current: float, initial: float) -> float:
    if initial <= 0:
        return 0.0
    return (current - initial) / initial


def pass_tracking(item: dict[str, Any], t: TokenView) -> tuple[bool, str]:
    holder_growth = growth_pct(t.holder_count, to_float(item.get("first_holder_count")))
    liquidity_change = growth_pct(t.liquidity, to_float(item.get("first_liquidity")))
    checks = [
        (to_int(item.get("scan_count")) >= MIN_SCANS_BEFORE_DEEP, "needs more scans"),
        (holder_growth >= MIN_HOLDER_GROWTH_PCT, f"holder growth {pct(holder_growth)} weak"),
        (liquidity_change >= MAX_LIQUIDITY_DROP_PCT, f"liquidity change {pct(liquidity_change)} weak"),
        (buy_pressure(t) >= MIN_BUY_PRESSURE, f"buy pressure {pct(buy_pressure(t))} weak"),
        (t.bundler_rate <= MAX_BUNDLER_RATE, f"bundler {pct(t.bundler_rate)} too high"),
        (t.bot_degen_rate <= MAX_BOT_DEGEN_RATE, f"bot {pct(t.bot_degen_rate)} too high"),
    ]
    for ok, reason in checks:
        if not ok:
            return False, reason
    return True, "ok"


def merge_deep_info(t: TokenView, info: dict[str, Any] | None) -> TokenView:
    if not info:
        return t
    stat = info.get("stat") if isinstance(info.get("stat"), dict) else {}
    dev = info.get("dev") if isinstance(info.get("dev"), dict) else {}
    link = info.get("link") if isinstance(info.get("link"), dict) else {}
    market_cap = t.market_cap
    price = to_float(info.get("price"), t.price)
    supply = to_float(info.get("circulating_supply"))
    if price > 0 and supply > 0:
        market_cap = price * supply
    narrative = (
        link.get("description")
        or link.get("twitter_username")
        or link.get("website")
        or t.narrative
    )
    return TokenView(
        **{
            **t.__dict__,
            "market_cap": market_cap,
            "price": price,
            "liquidity": to_float(info.get("liquidity"), t.liquidity),
            "holder_count": to_int(info.get("holder_count"), t.holder_count),
            "top10_holder_rate": to_float(dev.get("top_10_holder_rate") or stat.get("top_10_holder_rate"), t.top10_holder_rate),
            "dev_team_hold_rate": to_float(stat.get("dev_team_hold_rate"), t.dev_team_hold_rate),
            "bundler_rate": to_float(stat.get("top_bundler_trader_percentage"), t.bundler_rate),
            "bot_degen_rate": to_float(stat.get("bot_degen_rate"), t.bot_degen_rate),
            "entrapment_ratio": to_float(stat.get("top_entrapment_trader_percentage"), t.entrapment_ratio),
            "narrative": str(narrative or t.narrative)[:120],
        }
    )


def pass_deep_analysis(t: TokenView) -> tuple[bool, str]:
    max_dup = max(t.image_dup, t.twitter_dup, t.telegram_dup, t.website_dup)
    checks = [
        (t.rug_ratio <= MAX_RUG_RATIO, f"rug {pct(t.rug_ratio)} too high"),
        (t.entrapment_ratio <= MAX_ENTRAPMENT_RATIO, f"entrapment {pct(t.entrapment_ratio)} too high"),
        (max_dup <= MAX_SOCIAL_DUPLICATES, f"social dup {max_dup} too high"),
        (t.top10_holder_rate <= MAX_TOP10_HOLDER_RATE, f"top10 {pct(t.top10_holder_rate)} too high"),
    ]
    for ok, reason in checks:
        if not ok:
            return False, reason
    return True, "ok"


def analyze_top_holders(holders: list[dict[str, Any]]) -> dict[str, Any]:
    non_pool = [
        holder
        for holder in holders
        if to_int(holder.get("addr_type")) != 2
        and "pool" not in str(holder.get("exchange") or "").lower()
    ]
    buy_volume = sum(to_float(h.get("buy_volume_cur")) for h in non_pool)
    sell_volume = sum(to_float(h.get("sell_volume_cur")) for h in non_pool)
    netflow = sum(to_float(h.get("netflow_usd")) for h in non_pool)
    net_buy_count = sum(1 for h in non_pool if to_float(h.get("netflow_usd")) > 0)
    net_sell_count = sum(1 for h in non_pool if to_float(h.get("netflow_usd")) < 0)
    active_buy_count = sum(1 for h in non_pool if to_float(h.get("buy_volume_cur")) > 0)
    active_sell_count = sum(1 for h in non_pool if to_float(h.get("sell_volume_cur")) > 0)
    top100_pct = sum(to_float(h.get("amount_percentage")) for h in holders)
    non_pool_pct = sum(to_float(h.get("amount_percentage")) for h in non_pool)
    top10_pct = sum(to_float(h.get("amount_percentage")) for h in holders[:10])
    ratio = net_buy_count / max(1, net_buy_count + net_sell_count)
    accumulation = (
        netflow >= MIN_TOP100_NETFLOW_USD
        and net_buy_count >= MIN_TOP100_NET_BUY_COUNT
        and ratio >= MIN_TOP100_ACCUMULATION_RATIO
        and buy_volume > sell_volume
    )
    top_buyers = sorted(non_pool, key=lambda h: to_float(h.get("netflow_usd")), reverse=True)[:3]
    top_sellers = sorted(non_pool, key=lambda h: to_float(h.get("netflow_usd")))[:3]
    return {
        "holder_count_checked": len(holders),
        "non_pool_count": len(non_pool),
        "top100_pct": top100_pct,
        "non_pool_pct": non_pool_pct,
        "top10_pct": top10_pct,
        "buy_volume": buy_volume,
        "sell_volume": sell_volume,
        "netflow": netflow,
        "net_buy_count": net_buy_count,
        "net_sell_count": net_sell_count,
        "active_buy_count": active_buy_count,
        "active_sell_count": active_sell_count,
        "accumulation_ratio": ratio,
        "accumulation": accumulation,
        "top_buyers": summarize_holders(top_buyers),
        "top_sellers": summarize_holders(top_sellers),
    }


def summarize_holders(holders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summary = []
    for holder in holders:
        tags = holder.get("maker_token_tags") or holder.get("tags") or []
        summary.append(
            {
                "rank": holder.get("wallet_tag_v2") or "",
                "address": str(holder.get("address") or "")[:8],
                "name": holder.get("name") or "",
                "pct": to_float(holder.get("amount_percentage")),
                "usd": to_float(holder.get("usd_value")),
                "netflow": to_float(holder.get("netflow_usd")),
                "buy": to_float(holder.get("buy_volume_cur")),
                "sell": to_float(holder.get("sell_volume_cur")),
                "tags": ",".join(str(tag) for tag in tags[:3]) if isinstance(tags, list) else str(tags),
            }
        )
    return summary


def holder_flow_text(flow: dict[str, Any]) -> str:
    if not flow:
        return "Top100筹码流向：暂无数据"
    status = "确认吸筹" if flow.get("accumulation") else "未确认吸筹"
    return (
        f"Top100筹码流向：{status} | 净流入={usd(to_float(flow.get('netflow')))} "
        f"| 买入={usd(to_float(flow.get('buy_volume')))} 卖出={usd(to_float(flow.get('sell_volume')))} "
        f"| 净买钱包={flow.get('net_buy_count')}/{to_int(flow.get('net_buy_count')) + to_int(flow.get('net_sell_count'))} "
        f"| Top100占比={pct(to_float(flow.get('top100_pct')))} 非池子占比={pct(to_float(flow.get('non_pool_pct')))}"
    )


def score_token(t: TokenView, item: dict[str, Any]) -> int:
    score = 50
    holder_growth = growth_pct(t.holder_count, to_float(item.get("first_holder_count")))
    liquidity_change = growth_pct(t.liquidity, to_float(item.get("first_liquidity")))

    score += min(15, int(holder_growth * 100))
    score += min(10, int(max(0.0, liquidity_change) * 50))
    score += int((buy_pressure(t) - 0.5) * 40)
    score += min(10, t.smart_degen_count * 2)
    score += min(8, t.renowned_count * 2)
    score -= int(t.top10_holder_rate * 50)
    score -= int(t.bundler_rate * 35)
    score -= int(t.bot_degen_rate * 25)
    score -= int(t.rug_ratio * 60)
    score -= int(t.entrapment_ratio * 50)
    holder_flow = item.get("holder_flow") if isinstance(item.get("holder_flow"), dict) else {}
    if holder_flow.get("accumulation"):
        score += 12
    score += min(8, int(max(0.0, to_float(holder_flow.get("netflow"))) / 25_000))
    if to_float(holder_flow.get("sell_volume")) > to_float(holder_flow.get("buy_volume")):
        score -= 8
    return max(0, min(100, score))


def build_message(t: TokenView, item: dict[str, Any], score: int) -> str:
    created = "unknown"
    if t.creation_ts:
        created = datetime.fromtimestamp(t.creation_ts, tz=timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    holder_growth = growth_pct(t.holder_count, to_float(item.get("first_holder_count")))
    liquidity_change = growth_pct(t.liquidity, to_float(item.get("first_liquidity")))
    return "\n".join(
        [
            f"Solana代币预警：{t.symbol}",
            f"CA: {t.address}",
            f"发现窗口：{','.join(t.discovery_intervals) or 'n/a'}",
            f"市值：{usd(t.market_cap)} | 价格：${t.price:.10f}",
            f"创建时间：{created}（币龄 {age_text(t.creation_ts)}）",
            f"叙事：{t.narrative}",
            f"KOL/聪明钱：KOL={t.renowned_count}，Smart={t.smart_degen_count}",
            f"Top10持仓：{pct(t.top10_holder_rate)} | Dev持仓：{pct(t.dev_team_hold_rate)}",
            f"持有人：{t.holder_count}（跟踪以来 {pct(holder_growth)}）",
            f"流动性：{usd(t.liquidity)}（跟踪以来 {pct(liquidity_change)}）",
            f"买卖压力：买入={t.buys}，卖出={t.sells}，买压={pct(buy_pressure(t))}",
            holder_flow_text(item.get("holder_flow") if isinstance(item.get("holder_flow"), dict) else {}),
            f"风险：rug={pct(t.rug_ratio)}，捆绑={pct(t.bundler_rate)}，机器人={pct(t.bot_degen_rate)}，陷阱={pct(t.entrapment_ratio)}",
            f"购买评分：{score}/100",
            f"触发原因：{format_alert_reason(str(item.get('alert_reason') or 'score_only_watch'))}",
        ]
    )


def format_alert_reason(reason: str) -> str:
    mapping = {
        "top100_accumulation": "Top100确认吸筹",
        "score_only_watch": "观察名单",
    }
    parts = [part for part in reason.split(",") if part]
    return "，".join(mapping.get(part, part.replace("score>=", "评分>=")) for part in parts) or "观察名单"


def send_telegram(message: str) -> bool:
    token = os.getenv("TG_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TG_CHAT_ID", "").strip()
    if not token or not chat_id:
        try:
            config = importlib.import_module("config")
            token = token or str(getattr(config, "TG_BOT_TOKEN", "")).strip()
            chat_id = chat_id or str(getattr(config, "TG_CHAT_ID", "")).strip()
        except ImportError:
            pass
    if not token or not chat_id:
        print("TG_BOT_TOKEN/TG_CHAT_ID not configured; printing alert only.")
        print(message)
        return False
    body = urlencode({"chat_id": chat_id, "text": message, "disable_web_page_preview": "true"}).encode()
    request = Request(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=15) as response:
            return 200 <= response.status < 300
    except OSError as exc:
        print(f"Telegram send failed: {exc}", file=sys.stderr)
        return False


def alert_reason(item: dict[str, Any], score: int, min_score: int) -> str | None:
    holder_flow = item.get("holder_flow") if isinstance(item.get("holder_flow"), dict) else {}
    reasons = []
    if score >= min_score:
        reasons.append(f"score>={min_score}")
    if holder_flow.get("accumulation"):
        reasons.append("top100_accumulation")
    return ",".join(reasons) if reasons else None


def should_alert(state: dict[str, Any], address: str, reason: str | None) -> bool:
    if not reason:
        return False
    last = to_int(state.setdefault("alerts", {}).get(address))
    return now_ts() - last >= ALERT_COOLDOWN_SEC


def scan_once(state: dict[str, Any], args: argparse.Namespace) -> list[tuple[TokenView, int, str]]:
    raw_tokens = fetch_rank(args.limit, args.windows)
    alerts: list[tuple[TokenView, int, str]] = []
    print(f"[{datetime.now().strftime('%H:%M:%S')}] windows={','.join(args.windows)} fetched={len(raw_tokens)}")

    for raw in raw_tokens:
        token = normalize_token(raw)
        ok, reason = pass_discovery(token)
        if not ok:
            if args.verbose:
                print(f"skip discovery {token.symbol} {token.address[:6]}: {reason}")
            continue

        item = update_tracking(state, token)
        ok, reason = pass_tracking(item, token)
        if not ok:
            if args.verbose:
                print(f"tracking {token.symbol} {token.address[:6]}: {reason}")
            continue

        deep = fetch_token_info(token.address)
        token = merge_deep_info(token, deep)
        ok, reason = pass_deep_analysis(token)
        if not ok:
            if args.verbose:
                print(f"skip deep {token.symbol} {token.address[:6]}: {reason}")
            continue

        holders = fetch_top_holders(token.address)
        holder_flow = analyze_top_holders(holders)
        item["holder_flow"] = holder_flow
        if args.verbose:
            print(f"holders {token.symbol} {token.address[:6]}: {holder_flow_text(holder_flow)}")

        score = score_token(token, item)
        reason = alert_reason(item, score, args.min_score)
        item["alert_reason"] = reason or ""
        message = build_message(token, item, score)
        if should_alert(state, token.address, reason):
            alerts.append((token, score, message))
            state["alerts"][token.address] = now_ts()

    return alerts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scan Solana tokens with gmgn-cli and optionally notify Telegram.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="Run one scan and exit. This is the default.")
    mode.add_argument("--watch", action="store_true", help="Run continuously.")
    parser.add_argument("--notify", action="store_true", help="Send passing alerts to Telegram.")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL_SEC, help="Watch interval in seconds.")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="GMGN rank size, max 100.")
    parser.add_argument(
        "--windows",
        default=",".join(DISCOVERY_INTERVALS),
        help="Comma-separated GMGN trending windows, default: 1m,5m.",
    )
    parser.add_argument("--min-score", type=int, default=MIN_SCORE_TO_ALERT, help="Minimum score to alert.")
    parser.add_argument("--state-file", type=Path, default=STATE_PATH, help="Local JSON state file.")
    parser.add_argument("--verbose", action="store_true", help="Print skipped-token reasons.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.windows = tuple(w.strip() for w in str(args.windows).split(",") if w.strip())
    state = load_state(args.state_file)

    while True:
        alerts = scan_once(state, args)
        save_state(args.state_file, state)

        for token, score, message in alerts:
            print(f"ALERT {token.symbol} {token.address} score={score}")
            if args.notify:
                send_telegram(message)
            else:
                print(message)

        if not args.watch:
            return 0
        sleep_seconds = max(30, args.interval)
        print(f"Next scan in {sleep_seconds}s")
        time.sleep(sleep_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
