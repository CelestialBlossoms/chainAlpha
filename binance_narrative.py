#!/usr/bin/env python3
"""Fetch and cache Binance Web3 narrative metadata for Solana tokens."""

from __future__ import annotations

import json
import os
import re
import time
from typing import Any
from urllib.parse import quote_plus

import requests
from psycopg2.extras import Json

from db_client import db_op
from redis_client import get_redis_client, redis_key


BINANCE_CHAIN_ID = os.getenv("BINANCE_SOL_CHAIN_ID", "CT_501")
BINANCE_NARRATIVE_ENABLED = os.getenv("BINANCE_NARRATIVE_ENABLED", "1") != "0"
BINANCE_NARRATIVE_TTL_SEC = int(os.getenv("BINANCE_NARRATIVE_TTL_SEC", "86400"))
BINANCE_NARRATIVE_TIMEOUT_SEC = int(os.getenv("BINANCE_NARRATIVE_TIMEOUT_SEC", "12"))
BINANCE_USER_AGENT = os.getenv("BINANCE_WEB3_USER_AGENT", "binance-web3/1.1 (Skill)")
DEEPSEEK_TRANSLATE_ENABLED = os.getenv("BINANCE_NARRATIVE_DEEPSEEK_TRANSLATE", "1") != "0"
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-v4-pro")
DEEPSEEK_TIMEOUT = int(os.getenv("DEEPSEEK_TIMEOUT", "45"))

TOKEN_SEARCH_URL = "https://web3.binance.com/bapi/defi/v5/public/wallet-direct/buw/wallet/market/token/search/ai"
TOKEN_META_URL = "https://web3.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/dex/market/token/meta/info/ai"
TOKEN_DYNAMIC_URL = "https://web3.binance.com/bapi/defi/v4/public/wallet-direct/buw/wallet/market/token/dynamic/info/ai"
TOPIC_RUSH_URL = "https://web3.binance.com/bapi/defi/v2/public/wallet-direct/buw/wallet/market/token/social-rush/rank/list/ai"
CHINESE_RE = re.compile(r"[\u4e00-\u9fff]")
ENGLISH_RE = re.compile(r"[A-Za-z]")

NARRATIVE_CATEGORY_KEYWORDS = {
    "政治": [
        "总统", "选举", "特朗普", "拜登", "政府", "政治", "国会", "白宫", "民主党", "共和党",
        "法律", "法官", "法院", "政策", "税收", "投票", "竞选", "党派", "国家", "国旗",
        "爱国", "自由", "军事", "战争", "军队", "america", "usa", "trump", "biden", "election",
        "president", "government", "congress", "white house", "democrat", "republican", "elon",
    ],
    "动物": [
        "猫", "狗", "熊猫", "熊", "兔", "鱼", "马", "牛", "羊", "鸡", "鸭", "鹅", "蛇", "鼠",
        "虎", "龙", "狮", "狼", "狐", "鹰", "鸟", "鲸", "鲨", "青蛙", "猴子", "猩猩", "大象",
        "猪", "企鹅", "宠物", "动物", "野兽", "dog", "cat", "bear", "bull", "ape", "pepe",
        "doge", "shib", "frog", "toad", "fish", "lobster", "rabbit", "penguin", "monkey", "wolf",
    ],
    "应用": [
        "ai", "人工智能", "平台", "应用", "工具", "软件", "协议", "网络", "系统", "defi", "dex",
        "交易所", "钱包", "链", "智能合约", "nft", "gamefi", "app", "bot", "机器人", "自动化",
        "算法", "数据", "分析", "支付", "跨链", "layer", "扩容", "基础设施", "开发", "代码",
        "open source", "开源", "builder", "build", "技术", "trading", "swap", "bridge", "oracle",
        "agi", "llm", "模型", "gpt", "claude", "openai", "anthropic", "ide", "saas", "cloud",
        "agent", "agents",
    ],
    "抽象": [
        "meme", "迷因", "梗", "搞笑", "讽刺", "幽默", "表情包", "文化", "社区", "社交", "病毒",
        "传播", "信仰", "宗教", "哲学", "意识", "精神", "灵魂", "死亡", "重生", "永恒", "虚无",
        "混沌", "秩序", "艺术", "音乐", "绘画", "设计", "创意", "情绪", "感觉", "氛围", "vibe",
        "energy", "梦想", "希望", "爱", "恨", "恐惧", "抽象", "幻想", "童话", "传说", "culture",
        "theme", "themes",
    ],
}


def _headers() -> dict[str, str]:
    return {"Accept-Encoding": "identity", "User-Agent": BINANCE_USER_AGENT}


def _get_json(url: str) -> dict[str, Any] | list[Any] | None:
    try:
        resp = requests.get(url, headers=_headers(), timeout=BINANCE_NARRATIVE_TIMEOUT_SEC)
        if not resp.ok:
            print(f"binance narrative http {resp.status_code}: {url[:120]}")
            return None
        return resp.json()
    except Exception as exc:
        print(f"binance narrative request failed: {exc}")
        return None


def _cache_key(address: str) -> str:
    return redis_key("binance:narrative", "sol", address)


def _load_cache(address: str) -> dict[str, Any] | None:
    client = get_redis_client()
    if client is None:
        return None
    try:
        raw = client.get(_cache_key(address))
        if not raw:
            return None
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception as exc:
        print(f"binance narrative cache read failed {address[:8]}: {exc}")
        return None


def _save_cache(address: str, payload: dict[str, Any]) -> None:
    client = get_redis_client()
    if client is None:
        return
    try:
        client.setex(_cache_key(address), BINANCE_NARRATIVE_TTL_SEC, json.dumps(payload, ensure_ascii=False, default=str))
    except Exception as exc:
        print(f"binance narrative cache write failed {address[:8]}: {exc}")


def _flatten_tags(*sources: dict[str, Any]) -> list[str]:
    tags: list[str] = []
    seen = set()
    for source in sources:
        if not isinstance(source, dict):
            continue
        for key in ("tokenTag", "tagsInfo"):
            tag_obj = source.get(key)
            if not isinstance(tag_obj, dict):
                continue
            for _, values in tag_obj.items():
                if not isinstance(values, list):
                    continue
                for item in values:
                    name = str((item or {}).get("tagName") or "").strip()
                    if name and name not in seen:
                        seen.add(name)
                        tags.append(name)
    return tags


def _good_description(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    low = text.lower()
    generic = ("created on http", "created on rapidlaunch", "created on pump", "created on https://")
    if any(low.startswith(prefix) for prefix in generic):
        return ""
    return text[:500]


def looks_english(text: str) -> bool:
    text = str(text or "").strip()
    if not text:
        return False
    if CHINESE_RE.search(text):
        return False
    letters = ENGLISH_RE.findall(text)
    return len(letters) >= 20 and len(letters) / max(len(text), 1) >= 0.35


def translate_narrative_to_chinese(text: str) -> str:
    text = str(text or "").strip()
    if not text or not looks_english(text):
        return text
    if not DEEPSEEK_TRANSLATE_ENABLED or not DEEPSEEK_API_KEY:
        return text
    prompt = (
        "将下面的加密 meme 代币叙事翻译并压缩为中文。"
        "要求：只输出中文，不要解释，不要添加投资建议；保留代币名、核心热点、社交传播点；控制在120字以内。\n\n"
        f"{text}"
    )
    try:
        resp = requests.post(
            f"{DEEPSEEK_BASE_URL.rstrip('/')}/chat/completions",
            headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": DEEPSEEK_MODEL,
                "messages": [
                    {"role": "system", "content": "你是加密代币叙事翻译器，只输出简洁中文。"},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.2,
                "max_tokens": 300,
            },
            timeout=DEEPSEEK_TIMEOUT,
        )
        if not resp.ok:
            print(f"deepseek narrative translate http={resp.status_code} {resp.text[:160]}")
            return text
        data = resp.json()
        translated = (
            ((data.get("choices") or [{}])[0].get("message") or {}).get("content")
            if isinstance(data, dict) else ""
        )
        translated = str(translated or "").strip()
        return translated[:500] if translated else text
    except Exception as exc:
        print(f"deepseek narrative translate failed: {exc}")
    return text


def classify_narrative_category(desc: Any = "", narrative_type: Any = "", tags: Any = None) -> str:
    parts = [str(desc or ""), str(narrative_type or "")]
    if isinstance(tags, list):
        parts.extend(str(item or "") for item in tags)
    elif tags:
        parts.append(str(tags))
    text = " ".join(parts).lower()
    scores = {
        category: sum(1 for keyword in keywords if keyword.lower() in text)
        for category, keywords in NARRATIVE_CATEGORY_KEYWORDS.items()
    }
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "其他"


def _first_topic_match(address: str, symbol: str = "", name: str = "") -> dict[str, Any]:
    keywords = []
    for value in (symbol, name):
        value = str(value or "").strip()
        if value and value.lower() not in {item.lower() for item in keywords}:
            keywords.append(value)
    for keyword in keywords[:2]:
        url = (
            f"{TOPIC_RUSH_URL}?chainId={BINANCE_CHAIN_ID}&rankType=10&sort=10&asc=false"
            f"&keywords={quote_plus(keyword)}"
        )
        payload = _get_json(url)
        rows = payload.get("data") if isinstance(payload, dict) else []
        if not isinstance(rows, list):
            continue
        for topic in rows:
            token_list = topic.get("tokenList") if isinstance(topic, dict) else []
            if not isinstance(token_list, list):
                continue
            for token in token_list:
                if str((token or {}).get("contractAddress") or "").strip() == address:
                    return topic
    return {}


def _topic_summary(topic: dict[str, Any]) -> str:
    summary = topic.get("aiSummary") if isinstance(topic, dict) else {}
    if isinstance(summary, str):
        return summary.strip()[:500]
    if not isinstance(summary, dict):
        return ""
    preferred = (
        "aiSummaryCn",
        "aiSummaryZh",
        "cn",
        "zh",
        "summaryCn",
        "summaryZh",
        "socialSummaryBriefTranslated",
        "socialSummaryDetailTranslated",
        "aiSummaryEn",
        "en",
        "summary",
        "socialSummaryBrief",
        "socialSummaryDetail",
    )
    for key in preferred:
        value = summary.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()[:500]
    for value in summary.values():
        if isinstance(value, str) and value.strip():
            return value.strip()[:500]
        if isinstance(value, dict):
            nested = _topic_summary({"aiSummary": value})
            if nested:
                return nested
    return ""


def _topic_name(topic: dict[str, Any]) -> str:
    name = topic.get("name") if isinstance(topic, dict) else {}
    if isinstance(name, dict):
        return str(name.get("topicNameCn") or name.get("topicNameEn") or name.get("name") or "").strip()
    return str(name or "").strip()


def ensure_token_narratives_table() -> None:
    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS token_narratives (
                ca TEXT PRIMARY KEY,
                chain TEXT DEFAULT 'sol',
                source TEXT NOT NULL DEFAULT 'binance_web3',
                symbol TEXT,
                name TEXT,
                narrative_desc TEXT,
                narrative_type TEXT,
                tags JSONB DEFAULT '[]'::jsonb,
                raw JSONB DEFAULT '{}'::jsonb,
                updated_at TIMESTAMPTZ DEFAULT now()
            );
            DO $$
            BEGIN
                IF to_regclass('public.bottom_watchlist_tokens') IS NOT NULL THEN
                    ALTER TABLE bottom_watchlist_tokens
                        ADD COLUMN IF NOT EXISTS narrative_desc TEXT;
                    ALTER TABLE bottom_watchlist_tokens
                        ADD COLUMN IF NOT EXISTS narrative_type TEXT;
                END IF;
            END $$;
            """
        )

    db_op(_op)


def save_token_narrative(address: str, narrative: dict[str, Any]) -> None:
    if not address or not narrative:
        return
    ensure_token_narratives_table()

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO token_narratives (
                ca, chain, source, symbol, name, narrative_desc, narrative_type, tags, raw, updated_at
            ) VALUES (%s, 'sol', %s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (ca) DO UPDATE SET
                source = EXCLUDED.source,
                symbol = COALESCE(EXCLUDED.symbol, token_narratives.symbol),
                name = COALESCE(EXCLUDED.name, token_narratives.name),
                narrative_desc = COALESCE(NULLIF(EXCLUDED.narrative_desc, ''), token_narratives.narrative_desc),
                narrative_type = COALESCE(NULLIF(EXCLUDED.narrative_type, ''), token_narratives.narrative_type),
                tags = EXCLUDED.tags,
                raw = EXCLUDED.raw,
                updated_at = now()
            """,
            (
                address,
                narrative.get("source") or "binance_web3",
                narrative.get("symbol"),
                narrative.get("name"),
                narrative.get("narrative_desc") or "",
                narrative.get("narrative_type") or "",
                Json(narrative.get("tags") or []),
                Json(narrative.get("raw") or {}),
            ),
        )
        cur.execute("SELECT to_regclass('public.bottom_watchlist_tokens')")
        if cur.fetchone()[0]:
            cur.execute(
                """
                UPDATE bottom_watchlist_tokens
                SET narrative_desc = COALESCE(NULLIF(%s, ''), narrative_desc),
                    narrative_type = COALESCE(NULLIF(%s, ''), narrative_type)
                WHERE ca = %s
                """,
                (narrative.get("narrative_desc") or "", narrative.get("narrative_type") or "", address),
            )

    db_op(_op)


def load_db_narrative(address: str) -> dict[str, Any] | None:
    if not address:
        return None
    ensure_token_narratives_table()

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            SELECT narrative_desc, narrative_type, symbol, name, tags, raw
            FROM token_narratives
            WHERE ca = %s
            LIMIT 1
            """,
            (address,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "address": address,
            "narrative_desc": row[0] or "",
            "narrative_type": row[1] or "",
            "symbol": row[2],
            "name": row[3],
            "tags": row[4] if isinstance(row[4], list) else [],
            "raw": row[5] if isinstance(row[5], dict) else {},
            "source": "db",
        }

    return db_op(_op)


def get_binance_narrative(
    address: str,
    *,
    symbol: str | None = None,
    name: str | None = None,
    force: bool = False,
    save: bool = True,
) -> dict[str, Any]:
    address = str(address or "").strip()
    if not address or not BINANCE_NARRATIVE_ENABLED:
        return {}
    if not force:
        cached = _load_cache(address)
        if cached:
            return cached

    search_url = (
        f"{TOKEN_SEARCH_URL}?keyword={quote_plus(address)}&chainIds={BINANCE_CHAIN_ID}&orderBy=volume24h"
    )
    search_payload = _get_json(search_url)
    search_rows = search_payload.get("data") if isinstance(search_payload, dict) else []
    search_match = {}
    if isinstance(search_rows, list):
        search_match = next(
            (row for row in search_rows if str((row or {}).get("contractAddress") or "").strip() == address),
            search_rows[0] if search_rows else {},
        )

    meta_url = f"{TOKEN_META_URL}?chainId={BINANCE_CHAIN_ID}&contractAddress={quote_plus(address)}"
    meta_payload = _get_json(meta_url)
    meta = meta_payload.get("data") if isinstance(meta_payload, dict) and isinstance(meta_payload.get("data"), dict) else {}

    dynamic_url = f"{TOKEN_DYNAMIC_URL}?chainId={BINANCE_CHAIN_ID}&contractAddress={quote_plus(address)}"
    dynamic_payload = _get_json(dynamic_url)
    dynamic = dynamic_payload.get("data") if isinstance(dynamic_payload, dict) and isinstance(dynamic_payload.get("data"), dict) else {}

    token_symbol = str(symbol or meta.get("symbol") or search_match.get("symbol") or "").strip()
    token_name = str(name or meta.get("name") or search_match.get("name") or "").strip()
    topic = _first_topic_match(address, token_symbol, token_name)

    tags = _flatten_tags(search_match, dynamic)
    desc = _topic_summary(topic) or _good_description(meta.get("description"))
    topic_name = _topic_name(topic)
    type_parts = []
    topic_type = str(topic.get("type") or "").strip() if isinstance(topic, dict) else ""
    if topic_type:
        type_parts.append(topic_type)
    if meta.get("aiNarrativeFlag") or (search_match.get("metaInfo") or {}).get("aiNarrativeFlag"):
        type_parts.append("AI Narrative")
    for tag in tags:
        if tag in {"Pumpfun", "Alpha", "Community Recognized", "DEX Paid", "Token Volume Surging"}:
            type_parts.append(tag)
    if not type_parts:
        type_parts.append("Binance Web3")
    narrative_type = " / ".join(dict.fromkeys(type_parts))
    if not desc:
        tag_text = ", ".join(tags[:6])
        base = token_name or token_symbol or address[:8]
        desc = f"{base} Binance Web3 tags: {tag_text}" if tag_text else ""
    if topic_name and desc and topic_name not in desc:
        desc = f"{topic_name}: {desc}"
    original_desc = desc
    translated_desc = translate_narrative_to_chinese(desc)

    result = {
        "address": address,
        "symbol": token_symbol,
        "name": token_name,
        "narrative_desc": translated_desc[:500],
        "narrative_desc_original": original_desc[:500],
        "narrative_translated": translated_desc != original_desc,
        "narrative_type": narrative_type[:180],
        "narrative_category": classify_narrative_category(translated_desc, narrative_type, tags),
        "tags": tags,
        "source": "binance_web3",
        "updated_ts": int(time.time()),
        "raw": {
            "search": search_match,
            "meta": meta,
            "dynamic": dynamic,
            "topic": topic,
        },
    }
    _save_cache(address, result)
    if save and (result.get("narrative_desc") or result.get("narrative_type")):
        try:
            save_token_narrative(address, result)
        except Exception as exc:
            print(f"binance narrative db save failed {address[:8]}: {exc}")
    return result


def resolve_cached_or_db_narrative(address: str) -> dict[str, Any]:
    cached = _load_cache(address)
    if cached:
        return cached
    try:
        return load_db_narrative(address) or {}
    except Exception as exc:
        print(f"binance narrative db read failed {str(address)[:8]}: {exc}")
        return {}


def compact_narrative(narrative: dict[str, Any] | None) -> dict[str, Any]:
    """Return a small payload suitable for Redis Stream/frontend messages."""
    if not isinstance(narrative, dict):
        return {}
    return {
        "address": narrative.get("address") or "",
        "symbol": narrative.get("symbol") or "",
        "name": narrative.get("name") or "",
        "narrative_desc": narrative.get("narrative_desc") or "",
        "narrative_desc_original": narrative.get("narrative_desc_original") or "",
        "narrative_translated": bool(narrative.get("narrative_translated")),
        "narrative_type": narrative.get("narrative_type") or "",
        "narrative_category": narrative.get("narrative_category")
        or classify_narrative_category(
            narrative.get("narrative_desc") or "",
            narrative.get("narrative_type") or "",
            narrative.get("tags") or [],
        ),
        "tags": (narrative.get("tags") or [])[:12],
        "source": narrative.get("source") or "",
        "updated_ts": narrative.get("updated_ts") or 0,
    }
