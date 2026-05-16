#!/usr/bin/env python3
"""
Unified bottom push performance report → HTML directly (no CSV intermediate).
Uses Binance K-line API for candles, Binance dynamic API for current mcap.

Usage:
    D:/software/anaconda/envs/py312/python.exe scripts/bottom_push_perf_report.py
    D:/software/anaconda/envs/py312/python.exe scripts/bottom_push_perf_report.py --date 2026-05-16
"""
from __future__ import annotations

import argparse, sys, time
from collections import Counter
from datetime import date, datetime, timedelta, time as dt_time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from db_client import db_op

DEFAULT_CHAIN = "sol"
DEFAULT_RESOLUTION = "5m"
DEFAULT_TZ = "Asia/Shanghai"
DEFAULT_OUTPUT = "gmgn_outputs/bottom_push_perf_report.html"

BINANCE_KLINE_URL = "https://dquery.sintral.io/u-kline/v1/k-line/candles"
BINANCE_DYNAMIC_URL = "https://web3.binance.com/bapi/defi/v4/public/wallet-direct/buw/wallet/market/token/dynamic/info/ai"
BINANCE_CHAIN_ID = "CT_501"
BINANCE_HEADERS = {"Accept-Encoding": "identity", "User-Agent": "binance-web3/1.1 (Skill)"}
MAX_WORKERS = 8
MAX_KLINE_BARS = 500

SIG_COLORS = {'abnormal': '#ef4444', 'new_revival': '#f59e0b', 'quiet_runup': '#8b5cf6',
              'quiet_breakout': '#3b82f6', 'drop_50w': '#06b6d4', 'drop_40w': '#06b6d4'}
SIG_ORDER = ['abnormal', 'new_revival', 'quiet_runup', 'quiet_breakout', 'drop_50w', 'drop_40w']

NARRATIVE_KEYWORDS = {
    "政治": [
        "总统", "选举", "特朗普", "拜登", "政府", "政治", "国会", "白宫", "民主党", "共和党",
        "法律", "法官", "法院", "政策", "税收", "投票", "竞选", "党派",
        "普京", "泽连斯基", "联合国", "外交", "制裁", "民主", "独裁",
        "腐败", "抗议", "游行", "革命", "军事", "战争", "军队",
        "America", "USA", "国家", "国旗", "爱国", "自由", "马斯克", "DOGE", "elon",
    ],
    "动物": [
        "猫", "狗", "熊猫", "熊", "兔", "鱼", "马", "牛", "羊", "鸡", "鸭", "鹅",
        "蛇", "鼠", "虎", "龙", "狮", "狼", "狐", "鹰", "鸟", "鲸", "鲨",
        "青蛙", "蟾蜍", "猴子", "猩猩", "大象", "长颈鹿", "斑马", "河马",
        "豚", "鹿", "猪", "虫", "蝶", "蜂", "蚁", "龟", "鳄",
        "宠物", "动物", "野兽", "dog", "cat", "bear", "bull", "ape",
        "pepe", "doge", "shib", "frog", "toad", "龙虾", "考拉", "袋鼠", "企鹅",
        "BUFO", "LOBSTER", "RABBIT", "Bear", "fish",
    ],
    "应用": [
        "AI", "人工智能", "平台", "应用", "工具", "软件", "协议", "网络", "系统",
        "DeFi", "DEX", "交易所", "钱包", "链", "智能合约", "NFT", "GameFi",
        "app", "bot", "机器人", "自动化", "算法", "数据", "分析",
        "支付", "跨链", "Layer", "扩容", "基础设施", "开发", "代码",
        "open", "source", "开源", "builder", "build", "技术",
        "交易", "Trading", "swap", "bridge", "oracle",
        "AGI", "LLM", "模型", "GPT", "Claude", "OpenAI", "Anthropic",
        "IDE", "SaaS", "cloud", "存储",
    ],
    "抽象": [
        "meme", "迷因", "梗", "搞笑", "讽刺", "幽默", "表情包",
        "文化", "社区", "社交", "病毒", "传播", "网络",
        "信仰", "宗教", "哲学", "意识", "精神", "灵魂",
        "死亡", "重生", "永恒", "虚无", "混沌", "秩序",
        "艺术", "音乐", "绘画", "设计", "创意",
        "情绪", "感觉", "氛围", "vibe", "energy",
        "梦想", "希望", "爱", "恨", "恐惧", "抽象", "幻想", "童话", "传说",
    ],
}
NAR_COLORS = {'政治': '#ef4444', '动物': '#10b981', '抽象': '#8b5cf6', '应用': '#3b82f6', '其他': '#64748b'}


def to_float(v, default=0.0):
    try:
        if v in (None, ""): return default
        return float(str(v).replace(",", ""))
    except (TypeError, ValueError): return default


def to_int(v, default=0):
    try:
        if v in (None, ""): return default
        return int(float(v))
    except (TypeError, ValueError): return default


def fm(v):
    v = to_float(v)
    if v >= 1_000_000: return f"${v / 1_000_000:.2f}M"
    if v >= 1_000: return f"${v / 1_000:.1f}K"
    return f"${v:.0f}"


def fp(v, signed=False):
    v = to_float(v)
    prefix = '+' if signed and v > 0 else ''
    return f"{prefix}{v:.1f}%"


def classify_narrative(desc, ntype):
    text = f"{desc} {ntype}".lower()
    scores = {}
    for cat, keywords in NARRATIVE_KEYWORDS.items():
        scores[cat] = sum(1 for kw in keywords if kw.lower() in text)
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "其他"


def resolution_seconds(res):
    return {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "4h": 14400, "1d": 86400}.get(res, 300)


def _res_to_interval(res):
    return {"1m": "1min", "5m": "5min", "15m": "15min", "30m": "30min", "1h": "1h", "4h": "4h", "1d": "1d"}.get(res, "5min")


PLATFORM = {"sol": "solana", "bsc": "bsc", "base": "base", "eth": "ethereum"}


def fetch_kline(address, resolution, from_ts, to_ts):
    params = {
        "address": address, "platform": "solana", "interval": _res_to_interval(resolution),
        "limit": MAX_KLINE_BARS, "to": to_ts * 1000, "pm": "p",
    }
    try:
        r = requests.get(BINANCE_KLINE_URL, params=params, headers=BINANCE_HEADERS, timeout=30)
        if r.status_code != 200: return []
        raw = (r.json().get("data") or [])
        candles = []
        for item in raw:
            if not isinstance(item, list) or len(item) < 6: continue
            ts = int(item[5] / 1000) if item[5] > 10_000_000_000 else int(item[5])
            if ts < from_ts: continue
            candles.append({"ts": ts, "open": float(item[0]), "high": float(item[1]),
                           "low": float(item[2]), "close": float(item[3]), "volume": float(item[4])})
        candles.sort(key=lambda c: c["ts"])
        return candles
    except Exception as e:
        print(f"  K-line failed: {e}")
        return []


def fetch_binance_mcap(address):
    url = f"{BINANCE_DYNAMIC_URL}?chainId={BINANCE_CHAIN_ID}&contractAddress={address}"
    try:
        r = requests.get(url, headers=BINANCE_HEADERS, timeout=10)
        if r.ok:
            d = (r.json().get("data") or {})
            return to_float(d.get("marketCap")), to_float(d.get("price")), True
    except Exception: pass
    return 0, 0, False


def fetch_narratives():
    result = {}
    def _op(conn):
        cur = conn.cursor()
        cur.execute("SELECT address, extra->>'narrative_desc', extra->>'narrative_type' FROM bottom_top100_push_records WHERE extra->>'narrative_desc' IS NOT NULL AND extra->>'narrative_desc' != ''")
        for row in cur.fetchall(): result[row[0]] = (row[1] or '', row[2] or '')
    db_op(_op)
    return result


def fetch_first_pushes(start_ts, end_ts, chain):
    def _op(conn):
        cur = conn.cursor()
        cur.execute("""SELECT COUNT(*) FROM bottom_top100_push_records
            WHERE chain=%s AND event_ts>=%s AND event_ts<%s AND COALESCE(signal_type,'')<>''""",
            (chain, start_ts, end_ts))
        total = int(cur.fetchone()[0] or 0)
        cur.execute("""WITH todays AS (
            SELECT * FROM bottom_top100_push_records
            WHERE chain=%s AND event_ts>=%s AND event_ts<%s AND COALESCE(signal_type,'')<>''
        ), firsts AS (
            SELECT DISTINCT ON (address) id, pushed_at, event_ts, chain, address, symbol,
                   signal_type, abnormal_rule, trend_interval,
                   current_mcap, first_signal_mcap, first_signal_ts, first_signal_change_pct,
                   price_change_pct, max_abnormal_mcap, ath_mcap, liquidity, pool_mcap_ratio,
                   age_sec, extra
            FROM todays ORDER BY address, event_ts ASC, id ASC
        ) SELECT f.* FROM firsts f ORDER BY f.event_ts ASC, f.id ASC""",
            (chain, start_ts, end_ts))
        rows = []
        for r in cur.fetchall():
            extra = r[19] if len(r) > 19 and isinstance(r[19], dict) else {}
            risk_tags = extra.get("risk_tags", []) if isinstance(extra, dict) else []
            rows.append({
                "id": r[0], "event_ts": int(r[2] or 0), "address": r[4], "symbol": r[5] or extra.get("symbol", "UNKNOWN"),
                "signal_type": r[6] or "", "abnormal_rule": r[7] or "",
                "current_mcap": to_float(r[9]), "first_signal_mcap": to_float(r[10]),
                "first_signal_ts": int(r[11] or 0), "first_signal_change_pct": to_float(r[12]),
                "price_change_pct": to_float(r[13]), "ath_mcap": to_float(r[15]),
                "liquidity": to_float(r[16]), "pool_mcap_ratio": to_float(r[17]),
                "age_sec": int(r[18] or 0), "extra": extra, "risk_tags": risk_tags,
            })
        return rows, total
    return db_op(_op)


def analyze_push(push, candles, resolution, tz):
    event_ts = push["event_ts"]
    step = resolution_seconds(resolution)

    signal_candle = None
    for c in candles:
        ct = c["ts"]
        if ct <= event_ts < ct + step:
            signal_candle = c; break
    if signal_candle is None:
        before = [c for c in candles if c["ts"] <= event_ts]
        if before: signal_candle = before[-1]

    post = [c for c in candles if c["ts"] + step > event_ts]
    if not post:
        return {**push, "valid": False, "invalid_reason": "no_post_signal_kline", "candles": len(candles)}
    if signal_candle is None:
        signal_candle = post[0]

    entry_price = to_float(signal_candle.get("close")) or to_float(signal_candle.get("open"))
    if entry_price <= 0:
        return {**push, "valid": False, "invalid_reason": "invalid_entry_price", "candles": len(post)}

    peak_idx, peak = max(enumerate(post), key=lambda x: to_float(x[1].get("high")))
    low_idx, low = min(enumerate(post), key=lambda x: to_float(x[1].get("low")))
    peak_price = to_float(peak.get("high"))
    lowest_price = to_float(low.get("low"))
    current_price = to_float(post[-1].get("close"))

    post_peak = post[peak_idx + 1:]
    if post_peak:
        pp_low_idx, pp_low = min(enumerate(post_peak, start=peak_idx + 1), key=lambda x: to_float(x[1].get("low")))
        pp_low_price = to_float(pp_low.get("low"))
        pp_low_ts = int(pp_low.get("ts") or 0)
    else:
        pp_low_idx, pp_low_price, pp_low_ts = peak_idx, current_price, int(post[-1].get("ts") or 0)

    max_gain = (peak_price / entry_price - 1) * 100 if peak_price > 0 else 0
    cur_ret = (current_price / entry_price - 1) * 100 if current_price > 0 else 0
    dd_high = (1 - pp_low_price / peak_price) * 100 if peak_price > 0 and pp_low_price > 0 else 0
    dd_entry = min(0.0, (lowest_price / entry_price - 1) * 100 if lowest_price > 0 else 0)

    return {
        **push, "valid": True, "resolution": resolution,
        "entry_price": entry_price,
        "entry_time": datetime.fromtimestamp(int(signal_candle.get("ts") or 0), tz).strftime("%H:%M"),
        "peak_price": peak_price,
        "peak_time": datetime.fromtimestamp(int(peak.get("ts") or 0), tz).strftime("%H:%M"),
        "current_price": current_price,
        "current_time": datetime.fromtimestamp(int(post[-1].get("ts") or 0), tz).strftime("%H:%M"),
        "max_gain_pct": max_gain, "current_return_pct": cur_ret,
        "entry_drawdown_pct": dd_entry, "high_to_low_drawdown_pct": max(0.0, dd_high),
        "time_to_peak_min": max(0.0, (int(peak.get("ts") or 0) - event_ts) / 60),
        "candles": len(post),
        "event_time": datetime.fromtimestamp(event_ts, tz).strftime("%Y-%m-%d %H:%M"),
        "peak_time_full": datetime.fromtimestamp(int(peak.get("ts") or 0), tz).strftime("%Y-%m-%d %H:%M"),
    }


def build_html(rows, day_label, total_pushes, tz):
    valid = [r for r in rows if r.get("valid")]
    gains = sorted([r["max_gain_pct"] for r in valid], reverse=True)
    gain_dist = Counter()
    for g in gains:
        if g >= 200: gain_dist['>=200%'] += 1
        elif g >= 100: gain_dist['100-200%'] += 1
        elif g >= 50: gain_dist['50-100%'] += 1
        elif g >= 10: gain_dist['10-50%'] += 1
        else: gain_dist['<10%'] += 1

    signal_dist = Counter(r["signal_type"] for r in valid)
    result_dist = Counter(r.get("result", "") for r in valid)
    binance_hits = sum(1 for r in valid if r.get("binance_ok"))
    nar_dist = Counter(r.get("narrative_cat", "") for r in valid)
    risk_tag_dist = Counter()
    risk_tag_success = Counter()
    for r in valid:
        for t in r.get("risk_tags", []):
            risk_tag_dist[t] += 1
            if r.get("result") == "成功":
                risk_tag_success[t] += 1

    nar_stats = {}
    for cat in ['政治', '动物', '抽象', '应用', '其他']:
        cr_rows = [r for r in valid if r.get("narrative_cat") == cat]
        if not cr_rows: continue
        succ = sum(1 for r in cr_rows if r.get("result") == "成功")
        nar_stats[cat] = {"count": len(cr_rows), "success": succ, "fail": len(cr_rows) - succ,
                          "avg_gain": sum(r["max_gain_pct"] for r in cr_rows) / len(cr_rows)}

    sorted_rows = sorted(valid, key=lambda r: r["max_gain_pct"], reverse=True)

    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>底部异动推送绩效分析 - {day_label}</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0f172a;color:#e2e8f0;padding:20px 30px}}
h1{{font-size:1.5rem;margin-bottom:4px}}
.subtitle{{color:#64748b;font-size:.85rem;margin-bottom:20px}}
h2{{font-size:1.05rem;margin:24px 0 10px;color:#94a3b8}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px;margin-bottom:24px}}
.card{{background:#1e293b;border-radius:10px;padding:16px;border:1px solid #334155}}
.card .label{{font-size:.72rem;color:#64748b;text-transform:uppercase;letter-spacing:.5px}}
.card .value{{font-size:1.4rem;font-weight:700;margin-top:4px}}
.green{{color:#10b981}}.red{{color:#ef4444}}.yellow{{color:#f59e0b}}.purple{{color:#8b5cf6}}.cyan{{color:#06b6d4}}
.charts{{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:24px}}
@media(max-width:800px){{.charts{{grid-template-columns:1fr}}}}
.chart-box{{background:#1e293b;border-radius:10px;padding:16px;border:1px solid #334155}}
.chart-box h3{{font-size:.85rem;color:#94a3b8;margin-bottom:12px}}
.bar-row{{display:flex;align-items:center;margin-bottom:6px;font-size:.8rem}}
.bar-label{{width:80px;text-align:right;margin-right:10px;color:#94a3b8;flex-shrink:0}}
.bar-track{{flex:1;background:#334155;border-radius:4px;height:22px;overflow:hidden}}
.bar-fill{{height:100%;border-radius:4px}}
.bar-count{{width:90px;font-size:.75rem;margin-left:8px;color:#cbd5e1;flex-shrink:0}}
.search-box{{background:#1e293b;border:1px solid #334155;border-radius:8px;padding:8px 14px;color:#e2e8f0;width:220px;font-size:.8rem;margin-bottom:10px}}
.search-box::placeholder{{color:#64748b}}
.filter{{display:flex;gap:8px;margin-bottom:14px;flex-wrap:wrap}}
.fbtn{{padding:6px 14px;border-radius:20px;border:1px solid #334155;background:#1e293b;color:#94a3b8;cursor:pointer;font-size:.78rem;transition:all .2s}}
.fbtn:hover{{border-color:#64748b;color:#e2e8f0}}
.fbtn.active{{border-color:#3b82f6;color:#60a5fa;background:#1e3050}}
.nar-cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;margin-bottom:20px}}
.nar-card{{background:#1e293b;border-radius:10px;padding:14px;border:1px solid #334155}}
.nar-card .nar-title{{font-size:.85rem;font-weight:700;margin-bottom:8px}}
.nar-card .nar-stat{{font-size:.75rem;color:#94a3b8;margin-bottom:3px}}
.nar-card .nar-stat b{{color:#e2e8f0}}
.table-wrap{{overflow-x:auto;border-radius:10px;border:1px solid #334155}}
table{{width:100%;border-collapse:collapse;font-size:.76rem;background:#1e293b}}
th{{background:#334155;padding:9px 7px;text-align:left;font-weight:600;color:#94a3b8;font-size:.68rem;text-transform:uppercase;letter-spacing:.5px;white-space:nowrap;position:sticky;top:0;z-index:1}}
th.sortable{{cursor:pointer;user-select:none;transition:color .15s}}
th.sortable:hover{{color:#e2e8f0}}
th.sortable .sort-arrow{{font-size:.6rem;margin-left:3px;opacity:.5}}
th.sortable.asc .sort-arrow,.th.sortable.desc .sort-arrow{{opacity:1}}
td{{padding:6px 7px;border-bottom:1px solid #1e293b;white-space:nowrap}}
tr:nth-child(even) td{{background:#1a2332}}
tr:hover td{{background:#162032}}
.badge{{display:inline-block;padding:2px 8px;border-radius:12px;font-size:.68rem;font-weight:600}}
.positive{{color:#10b981;font-weight:600}}.negative{{color:#ef4444}}
.ca-link{{font-family:monospace;font-size:.68rem;color:#60a5fa;cursor:pointer;user-select:all}}
.ca-link:hover{{color:#93c5fd;text-decoration:underline}}
.ca-link.ca-copied{{color:#10b981!important}}
.result-badge{{padding:3px 10px;border-radius:12px;font-size:.72rem;font-weight:700}}
.result-success{{background:#064e3b;color:#34d399;border:1px solid #065f46}}
.result-fail{{background:#451a1a;color:#fca5a5;border:1px solid #7f1d1d}}
.note{{background:#1e3050;border:1px solid #1e3a5f;border-radius:8px;padding:10px 16px;font-size:.78rem;color:#93c5fd;margin-bottom:20px}}
.time-col{{color:#94a3b8;font-size:.73rem}}
.footer{{text-align:center;color:#475569;font-size:.7rem;margin-top:30px;padding:16px}}
</style></head>
<body>
<h1>底部异动推送绩效分析</h1>
<p class="subtitle">{day_label} | {len(valid)} 个首次异动 CA | 推送总记录 {total_pushes} 条(去重) | Entry=信号时刻收盘价 | K线+市值: Binance API | 命中 {binance_hits}/{len(valid)}</p>
<div class="note">涨幅 ≥10% = <b style="color:#34d399">成功</b>, &lt;10% = <b style="color:#fca5a5">失败</b> | 点击列表头可排序 | 叙事分类基于关键词匹配</div>

<div class="grid">
<div class="card"><div class="label">样本数</div><div class="value purple">{len(valid)}</div></div>
<div class="card"><div class="label">成功 (≥10%)</div><div class="value green">{result_dist.get('成功',0)}</div></div>
<div class="card"><div class="label">失败 (&lt;10%)</div><div class="value red">{result_dist.get('失败',0)}</div></div>
<div class="card"><div class="label">成功率</div><div class="value cyan">{result_dist.get('成功',0)/len(valid)*100:.0f}%</div></div>
<div class="card"><div class="label">平均最高涨幅</div><div class="value green">{sum(gains)/len(gains):.1f}%</div></div>
<div class="card"><div class="label">中位最高涨幅</div><div class="value yellow">{sorted(gains)[len(gains)//2]:.1f}%</div></div>
</div>

<h2>叙事类别分析</h2>
<div class="nar-cards">"""

    for cat in ['政治', '动物', '抽象', '应用', '其他']:
        s = nar_stats.get(cat)
        if not s: continue
        c = NAR_COLORS.get(cat, '#64748b')
        html += f"""<div class="nar-card"><div class="nar-title" style="color:{c}">{cat} ({s['count']}个)</div>
<div class="nar-stat">成功: <b style="color:#34d399">{s['success']}</b> | 失败: <b style="color:#fca5a5">{s['fail']}</b></div>
<div class="nar-stat">成功率: <b>{s['success']/s['count']*100:.0f}%</b> | 平均涨幅: <b>{s['avg_gain']:.1f}%</b></div></div>"""
    html += '</div>\n'

    # Risk tag summary
    TAG_COLORS = {"瞬爆": "#f59e0b", "天花板": "#ef4444", "大市值": "#8b5cf6", "无量": "#64748b"}
    tag_order = ["瞬爆", "天花板", "大市值", "无量"]
    html += '<h2>风险标签分析</h2>\n<div class="nar-cards">\n'
    for tag in tag_order:
        total = risk_tag_dist.get(tag, 0)
        if total == 0: continue
        succ = risk_tag_success.get(tag, 0)
        fail = total - succ
        c = TAG_COLORS.get(tag, "#64748b")
        html += f'<div class="nar-card"><div class="nar-title" style="color:{c}">[{tag}] 标签 ({total}个)</div>'
        html += f'<div class="nar-stat">成功: <b style="color:#34d399">{succ}</b> | 失败: <b style="color:#fca5a5">{fail}</b></div>'
        html += f'<div class="nar-stat">成功率: <b>{succ/max(total,1)*100:.0f}%</b></div></div>\n'
    html += '</div>\n'

    html += '<div class="charts">\n<div class="chart-box"><h3>信号时刻 → 后续最高点涨幅分布</h3>\n'
    for label in ['>=200%', '100-200%', '50-100%', '10-50%', '<10%']:
        cnt = gain_dist.get(label, 0)
        p = cnt / len(valid) * 100
        bar_color = '#10b981' if label in ('>=200%', '100-200%', '50-100%') else ('#f59e0b' if label == '10-50%' else '#ef4444')
        html += f'<div class="bar-row"><span class="bar-label">{label}</span><div class="bar-track"><div class="bar-fill" style="width:{p}%;background:{bar_color}"></div></div><span class="bar-count">{cnt} ({p:.1f}%)</span></div>\n'
    html += '</div>\n<div class="chart-box"><h3>叙事类别成功率对比</h3>\n'
    for cat in ['政治', '动物', '抽象', '应用', '其他']:
        s = nar_stats.get(cat)
        if not s: continue
        p = s['success'] / s['count'] * 100
        c = NAR_COLORS[cat]
        html += f'<div class="bar-row"><span class="bar-label">{cat}</span><div class="bar-track"><div class="bar-fill" style="width:{p}%;background:{c}"></div></div><span class="bar-count">{s["success"]}/{s["count"]} ({p:.0f}%)</span></div>\n'
    html += '</div>\n</div>\n'

    html += '<h2>异动代币绩效明细 <span style="font-size:.75rem;color:#64748b;font-weight:400">(点击列头排序)</span></h2>\n'
    html += '<input class="search-box" type="text" id="s" placeholder="搜索 symbol / address..." onkeyup="ft()">\n'
    html += '<div class="filter"><button class="fbtn active" onclick="fs(\'all\')">全部</button>\n'
    html += '<button class="fbtn" onclick="fs(\'成功\')">成功</button>\n<button class="fbtn" onclick="fs(\'失败\')">失败</button>\n'
    for cat in ['政治', '动物', '抽象', '应用', '其他']:
        if nar_dist.get(cat, 0) > 0:
            c = NAR_COLORS[cat]
            html += f'<button class="fbtn" style="border-color:{c};color:{c}" onclick="fs(\'{cat}\')">{cat} ({nar_dist[cat]})</button>\n'
    html += '</div>\n<div class="table-wrap"><table id="t"><thead><tr>'
    html += '<th class="sortable" onclick="sortTable(0,\'num\')"># <span class="sort-arrow">▼</span></th>'
    html += '<th>结果</th><th>风险标签</th><th>叙事</th><th>CA</th><th>Symbol</th><th>信号类型</th>'
    html += '<th class="sortable" onclick="sortTable(7,\'num\')">异动市值 <span class="sort-arrow">▼</span></th>'
    html += '<th class="sortable" onclick="sortTable(8,\'num\')">ATH市值 <span class="sort-arrow">▼</span></th>'
    html += '<th class="sortable" onclick="sortTable(9,\'num\')">Entry收盘 <span class="sort-arrow">▼</span></th>'
    html += '<th class="sortable" onclick="sortTable(10,\'num\')">Binance市值 <span class="sort-arrow">▼</span></th>'
    html += '<th class="sortable" onclick="sortTable(11,\'num\')">最高涨幅 <span class="sort-arrow">▼</span></th>'
    html += '<th class="sortable" onclick="sortTable(12,\'num\')">当前收益 <span class="sort-arrow">▼</span></th>'
    html += '<th class="sortable" onclick="sortTable(13,\'num\')">高点回撤 <span class="sort-arrow">▼</span></th>'
    html += '<th class="sortable" onclick="sortTable(14,\'str\')">异动时间 <span class="sort-arrow">▼</span></th>'
    html += '<th class="sortable" onclick="sortTable(15,\'str\')">峰值时间 <span class="sort-arrow">▼</span></th>'
    html += '<th class="sortable" onclick="sortTable(16,\'num\')">至峰顶 <span class="sort-arrow">▼</span></th>'
    html += '<th class="sortable" onclick="sortTable(17,\'num\')">K线 <span class="sort-arrow">▼</span></th>'
    html += '</tr></thead><tbody>\n'

    for i, r in enumerate(sorted_rows, 1):
        sig = r["signal_type"]; bg = SIG_COLORS.get(sig, '#64748b')
        gain = r["max_gain_pct"]; cr = r["current_return_pct"]; dd = r["high_to_low_drawdown_pct"]
        result = r.get("result", ""); ncat = r.get("narrative_cat", "")
        gc = 'positive' if gain > 0 else 'negative'; cc = 'positive' if cr > 0 else 'negative'
        rb = 'result-success' if result == '成功' else 'result-fail'
        b_mcap = r.get("binance_mcap", 0); b_ok = r.get("binance_ok", False)
        nc = NAR_COLORS.get(ncat, '#64748b')

        html += f'<tr data-result="{result}" data-nar="{ncat}">'
        html += f'<td data-value="{i}">{i}</td>'
        html += f'<td><span class="result-badge {rb}">{result}</span></td>'
        tags = r.get("risk_tags", [])
        tag_parts = []
        for t in tags:
            tc = TAG_COLORS.get(t, "#64748b")
            tag_parts.append(f'<span class="badge" style="background:{tc}22;color:{tc};border:1px solid {tc}44;margin-right:2px">{t}</span>')
        html += '<td>' + ("".join(tag_parts) if tag_parts else "-") + '</td>'
        html += f'<td><span class="badge" style="background:{nc}22;color:{nc};border:1px solid {nc}44">{ncat}</span></td>'
        html += f'<td><span class="ca-link" onclick="copyCA(this)" title="点击复制CA">{r["address"]}</span></td>'
        html += f'<td><b>${r["symbol"]}</b></td>'
        html += f'<td><span class="badge" style="background:{bg}22;color:{bg};border:1px solid {bg}44">{sig}</span></td>'
        html += f'<td data-value="{r["current_mcap"]}">{fm(r["current_mcap"])}</td>'
        html += f'<td data-value="{r["ath_mcap"]}">{fm(r["ath_mcap"])}</td>'
        html += f'<td data-value="{r["entry_price"]}">{fm(r["entry_price"])}</td>'
        html += f'<td data-value="{b_mcap}" style="color:#06b6d4">{"$"+fm(b_mcap).lstrip("$") if b_ok else "-"}</td>'
        html += f'<td class="{gc}" data-value="{gain}"><b>{fp(gain, True)}</b></td>'
        html += f'<td class="{cc}" data-value="{cr}">{fp(cr, True)}</td>'
        html += f'<td style="color:#ef4444" data-value="{dd}">{fp(dd)}</td>'
        html += f'<td class="time-col" data-value="{r["event_time"]}">{r["event_time"]}</td>'
        html += f'<td class="time-col" data-value="{r["peak_time_full"]}">{r["peak_time_full"]}</td>'
        html += f'<td data-value="{r["time_to_peak_min"]:.0f}">{r["time_to_peak_min"]:.0f}m</td>'
        html += f'<td data-value="{r["candles"]}">{r["candles"]}</td>'
        html += '</tr>\n'

    html += '''</tbody></table></div>
<script>
var sortCol=-1,sortDir=1;
function fs(t){
  document.querySelectorAll(".fbtn").forEach(function(b){b.classList.remove("active")});
  event.target.classList.add("active");
  document.querySelectorAll("#t tbody tr").forEach(function(r){
    var show=false;
    if(t==="all")show=true;
    else if(t==="成功")show=r.dataset.result==="成功";
    else if(t==="失败")show=r.dataset.result==="失败";
    else show=r.dataset.nar===t;
    r.style.display=show?"":"none";
  });
}
function ft(){
  var q=document.getElementById("s").value.toLowerCase();
  document.querySelectorAll("#t tbody tr").forEach(function(r){
    r.style.display=r.textContent.toLowerCase().includes(q)?"":"none";
  });
}
function sortTable(col,type){
  var ths=document.querySelectorAll("#t thead th");
  if(sortCol===col){sortDir*=-1}else{sortCol=col;sortDir=-1}
  ths.forEach(function(th,i){
    th.classList.remove("asc","desc");
    if(i===col)th.classList.add(sortDir===1?"asc":"desc");
    var arrow=th.querySelector(".sort-arrow");
    if(arrow)arrow.textContent=sortDir===1?"▲":"▼";
  });
  var tbody=document.querySelector("#t tbody");
  var rows=Array.from(tbody.querySelectorAll("tr"));
  rows.sort(function(a,b){
    var va=a.cells[col].getAttribute("data-value")||"";
    var vb=b.cells[col].getAttribute("data-value")||"";
    if(type==="num"){var na=parseFloat(va)||0,nb=parseFloat(vb)||0;return (na-nb)*sortDir;}
    return va.localeCompare(vb)*sortDir;
  });
  rows.forEach(function(r){tbody.appendChild(r)});
}
function copyCA(el){
  var t=el.textContent.trim();
  navigator.clipboard.writeText(t).then(function(){
    el.classList.add("ca-copied");el.title="已复制!";
    setTimeout(function(){el.classList.remove("ca-copied");el.title="点击复制CA"},1200);
  }).catch(function(){
    var ta=document.createElement("textarea");ta.value=t;ta.style.position="fixed";ta.style.opacity="0";
    document.body.appendChild(ta);ta.select();document.execCommand("copy");document.body.removeChild(ta);
    el.classList.add("ca-copied");setTimeout(function(){el.classList.remove("ca-copied")},1200);
  });
}
</script>
<div class="footer">Binance K-line API | Binance Dynamic API for current mcap | Click headers to sort | Entry=信号时刻K线收盘价</div>
</body></html>'''

    return html


def local_day_bounds(day_str, tz_name):
    tz = ZoneInfo(tz_name)
    if day_str:
        target = date.fromisoformat(day_str)
    else:
        target = datetime.now(tz).date()
    start_dt = datetime.combine(target, dt_time.min, tzinfo=tz)
    end_dt = start_dt + timedelta(days=1)
    if target == datetime.now(tz).date():
        end_dt = min(end_dt, datetime.now(tz))
    return int(start_dt.timestamp()), int(end_dt.timestamp()), target.isoformat(), tz


def main():
    parser = argparse.ArgumentParser(description="Bottom push performance report → HTML")
    parser.add_argument("--date", help="Date YYYY-MM-DD, default today.")
    parser.add_argument("--tz", default=DEFAULT_TZ)
    parser.add_argument("--chain", default=DEFAULT_CHAIN)
    parser.add_argument("--resolution", default=DEFAULT_RESOLUTION, choices=("1m","5m","15m","1h","4h","1d"))
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="HTML output path")
    parser.add_argument("--sleep", type=float, default=0.1, help="Sleep between tokens (seconds)")
    args = parser.parse_args()

    start_ts, end_ts, day_label, tz = local_day_bounds(args.date, args.tz)

    print(f"Fetching push records for {day_label}...")
    pushes, total_pushes = fetch_first_pushes(start_ts, end_ts, args.chain)
    print(f"  {total_pushes} total pushes, {len(pushes)} first-per-CA")

    print("Fetching narratives from DB...")
    narratives = fetch_narratives()
    print(f"  {len(narratives)} narratives loaded")

    step = resolution_seconds(args.resolution)
    rows = []
    for idx, push in enumerate(pushes, 1):
        addr = push["address"]
        label = f"${push['symbol']}({addr[:8]})"
        print(f"[{idx}/{len(pushes)}] {label} {push['signal_type']}")

        # K-line
        candles = fetch_kline(addr, args.resolution,
                              max(start_ts, push["event_ts"] - step), end_ts)
        row = analyze_push(push, candles, args.resolution, tz)

        # Narrative
        nd, nt = narratives.get(addr, ('', ''))
        row["narrative_desc"] = nd
        row["narrative_type"] = nt
        row["narrative_cat"] = classify_narrative(nd, nt)

        # Risk tags from push record
        row["risk_tags"] = push.get("risk_tags", [])

        # Result
        gain = row.get("max_gain_pct", 0)
        row["result"] = "成功" if gain >= 10 else "失败"

        # Binance current mcap
        b_mcap, b_price, b_ok = fetch_binance_mcap(addr)
        row["binance_mcap"] = b_mcap
        row["binance_price"] = b_price
        row["binance_ok"] = b_ok

        rows.append(row)

        if args.sleep > 0 and idx < len(pushes):
            time.sleep(args.sleep)

    html = build_html(rows, day_label, total_pushes, tz)
    out = ROOT / args.output
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding='utf-8')
    print(f"\nHTML: {out} ({out.stat().st_size:,} bytes)")

    valid = [r for r in rows if r.get("valid")]
    gains = [r["max_gain_pct"] for r in valid]
    result_dist = Counter(r.get("result", "") for r in valid)
    print(f"样本: {len(valid)} | 成功: {result_dist.get('成功',0)} | 失败: {result_dist.get('失败',0)}")
    print(f"平均涨幅: {sum(gains)/len(gains):.1f}% | 中位: {sorted(gains)[len(gains)//2]:.1f}%")


if __name__ == '__main__':
    main()
