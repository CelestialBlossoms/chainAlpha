#!/usr/bin/env python3
"""Generate an HTML report for alpha 1m push Balance/Binance analysis."""

from __future__ import annotations

import argparse
import csv
import html
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from db_client import db_op
from scripts._utils_data import (
    to_float, fmt_money, fmt_pct, median, average, percentile,
    mcap_bucket, gain_bucket, drawdown_bucket, group_stat, bucket_counts,
)
from scripts._utils_html import bar_chart, group_table, html_page, stat_cards

DEFAULT_CSV = ROOT / "gmgn_outputs" / "alpha_1m_push_balance_analysis_20260520.csv"
DEFAULT_OUTPUT = ROOT / "gmgn_outputs" / "alpha_1m_push_balance_analysis_20260520.html"
DEFAULT_PATH_CSV = ROOT / "gmgn_outputs" / "alpha_1m_push_20pct_paths_20260520.csv"

NARRATIVE_ORDER = ["动物", "政治", "新闻热点", "抽象", "应用"]
NARRATIVE_KEYWORDS = {
    "政治": [
        "trump", "biden", "obama", "hunter biden", "president", "election", "vote",
        "government", "congress", "white house", "democrat", "republican", "senate",
        "maga", "america", "usa", "政治", "总统", "拜登", "特朗普", "川普", "奥巴马",
        "亨特·拜登", "亨特拜登", "选举", "政府", "白宫", "国会", "民主党", "共和党",
    ],
    "动物": [
        "cat", "dog", "deer", "tygr", "tiger", "unicorn", "ape", "monkey", "pepe",
        "frog", "toad", "bear", "bull", "fish", "lobster", "rabbit", "penguin", "wolf",
        "kitty", "roaring kitty", "marketcat", "cat", "upside down cat", "joyful deer",
        "鹿", "猫", "狗", "虎", "老虎", "独角兽", "青蛙", "熊", "牛",
        "鱼", "兔", "企鹅", "狼", "动物", "吉祥物",
    ],
    "新闻热点": [
        "breaking", "news", "report", "reported", "viral", "tiktok", "trend",
        "事件", "热点", "新闻", "病毒视频", "爆火", "传播", "热议",
        "视频", "直播", "观看", "赞", "公开", "文件称", "每日邮报",
        "案件", "金价", "误操作", "hyperliquid", "cia", "网关进程", "研究",
    ],
    "应用": [
        "ai", "agent", "agents", "agi", "llm", "gpt", "claude", "openai", "anthropic",
        "gemini", "algorithm", "bot", "app", "platform", "protocol", "defi",
        "wallet", "phantom", "ai narrative", "应用", "平台",
        "工具", "协议", "钱包", "算法", "模型", "代理", "人工智能", "技术", "回购机制",
        "推广", "空投", "交易者", "直播引导", "开发者",
    ],
    "抽象": [
        "meme", "culture", "theme", "themes", "vibe", "energy", "hopium", "happiness",
        "conviction", "hope", "joy", "purple alien", "alien", "quantum", "code",
        "55515", "q*", "memecoin", "study", "symbol", "belief", "faith", "情绪",
        "希望", "信念", "抽象", "文化", "社区", "运动", "外星人", "量子", "代码",
        "疼痛", "胶囊", "稳定", "乐观", "模因", "脑腐",
    ],
}


def peak_bucket(row: dict[str, Any]) -> str:
    max_gain = row["max_gain"]
    if max_gain <= row["gain_5m"] + 1e-9:
        return "<=5m"
    if max_gain <= row["gain_15m"] + 1e-9:
        return "5-15m"
    if max_gain <= row["gain_30m"] + 1e-9:
        return "15-30m"
    if max_gain <= row["gain_60m"] + 1e-9:
        return "30-60m"
    return ">60m"


def load_rows(csv_path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        for raw in csv.DictReader(handle):
            max_gain = to_float(raw.get("max_gain_pct"))
            current_return = to_float(raw.get("current_return_pct"))
            max_drawdown = to_float(raw.get("max_drawdown_pct"))
            row = {
                **raw,
                "valid_bool": str(raw.get("valid", "")).lower() == "true",
                "max_gain": max_gain,
                "current_return": current_return,
                "max_drawdown": max_drawdown,
                "entry_mcap_value": to_float(raw.get("entry_mcap")),
                "pool_liquidity_value": to_float(raw.get("pool_liquidity")),
                "fee_sol_value": to_float(raw.get("fee_sol")),
                "holder_count_value": to_float(raw.get("holder_count")),
                "buy_score_value": to_float(raw.get("buy_score")),
                "sm_count_value": to_float(raw.get("sm_count")),
                "kol_count_value": to_float(raw.get("kol_count")),
                "top10_rate_value": to_float(raw.get("top10_rate")),
                "snipers_value": to_float(raw.get("snipers")),
                "rug_ratio_value": to_float(raw.get("rug_ratio")),
                "vol_ratio_3m_value": to_float(raw.get("vol_ratio_3m")),
                "vol_ratio_10m_value": to_float(raw.get("vol_ratio_10m")),
                "age_min_value": to_float(raw.get("age_min_at_push")),
                "liq_mcap_ratio_value": to_float(raw.get("liq_mcap_ratio")),
                "gain_5m": to_float(raw.get("gain_5m_pct")),
                "gain_15m": to_float(raw.get("gain_15m_pct")),
                "gain_30m": to_float(raw.get("gain_30m_pct")),
                "gain_60m": to_float(raw.get("gain_60m_pct")),
                "close_5m": to_float(raw.get("close_5m_pct")),
                "close_15m": to_float(raw.get("close_15m_pct")),
                "close_30m": to_float(raw.get("close_30m_pct")),
                "close_60m": to_float(raw.get("close_60m_pct")),
            }
            row["mcap_bucket"] = mcap_bucket(row["entry_mcap_value"])
            row["peak_bucket"] = peak_bucket(row)
            rows.append(row)
    return rows


def load_path_rows(path: Path) -> dict[int, dict[str, Any]]:
    if not path.exists():
        return {}
    rows = {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for raw in csv.DictReader(handle):
            row_id = int(to_float(raw.get("id")))
            if row_id <= 0:
                continue
            rows[row_id] = {
                "path_class": raw.get("path_class") or "",
                "reached_20": str(raw.get("reached_20", "")).lower() == "true",
                "time_to_20_min": to_float(raw.get("time_to_20_min")),
                "min_before_20_pct": to_float(raw.get("min_before_20_pct")),
                "path_max_mcap_est": to_float(raw.get("max_mcap_est")),
                "path_current_mcap_est": to_float(raw.get("current_mcap_est")),
                "path_min_mcap_est": to_float(raw.get("min_mcap_est")),
            }
    return rows


def load_narratives(ids: list[int]) -> dict[int, dict[str, str]]:
    if not ids:
        return {}

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id,
                   COALESCE(raw_stats->>'name', '') AS name,
                   COALESCE(raw_stats->>'narrative_desc', raw_stats->>'narrative', '') AS narrative_desc,
                   COALESCE(raw_stats->>'narrative_type', '') AS narrative_type
            FROM alpha_push_events
            WHERE id = ANY(%s)
            """,
            (ids,),
        )
        return {
            int(row_id): {
                "name": name or "",
                "narrative_desc": desc or "",
                "narrative_type": narrative_type or "",
            }
            for row_id, name, desc, narrative_type in cur.fetchall()
        }

    try:
        return db_op(_op)
    except Exception as exc:
        print(f"warning: load narratives failed: {exc}", file=sys.stderr)
        return {}


def classify_narrative(row: dict[str, Any]) -> str:
    text = " ".join(
        str(row.get(key) or "")
        for key in ("symbol", "name", "narrative_desc", "narrative_type")
    ).lower()
    scores = {
        category: sum(1 for keyword in keywords if keyword.lower() in text)
        for category, keywords in NARRATIVE_KEYWORDS.items()
    }
    # Prefer concrete identity categories over generic "posted/news" wording.
    if scores["动物"] > 0:
        return "动物"
    if scores["政治"] > 0:
        return "政治"
    if scores["应用"] >= 2 and scores["新闻热点"] <= 2:
        return "应用"
    if scores["新闻热点"] > 0:
        return "新闻热点"
    if scores["应用"] > 0:
        return "应用"
    return "抽象"


def render_html(rows: list[dict[str, Any]]) -> str:
    valid = [row for row in rows if row["valid_bool"]]
    total = len(valid)
    gains = [row["max_gain"] for row in valid]
    currents = [row["current_return"] for row in valid]
    drawdowns = [row["max_drawdown"] for row in valid]
    overall = group_stat(valid)

    gain_counts = defaultdict(int)
    current_counts = defaultdict(int)
    drawdown_counts = defaultdict(int)
    for row in valid:
        gain_counts[gain_bucket(row["max_gain"])] += 1
        current_counts[current_bucket(row["current_return"])] += 1
        drawdown_counts[drawdown_bucket(row["max_drawdown"])] += 1

    gain_order = [">=200%", "100-200%", "50-100%", "30-50%", "10-30%", "<10%"]
    current_order = [">=100%", "30-100%", "0-30%", "-50-0%", "-80--50%", "<-80%"]
    drawdown_order = [">=-20%", "-50--20%", "-80--50%", "<-80%"]
    palette = ["#22c55e", "#10b981", "#06b6d4", "#f59e0b", "#fb923c", "#ef4444"]

    by_mcap = [
        (label, [row for row in valid if row["mcap_bucket"] == label])
        for label in ["<20K", "20-50K", "50-100K", ">=100K"]
    ]
    by_peak = [
        (label, [row for row in valid if row["peak_bucket"] == label])
        for label in ["<=5m", "5-15m", "15-30m", "30-60m", ">60m"]
    ]
    by_signal_quality = [
        ("mcap>=50K & sm>=3", [row for row in valid if row["entry_mcap_value"] >= 50_000 and row["sm_count_value"] >= 3]),
        ("mcap>=50K & fee>=5", [row for row in valid if row["entry_mcap_value"] >= 50_000 and row["fee_sol_value"] >= 5]),
        ("5m收盘仍上涨", [row for row in valid if row["close_5m"] > 0]),
        ("15m收盘仍上涨", [row for row in valid if row["close_15m"] > 0]),
        ("sm=0 & kol=0", [row for row in valid if row["sm_count_value"] == 0 and row["kol_count_value"] == 0]),
        ("20-50K & 5m收盘不涨", [row for row in valid if 20_000 <= row["entry_mcap_value"] < 50_000 and row["close_5m"] <= 0]),
    ]
    by_narrative = [
        (label, [row for row in valid if row.get("narrative_category") == label])
        for label in NARRATIVE_ORDER
    ]
    narrative_counts = bucket_counts(valid, "narrative_category", NARRATIVE_ORDER)
    path_order = ["回撤后上涨", "直接上涨", "直接下跌归零", "未达20%观察"]
    by_path = [
        (label, [row for row in valid if row.get("path_class") == label])
        for label in path_order
    ]
    path_counts = bucket_counts(valid, "path_class", path_order)
    add_candidates = [row for row in valid if row.get("reached_20")]
    zero_down = [row for row in valid if row.get("path_class") == "直接下跌归零"]

    table_rows = []
    for row in sorted(valid, key=lambda item: item["max_gain"], reverse=True):
        current_class = "positive" if row["current_return"] > 0 else "negative"
        gain_class = "positive" if row["max_gain"] >= 30 else "muted"
        drawdown_class = "negative" if row["max_drawdown"] <= -50 else "muted"
        table_rows.append(
            f"""
            <tr data-search="{html.escape((row.get('symbol','') + ' ' + row.get('address','') + ' ' + row.get('narrative_desc','') + ' ' + row.get('narrative_category','')).lower())}">
              <td>{html.escape(row.get('pushed_at', ''))}</td>
              <td>{html.escape(row.get('symbol', ''))}</td>
              <td><span class="addr">{html.escape(row.get('address', ''))}</span></td>
              <td>{html.escape(row.get('narrative_category', '抽象'))}</td>
              <td class="narrative" title="{html.escape(row.get('narrative_desc', ''))}">{html.escape((row.get('narrative_desc') or row.get('name') or '')[:72])}</td>
              <td>{html.escape(row.get('path_class') or '')}</td>
              <td data-sort="{row.get('time_to_20_min', 0):.8f}">{row.get('time_to_20_min', 0):.1f}m</td>
              <td data-sort="{row.get('min_before_20_pct', 0):.8f}">{fmt_pct(row.get('min_before_20_pct', 0))}</td>
              <td data-sort="{row['entry_mcap_value']:.8f}">{fmt_money(row['entry_mcap_value'])}</td>
              <td data-sort="{row.get('path_max_mcap_est', 0):.8f}">{fmt_money(row.get('path_max_mcap_est', 0))}</td>
              <td data-sort="{row.get('path_current_mcap_est', 0):.8f}">{fmt_money(row.get('path_current_mcap_est', 0))}</td>
              <td data-sort="{row.get('path_min_mcap_est', 0):.8f}">{fmt_money(row.get('path_min_mcap_est', 0))}</td>
              <td data-sort="{row['pool_liquidity_value']:.8f}">{fmt_money(row['pool_liquidity_value'])}</td>
              <td data-sort="{row['holder_count_value']:.8f}">{row['holder_count_value']:.0f}</td>
              <td data-sort="{row['fee_sol_value']:.8f}">{row['fee_sol_value']:.1f}</td>
              <td data-sort="{row['sm_count_value']:.8f}">{row['sm_count_value']:.0f}</td>
              <td data-sort="{row['kol_count_value']:.8f}">{row['kol_count_value']:.0f}</td>
              <td data-sort="{row['top10_rate_value']:.8f}">{row['top10_rate_value']:.1f}%</td>
              <td data-sort="{row['rug_ratio_value']:.8f}">{row['rug_ratio_value']:.3f}</td>
              <td data-sort="{row['max_gain']:.8f}" class="{gain_class}">{fmt_pct(row['max_gain'])}</td>
              <td data-sort="{row['current_return']:.8f}" class="{current_class}">{fmt_pct(row['current_return'])}</td>
              <td data-sort="{row['max_drawdown']:.8f}" class="{drawdown_class}">{fmt_pct(row['max_drawdown'])}</td>
              <td data-sort="{row['gain_5m']:.8f}">{fmt_pct(row['gain_5m'])}</td>
              <td data-sort="{row['gain_15m']:.8f}">{fmt_pct(row['gain_15m'])}</td>
              <td data-sort="{row['gain_60m']:.8f}">{fmt_pct(row['gain_60m'])}</td>
              <td data-sort="{row['close_5m']:.8f}">{fmt_pct(row['close_5m'])}</td>
              <td data-sort="{row['close_15m']:.8f}">{fmt_pct(row['close_15m'])}</td>
              <td data-sort="{row['vol_ratio_3m_value']:.8f}">{row['vol_ratio_3m_value']:.2f}x</td>
              <td>{html.escape(row['peak_bucket'])}</td>
            </tr>
            """
        )

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Alpha 1m 新币推送涨跌分析</title>
  <style>
    :root {{
      color-scheme: dark;
      --bg: #0f172a;
      --panel: #172033;
      --panel2: #1f2a3d;
      --line: #2d3b52;
      --text: #e5edf8;
      --muted: #93a4b8;
      --green: #22c55e;
      --red: #ef4444;
      --yellow: #f59e0b;
      --blue: #38bdf8;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; padding: 24px; background: var(--bg); color: var(--text); font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }}
    h1 {{ margin: 0 0 6px; font-size: 24px; }}
    h2 {{ margin: 28px 0 12px; font-size: 17px; color: #cbd5e1; }}
    .sub {{ margin: 0 0 18px; color: var(--muted); font-size: 13px; }}
    .note {{ margin: 0 0 20px; padding: 12px 14px; border: 1px solid #28456c; background: #10233d; color: #bfdbfe; border-radius: 8px; font-size: 13px; }}
    .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; margin-bottom: 18px; }}
    .card {{ background: var(--panel); border: 1px solid var(--line); border-radius: 8px; padding: 14px; }}
    .label {{ color: var(--muted); font-size: 12px; margin-bottom: 5px; }}
    .value {{ font-weight: 700; font-size: 22px; }}
    .positive {{ color: var(--green); font-weight: 650; }}
    .negative {{ color: var(--red); font-weight: 650; }}
    .muted {{ color: var(--muted); }}
    .charts {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 14px; }}
    .chart {{ background: var(--panel); border: 1px solid var(--line); border-radius: 8px; padding: 14px; }}
    .chart-title {{ color: #cbd5e1; font-size: 13px; font-weight: 650; margin-bottom: 10px; }}
    .bar-row {{ display: flex; align-items: center; gap: 8px; margin: 7px 0; font-size: 12px; }}
    .bar-label {{ width: 78px; color: var(--muted); text-align: right; flex: 0 0 auto; }}
    .bar-track {{ height: 20px; background: #263448; border-radius: 5px; overflow: hidden; flex: 1; }}
    .bar-fill {{ height: 100%; border-radius: 5px; }}
    .bar-count {{ width: 82px; color: #cbd5e1; flex: 0 0 auto; }}
    .table-wrap {{ overflow: auto; border: 1px solid var(--line); border-radius: 8px; background: var(--panel); }}
    table {{ width: 100%; border-collapse: collapse; font-size: 12px; }}
    th, td {{ padding: 8px 9px; border-bottom: 1px solid #243145; white-space: nowrap; text-align: left; }}
    th {{ position: sticky; top: 0; background: var(--panel2); color: #b6c5d8; cursor: pointer; user-select: none; z-index: 1; }}
    .compact th, .compact td {{ padding: 8px 10px; }}
    tbody tr:nth-child(even) td {{ background: rgba(255, 255, 255, 0.018); }}
    tbody tr:hover td {{ background: rgba(56, 189, 248, 0.07); }}
    .addr {{ font-family: Consolas, monospace; color: #7dd3fc; user-select: all; }}
    .narrative {{ max-width: 420px; overflow: hidden; text-overflow: ellipsis; }}
    .toolbar {{ display: flex; gap: 10px; align-items: center; margin-bottom: 10px; }}
    input {{ background: var(--panel); border: 1px solid var(--line); color: var(--text); border-radius: 7px; padding: 9px 10px; min-width: 280px; }}
    footer {{ color: #64748b; font-size: 12px; margin-top: 28px; text-align: center; }}
  </style>
</head>
<body>
  <h1>Alpha 1m 新币推送涨跌分析</h1>
  <p class="sub">样本 {total} 条，数据源为 alpha_push_events.trend_interval=1m + Balance/Binance 1min K 线。Entry 为推送后首根 1m K 的 open。</p>
  <div class="note">重点口径：最高涨幅用于衡量信号能否捕捉冲高；当前涨跌和最大跌幅用于衡量冲高后的留存与回撤风险。</div>

  <div class="cards">
    <div class="card"><div class="label">最高涨幅 >=30%</div><div class="value positive">{overall['hit30']:.1f}%</div></div>
    <div class="card"><div class="label">符合添加(>=20%)</div><div class="value positive">{len(add_candidates)}/{total}</div></div>
    <div class="card"><div class="label">直接下跌归零</div><div class="value negative">{len(zero_down)}</div></div>
    <div class="card"><div class="label">最高涨幅 >=100%</div><div class="value positive">{overall['hit100']:.1f}%</div></div>
    <div class="card"><div class="label">当前仍上涨</div><div class="value">{overall['alive']:.1f}%</div></div>
    <div class="card"><div class="label">最高涨幅中位</div><div class="value positive">{fmt_pct(overall['median_gain'])}</div></div>
    <div class="card"><div class="label">当前涨跌中位</div><div class="value negative">{fmt_pct(overall['median_current'])}</div></div>
    <div class="card"><div class="label">最大跌幅中位</div><div class="value negative">{fmt_pct(overall['median_drawdown'])}</div></div>
    <div class="card"><div class="label">最高涨幅 P75</div><div class="value positive">{fmt_pct(percentile(gains, 75))}</div></div>
    <div class="card"><div class="label">当前涨跌 P25</div><div class="value negative">{fmt_pct(percentile(currents, 25))}</div></div>
  </div>

  <div class="charts">
    <div class="chart"><div class="chart-title">最高涨幅分布</div>{bar_chart([(label, gain_counts.get(label, 0)) for label in gain_order], total, palette)}</div>
    <div class="chart"><div class="chart-title">当前涨跌分布</div>{bar_chart([(label, current_counts.get(label, 0)) for label in current_order], total, palette)}</div>
    <div class="chart"><div class="chart-title">最大跌幅分布</div>{bar_chart([(label, drawdown_counts.get(label, 0)) for label in drawdown_order], total, ["#22c55e", "#f59e0b", "#fb923c", "#ef4444"])}</div>
    <div class="chart"><div class="chart-title">峰值出现时间</div>{bar_chart(bucket_counts(valid, "peak_bucket", ["<=5m", "5-15m", "15-30m", "30-60m", ">60m"]), total, palette)}</div>
    <div class="chart"><div class="chart-title">叙事分类分布</div>{bar_chart(narrative_counts, total, ["#22c55e", "#ef4444", "#f59e0b", "#8b5cf6", "#38bdf8"])}</div>
    <div class="chart"><div class="chart-title">20%路径分类</div>{bar_chart(path_counts, total, ["#22c55e", "#38bdf8", "#ef4444", "#f59e0b"])}</div>
  </div>

  {group_table("按20%路径分类", by_path)}
  {group_table("按叙事分类", by_narrative)}
  {group_table("按推送市值分组", by_mcap)}
  {group_table("按峰值出现时间分组", by_peak)}
  {group_table("关键条件组合", by_signal_quality)}

  <section>
    <h2>每个 CA 的涨幅/跌幅明细</h2>
    <div class="toolbar"><input id="search" placeholder="搜索 symbol 或 CA" oninput="filterRows()"></div>
    <div class="table-wrap">
      <table id="detail">
        <thead>
          <tr>
            <th onclick="sortTable(0, false)">推送时间</th>
            <th onclick="sortTable(1, false)">Symbol</th>
            <th>CA</th>
            <th onclick="sortTable(3, false)">叙事分类</th>
            <th>叙事</th>
            <th onclick="sortTable(5, false)">20%路径</th>
            <th onclick="sortTable(6, true)">到20%时间</th>
            <th onclick="sortTable(7, true)">20%前回撤</th>
            <th onclick="sortTable(8, true)">推送市值</th>
            <th onclick="sortTable(9, true)">峰值市值估算</th>
            <th onclick="sortTable(10, true)">当前市值估算</th>
            <th onclick="sortTable(11, true)">最低市值估算</th>
            <th onclick="sortTable(12, true)">池子</th>
            <th onclick="sortTable(13, true)">Holder</th>
            <th onclick="sortTable(14, true)">Fee</th>
            <th onclick="sortTable(15, true)">SM</th>
            <th onclick="sortTable(16, true)">KOL</th>
            <th onclick="sortTable(17, true)">Top10</th>
            <th onclick="sortTable(18, true)">Rug</th>
            <th onclick="sortTable(19, true)">最高涨幅</th>
            <th onclick="sortTable(20, true)">当前涨跌</th>
            <th onclick="sortTable(21, true)">最大跌幅</th>
            <th onclick="sortTable(22, true)">5m最高</th>
            <th onclick="sortTable(23, true)">15m最高</th>
            <th onclick="sortTable(24, true)">60m最高</th>
            <th onclick="sortTable(25, true)">5m收盘</th>
            <th onclick="sortTable(26, true)">15m收盘</th>
            <th onclick="sortTable(27, true)">3m量比</th>
            <th onclick="sortTable(28, false)">峰值时间</th>
          </tr>
        </thead>
        <tbody>{''.join(table_rows)}</tbody>
      </table>
    </div>
  </section>

  <footer>Generated from {html.escape(str(DEFAULT_CSV.relative_to(ROOT)))}.</footer>
  <script>
    let sortState = {{}};
    function cellValue(row, index, numeric) {{
      const cell = row.children[index];
      if (!cell) return numeric ? 0 : "";
      if (numeric) return parseFloat(cell.dataset.sort || cell.textContent.replace(/[$,%xKMSOL]/g, "")) || 0;
      return cell.textContent.trim().toLowerCase();
    }}
    function sortTable(index, numeric) {{
      const table = document.getElementById("detail");
      const tbody = table.tBodies[0];
      const rows = Array.from(tbody.rows);
      const key = index + ":" + numeric;
      const dir = sortState[key] === "asc" ? "desc" : "asc";
      sortState = {{ [key]: dir }};
      rows.sort((a, b) => {{
        const av = cellValue(a, index, numeric);
        const bv = cellValue(b, index, numeric);
        if (av < bv) return dir === "asc" ? -1 : 1;
        if (av > bv) return dir === "asc" ? 1 : -1;
        return 0;
      }});
      rows.forEach(row => tbody.appendChild(row));
    }}
    function filterRows() {{
      const q = document.getElementById("search").value.trim().toLowerCase();
      document.querySelectorAll("#detail tbody tr").forEach(row => {{
        row.style.display = row.dataset.search.includes(q) ? "" : "none";
      }});
    }}
  </script>
</body>
</html>
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Alpha 1m push Balance/Binance HTML report.")
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    rows = load_rows(args.csv)
    narratives = load_narratives([int(to_float(row.get("id"))) for row in rows if to_float(row.get("id")) > 0])
    paths = load_path_rows(DEFAULT_PATH_CSV)
    for row in rows:
        row_id = int(to_float(row.get("id")))
        narrative = narratives.get(row_id, {})
        row["name"] = narrative.get("name", "")
        row["narrative_desc"] = narrative.get("narrative_desc", "")
        row["narrative_type"] = narrative.get("narrative_type", "")
        row["narrative_category"] = classify_narrative(row)
        row.update(paths.get(row_id, {}))
    html_text = render_html(rows)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(html_text, encoding="utf-8")
    print(f"HTML: {args.output}")


if __name__ == "__main__":
    main()
