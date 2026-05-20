#!/usr/bin/env python3
"""Generate an HTML report for Deep Alpha Pro 1m new-token pushes."""

from __future__ import annotations

import argparse
import html
import math
import os
import sys
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from binance_narrative import deepseek_analyze_narrative, keyword_classify_narrative_category
from db_client import db_op
from scripts._utils_data import average, median, to_float, to_int
from scripts._utils_kline import fetch_range


REPORT_TZ = ZoneInfo(os.getenv("REPORT_TZ", "Asia/Shanghai"))
DEFAULT_OUTPUT = ROOT / "gmgn_outputs" / "deep_alpha_1m_push_report.html"
INTERVAL_SECONDS = {
    "1min": 60,
    "5min": 300,
    "15min": 900,
    "1h": 3600,
}
CATEGORY_ORDER = ["应用", "抽象", "动物", "政治", "其他", "未知"]


def to_ts(value: Any) -> int:
    if isinstance(value, datetime):
        return int(value.timestamp())
    try:
        ts = int(float(value))
        return ts // 1000 if ts > 10_000_000_000 else ts
    except (TypeError, ValueError):
        return 0


def fmt_time(ts: Any) -> str:
    value = to_ts(ts)
    if value <= 0:
        return "-"
    return datetime.fromtimestamp(value, REPORT_TZ).strftime("%m-%d %H:%M")


def fmt_full_time(ts: Any) -> str:
    value = to_ts(ts)
    if value <= 0:
        return "-"
    return datetime.fromtimestamp(value, REPORT_TZ).strftime("%Y-%m-%d %H:%M:%S")


def parse_date(value: str) -> int:
    text = str(value or "").strip()
    if not text:
        return 0
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return int(datetime.strptime(text, fmt).replace(tzinfo=REPORT_TZ).timestamp())
        except ValueError:
            continue
    raise ValueError(f"unsupported date format: {value}")


def fmt_money(value: Any) -> str:
    amount = to_float(value)
    sign = "-" if amount < 0 else ""
    amount = abs(amount)
    if amount >= 1_000_000:
        return f"{sign}${amount / 1_000_000:.2f}M"
    if amount >= 1_000:
        return f"{sign}${amount / 1_000:.1f}K"
    return f"{sign}${amount:.0f}"


def fmt_pct(value: Any, signed: bool = True) -> str:
    number = to_float(value)
    prefix = "+" if signed and number > 0 else ""
    return f"{prefix}{number:.1f}%"


def pct(price: float, base: float) -> float:
    return (price / base - 1) * 100 if price > 0 and base > 0 else 0.0


def mcap_from_pct(entry_mcap: float, change_pct: float) -> float:
    return entry_mcap * (1 + change_pct / 100) if entry_mcap > 0 else 0.0


def safe_category(value: Any) -> str:
    text = str(value or "").strip()
    return text if text in CATEGORY_ORDER else (text or "未知")


def narrative_from_raw(raw: dict[str, Any]) -> tuple[str, str, str]:
    narrative_obj = raw.get("binance_narrative") if isinstance(raw.get("binance_narrative"), dict) else {}
    desc = (
        raw.get("narrative_desc")
        or raw.get("narrative")
        or narrative_obj.get("narrative_desc")
        or ""
    )
    narrative_type = raw.get("narrative_type") or narrative_obj.get("narrative_type") or ""
    category = raw.get("narrative_category") or narrative_obj.get("narrative_category") or ""
    return str(desc or ""), str(narrative_type or ""), str(category or "")


def fetch_events(
    limit: int,
    since_ts: int,
    until_ts: int,
    include_repeat: bool,
) -> list[dict[str, Any]]:
    def _op(conn):
        cur = conn.cursor()
        cur.execute("SELECT to_regclass('public.token_narratives')")
        has_narratives = bool(cur.fetchone()[0])
        narrative_select = "COALESCE(n.raw->>'narrative_category', '')" if has_narratives else "''"
        narrative_join = "LEFT JOIN token_narratives n ON n.ca = e.address" if has_narratives else ""
        where = [
            "e.trend_interval = '1m'",
            "COALESCE(e.source, '1m') = '1m'",
        ]
        params: list[Any] = []
        if since_ts > 0:
            where.append("e.pushed_at >= to_timestamp(%s)")
            params.append(since_ts)
        if until_ts > 0:
            where.append("e.pushed_at <= to_timestamp(%s)")
            params.append(until_ts)
        if not include_repeat:
            where.append("COALESCE(e.repeat_alert, false) = false")
        sql = f"""
            SELECT
                e.id,
                e.address,
                e.chain,
                e.symbol,
                e.entry_mcap,
                e.entry_price,
                e.holder_count,
                e.fee_sol,
                e.buy_score,
                e.pushed_at,
                e.alert_no,
                COALESCE(e.repeat_alert, false),
                e.repeat_alert_type,
                e.raw_stats,
                {narrative_select} AS db_narrative_category
            FROM alpha_push_events e
            {narrative_join}
            WHERE {' AND '.join(where)}
            ORDER BY e.pushed_at DESC, e.id DESC
        """
        if limit > 0:
            sql += " LIMIT %s"
            params.append(limit)
        cur.execute(sql, params)
        rows = []
        for row in cur.fetchall():
            raw = row[13] if isinstance(row[13], dict) else {}
            narrative_desc, narrative_type, raw_category = narrative_from_raw(raw)
            rows.append(
                {
                    "id": int(row[0]),
                    "address": row[1],
                    "chain": row[2] or "sol",
                    "symbol": row[3] or raw.get("symbol") or "UNKNOWN",
                    "entry_mcap": to_float(row[4]),
                    "entry_price": to_float(row[5]) or to_float(raw.get("price")),
                    "holder_count": to_int(row[6]),
                    "fee_sol": to_float(row[7]),
                    "buy_score": to_int(row[8]),
                    "pushed_at": row[9],
                    "pushed_ts": to_ts(row[9]),
                    "alert_no": to_int(row[10]),
                    "repeat_alert": bool(row[11]),
                    "repeat_alert_type": row[12] or "",
                    "raw_stats": raw,
                    "narrative_desc": narrative_desc,
                    "narrative_type": narrative_type,
                    "narrative_category": raw_category or row[14] or "",
                }
            )
        return rows

    return db_op(_op) or []


def resolve_narrative_category(event: dict[str, Any], use_deepseek: bool = False) -> str:
    existing = safe_category(event.get("narrative_category"))
    if existing != "未知":
        return existing
    desc = event.get("narrative_desc") or ""
    narrative_type = event.get("narrative_type") or ""
    if use_deepseek:
        analysis = deepseek_analyze_narrative(
            desc,
            narrative_type,
            [],
            symbol=event.get("symbol") or "",
        )
        category = safe_category(analysis.get("narrative_category"))
        if category != "未知":
            return category
    return safe_category(keyword_classify_narrative_category(desc, narrative_type, []))


def analyze_event(
    event: dict[str, Any],
    now_ts: int,
    interval: str,
    use_deepseek_category: bool,
) -> dict[str, Any]:
    step = INTERVAL_SECONDS.get(interval, 60)
    from_ts = max(0, event["pushed_ts"] - step * 2)
    candles = fetch_range(event["address"], from_ts, now_ts, interval=interval)
    post = [candle for candle in candles if int(candle.get("ts") or 0) + step > event["pushed_ts"]]
    if not post:
        return {
            **event,
            "valid": False,
            "invalid_reason": "no_post_kline",
            "narrative_category": resolve_narrative_category(event, use_deepseek_category),
            "candles": 0,
        }

    signal_candle = post[0]
    entry_price = (
        to_float(event.get("entry_price"))
        or to_float(signal_candle.get("open"))
        or to_float(signal_candle.get("close"))
    )
    if entry_price <= 0:
        return {
            **event,
            "valid": False,
            "invalid_reason": "invalid_entry_price",
            "narrative_category": resolve_narrative_category(event, use_deepseek_category),
            "candles": len(post),
        }

    peak_candle = max(post, key=lambda candle: to_float(candle.get("high")))
    low_candle = min(post, key=lambda candle: to_float(candle.get("low")))
    current_candle = post[-1]
    peak_price = to_float(peak_candle.get("high"))
    low_price = to_float(low_candle.get("low"))
    current_price = to_float(current_candle.get("close"))

    peak_gain_pct = pct(peak_price, entry_price)
    current_gain_pct = pct(current_price, entry_price)
    entry_drawdown_pct = pct(low_price, entry_price)
    drawdown_from_peak_pct = pct(current_price, peak_price)
    entry_mcap = to_float(event.get("entry_mcap"))
    peak_mcap = mcap_from_pct(entry_mcap, peak_gain_pct)
    current_mcap = mcap_from_pct(entry_mcap, current_gain_pct)
    low_mcap = mcap_from_pct(entry_mcap, entry_drawdown_pct)
    peak_ts = to_ts(peak_candle.get("ts"))

    return {
        **event,
        "valid": True,
        "invalid_reason": "",
        "narrative_category": resolve_narrative_category(event, use_deepseek_category),
        "entry_price_used": entry_price,
        "current_price": current_price,
        "peak_price": peak_price,
        "low_price": low_price,
        "peak_gain_pct": peak_gain_pct,
        "current_gain_pct": current_gain_pct,
        "entry_drawdown_pct": entry_drawdown_pct,
        "drawdown_from_peak_pct": drawdown_from_peak_pct,
        "entry_mcap": entry_mcap,
        "peak_mcap": peak_mcap,
        "current_mcap": current_mcap,
        "low_mcap": low_mcap,
        "pushed_time": fmt_full_time(event.get("pushed_ts")),
        "peak_time": fmt_full_time(peak_ts),
        "peak_ts": peak_ts,
        "minutes_to_peak": max(0, (peak_ts - event["pushed_ts"]) / 60),
        "current_time": fmt_full_time(current_candle.get("ts")),
        "volume_usd": sum(to_float(candle.get("volume")) for candle in post),
        "candles": len(post),
    }


def category_sort_key(category: str) -> tuple[int, str]:
    category = safe_category(category)
    try:
        return (CATEGORY_ORDER.index(category), category)
    except ValueError:
        return (len(CATEGORY_ORDER), category)


def group_stat(rows: list[dict[str, Any]]) -> dict[str, Any]:
    valid = [row for row in rows if row.get("valid")]
    if not valid:
        return {
            "count": len(rows),
            "valid": 0,
            "hit30": 0,
            "hit100": 0,
            "alive": 0,
            "median_peak": 0,
            "median_current": 0,
            "median_drawdown": 0,
            "max_peak": 0,
            "worst_drawdown": 0,
        }
    peaks = [to_float(row.get("peak_gain_pct")) for row in valid]
    currents = [to_float(row.get("current_gain_pct")) for row in valid]
    peak_drawdowns = [to_float(row.get("drawdown_from_peak_pct")) for row in valid]
    return {
        "count": len(rows),
        "valid": len(valid),
        "hit30": sum(1 for value in peaks if value >= 30) / len(valid) * 100,
        "hit100": sum(1 for value in peaks if value >= 100) / len(valid) * 100,
        "alive": sum(1 for value in currents if value > 0) / len(valid) * 100,
        "median_peak": median(peaks),
        "median_current": median(currents),
        "median_drawdown": median(peak_drawdowns),
        "max_peak": max(peaks),
        "worst_drawdown": min(peak_drawdowns),
    }


def pct_bar(value: float, scale: float, positive_label: str = "") -> str:
    number = to_float(value)
    width = min(100.0, abs(number) / max(scale, 1) * 100)
    cls = "pos" if number >= 0 else "neg"
    label = positive_label or fmt_pct(number)
    return (
        f'<div class="pct-wrap"><div class="pct-fill {cls}" style="width:{width:.1f}%"></div>'
        f'<span class="{cls}">{html.escape(label)}</span></div>'
    )


def render_cards(rows: list[dict[str, Any]]) -> str:
    valid = [row for row in rows if row.get("valid")]
    stat = group_stat(rows)
    current_values = [to_float(row.get("current_gain_pct")) for row in valid]
    peak_values = [to_float(row.get("peak_gain_pct")) for row in valid]
    cards = [
        ("推送数", str(len(rows)), ""),
        ("有效K线", f"{len(valid)}/{len(rows)}", ""),
        ("峰值>=30%", f"{stat['hit30']:.1f}%", "pos"),
        ("峰值>=100%", f"{stat['hit100']:.1f}%", "pos"),
        ("当前仍上涨", f"{stat['alive']:.1f}%", "pos" if stat["alive"] >= 50 else "warn"),
        ("峰值中位涨幅", fmt_pct(stat["median_peak"], signed=False), "pos"),
        ("当前中位涨跌", fmt_pct(median(current_values)), "pos" if median(current_values) > 0 else "neg"),
        ("最大峰值涨幅", fmt_pct(max(peak_values) if peak_values else 0, signed=False), "pos"),
    ]
    return "".join(
        f'<div class="card"><div class="label">{html.escape(label)}</div>'
        f'<div class="value {cls}">{html.escape(value)}</div></div>'
        for label, value, cls in cards
    )


def render_category_summary(rows: list[dict[str, Any]]) -> str:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[safe_category(row.get("narrative_category"))].append(row)
    parts = []
    for category in sorted(grouped, key=category_sort_key):
        stat = group_stat(grouped[category])
        parts.append(
            f"""
            <tr>
              <td>{html.escape(category)}</td>
              <td>{stat['valid']}/{stat['count']}</td>
              <td class="pos">{stat['hit30']:.1f}%</td>
              <td class="pos">{stat['hit100']:.1f}%</td>
              <td class="{ 'pos' if stat['alive'] >= 50 else 'warn' }">{stat['alive']:.1f}%</td>
              <td class="pos">{fmt_pct(stat['median_peak'], signed=False)}</td>
              <td class="{ 'pos' if stat['median_current'] > 0 else 'neg' }">{fmt_pct(stat['median_current'])}</td>
              <td class="neg">{fmt_pct(stat['median_drawdown'])}</td>
              <td class="pos">{fmt_pct(stat['max_peak'], signed=False)}</td>
              <td class="neg">{fmt_pct(stat['worst_drawdown'])}</td>
            </tr>
            """
        )
    return f"""
    <section>
      <h2>叙事分类表现</h2>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>叙事分类</th><th>有效/总数</th><th>峰值>=30%</th><th>峰值>=100%</th>
              <th>当前仍上涨</th><th>峰值涨幅中位</th><th>当前涨跌中位</th>
              <th>峰值回撤中位</th><th>最大峰值涨幅</th><th>最大回撤</th>
            </tr>
          </thead>
          <tbody>{''.join(parts)}</tbody>
        </table>
      </div>
    </section>
    """


def render_bucket_bars(title: str, counts: Counter[str], total: int, palette: list[str]) -> str:
    rows = []
    for index, (label, count) in enumerate(counts.most_common()):
        pct_value = count / total * 100 if total else 0
        color = palette[index % len(palette)]
        rows.append(
            f"""
            <div class="bar-row">
              <span class="bar-label">{html.escape(label)}</span>
              <div class="bar-track"><div class="bar-fill" style="width:{pct_value:.1f}%;background:{color}"></div></div>
              <span class="bar-count">{count} ({pct_value:.1f}%)</span>
            </div>
            """
        )
    return f'<div class="chart"><div class="chart-title">{html.escape(title)}</div>{"".join(rows)}</div>'


def gain_bucket(value: float) -> str:
    if value >= 300:
        return ">=300%"
    if value >= 200:
        return "200-300%"
    if value >= 100:
        return "100-200%"
    if value >= 50:
        return "50-100%"
    if value >= 30:
        return "30-50%"
    if value >= 10:
        return "10-30%"
    return "<10%"


def current_bucket(value: float) -> str:
    if value >= 100:
        return ">=100%"
    if value >= 30:
        return "30-100%"
    if value >= 0:
        return "0-30%"
    if value >= -50:
        return "-50-0%"
    if value >= -80:
        return "-80--50%"
    return "<-80%"


def drawdown_bucket(value: float) -> str:
    if value >= -20:
        return ">=-20%"
    if value >= -50:
        return "-50--20%"
    if value >= -80:
        return "-80--50%"
    return "<-80%"


def render_charts(rows: list[dict[str, Any]]) -> str:
    valid = [row for row in rows if row.get("valid")]
    palette = ["#22c55e", "#38bdf8", "#f59e0b", "#a78bfa", "#ef4444", "#14b8a6"]
    category_counts = Counter(safe_category(row.get("narrative_category")) for row in rows)
    peak_counts = Counter(gain_bucket(to_float(row.get("peak_gain_pct"))) for row in valid)
    current_counts = Counter(current_bucket(to_float(row.get("current_gain_pct"))) for row in valid)
    drawdown_counts = Counter(drawdown_bucket(to_float(row.get("drawdown_from_peak_pct"))) for row in valid)
    return (
        '<section><h2>分布概览</h2><div class="charts">'
        + render_bucket_bars("叙事分类数量", category_counts, len(rows), palette)
        + render_bucket_bars("峰值涨幅分布", peak_counts, len(valid), palette)
        + render_bucket_bars("当前涨跌分布", current_counts, len(valid), palette)
        + render_bucket_bars("从峰值回撤分布", drawdown_counts, len(valid), palette)
        + "</div></section>"
    )


def render_detail_table(rows: list[dict[str, Any]]) -> str:
    valid = [row for row in rows if row.get("valid")]
    max_peak_abs = max([abs(to_float(row.get("peak_gain_pct"))) for row in valid] or [1])
    max_current_abs = max([abs(to_float(row.get("current_gain_pct"))) for row in valid] or [1])
    max_drawdown_abs = max([abs(to_float(row.get("drawdown_from_peak_pct"))) for row in valid] or [1])
    sorted_rows = sorted(
        rows,
        key=lambda row: (
            category_sort_key(row.get("narrative_category"))[0],
            -to_float(row.get("peak_gain_pct")),
            -to_float(row.get("current_gain_pct")),
        ),
    )
    body = []
    for row in sorted_rows:
        valid_row = bool(row.get("valid"))
        narrative = row.get("narrative_desc") or row.get("narrative_type") or ""
        search = " ".join(
            str(row.get(key) or "")
            for key in ("symbol", "address", "narrative_category", "narrative_desc", "narrative_type")
        ).lower()
        body.append(
            f"""
            <tr data-search="{html.escape(search)}">
              <td data-sort="{category_sort_key(row.get('narrative_category'))[0]}"><span class="tag">{html.escape(safe_category(row.get('narrative_category')))}</span></td>
              <td>{html.escape(str(row.get('symbol') or 'UNKNOWN'))}</td>
              <td><span class="addr">{html.escape(str(row.get('address') or ''))}</span></td>
              <td data-sort="{row.get('pushed_ts') or 0}">{html.escape(fmt_time(row.get('pushed_ts')))}</td>
              <td data-sort="{to_float(row.get('entry_mcap'))}">{fmt_money(row.get('entry_mcap'))}</td>
              <td data-sort="{to_float(row.get('current_gain_pct'))}">{pct_bar(to_float(row.get('current_gain_pct')), max_current_abs) if valid_row else html.escape(row.get('invalid_reason') or 'no data')}</td>
              <td data-sort="{to_float(row.get('peak_gain_pct'))}">{pct_bar(to_float(row.get('peak_gain_pct')), max_peak_abs, fmt_pct(row.get('peak_gain_pct'), signed=False)) if valid_row else '-'}</td>
              <td data-sort="{to_float(row.get('current_mcap'))}">{fmt_money(row.get('current_mcap')) if valid_row else '-'}</td>
              <td data-sort="{to_float(row.get('peak_mcap'))}">{fmt_money(row.get('peak_mcap')) if valid_row else '-'}</td>
              <td data-sort="{to_float(row.get('drawdown_from_peak_pct'))}">{pct_bar(to_float(row.get('drawdown_from_peak_pct')), max_drawdown_abs) if valid_row else '-'}</td>
              <td data-sort="{to_float(row.get('entry_drawdown_pct'))}"><span class="neg">{fmt_pct(row.get('entry_drawdown_pct')) if valid_row else '-'}</span></td>
              <td data-sort="{row.get('peak_ts') or 0}">{html.escape(fmt_time(row.get('peak_ts'))) if valid_row else '-'}</td>
              <td data-sort="{to_float(row.get('minutes_to_peak'))}">{to_float(row.get('minutes_to_peak')):.0f}m</td>
              <td data-sort="{to_float(row.get('volume_usd'))}">{fmt_money(row.get('volume_usd')) if valid_row else '-'}</td>
              <td data-sort="{to_float(row.get('holder_count'))}">{to_int(row.get('holder_count'))}</td>
              <td data-sort="{to_float(row.get('fee_sol'))}">{to_float(row.get('fee_sol')):.2f}</td>
              <td class="narrative" title="{html.escape(narrative)}">{html.escape(narrative[:120])}</td>
            </tr>
            """
        )
    return f"""
    <section>
      <h2>明细：按叙事分类 + 峰值涨幅排序</h2>
      <div class="toolbar">
        <input id="search" placeholder="搜索 Symbol / CA / 叙事 / 分类" oninput="filterRows()">
      </div>
      <div class="table-wrap">
        <table id="detail">
          <thead>
            <tr>
              <th onclick="sortTable(0, true)">叙事</th>
              <th onclick="sortTable(1, false)">Symbol</th>
              <th>CA</th>
              <th onclick="sortTable(3, true)">异动时间</th>
              <th onclick="sortTable(4, true)">异动市值</th>
              <th onclick="sortTable(5, true)">现在涨幅</th>
              <th onclick="sortTable(6, true)">巅峰涨幅</th>
              <th onclick="sortTable(7, true)">现在市值</th>
              <th onclick="sortTable(8, true)">巅峰市值</th>
              <th onclick="sortTable(9, true)">峰值回撤</th>
              <th onclick="sortTable(10, true)">最大下探</th>
              <th onclick="sortTable(11, true)">巅峰时间</th>
              <th onclick="sortTable(12, true)">到峰值</th>
              <th onclick="sortTable(13, true)">成交量</th>
              <th onclick="sortTable(14, true)">持有人</th>
              <th onclick="sortTable(15, true)">手续费SOL</th>
              <th>叙事摘要</th>
            </tr>
          </thead>
          <tbody>{''.join(body)}</tbody>
        </table>
      </div>
    </section>
    """


def render_leaderboards(rows: list[dict[str, Any]]) -> str:
    valid = [row for row in rows if row.get("valid")]
    boards = [
        ("巅峰涨幅 Top 15", sorted(valid, key=lambda row: to_float(row.get("peak_gain_pct")), reverse=True)[:15], "peak_gain_pct"),
        ("当前涨幅 Top 15", sorted(valid, key=lambda row: to_float(row.get("current_gain_pct")), reverse=True)[:15], "current_gain_pct"),
        ("从峰值回撤 Top 15", sorted(valid, key=lambda row: to_float(row.get("drawdown_from_peak_pct")))[:15], "drawdown_from_peak_pct"),
    ]
    parts = []
    for title, items, key in boards:
        rows_html = []
        for index, row in enumerate(items, 1):
            cls = "pos" if to_float(row.get(key)) >= 0 else "neg"
            rows_html.append(
                f"""
                <tr>
                  <td>{index}</td><td>{html.escape(str(row.get('symbol') or 'UNKNOWN'))}</td>
                  <td><span class="addr">{html.escape(str(row.get('address') or '')[:12])}...</span></td>
                  <td><span class="{cls}">{fmt_pct(row.get(key), signed=key != 'peak_gain_pct')}</span></td>
                  <td>{fmt_money(row.get('entry_mcap'))} -> {fmt_money(row.get('peak_mcap'))} -> {fmt_money(row.get('current_mcap'))}</td>
                  <td>{html.escape(safe_category(row.get('narrative_category')))}</td>
                </tr>
                """
            )
        parts.append(
            f"""
            <div class="leader">
              <h3>{html.escape(title)}</h3>
              <table><tbody>{''.join(rows_html)}</tbody></table>
            </div>
            """
        )
    return f'<section><h2>极值榜</h2><div class="leaders">{"".join(parts)}</div></section>'


def render_html(rows: list[dict[str, Any]], *, title: str, subtitle: str) -> str:
    generated_at = datetime.now(REPORT_TZ).strftime("%Y-%m-%d %H:%M:%S")
    style = """
    :root { color-scheme: dark; --bg:#0b1020; --panel:#111827; --panel2:#172033; --line:#2a3650; --text:#e5edf8; --muted:#95a3b8; --green:#22c55e; --red:#ef4444; --yellow:#f59e0b; --blue:#38bdf8; }
    * { box-sizing: border-box; }
    body { margin:0; padding:24px; background:var(--bg); color:var(--text); font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; }
    h1 { margin:0 0 6px; font-size:26px; }
    h2 { margin:28px 0 12px; font-size:18px; color:#dbeafe; }
    h3 { margin:0 0 10px; font-size:14px; color:#cbd5e1; }
    .sub,.note,footer { color:var(--muted); font-size:13px; }
    .note { margin:16px 0; padding:12px 14px; border:1px solid #28456c; background:#10233d; border-radius:8px; color:#bfdbfe; }
    .cards { display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:12px; margin:18px 0; }
    .card,.chart,.leader { background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:14px; }
    .label { color:var(--muted); font-size:12px; margin-bottom:5px; }
    .value { font-size:22px; font-weight:750; }
    .pos { color:var(--green); font-weight:700; }
    .neg { color:var(--red); font-weight:700; }
    .warn { color:var(--yellow); font-weight:700; }
    .charts,.leaders { display:grid; grid-template-columns:repeat(auto-fit,minmax(300px,1fr)); gap:14px; }
    .bar-row { display:flex; align-items:center; gap:8px; margin:8px 0; font-size:12px; }
    .bar-label { width:82px; color:var(--muted); text-align:right; flex:0 0 auto; }
    .bar-track { height:20px; background:#243145; border-radius:5px; overflow:hidden; flex:1; }
    .bar-fill { height:100%; border-radius:5px; }
    .bar-count { width:86px; color:#cbd5e1; }
    .table-wrap { overflow:auto; border:1px solid var(--line); border-radius:8px; background:var(--panel); }
    table { width:100%; border-collapse:collapse; font-size:12px; }
    th,td { padding:8px 9px; border-bottom:1px solid #243145; white-space:nowrap; text-align:left; }
    th { position:sticky; top:0; background:var(--panel2); color:#cbd5e1; cursor:pointer; z-index:1; }
    tbody tr:nth-child(even) td { background:rgba(255,255,255,.018); }
    tbody tr:hover td { background:rgba(56,189,248,.08); }
    .addr { font-family:Consolas,monospace; color:#7dd3fc; user-select:all; }
    .tag { display:inline-block; padding:3px 7px; border-radius:999px; background:#1e293b; border:1px solid #334155; color:#dbeafe; }
    .narrative { max-width:460px; overflow:hidden; text-overflow:ellipsis; }
    .toolbar { margin-bottom:10px; }
    input { background:var(--panel); border:1px solid var(--line); color:var(--text); border-radius:7px; padding:9px 10px; min-width:320px; }
    .pct-wrap { position:relative; min-width:118px; height:22px; background:#1f2937; border-radius:5px; overflow:hidden; }
    .pct-fill { position:absolute; inset:0 auto 0 0; opacity:.28; }
    .pct-fill.pos { background:var(--green); }
    .pct-fill.neg { background:var(--red); }
    .pct-wrap span { position:relative; display:block; line-height:22px; padding-left:7px; }
    footer { margin-top:28px; text-align:center; }
    """
    script = """
    let sortState = {};
    function cellValue(row, index, numeric) {
      const cell = row.children[index];
      if (!cell) return numeric ? 0 : "";
      if (numeric) return parseFloat(cell.dataset.sort || cell.textContent.replace(/[$,%mKMSOL]/g, "")) || 0;
      return cell.textContent.trim().toLowerCase();
    }
    function sortTable(index, numeric) {
      const table = document.getElementById("detail");
      const tbody = table.tBodies[0];
      const rows = Array.from(tbody.rows);
      const key = index + ":" + numeric;
      const dir = sortState[key] === "asc" ? "desc" : "asc";
      sortState = { [key]: dir };
      rows.sort((a, b) => {
        const av = cellValue(a, index, numeric);
        const bv = cellValue(b, index, numeric);
        if (av < bv) return dir === "asc" ? -1 : 1;
        if (av > bv) return dir === "asc" ? 1 : -1;
        return 0;
      });
      rows.forEach(row => tbody.appendChild(row));
    }
    function filterRows() {
      const q = document.getElementById("search").value.trim().toLowerCase();
      document.querySelectorAll("#detail tbody tr").forEach(row => {
        row.style.display = row.dataset.search.includes(q) ? "" : "none";
      });
    }
    """
    body = (
        f'<p class="sub">{html.escape(subtitle)}</p>'
        '<div class="note">市值口径：推送市值 × 推送后K线价格倍数，因此用于复盘同一CA推送后的相对涨跌；当前市值和峰值市值为估算值。默认按叙事分类排序，分类内按巅峰涨幅从高到低。</div>'
        f'<section class="cards">{render_cards(rows)}</section>'
        f'{render_charts(rows)}'
        f'{render_category_summary(rows)}'
        f'{render_leaderboards(rows)}'
        f'{render_detail_table(rows)}'
    )
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <style>{style}</style>
</head>
<body>
  <h1>{html.escape(title)}</h1>
  {body}
  <footer>Generated at {html.escape(generated_at)} | Deep Alpha Pro 1m push report</footer>
  <script>{script}</script>
</body>
</html>
"""


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate Deep Alpha Pro 1m new-token push HTML report.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--limit", type=int, default=300, help="Max push events to analyze; 0 means no limit.")
    parser.add_argument("--days", type=float, default=2, help="Look back N days when --since is omitted; 0 means all.")
    parser.add_argument("--since", default="", help="Start time: YYYY-MM-DD or YYYY-MM-DD HH:MM.")
    parser.add_argument("--until", default="", help="End time: YYYY-MM-DD or YYYY-MM-DD HH:MM.")
    parser.add_argument("--interval", default="1min", choices=sorted(INTERVAL_SECONDS), help="K-line interval.")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--include-repeat", action="store_true", help="Include repeated alerts for the same CA.")
    parser.add_argument("--deepseek-missing-category", action="store_true", help="Use DeepSeek for rows missing narrative category.")
    args = parser.parse_args()

    now_ts = int(time.time())
    since_ts = parse_date(args.since) if args.since else 0
    if since_ts <= 0 and args.days > 0:
        since_ts = int((datetime.now(REPORT_TZ) - timedelta(days=args.days)).timestamp())
    until_ts = parse_date(args.until) if args.until else now_ts
    events = fetch_events(args.limit, since_ts, until_ts, args.include_repeat)
    print(f"events={len(events)} since={fmt_full_time(since_ts)} until={fmt_full_time(until_ts)} interval={args.interval}")

    rows: list[dict[str, Any]] = []
    if events:
        workers = max(1, min(args.workers, 12))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(analyze_event, event, until_ts, args.interval, args.deepseek_missing_category): event
                for event in events
            }
            for index, future in enumerate(as_completed(futures), 1):
                event = futures[future]
                try:
                    row = future.result()
                except Exception as exc:
                    row = {
                        **event,
                        "valid": False,
                        "invalid_reason": str(exc),
                        "narrative_category": resolve_narrative_category(event, False),
                        "candles": 0,
                    }
                rows.append(row)
                print(
                    f"[{index}/{len(events)}] ${event.get('symbol') or 'UNKNOWN'} {event['address'][:8]} "
                    f"peak={fmt_pct(row.get('peak_gain_pct', 0), signed=False)} "
                    f"now={fmt_pct(row.get('current_gain_pct', 0))} "
                    f"cat={row.get('narrative_category') or '未知'}"
                )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    html_text = render_html(
        rows,
        title="Deep Alpha 1m 打新推送复盘",
        subtitle=(
            f"范围 {fmt_full_time(since_ts) if since_ts else '全部'} 至 {fmt_full_time(until_ts)} | "
            f"K线 {args.interval} | 样本 {len(rows)}"
        ),
    )
    args.output.write_text(html_text, encoding="utf-8")
    print(f"HTML: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
