#!/usr/bin/env python3
"""Generate success/failure CA analysis HTML from bottom push performance CSV."""

from __future__ import annotations

import argparse
import csv
import html
import json
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from db_client import db_op


DEFAULT_CSV = ROOT / "gmgn_outputs" / "bottom_push_perf_20260518_latest.csv"
DEFAULT_OUTPUT = ROOT / "gmgn_outputs" / "ca_success_fail_analysis_20260518.html"
TZ = ZoneInfo("Asia/Shanghai")

SIGNAL_COLORS = {
    "abnormal": "#ef4444",
    "new_revival": "#f59e0b",
    "quiet_runup": "#8b5cf6",
    "quiet_breakout": "#3b82f6",
    "drop_50w": "#06b6d4",
    "drop_40w": "#06b6d4",
}


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return default


def to_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def fmt_money(value: Any) -> str:
    amount = to_float(value)
    if abs(amount) >= 1_000_000:
        return f"${amount / 1_000_000:.2f}M"
    if abs(amount) >= 1_000:
        return f"${amount / 1_000:.1f}K"
    return f"${amount:.0f}"


def fmt_pct(value: Any, signed: bool = False) -> str:
    number = to_float(value)
    prefix = "+" if signed and number > 0 else ""
    return f"{prefix}{number:.1f}%"


def median(values: list[float]) -> float:
    return statistics.median(values) if values else 0.0


def average(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def age_bucket(hours: float) -> str:
    if hours <= 6:
        return "<=6h"
    if hours <= 12:
        return "6-12h"
    if hours <= 24:
        return "12-24h"
    if hours <= 48:
        return "24-48h"
    if hours <= 168:
        return "2-7d"
    return ">7d"


def mcap_bucket(mcap: float) -> str:
    if mcap < 50_000:
        return "<50K"
    if mcap < 100_000:
        return "50-100K"
    if mcap < 200_000:
        return "100-200K"
    if mcap < 500_000:
        return "200-500K"
    return ">500K"


def peak_bucket(minutes: float) -> str:
    if minutes <= 5:
        return "<=5m"
    if minutes <= 30:
        return "5-30m"
    if minutes <= 120:
        return "30-120m"
    if minutes <= 480:
        return "2-8h"
    return ">8h"


def narrative_tags(narrative_type: str, narrative_desc: str) -> list[str]:
    text = f"{narrative_type} {narrative_desc}".lower()
    rules = {
        "AI/agent": ["ai narrative", "claude", "llm", "agent", "algorithm", "ide", "web3", "协议", "算法", "开源"],
        "culture/meme": ["culture", "meme", "迷因", "梗", "tiktok", "wallstreetbets", "互联网"],
        "pumpfun": ["pumpfun"],
        "DEX paid": ["dex paid", "dex付费", "dex已付费"],
        "volume surging": ["token volume surging", "交易量激增"],
        "weak info": ["缺乏有效信息", "待核实", "疑似"],
    }
    tags = [name for name, needles in rules.items() if any(needle in text for needle in needles)]
    return tags or ["other"]


def load_rows(csv_path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            event_ts = to_int(row.get("event_ts"))
            age_sec = to_float(row.get("age_sec"))
            created_ts = event_ts - int(age_sec) if event_ts and age_sec else 0
            gain = to_float(row.get("max_gain_pct"))
            row.update(
                {
                    "result": "success" if gain >= 10 else "fail",
                    "gain": gain,
                    "mcap": to_float(row.get("current_mcap")),
                    "ath_mcap_value": to_float(row.get("ath_mcap")),
                    "volume": to_float(row.get("volume_usd")),
                    "liquidity_value": to_float(row.get("liquidity")),
                    "pool_ratio": to_float(row.get("pool_mcap_ratio")),
                    "peak_minutes": to_float(row.get("time_to_peak_min")),
                    "entry_drawdown": to_float(row.get("entry_drawdown_pct")),
                    "high_drawdown": to_float(row.get("high_to_low_drawdown_pct")),
                    "current_return": to_float(row.get("current_return_pct")),
                    "age_hours": age_sec / 3600 if age_sec else 0.0,
                    "created_ts": created_ts,
                    "created_time": datetime.fromtimestamp(created_ts, TZ).strftime("%Y-%m-%d %H:%M") if created_ts else "",
                    "ath_ratio": to_float(row.get("ath_mcap")) / max(1.0, to_float(row.get("current_mcap"))),
                }
            )
            rows.append(row)
    return rows


def load_narratives(addresses: list[str]) -> dict[str, dict[str, str]]:
    if not addresses:
        return {}

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            SELECT DISTINCT ON (address)
                address,
                COALESCE(extra->>'narrative_category', '') AS category,
                COALESCE(extra->>'narrative_type', '') AS narrative_type,
                COALESCE(extra->>'narrative_desc', '') AS narrative_desc
            FROM bottom_top100_push_records
            WHERE address = ANY(%s)
            ORDER BY address, event_ts ASC
            """,
            (addresses,),
        )
        return {
            address: {
                "category": category or "未知",
                "narrative_type": narrative_type or "",
                "narrative_desc": narrative_desc or "",
            }
            for address, category, narrative_type, narrative_desc in cur.fetchall()
        }

    try:
        return db_op(_op)
    except Exception as exc:
        print(f"warning: narrative DB lookup failed: {exc}", file=sys.stderr)
        return {}


def group_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {}
    return {
        "count": len(rows),
        "median_gain": median([r["gain"] for r in rows]),
        "avg_gain": average([r["gain"] for r in rows]),
        "median_current_return": median([r["current_return"] for r in rows]),
        "median_mcap": median([r["mcap"] for r in rows]),
        "avg_mcap": average([r["mcap"] for r in rows]),
        "median_ath_ratio": median([r["ath_ratio"] for r in rows]),
        "median_volume": median([r["volume"] for r in rows]),
        "avg_volume": average([r["volume"] for r in rows]),
        "median_liquidity": median([r["liquidity_value"] for r in rows]),
        "median_pool_ratio": median([r["pool_ratio"] for r in rows]),
        "median_peak": median([r["peak_minutes"] for r in rows]),
        "avg_peak": average([r["peak_minutes"] for r in rows]),
        "median_entry_dd": median([r["entry_drawdown"] for r in rows]),
        "median_high_dd": median([r["high_drawdown"] for r in rows]),
        "median_age": median([r["age_hours"] for r in rows]),
        "avg_age": average([r["age_hours"] for r in rows]),
    }


def counter_bars(counter: Counter[str], order: list[str], total: int, color: str) -> str:
    parts = []
    for label in order:
        count = counter.get(label, 0)
        pct = count / total * 100 if total else 0
        parts.append(
            f"""
            <div class="bar-row">
              <span class="bar-label">{html.escape(label)}</span>
              <div class="bar-track"><div class="bar-fill" style="width:{pct:.1f}%;background:{color}"></div></div>
              <span class="bar-count">{count} ({pct:.0f}%)</span>
            </div>
            """
        )
    return "\n".join(parts)


def render(rows: list[dict[str, Any]], output: Path) -> None:
    success = [row for row in rows if row["result"] == "success"]
    fail = [row for row in rows if row["result"] == "fail"]
    s = group_stats(success)
    f = group_stats(fail)
    all_count = len(rows)
    success_rate = len(success) / all_count * 100 if all_count else 0

    signal_counts = Counter(row["signal_type"] for row in rows)
    narrative_counts = defaultdict(Counter)
    tag_counts = defaultdict(Counter)
    for row in rows:
        narrative_counts[row["result"]][row["narrative_category"]] += 1
        for tag in row["narrative_tags"]:
            tag_counts[row["result"]][tag] += 1

    rows_sorted = sorted(rows, key=lambda row: row["gain"], reverse=True)
    table_rows = []
    for idx, row in enumerate(rows_sorted, 1):
        is_success = row["result"] == "success"
        result_label = "成功" if is_success else "失败"
        sig_color = SIGNAL_COLORS.get(row["signal_type"], "#64748b")
        narrative = html.escape(row.get("narrative_desc") or "")
        table_rows.append(
            f"""
            <tr data-result="{row['result']}" data-symbol="{html.escape(row.get('symbol', ''))}">
              <td>{idx}</td>
              <td><span class="result {'ok' if is_success else 'bad'}">{result_label}</span></td>
              <td><b>${html.escape(row.get('symbol', ''))}</b></td>
              <td><span class="ca" title="{html.escape(row.get('address', ''))}">{html.escape(row.get('address', ''))}</span></td>
              <td><span class="badge" style="--c:{sig_color}">{html.escape(row.get('signal_type', ''))}</span></td>
              <td>{fmt_money(row['mcap'])}</td>
              <td>{row['ath_ratio']:.1f}x</td>
              <td>{fmt_money(row['volume'])}</td>
              <td>{fmt_money(row['liquidity_value'])}</td>
              <td>{row['pool_ratio']:.1%}</td>
              <td class="{'pos' if row['gain'] >= 10 else 'warn'}">{fmt_pct(row['gain'], True)}</td>
              <td class="{'pos' if row['current_return'] > 0 else 'neg'}">{fmt_pct(row['current_return'], True)}</td>
              <td class="neg">{fmt_pct(row['entry_drawdown'])}</td>
              <td>{row['peak_minutes']:.0f}m</td>
              <td>{row['age_hours']:.1f}h</td>
              <td>{html.escape(row['created_time'])}</td>
              <td>{html.escape(row['narrative_category'])}</td>
              <td class="desc" title="{narrative}">{narrative}</td>
            </tr>
            """
        )

    css = """
    *{box-sizing:border-box}body{margin:0;background:#101418;color:#e5edf5;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Arial,sans-serif}
    main{max-width:1440px;margin:0 auto;padding:28px}h1{font-size:26px;margin:0 0 6px}h2{font-size:17px;margin:28px 0 12px;color:#cbd7e3}
    .sub{color:#8da0b5;font-size:13px;margin-bottom:18px}.note{background:#18212b;border:1px solid #283645;border-radius:8px;padding:12px 14px;color:#a9bdd1;font-size:13px;line-height:1.65}
    .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px;margin:18px 0}.card{background:#17202a;border:1px solid #283645;border-radius:8px;padding:14px}
    .label{font-size:12px;color:#8496a8}.value{font-size:24px;font-weight:750;margin-top:5px}.green{color:#35d08f}.red{color:#ff6b6b}.yellow{color:#f2b84b}.blue{color:#5ca8ff}
    .panels{display:grid;grid-template-columns:1fr 1fr;gap:14px}.panel{background:#17202a;border:1px solid #283645;border-radius:8px;padding:14px}
    .bar-row{display:flex;align-items:center;gap:10px;margin:7px 0;font-size:12px}.bar-label{width:88px;text-align:right;color:#9dafc0}.bar-track{height:20px;flex:1;background:#26313e;border-radius:5px;overflow:hidden}.bar-fill{height:100%}.bar-count{width:76px;color:#bcccdc}
    table{width:100%;border-collapse:collapse;background:#17202a;border:1px solid #283645;border-radius:8px;overflow:hidden;font-size:12px}th{position:sticky;top:0;background:#222e3b;color:#aebdcb;text-align:left;padding:9px 8px;white-space:nowrap}td{padding:8px;border-top:1px solid #26313e;white-space:nowrap}tr:hover td{background:#1d2834}
    .table-wrap{overflow:auto;border-radius:8px}.result,.badge{display:inline-block;border-radius:999px;padding:3px 8px;font-weight:700;font-size:11px}.result.ok{background:#0e3f30;color:#4ee3a0}.result.bad{background:#4b1c22;color:#ff9aa7}.badge{color:var(--c);border:1px solid color-mix(in srgb,var(--c) 55%,transparent);background:color-mix(in srgb,var(--c) 14%,transparent)}
    .ca{font-family:Consolas,monospace;color:#7db7ff;display:inline-block;max-width:190px;overflow:hidden;text-overflow:ellipsis}.pos{color:#35d08f;font-weight:700}.neg{color:#ff7777}.warn{color:#f2b84b;font-weight:700}.desc{max-width:420px;overflow:hidden;text-overflow:ellipsis;color:#b8c7d6}
    .summary{display:grid;grid-template-columns:1fr 1fr;gap:14px}.summary ul{margin:0;padding-left:18px;color:#c2cfdd;line-height:1.8;font-size:13px}.small{font-size:12px;color:#8da0b5}
    @media(max-width:900px){main{padding:16px}.panels,.summary{grid-template-columns:1fr}}
    """

    html_text = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>异动推送 CA 成功/失败分析 - 2026-05-18</title>
  <style>{css}</style>
</head>
<body>
<main>
  <h1>异动推送 CA 成功/失败分析</h1>
  <div class="sub">数据源：{html.escape(str(DEFAULT_CSV.relative_to(ROOT)))} | 口径：max_gain_pct >= 10% 为成功 | 时间：Asia/Shanghai</div>
  <div class="note">本报告只做结构复盘：K线、量能、市值、创建时间和叙事特征。不包含买卖、仓位、止盈止损建议。</div>

  <div class="grid">
    <div class="card"><div class="label">样本数</div><div class="value blue">{all_count}</div></div>
    <div class="card"><div class="label">成功 / 失败</div><div class="value"><span class="green">{len(success)}</span> / <span class="red">{len(fail)}</span></div></div>
    <div class="card"><div class="label">成功率</div><div class="value yellow">{success_rate:.0f}%</div></div>
    <div class="card"><div class="label">成功组中位最高涨幅</div><div class="value green">{fmt_pct(s.get('median_gain', 0), True)}</div></div>
    <div class="card"><div class="label">失败组中位最高涨幅</div><div class="value red">{fmt_pct(f.get('median_gain', 0), True)}</div></div>
    <div class="card"><div class="label">成功/失败中位量能</div><div class="value">{fmt_money(s.get('median_volume', 0))} / {fmt_money(f.get('median_volume', 0))}</div></div>
  </div>

  <h2>关键差异</h2>
  <div class="summary">
    <div class="panel">
      <ul>
        <li>成功组推送后量能中位数 {fmt_money(s.get('median_volume', 0))}，失败组 {fmt_money(f.get('median_volume', 0))}。</li>
        <li>成功组到峰值时间中位数 {s.get('median_peak', 0):.0f}m，失败组 {f.get('median_peak', 0):.0f}m。</li>
        <li>成功组代币年龄中位数 {s.get('median_age', 0):.1f}h，失败组 {f.get('median_age', 0):.1f}h。</li>
      </ul>
    </div>
    <div class="panel">
      <ul>
        <li>失败组更集中在短峰：峰值 <=5m 的数量更高，说明推送时接近短线高点。</li>
        <li>两组叙事都偏 AI/DEX Paid/Pumpfun，叙事大类区分度弱。</li>
        <li>失败组弱信息叙事更多，包括“疑似、待核实、同名密集发行”等描述。</li>
      </ul>
    </div>
  </div>

  <h2>成功/失败统计</h2>
  <div class="table-wrap">
    <table>
      <thead><tr><th>组别</th><th>数量</th><th>最高涨幅中位数</th><th>当前回报中位数</th><th>推送市值中位数</th><th>ATH/市值</th><th>量能中位数</th><th>流动性中位数</th><th>池子/市值</th><th>至峰值</th><th>入场回撤</th><th>代币年龄</th></tr></thead>
      <tbody>
        <tr><td><span class="result ok">成功</span></td><td>{s.get('count', 0)}</td><td class="pos">{fmt_pct(s.get('median_gain', 0), True)}</td><td class="pos">{fmt_pct(s.get('median_current_return', 0), True)}</td><td>{fmt_money(s.get('median_mcap', 0))}</td><td>{s.get('median_ath_ratio', 0):.1f}x</td><td>{fmt_money(s.get('median_volume', 0))}</td><td>{fmt_money(s.get('median_liquidity', 0))}</td><td>{s.get('median_pool_ratio', 0):.1%}</td><td>{s.get('median_peak', 0):.0f}m</td><td class="neg">{fmt_pct(s.get('median_entry_dd', 0))}</td><td>{s.get('median_age', 0):.1f}h</td></tr>
        <tr><td><span class="result bad">失败</span></td><td>{f.get('count', 0)}</td><td class="warn">{fmt_pct(f.get('median_gain', 0), True)}</td><td class="neg">{fmt_pct(f.get('median_current_return', 0), True)}</td><td>{fmt_money(f.get('median_mcap', 0))}</td><td>{f.get('median_ath_ratio', 0):.1f}x</td><td>{fmt_money(f.get('median_volume', 0))}</td><td>{fmt_money(f.get('median_liquidity', 0))}</td><td>{f.get('median_pool_ratio', 0):.1%}</td><td>{f.get('median_peak', 0):.0f}m</td><td class="neg">{fmt_pct(f.get('median_entry_dd', 0))}</td><td>{f.get('median_age', 0):.1f}h</td></tr>
      </tbody>
    </table>
  </div>

  <h2>分布对比</h2>
  <div class="panels">
    <div class="panel"><b>成功组市值分布</b>{counter_bars(Counter(mcap_bucket(r['mcap']) for r in success), ['<50K','50-100K','100-200K','200-500K','>500K'], len(success), '#35d08f')}</div>
    <div class="panel"><b>失败组市值分布</b>{counter_bars(Counter(mcap_bucket(r['mcap']) for r in fail), ['<50K','50-100K','100-200K','200-500K','>500K'], len(fail), '#ff6b6b')}</div>
    <div class="panel"><b>成功组创建时间/年龄</b>{counter_bars(Counter(age_bucket(r['age_hours']) for r in success), ['<=6h','6-12h','12-24h','24-48h','2-7d','>7d'], len(success), '#35d08f')}</div>
    <div class="panel"><b>失败组创建时间/年龄</b>{counter_bars(Counter(age_bucket(r['age_hours']) for r in fail), ['<=6h','6-12h','12-24h','24-48h','2-7d','>7d'], len(fail), '#ff6b6b')}</div>
    <div class="panel"><b>成功组至峰值时间</b>{counter_bars(Counter(peak_bucket(r['peak_minutes']) for r in success), ['<=5m','5-30m','30-120m','2-8h','>8h'], len(success), '#35d08f')}</div>
    <div class="panel"><b>失败组至峰值时间</b>{counter_bars(Counter(peak_bucket(r['peak_minutes']) for r in fail), ['<=5m','5-30m','30-120m','2-8h','>8h'], len(fail), '#ff6b6b')}</div>
  </div>

  <h2>叙事特征</h2>
  <div class="panels">
    <div class="panel"><b>成功组叙事标签</b>{counter_bars(tag_counts['success'], ['AI/agent','culture/meme','pumpfun','DEX paid','volume surging','weak info','other'], len(success), '#5ca8ff')}</div>
    <div class="panel"><b>失败组叙事标签</b>{counter_bars(tag_counts['fail'], ['AI/agent','culture/meme','pumpfun','DEX paid','volume surging','weak info','other'], len(fail), '#f2b84b')}</div>
  </div>

  <h2>CA 明细</h2>
  <div class="small">按最高涨幅降序。CA 单元格保留完整地址，悬停可看完整内容。</div>
  <div class="table-wrap">
    <table>
      <thead><tr><th>#</th><th>结果</th><th>Symbol</th><th>CA</th><th>信号</th><th>推送市值</th><th>ATH/市值</th><th>量能</th><th>流动性</th><th>池/市值</th><th>最高涨幅</th><th>当前回报</th><th>入场回撤</th><th>至峰值</th><th>年龄</th><th>创建时间</th><th>叙事</th><th>描述</th></tr></thead>
      <tbody>{''.join(table_rows)}</tbody>
    </table>
  </div>
</main>
</body>
</html>
"""
    output.write_text(html_text, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    rows = load_rows(args.csv)
    narratives = load_narratives([row["address"] for row in rows])
    for row in rows:
        narrative = narratives.get(row["address"], {})
        row["narrative_category"] = narrative.get("category") or "未知"
        row["narrative_type"] = narrative.get("narrative_type") or ""
        row["narrative_desc"] = narrative.get("narrative_desc") or ""
        row["narrative_tags"] = narrative_tags(row["narrative_type"], row["narrative_desc"])

    args.output.parent.mkdir(parents=True, exist_ok=True)
    render(rows, args.output)
    print(args.output)


if __name__ == "__main__":
    main()
