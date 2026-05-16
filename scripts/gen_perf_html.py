#!/usr/bin/env python3
"""Generate HTML report from analyze_bottom_top100_push_performance CSV output."""
import csv, sys
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).resolve().parents[1]
CSV_PATH = ROOT / "gmgn_outputs" / "bottom_push_perf_20260515.csv"
HTML_PATH = ROOT / "gmgn_outputs" / "bottom_push_perf_20260515.html"

SIGNAL_COLORS = {
    'abnormal': '#ef4444',
    'new_revival': '#f59e0b',
    'quiet_runup': '#8b5cf6',
    'quiet_breakout': '#3b82f6',
    'drop_50w': '#06b6d4',
    'drop_40w': '#06b6d4',
    'ema_golden_cross': '#10b981',
}

SIG_ORDER = ['abnormal', 'new_revival', 'quiet_runup', 'drop_50w', 'drop_40w', 'quiet_breakout', 'ema_golden_cross']


def fmt_money(v):
    v = float(v)
    if v >= 1_000_000:
        return f"${v / 1_000_000:.2f}M"
    if v >= 1_000:
        return f"${v / 1_000:.1f}K"
    return f"${v:.0f}"


def fmt_pct(v, signed=False):
    v = float(v)
    prefix = '+' if signed and v > 0 else ''
    return f"{prefix}{v:.1f}%"


def bucket(v):
    if v >= 500:
        return '>=500%'
    if v >= 200:
        return '200-500%'
    if v >= 100:
        return '100-200%'
    if v >= 50:
        return '50-100%'
    if v >= 0:
        return '0-50%'
    return '<0%'


def build_html(rows):
    valid = [r for r in rows if r.get('valid') == 'True']
    gains = sorted([float(r['max_gain_pct']) for r in valid], reverse=True)
    gain_dist = Counter(bucket(float(r['max_gain_pct'])) for r in valid)
    signal_dist = Counter(r['signal_type'] for r in valid)
    cur_positive = sum(1 for r in valid if float(r['current_return_pct']) > 0)

    rows_sorted = sorted(valid, key=lambda r: float(r['max_gain_pct']), reverse=True)

    css = """
* { margin:0; padding:0; box-sizing:border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f172a; color: #e2e8f0; padding: 20px 30px; }
h1 { font-size: 1.5rem; margin-bottom: 4px; }
.subtitle { color: #64748b; font-size: 0.85rem; margin-bottom: 24px; }
h2 { font-size: 1.05rem; margin: 24px 0 10px; color: #94a3b8; }

.grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin-bottom: 24px; }
.card { background: #1e293b; border-radius: 10px; padding: 16px; border: 1px solid #334155; }
.card .label { font-size: 0.72rem; color: #64748b; text-transform: uppercase; letter-spacing: 0.5px; }
.card .value { font-size: 1.5rem; font-weight: 700; margin-top: 4px; }
.green { color: #10b981; }
.red { color: #ef4444; }
.yellow { color: #f59e0b; }
.purple { color: #8b5cf6; }

.charts { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 24px; }
@media (max-width: 800px) { .charts { grid-template-columns: 1fr; } }
.chart-box { background: #1e293b; border-radius: 10px; padding: 16px; border: 1px solid #334155; }
.chart-box h3 { font-size: 0.85rem; color: #94a3b8; margin-bottom: 12px; }
.bar-row { display: flex; align-items: center; margin-bottom: 6px; font-size: 0.8rem; }
.bar-label { width: 80px; text-align: right; margin-right: 10px; color: #94a3b8; flex-shrink: 0; }
.bar-track { flex: 1; background: #334155; border-radius: 4px; height: 22px; overflow: hidden; }
.bar-fill { height: 100%; border-radius: 4px; transition: width 0.3s; }
.bar-fill.gain { background: linear-gradient(90deg, #10b981, #34d399); }
.bar-count { width: 80px; font-size: 0.75rem; margin-left: 8px; color: #cbd5e1; flex-shrink: 0; }

.search-box { background: #1e293b; border: 1px solid #334155; border-radius: 8px; padding: 8px 14px; color: #e2e8f0; width: 220px; font-size: 0.8rem; margin-bottom: 10px; }
.search-box::placeholder { color: #64748b; }

.signal-type-filter { display: flex; gap: 8px; margin-bottom: 14px; flex-wrap: wrap; }
.filter-btn { padding: 6px 14px; border-radius: 20px; border: 1px solid #334155; background: #1e293b; color: #94a3b8; cursor: pointer; font-size: 0.78rem; transition: all 0.2s; }
.filter-btn:hover { border-color: #64748b; color: #e2e8f0; }
.filter-btn.active { border-color: #3b82f6; color: #60a5fa; background: #1e3050; }

.table-wrap { overflow-x: auto; border-radius: 10px; border: 1px solid #334155; }
table { width: 100%; border-collapse: collapse; font-size: 0.78rem; background: #1e293b; }
th { background: #334155; padding: 10px 8px; text-align: left; font-weight: 600; color: #94a3b8; font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.5px; white-space: nowrap; position: sticky; top: 0; }
td { padding: 7px 8px; border-bottom: 1px solid #1e293b; white-space: nowrap; }
tr:nth-child(even) td { background: #1a2332; }
tr:hover td { background: #162032; }

.badge { display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 0.7rem; font-weight: 600; }
.positive { color: #10b981; font-weight: 600; }
.negative { color: #ef4444; }

.footer { text-align: center; color: #475569; font-size: 0.7rem; margin-top: 30px; padding: 16px; }
    """

    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>底部异动推送绩效分析 - 2026-05-15</title>
<style>{css}</style>
</head>
<body>

<h1>底部异动推送绩效分析</h1>
<p class="subtitle">2026-05-15 | 统计 {len(valid)} 个首次异动 CA | 推送总记录 231 条 | 5m K线 · entry=异动后首根open价</p>

<div class="grid">
  <div class="card"><div class="label">样本数</div><div class="value purple">{len(valid)}</div></div>
  <div class="card"><div class="label">平均最高涨幅</div><div class="value green">{sum(gains) / len(gains):.1f}%</div></div>
  <div class="card"><div class="label">中位最高涨幅</div><div class="value yellow">{sorted(gains)[len(gains)//2]:.1f}%</div></div>
  <div class="card"><div class="label">P75 最高涨幅</div><div class="value yellow">{sorted(gains)[int(len(gains)*0.75)]:.1f}%</div></div>
  <div class="card"><div class="label">当前仍盈利</div><div class="value green">{cur_positive}/{len(valid)} ({cur_positive/len(valid)*100:.0f}%)</div></div>
  <div class="card"><div class="label">平均高点回撤</div><div class="value red">{sum(float(r['high_to_low_drawdown_pct']) for r in valid)/len(valid):.1f}%</div></div>
</div>

<div class="charts">
  <div class="chart-box">
    <h3>首异动 → 最高点涨幅分布</h3>
"""

    for label in ['>=500%', '200-500%', '100-200%', '50-100%', '0-50%', '<0%']:
        cnt = gain_dist.get(label, 0)
        pct_val = cnt / len(valid) * 100
        html += f'<div class="bar-row"><span class="bar-label">{label}</span><div class="bar-track"><div class="bar-fill gain" style="width:{pct_val}%"></div></div><span class="bar-count">{cnt} ({pct_val:.1f}%)</span></div>\n'

    html += """  </div>
  <div class="chart-box">
    <h3>信号类型分布</h3>
"""
    for sig in SIG_ORDER:
        cnt = signal_dist.get(sig, 0)
        if cnt == 0:
            continue
        pct_val = cnt / len(valid) * 100
        color = SIGNAL_COLORS.get(sig, '#64748b')
        html += f'<div class="bar-row"><span class="bar-label">{sig}</span><div class="bar-track"><div class="bar-fill" style="width:{pct_val}%; background:{color}"></div></div><span class="bar-count">{cnt} ({pct_val:.1f}%)</span></div>\n'

    html += """  </div>
</div>

<h2>异动代币绩效明细</h2>
<input class="search-box" type="text" id="searchInput" placeholder="搜索 symbol / address..." onkeyup="filterTable()">
<div class="signal-type-filter">
  <button class="filter-btn active" onclick="filterSignal('all')">全部</button>
"""
    for sig in SIG_ORDER:
        if signal_dist.get(sig, 0) > 0:
            html += f'  <button class="filter-btn" onclick="filterSignal(\'{sig}\')">{sig} ({signal_dist.get(sig, 0)})</button>\n'

    html += """</div>

<div class="table-wrap">
<table id="perfTable">
<thead><tr>
  <th>#</th><th>异动时间</th><th>Symbol</th><th>Address</th><th>信号类型</th>
  <th>异动市值</th><th>ATH市值</th><th>Entry</th><th>当前价</th>
  <th>最高涨幅</th><th>当前收益</th><th>高点回撤</th><th>Entry跌幅</th>
  <th>峰值时间</th><th>至峰顶</th><th>K线数</th>
</tr></thead><tbody>
"""

    for i, r in enumerate(rows_sorted, 1):
        sig = r['signal_type']
        bg = SIGNAL_COLORS.get(sig, '#64748b')
        gain = float(r['max_gain_pct'])
        cur_ret = float(r['current_return_pct'])
        dd = float(r['high_to_low_drawdown_pct'])
        entry_dd = float(r['entry_drawdown_pct'])
        gain_class = 'positive' if gain > 0 else 'negative'
        cur_class = 'positive' if cur_ret > 0 else 'negative'

        html += (
            f'<tr data-signal="{sig}">'
            f'<td>{i}</td>'
            f'<td>{r["event_time"]}</td>'
            f'<td><b>${r["symbol"]}</b></td>'
            f'<td style="font-family:monospace;font-size:0.7rem;">{r["address"][:14]}...</td>'
            f'<td><span class="badge" style="background:{bg}22;color:{bg};border:1px solid {bg}44">{sig}</span></td>'
            f'<td>{fmt_money(r["current_mcap"])}</td>'
            f'<td>{fmt_money(r["ath_mcap"])}</td>'
            f'<td>{fmt_money(r["entry_price"])}</td>'
            f'<td>{fmt_money(r["current_price"])}</td>'
            f'<td class="{gain_class}"><b>{fmt_pct(gain, signed=True)}</b></td>'
            f'<td class="{cur_class}">{fmt_pct(cur_ret, signed=True)}</td>'
            f'<td style="color:#ef4444">{fmt_pct(dd)}</td>'
            f'<td style="color:#f59e0b">{fmt_pct(entry_dd)}</td>'
            f'<td>{r["peak_time"]}</td>'
            f'<td>{float(r["time_to_peak_min"]):.0f}m</td>'
            f'<td>{r["candles"]}</td>'
            f'</tr>\n'
        )

    html += """</tbody></table></div>

<script>
function filterSignal(type) {
  document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
  event.target.classList.add('active');
  document.querySelectorAll('#perfTable tbody tr').forEach(row => {
    row.style.display = (type === 'all' || row.dataset.signal === type) ? '' : 'none';
  });
}
function filterTable() {
  var q = document.getElementById('searchInput').value.toLowerCase();
  document.querySelectorAll('#perfTable tbody tr').forEach(row => {
    row.style.display = row.textContent.toLowerCase().includes(q) ? '' : 'none';
  });
}
</script>

<div class="footer">
  Generated by analyze_bottom_top100_push_performance.py | GMGN K-line 5m | Entry = first post-signal candle open price
</div>
</body></html>"""

    return html


def main():
    with CSV_PATH.open('r', encoding='utf-8-sig') as f:
        rows = list(csv.DictReader(f))
    html = build_html(rows)
    HTML_PATH.write_text(html, encoding='utf-8')
    print(f"HTML written: {HTML_PATH} ({HTML_PATH.stat().st_size:,} bytes)")


if __name__ == '__main__':
    main()
