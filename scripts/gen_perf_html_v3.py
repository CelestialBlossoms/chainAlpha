#!/usr/bin/env python3
"""
Enhance bottom push performance CSV with Binance current mcap + generate final HTML.
- Fetches current market cap from Binance Web3 dynamic API per token
- Classifies: max_gain >= 10% = 成功, < 10% = 失败
- HTML: CA click-to-copy, anomaly/peak time side-by-side
"""
import csv, json, sys, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from collections import Counter

import requests

ROOT = Path(__file__).resolve().parents[1]
CSV_IN = ROOT / "gmgn_outputs" / "bottom_push_perf_20260515_v2.csv"
CSV_OUT = ROOT / "gmgn_outputs" / "bottom_push_perf_20260515_v3.csv"
HTML_OUT = ROOT / "gmgn_outputs" / "bottom_push_perf_20260515_v3.html"

BINANCE_DYNAMIC_URL = "https://web3.binance.com/bapi/defi/v4/public/wallet-direct/buw/wallet/market/token/dynamic/info/ai"
BINANCE_CHAIN_ID = "CT_501"
BINANCE_HEADERS = {"Accept-Encoding": "identity", "User-Agent": "binance-web3/1.1 (Skill)"}
BINANCE_TIMEOUT = 10
MAX_WORKERS = 8

SIG_COLORS = {'abnormal': '#ef4444', 'new_revival': '#f59e0b', 'quiet_runup': '#8b5cf6',
              'drop_50w': '#06b6d4', 'drop_40w': '#06b6d4'}
SIG_ORDER = ['abnormal', 'new_revival', 'quiet_runup', 'drop_50w', 'drop_40w']


def fmt_money(v):
    v = float(v) if v else 0
    if v >= 1_000_000: return f"${v / 1_000_000:.2f}M"
    if v >= 1_000: return f"${v / 1_000:.1f}K"
    return f"${v:.0f}"


def fmt_pct(v, signed=False):
    v = float(v) if v else 0
    prefix = '+' if signed and v > 0 else ''
    return f"{prefix}{v:.1f}%"


def fetch_binance_dynamic(address: str) -> dict:
    """Fetch current price/mcap from Binance Web3 dynamic API."""
    url = f"{BINANCE_DYNAMIC_URL}?chainId={BINANCE_CHAIN_ID}&contractAddress={address}"
    try:
        r = requests.get(url, headers=BINANCE_HEADERS, timeout=BINANCE_TIMEOUT)
        if r.ok:
            data = (r.json().get("data") or {})
            if isinstance(data, dict):
                return {
                    "binance_price": float(data.get("price") or 0),
                    "binance_mcap": float(data.get("marketCap") or 0),
                    "binance_liquidity": float(data.get("liquidity") or 0),
                    "binance_holders": data.get("holders", 0),
                    "binance_ok": True,
                }
    except Exception:
        pass
    return {"binance_price": 0, "binance_mcap": 0, "binance_liquidity": 0, "binance_holders": 0, "binance_ok": False}


def enhance_csv():
    with CSV_IN.open('r', encoding='utf-8-sig') as f:
        rows = list(csv.DictReader(f))

    addresses = [r['address'] for r in rows if r.get('valid') == 'True']
    print(f"Fetching Binance current mcap for {len(addresses)} tokens ({MAX_WORKERS} threads)...")

    results = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(fetch_binance_dynamic, addr): addr for addr in addresses}
        done = 0
        for future in as_completed(futures):
            addr = futures[future]
            results[addr] = future.result()
            done += 1
            if done % 20 == 0:
                print(f"  {done}/{len(addresses)}")

    # Merge back
    fieldnames = list(rows[0].keys()) + ['binance_price', 'binance_mcap', 'binance_liquidity', 'binance_holders', 'binance_ok', 'result']
    enhanced = []
    for r in rows:
        addr = r['address']
        bd = results.get(addr, {})
        gain = float(r.get('max_gain_pct', 0))
        r['binance_price'] = str(bd.get('binance_price', ''))
        r['binance_mcap'] = str(bd.get('binance_mcap', ''))
        r['binance_liquidity'] = str(bd.get('binance_liquidity', ''))
        r['binance_holders'] = str(bd.get('binance_holders', ''))
        r['binance_ok'] = str(bd.get('binance_ok', False))
        r['result'] = '成功' if gain >= 10 else '失败'
        enhanced.append(r)

    with CSV_OUT.open('w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(enhanced)
    print(f"Enhanced CSV: {CSV_OUT}")
    return enhanced


def build_html(rows):
    valid = [r for r in rows if r.get('valid') == 'True']
    gains = sorted([float(r['max_gain_pct']) for r in valid], reverse=True)
    gain_dist = Counter()
    for g in gains:
        if g >= 500: gain_dist['>=500%'] += 1
        elif g >= 200: gain_dist['200-500%'] += 1
        elif g >= 100: gain_dist['100-200%'] += 1
        elif g >= 50: gain_dist['50-100%'] += 1
        elif g >= 0: gain_dist['0-50%'] += 1
        else: gain_dist['<0%'] += 1
    signal_dist = Counter(r['signal_type'] for r in valid)
    result_dist = Counter(r.get('result', '') for r in valid)
    cur_positive = sum(1 for r in valid if float(r['current_return_pct']) > 0)
    rows_sorted = sorted(valid, key=lambda r: float(r['max_gain_pct']), reverse=True)
    binance_hits = sum(1 for r in valid if r.get('binance_ok') == 'True')

    css = """
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0f172a;color:#e2e8f0;padding:20px 30px}
h1{font-size:1.5rem;margin-bottom:4px}
.subtitle{color:#64748b;font-size:.85rem;margin-bottom:20px}
h2{font-size:1.05rem;margin:24px 0 10px;color:#94a3b8}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px;margin-bottom:24px}
.card{background:#1e293b;border-radius:10px;padding:16px;border:1px solid #334155}
.card .label{font-size:.72rem;color:#64748b;text-transform:uppercase;letter-spacing:.5px}
.card .value{font-size:1.4rem;font-weight:700;margin-top:4px}
.green{color:#10b981}.red{color:#ef4444}.yellow{color:#f59e0b}.purple{color:#8b5cf6}.cyan{color:#06b6d4}
.charts{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:24px}
@media(max-width:800px){.charts{grid-template-columns:1fr}}
.chart-box{background:#1e293b;border-radius:10px;padding:16px;border:1px solid #334155}
.chart-box h3{font-size:.85rem;color:#94a3b8;margin-bottom:12px}
.bar-row{display:flex;align-items:center;margin-bottom:6px;font-size:.8rem}
.bar-label{width:80px;text-align:right;margin-right:10px;color:#94a3b8;flex-shrink:0}
.bar-track{flex:1;background:#334155;border-radius:4px;height:22px;overflow:hidden}
.bar-fill{height:100%;border-radius:4px}
.bar-count{width:90px;font-size:.75rem;margin-left:8px;color:#cbd5e1;flex-shrink:0}
.search-box{background:#1e293b;border:1px solid #334155;border-radius:8px;padding:8px 14px;color:#e2e8f0;width:220px;font-size:.8rem;margin-bottom:10px}
.search-box::placeholder{color:#64748b}
.filter{display:flex;gap:8px;margin-bottom:14px;flex-wrap:wrap}
.fbtn{padding:6px 14px;border-radius:20px;border:1px solid #334155;background:#1e293b;color:#94a3b8;cursor:pointer;font-size:.78rem;transition:all .2s}
.fbtn:hover{border-color:#64748b;color:#e2e8f0}
.fbtn.active{border-color:#3b82f6;color:#60a5fa;background:#1e3050}
.table-wrap{overflow-x:auto;border-radius:10px;border:1px solid #334155}
table{width:100%;border-collapse:collapse;font-size:.76rem;background:#1e293b}
th{background:#334155;padding:9px 7px;text-align:left;font-weight:600;color:#94a3b8;font-size:.68rem;text-transform:uppercase;letter-spacing:.5px;white-space:nowrap;position:sticky;top:0;z-index:1}
td{padding:6px 7px;border-bottom:1px solid #1e293b;white-space:nowrap}
tr:nth-child(even) td{background:#1a2332}
tr:hover td{background:#162032}
.badge{display:inline-block;padding:2px 8px;border-radius:12px;font-size:.68rem;font-weight:600}
.positive{color:#10b981;font-weight:600}.negative{color:#ef4444}
.ca-link{font-family:monospace;font-size:.68rem;color:#60a5fa;cursor:pointer;user-select:all;transition:color .15s}
.ca-link:hover{color:#93c5fd;text-decoration:underline}
.ca-link:active{color:#3b82f6}
.ca-copied{color:#10b981!important}
.result-badge{padding:3px 10px;border-radius:12px;font-size:.72rem;font-weight:700}
.result-success{background:#064e3b;color:#34d399;border:1px solid #065f46}
.result-fail{background:#451a1a;color:#fca5a5;border:1px solid #7f1d1d}
.note{background:#1e3050;border:1px solid #1e3a5f;border-radius:8px;padding:10px 16px;font-size:.78rem;color:#93c5fd;margin-bottom:20px}
.time-col{color:#94a3b8;font-size:.73rem}
.footer{text-align:center;color:#475569;font-size:.7rem;margin-top:30px;padding:16px}
"""

    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>底部异动推送绩效分析 - 2026-05-15</title>
<style>{css}</style></head>
<body>
<h1>底部异动推送绩效分析</h1>
<p class="subtitle">2026-05-15 | {len(valid)} 个首次异动 CA | 推送总记录 231 条 | 5m K线 · entry=信号时刻收盘价 | Binance实时市值: {binance_hits}/{len(valid)} 命中</p>
<div class="note">Entry 使用信号时刻K线收盘价 | 涨幅 ≥10% = <b style="color:#34d399">成功</b>，&lt;10% = <b style="color:#fca5a5">失败</b> | Binance市值来自 Web3 dynamic API 实时查询 | 点击 CA 即可复制</div>
<div class="grid">
<div class="card"><div class="label">样本数</div><div class="value purple">{len(valid)}</div></div>
<div class="card"><div class="label">成功 (≥10%)</div><div class="value green">{result_dist.get('成功',0)}</div></div>
<div class="card"><div class="label">失败 (&lt;10%)</div><div class="value red">{result_dist.get('失败',0)}</div></div>
<div class="card"><div class="label">成功率</div><div class="value cyan">{result_dist.get('成功',0)/len(valid)*100:.0f}%</div></div>
<div class="card"><div class="label">平均最高涨幅</div><div class="value green">{sum(gains)/len(gains):.1f}%</div></div>
<div class="card"><div class="label">中位最高涨幅</div><div class="value yellow">{sorted(gains)[len(gains)//2]:.1f}%</div></div>
<div class="card"><div class="label">P75 最高涨幅</div><div class="value yellow">{sorted(gains)[int(len(gains)*0.75)]:.1f}%</div></div>
<div class="card"><div class="label">当前仍盈利</div><div class="value green">{cur_positive}/{len(valid)}</div></div>
</div>
<div class="charts">
<div class="chart-box"><h3>信号时刻 → 后续最高点涨幅分布</h3>
"""
    for label in ['>=500%', '200-500%', '100-200%', '50-100%', '0-50%', '<0%']:
        cnt = gain_dist.get(label, 0)
        p = cnt / len(valid) * 100
        html += f'<div class="bar-row"><span class="bar-label">{label}</span><div class="bar-track"><div class="bar-fill" style="width:{p}%;background:linear-gradient(90deg,#10b981,#34d399)"></div></div><span class="bar-count">{cnt} ({p:.1f}%)</span></div>\n'
    html += '</div><div class="chart-box"><h3>信号类型 + 成败分布</h3>\n'
    for sig in SIG_ORDER:
        cnt = signal_dist.get(sig, 0)
        if cnt == 0: continue
        p = cnt / len(valid) * 100
        c = SIG_COLORS.get(sig, '#64748b')
        succ = sum(1 for r in valid if r['signal_type'] == sig and r.get('result') == '成功')
        html += f'<div class="bar-row"><span class="bar-label">{sig}</span><div class="bar-track"><div class="bar-fill" style="width:{p}%;background:{c}"></div></div><span class="bar-count">{cnt} ({p:.1f}%) 成功{succ}</span></div>\n'
    html += '</div></div>\n<h2>异动代币绩效明细</h2>\n'
    html += '<input class="search-box" type="text" id="s" placeholder="搜索 symbol / address..." onkeyup="ft()">\n<div class="filter"><button class="fbtn active" onclick="fs(\'all\')">全部</button>\n'
    html += '<button class="fbtn" onclick="fs(\'成功\')">成功</button>\n<button class="fbtn" onclick="fs(\'失败\')">失败</button>\n'
    for sig in SIG_ORDER:
        if signal_dist.get(sig, 0) > 0:
            html += f'<button class="fbtn" onclick="fs(\'{sig}\')">{sig} ({signal_dist.get(sig, 0)})</button>\n'
    html += '</div>\n<div class="table-wrap"><table id="t"><thead><tr>'
    html += '<th>#</th><th>结果</th><th>CA</th><th>Symbol</th><th>信号类型</th>'
    html += '<th>异动市值</th><th>ATH市值</th><th>Entry收盘</th><th>当前K线价</th><th>Binance市值</th><th>Binance价</th>'
    html += '<th>最高涨幅</th><th>当前收益</th><th>高点回撤</th>'
    html += '<th>异动时间</th><th>峰值时间</th><th>至峰顶</th><th>K线</th>'
    html += '</tr></thead><tbody>\n'

    for i, r in enumerate(rows_sorted, 1):
        sig = r['signal_type']; bg = SIG_COLORS.get(sig, '#64748b')
        gain = float(r['max_gain_pct']); cr = float(r['current_return_pct'])
        dd = float(r['high_to_low_drawdown_pct'])
        result = r.get('result', '')
        gc = 'positive' if gain > 0 else 'negative'; cc = 'positive' if cr > 0 else 'negative'
        rb_class = 'result-success' if result == '成功' else 'result-fail'
        b_mcap = float(r.get('binance_mcap', 0) or 0)
        b_price = float(r.get('binance_price', 0) or 0)
        b_ok = r.get('binance_ok') == 'True'

        html += f'<tr data-sig="{sig}" data-result="{result}">'
        html += f'<td>{i}</td>'
        html += f'<td><span class="result-badge {rb_class}">{result}</span></td>'
        html += f'<td><span class="ca-link" onclick="copyCA(this)" title="点击复制CA">{r["address"]}</span></td>'
        html += f'<td><b>${r["symbol"]}</b></td>'
        html += f'<td><span class="badge" style="background:{bg}22;color:{bg};border:1px solid {bg}44">{sig}</span></td>'
        html += f'<td>{fmt_money(r["current_mcap"])}</td>'
        html += f'<td>{fmt_money(r["ath_mcap"])}</td>'
        html += f'<td>{fmt_money(r["entry_price"])}</td>'
        html += f'<td>{fmt_money(r["current_price"])}</td>'
        html += f'<td style="color:#06b6d4">{"$"+fmt_money(b_mcap).lstrip("$") if b_ok else "-"}</td>'
        html += f'<td style="color:#06b6d4">{"$"+str(round(b_price,8)) if b_ok else "-"}</td>'
        html += f'<td class="{gc}"><b>{fmt_pct(gain, True)}</b></td>'
        html += f'<td class="{cc}">{fmt_pct(cr, True)}</td>'
        html += f'<td style="color:#ef4444">{fmt_pct(dd)}</td>'
        html += f'<td class="time-col">{r["event_time"]}</td>'
        html += f'<td class="time-col">{r["peak_time"]}</td>'
        html += f'<td>{float(r["time_to_peak_min"]):.0f}m</td>'
        html += f'<td>{r["candles"]}</td>'
        html += '</tr>\n'

    html += '''</tbody></table></div>
<script>
function fs(t){
  document.querySelectorAll(".fbtn").forEach(b=>b.classList.remove("active"));
  event.target.classList.add("active");
  document.querySelectorAll("#t tbody tr").forEach(r=>{
    var show = true;
    if(t!=="all" && t!=="成功" && t!=="失败"){ show = r.dataset.sig===t; }
    else if(t==="成功"){ show = r.dataset.result==="成功"; }
    else if(t==="失败"){ show = r.dataset.result==="失败"; }
    r.style.display = show ? "" : "none";
  });
}
function ft(){
  var q = document.getElementById("s").value.toLowerCase();
  document.querySelectorAll("#t tbody tr").forEach(r=>{
    r.style.display = r.textContent.toLowerCase().includes(q) ? "" : "none";
  });
}
function copyCA(el){
  var text = el.textContent.trim();
  navigator.clipboard.writeText(text).then(()=>{
    el.classList.add("ca-copied");
    el.title = "已复制!";
    setTimeout(()=>{ el.classList.remove("ca-copied"); el.title="点击复制CA"; }, 1200);
  }).catch(()=>{
    var ta = document.createElement("textarea");
    ta.value = text; ta.style.position="fixed"; ta.style.opacity="0";
    document.body.appendChild(ta); ta.select();
    document.execCommand("copy"); document.body.removeChild(ta);
    el.classList.add("ca-copied");
    setTimeout(()=>el.classList.remove("ca-copied"), 1200);
  });
}
</script>
<div class="footer">Generated by analyze_bottom_top100_push_performance.py + Binance Web3 Dynamic API | Entry = 信号时刻K线收盘价</div>
</body></html>'''
    return html


def main():
    enhanced = enhance_csv()
    html = build_html(enhanced)
    HTML_OUT.write_text(html, encoding='utf-8')
    print(f"HTML: {HTML_OUT} ({HTML_OUT.stat().st_size:,} bytes)")


if __name__ == '__main__':
    main()
