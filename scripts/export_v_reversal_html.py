#!/usr/bin/env python3
"""Export V-reversal tokens to standalone HTML."""
import sys, csv, json
from pathlib import Path
from datetime import datetime, timezone, timedelta

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from db_client import db_op

tz = timezone(timedelta(hours=8))
OUT = ROOT / "gmgn_outputs" / "v_reversal_analysis.html"


def main():
    perf = {}
    for fname in ["bottom_push_perf_20260515.csv", "bottom_push_perf_20260516.csv"]:
        p = ROOT / "gmgn_outputs" / fname
        if p.exists():
            with p.open("r", encoding="utf-8-sig") as f:
                for r in csv.DictReader(f):
                    if r["address"] not in perf:
                        perf[r["address"]] = r

    def load_db(conn):
        cur = conn.cursor()
        cur.execute("""SELECT address, symbol, max_gain_pct, current_return_pct, sig_pct,
            signal_type, event_ts, current_mcap, ath_mcap, entry_price, peak_price,
            current_price, time_to_peak_min, entry_drawdown_pct, high_to_low_drawdown_pct,
            volume_usd, candles, narrative_desc, narrative_type, narrative_cat, risk_tags
            FROM bottom_push_performance""")
        for r in cur.fetchall():
            if r[0] not in perf:
                perf[r[0]] = dict(zip(
                    ["symbol","max_gain_pct","current_return_pct","price_change_pct",
                     "signal_type","event_ts","current_mcap","ath_mcap","entry_price",
                     "peak_price","current_price","time_to_peak_min","entry_drawdown_pct",
                     "high_to_low_drawdown_pct","volume_usd","candles","narrative_desc",
                     "narrative_type","narrative_cat","risk_tags"], r[1:]))
    db_op(load_db)

    v_rev = []
    for addr, p in perf.items():
        gain = float(p.get("max_gain_pct", 0) or 0)
        dd = float(p.get("entry_drawdown_pct", 0) or 0)
        cur = float(p.get("current_return_pct", 0) or 0)
        if gain >= 10 and dd < -10 and cur > 0:
            mcap = float(p.get("current_mcap", 0) or 0)
            ath = float(p.get("ath_mcap", 0) or 0)
            peak_min = float(p.get("time_to_peak_min", 0) or 0)
            time_low = float(p.get("time_to_low_min", 0) or 0)
            recovery = peak_min - time_low
            risk_tags = p.get("risk_tags", "")
            if isinstance(risk_tags, str):
                try: risk_tags = json.loads(risk_tags)
                except: risk_tags = []

            v_rev.append(dict(
                symbol=p.get("symbol","?"), address=addr, gain=gain, dd=dd, cur=cur,
                mcap=mcap, ath=ath, ath_r=ath/max(1,mcap),
                sig_pct=float(p.get("price_change_pct",0) or 0),
                vol=float(p.get("volume_usd",0) or 0),
                peak_min=peak_min, time_low=time_low, recovery=recovery,
                dd_high=float(p.get("high_to_low_drawdown_pct",0) or 0),
                sig_type=p.get("signal_type",""),
                narrative=p.get("narrative_desc","") or "",
                narrative_cat=p.get("narrative_cat","") or "",
                risk_tags=risk_tags,
                event_ts=int(float(p.get("event_ts",0) or 0)),
            ))

    v_rev.sort(key=lambda x: -x["gain"])
    med = lambda arr: sorted(arr)[len(arr)//2] if arr else 0
    fm = lambda v: f"${v/1e6:.2f}M" if v>=1e6 else (f"${v/1e3:.0f}K" if v>=1e3 else f"${v:.0f}")
    fp = lambda v: f"{v:+.0f}%"

    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>V反型代币分析</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0f172a;color:#e2e8f0;padding:20px 30px}}
h1{{font-size:1.4rem;margin-bottom:4px}}
.sub{{color:#64748b;font-size:.85rem;margin-bottom:20px}}
h2{{font-size:1rem;margin:20px 0 10px;color:#94a3b8}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px;margin-bottom:20px}}
.card{{background:#1e293b;border-radius:10px;padding:14px;border:1px solid #334155}}
.card .l{{font-size:.7rem;color:#64748b;text-transform:uppercase}}
.card .v{{font-size:1.3rem;font-weight:700;margin-top:4px}}
.green{{color:#10b981}}.red{{color:#ef4444}}.yellow{{color:#f59e0b}}.purple{{color:#8b5cf6}}.cyan{{color:#06b6d4}}
table{{width:100%;border-collapse:collapse;font-size:.75rem;background:#1e293b;border-radius:10px;overflow:hidden;border:1px solid #334155}}
th{{background:#334155;padding:8px 6px;text-align:left;font-weight:600;color:#94a3b8;font-size:.65rem;text-transform:uppercase;position:sticky;top:0}}
td{{padding:6px;border-bottom:1px solid #1e293b;white-space:nowrap}}
tr:nth-child(even) td{{background:#1a2332}}
tr:hover td{{background:#162032}}
.badge{{display:inline-block;padding:2px 6px;border-radius:10px;font-size:.65rem;font-weight:600;margin-right:2px}}
.ca-link{{font-family:monospace;font-size:.65rem;color:#60a5fa}}
.footer{{text-align:center;color:#475569;font-size:.7rem;margin-top:30px;padding:16px}}
</style></head>
<body>
<h1>V反型代币分析</h1>
<p class="sub">条件: entry回撤<-10% + 最高涨>=10% + 当前仍盈利 | {len(v_rev)} 个</p>

<div class="grid">
<div class="card"><div class="l">V反数量</div><div class="v purple">{len(v_rev)}</div></div>
<div class="card"><div class="l">中位最终涨幅</div><div class="v green">+{med([v["gain"] for v in v_rev]):.0f}%</div></div>
<div class="card"><div class="l">中位最大回撤</div><div class="v red">{med([v["dd"] for v in v_rev]):.0f}%</div></div>
<div class="card"><div class="l">中位市值</div><div class="v yellow">{fm(med([v["mcap"] for v in v_rev]))}</div></div>
<div class="card"><div class="l">中位量能</div><div class="v cyan">{fm(med([v["vol"] for v in v_rev if v["vol"]>0]))}</div></div>
<div class="card"><div class="l">中位全周期</div><div class="v purple">{med([v["peak_min"] for v in v_rev]):.0f}min</div></div>
<div class="card"><div class="l">中位探底时间</div><div class="v red">{med([v["time_low"] for v in v_rev]):.0f}min</div></div>
<div class="card"><div class="l">中位回拉时间</div><div class="v green">{med([v["recovery"] for v in v_rev if v["recovery"]>0]):.0f}min</div></div>
</div>

<h2>V反代币明细</h2>
<table><thead><tr>
<th>#</th><th>Symbol</th><th>CA</th><th>市值</th><th>ATH</th><th>信号</th><th>跌</th><th>探底</th><th>回拉</th><th>最高涨</th><th>现在</th><th>量能</th><th>叙事</th><th>标签</th>
</tr></thead><tbody>
"""

    for i, v in enumerate(v_rev, 1):
        rec_str = f'{v["recovery"]:.0f}m' if v["recovery"] > 0 else "-"
        tags_html = ""
        for t in (v.get("risk_tags") or []):
            if "瞬爆" in str(t):
                tags_html += f'<span class="badge" style="background:#92400e;color:#fbbf24">{t}</span>'
            elif "天花板" in str(t):
                tags_html += f'<span class="badge" style="background:#7f1d1d;color:#fca5a5">{t}</span>'
            elif "黄金" in str(t):
                tags_html += f'<span class="badge" style="background:#064e3b;color:#34d399">{t}</span>'
            elif "无量" in str(t):
                tags_html += f'<span class="badge" style="background:#334155;color:#94a3b8">{t}</span>'
            else:
                tags_html += f'<span class="badge" style="background:#1e293b;color:#cbd5e1">{t}</span>'

        gain_c = "#10b981" if v["gain"] > 50 else ("#f59e0b" if v["gain"] > 20 else "#e2e8f0")
        dd_c = "#ef4444" if v["dd"] < -20 else "#f59e0b"

        html += f"""<tr>
<td>{i}</td>
<td><b>${v["symbol"]}</b></td>
<td><a class="ca-link" href="https://gmgn.ai/sol/token/{v["address"]}" target="_blank">{v["address"][:12]}..</a></td>
<td>{fm(v["mcap"])}</td>
<td>{v["ath_r"]:.1f}x</td>
<td>{v["sig_type"]}</td>
<td style="color:{dd_c};font-weight:700">{fp(v["dd"])}</td>
<td>{v["time_low"]:.0f}m</td>
<td>{rec_str}</td>
<td style="color:{gain_c};font-weight:700">{fp(v["gain"])}</td>
<td style="color:#10b981">{fp(v["cur"])}</td>
<td>{fm(v["vol"])}</td>
<td style="font-size:.7rem;max-width:180px;overflow:hidden;text-overflow:ellipsis">{v.get("narrative_cat","?")} {v.get("narrative","")[:50]}</td>
<td>{tags_html}</td>
</tr>
"""

    html += "</tbody></table>"
    html += '<div class="footer">V反 = entry回撤<-10% + 最高涨>=10% + 当前仍盈利</div>'
    html += "</body></html>"

    OUT.write_text(html, encoding="utf-8")
    print(f"Written: {OUT} ({OUT.stat().st_size:,} bytes, {len(v_rev)} tokens)")


if __name__ == "__main__":
    main()
