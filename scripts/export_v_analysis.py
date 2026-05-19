#!/usr/bin/env python3
"""Export V-reversal analysis to HTML - May 19."""
import sys, json
from pathlib import Path
from datetime import datetime, timezone, timedelta
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from db_client import db_op

tz = timezone(timedelta(hours=8))
OUT = ROOT / "gmgn_outputs" / "v_reversal_0519.html"

def main():
    def run(conn):
        cur = conn.cursor()
        cur.execute("""SELECT symbol, max_gain_pct, current_return_pct, entry_drawdown_pct,
            high_to_low_drawdown_pct, time_to_peak_min,
            signal_type, event_ts, current_mcap, ath_mcap, volume_usd, result, risk_tags
            FROM bottom_push_performance
            WHERE analysis_date='2026-05-19' AND entry_drawdown_pct < -5
            ORDER BY entry_drawdown_pct""")
        rows = cur.fetchall()

        fm = lambda v: "${:.1f}M".format(v/1e6) if v>=1e6 else ("${:.0f}K".format(v/1e3) if v>=1e3 else "${:.0f}".format(v))
        med = lambda arr: sorted(arr)[len(arr)//2] if arr else 0

        v_rev = [r for r in rows if (r[1] or 0) >= 10 and (r[2] or 0) > 0]
        deep_v = [r for r in rows if (r[3] or 0) < -20]

        dd_vals = [r[3] or 0 for r in rows]
        gain_vals = [r[1] or 0 for r in v_rev]
        b2p_vals = []
        for r in v_rev:
            dd = r[3] or 0; g = r[1] or 0
            if dd > -100:
                b2p_vals.append(((1+g/100)/(1+dd/100)-1)*100)

        css = """
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0f172a;color:#e2e8f0;padding:20px 30px}}
h1{{font-size:1.4rem;margin-bottom:4px}}
.sub{{color:#64748b;font-size:.85rem;margin-bottom:20px}}
h2{{font-size:1rem;margin:20px 0 10px;color:#94a3b8}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin-bottom:20px}}
.card{{background:#1e293b;border-radius:10px;padding:14px;border:1px solid #334155}}
.card .l{{font-size:.7rem;color:#64748b;text-transform:uppercase}}
.card .v{{font-size:1.3rem;font-weight:700;margin-top:4px}}
.green{{color:#10b981}}.red{{color:#ef4444}}.yellow{{color:#f59e0b}}.purple{{color:#8b5cf6}}.cyan{{color:#06b6d4}}
table{{width:100%;border-collapse:collapse;font-size:.75rem;background:#1e293b;border-radius:10px;overflow:hidden;border:1px solid #334155;margin-bottom:16px}}
th{{background:#334155;padding:8px 6px;text-align:left;font-weight:600;color:#94a3b8;font-size:.65rem;text-transform:uppercase;position:sticky;top:0}}
td{{padding:6px;border-bottom:1px solid #1e293b;white-space:nowrap}}
tr:nth-child(even) td{{background:#1a2332}}
tr:hover td{{background:#162032}}
.bar-row{{display:flex;align-items:center;margin-bottom:4px;font-size:.75rem}}
.bar-label{{width:100px;text-align:right;margin-right:10px;color:#94a3b8;flex-shrink:0}}
.bar-track{{flex:1;background:#334155;border-radius:4px;height:18px;overflow:hidden}}
.bar-fill{{height:100%;border-radius:4px}}
.bar-count{{width:80px;font-size:.7rem;margin-left:8px;color:#cbd5e1;flex-shrink:0}}
.footer{{text-align:center;color:#475569;font-size:.7rem;margin-top:30px;padding:16px}}
"""

        html = "<!DOCTYPE html>\n<html lang=\"zh-CN\">\n<head><meta charset=\"UTF-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1.0\">\n"
        html += "<title>V反分析 - 2026-05-19</title>\n<style>" + css + "</style></head>\n<body>\n"
        html += "<h1>V反深度分析 - 2026-05-19</h1>\n"
        html += '<p class="sub">回撤 >5% 的代币共 {} 个 | V反成功 {} 个 | 深V {} 个</p>\n'.format(len(rows), len(v_rev), len(deep_v))

        # Summary cards
        html += '<div class="grid">\n'
        html += '<div class="card"><div class="l">回撤>5%代币</div><div class="v purple">{}</div></div>\n'.format(len(rows))
        html += '<div class="card"><div class="l">V反成功</div><div class="v green">{}</div></div>\n'.format(len(v_rev))
        html += '<div class="card"><div class="l">中位跌幅</div><div class="v red">{}%</div></div>\n'.format(int(med(dd_vals)))
        html += '<div class="card"><div class="l">中位V底→V顶</div><div class="v green">+{}%</div></div>\n'.format(int(med(b2p_vals)) if b2p_vals else 0)
        html += '<div class="card"><div class="l">中位V顶涨幅</div><div class="v yellow">+{}%</div></div>\n'.format(int(med(gain_vals)))
        html += '<div class="card"><div class="l">深V(DD<-20%)</div><div class="v cyan">{}</div></div>\n'.format(len(deep_v))
        html += '</div>\n'

        # V-reversal table
        html += '<h2>V反成功代币 (gain>=10%, cur>0)</h2>\n'
        html += '<table><thead><tr><th>#</th><th>Symbol</th><th>市值</th><th>跌幅</th><th>V底→V顶</th><th>总涨幅</th><th>现在</th><th>峰顶</th><th>信号</th><th>标签</th></tr></thead><tbody>\n'
        for i, r in enumerate(sorted(v_rev, key=lambda x: -(x[1] or 0)), 1):
            gain = r[1] or 0; dd = r[3] or 0; cur = r[2] or 0
            peak = r[5] or 0; mcap = r[8] or 0
            b2p = ((1+gain/100)/(1+dd/100)-1)*100 if dd > -100 else 0
            tags = r[12]; tags_str = ",".join(tags) if isinstance(tags, list) else str(tags or "")
            t = datetime.fromtimestamp(r[7], tz).strftime("%H:%M") if r[7] else "?"
            dd_c = "#ef4444" if dd < -20 else "#f59e0b"
            cur_color = "#10b981" if cur > 0 else "#ef4444"
            html += '<tr><td>{}</td><td><b>${}</b> <span style="font-size:.65rem;color:#64748b">{}</span></td><td>{}</td><td style="color:{};font-weight:700">{}%</td><td style="color:#10b981;font-weight:700">+{:.0f}%</td><td style="color:#f59e0b">+{:.0f}%</td><td style="color:{}">{}%</td><td>{:.0f}m</td><td>{}</td><td style="font-size:.65rem">{}</td></tr>\n'.format(
                i, r[0], t, fm(mcap), dd_c, int(dd), b2p, gain, cur_color, cur, peak, r[6] or "", tags_str[:40])
        html += '</tbody></table>\n'

        # Deep V table
        html += '<h2>深V代币 (DD < -20%)</h2>\n'
        html += '<table><thead><tr><th>#</th><th>Symbol</th><th>市值</th><th>跌幅</th><th>总涨幅</th><th>结果</th><th>峰顶</th><th>信号</th><th>标签</th></tr></thead><tbody>\n'
        for i, r in enumerate(sorted(deep_v, key=lambda x: x[3] or 0), 1):
            gain = r[1] or 0; dd = r[3] or 0; mcap = r[8] or 0; peak = r[5] or 0
            result = r[11] or ""; tags = r[12]
            tags_str = ",".join(tags) if isinstance(tags, list) else str(tags or "")
            res_color = "#10b981" if result == "成功" else "#ef4444"
            html += '<tr><td>{}</td><td><b>${}</b></td><td>{}</td><td style="color:#ef4444;font-weight:700">{}%</td><td style="color:#f59e0b">+{:.0f}%</td><td style="color:{};font-weight:700">{}</td><td>{:.0f}m</td><td>{}</td><td style="font-size:.65rem">{}</td></tr>\n'.format(
                i, r[0], fm(mcap), int(dd), gain, res_color, result, peak, r[6] or "", tags_str[:40])
        html += '</tbody></table>\n'

        # DD distribution
        html += '<h2>回撤分布</h2>\n'
        buckets = [(-5,0,"0~-5%"),(-10,-5,"-5~-10%"),(-20,-10,"-10~-20%"),(-30,-20,"-20~-30%"),(-50,-30,"-30~-50%"),(-99,-50,"<-50%")]
        for lo, hi, lab in buckets:
            cnt = sum(1 for r in rows if lo >= (r[3] or 0) > hi)
            pct = cnt/len(rows)*100 if rows else 0
            bar_color = "#10b981" if lab in ["0~-5%","-5~-10%"] else ("#f59e0b" if lab in ["-10~-20%"] else "#ef4444")
            html += '<div class="bar-row"><span class="bar-label">{}</span><div class="bar-track"><div class="bar-fill" style="width:{}%;background:{}"></div></div><span class="bar-count">{}个 ({}%)</span></div>\n'.format(lab, int(pct*2), bar_color, cnt, int(pct))

        html += '<div class="footer">V反 = gain>=10% + cur>0 | 深V = DD<-20% | 数据: 2026-05-19</div>\n'
        html += '</body></html>'

        OUT.write_text(html, encoding="utf-8")
        print("Written: {} ({} bytes, {} rows)".format(OUT, OUT.stat().st_size, len(rows)))

    db_op(run)

if __name__ == "__main__":
    main()
