#!/usr/bin/env python3
"""Export failed CAs with full characteristics to TXT for manual analysis."""
import sys, csv, json
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import Counter

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from db_client import db_op

tz = timezone(timedelta(hours=8))
OUT = ROOT / "gmgn_outputs" / "failed_ca_analysis.txt"


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
            current_price, peak_mcap, time_to_peak_min, entry_drawdown_pct,
            high_to_low_drawdown_pct, volume_usd, candles, narrative_desc,
            narrative_type, narrative_cat, risk_tags, result, binance_mcap
            FROM bottom_push_performance""")
        for r in cur.fetchall():
            if r[0] not in perf:
                perf[r[0]] = {
                    "symbol": r[1], "max_gain_pct": r[2], "current_return_pct": r[3],
                    "price_change_pct": r[4], "signal_type": r[5], "event_ts": r[6],
                    "current_mcap": r[7], "ath_mcap": r[8], "entry_price": r[9],
                    "peak_price": r[10], "current_price": r[11], "peak_mcap": r[12],
                    "time_to_peak_min": r[13], "entry_drawdown_pct": r[14],
                    "high_to_low_drawdown_pct": r[15], "volume_usd": r[16],
                    "candles": r[17], "narrative_desc": r[18], "narrative_type": r[19],
                    "narrative_cat": r[20], "risk_tags": r[21], "result": r[22],
                    "binance_mcap": r[23],
                }
    db_op(load_db)

    def get_ages(conn):
        cur = conn.cursor()
        cur.execute("SELECT address, extra FROM bottom_top100_push_records")
        ages = {}
        for addr, extra in cur.fetchall():
            e = extra if isinstance(extra, dict) else {}
            age_sec = e.get("age_sec", 0) or 0
            if age_sec > 0:
                ages[addr] = int(age_sec)
        return ages
    ages = db_op(get_ages)

    # Collect failed tokens
    failed = []
    for addr, p in perf.items():
        gain = float(p.get("max_gain_pct", 0) or 0)
        result = p.get("result", "")
        if gain >= 10 and result != "失败":
            continue

        mcap = float(p.get("current_mcap", 0) or 0)
        ath = float(p.get("ath_mcap", 0) or 0)
        sig_pct = float(p.get("price_change_pct", 0) or 0)
        dd_entry = float(p.get("entry_drawdown_pct", 0) or 0)
        dd_high = float(p.get("high_to_low_drawdown_pct", 0) or 0)
        peak = float(p.get("time_to_peak_min", 0) or 0)
        vol = float(p.get("volume_usd", 0) or 0)
        candles = int(p.get("candles", 0) or 0)
        age_h = ages.get(addr, -1) / 3600 if ages.get(addr, 0) else -1
        sig_type = p.get("signal_type", "")
        risk_tags = p.get("risk_tags", "")
        if isinstance(risk_tags, str):
            try: risk_tags = json.loads(risk_tags)
            except: risk_tags = []

        failed.append(dict(
            symbol=p.get("symbol", "?"), address=addr, gain=gain,
            mcap=mcap, ath=ath, ath_r=ath / max(1, mcap),
            sig_pct=sig_pct, dd_entry=dd_entry, dd_high=dd_high,
            peak=peak, vol=vol, candles=candles, age_h=age_h,
            sig_type=sig_type, risk_tags=risk_tags,
            event_ts=int(float(p.get("event_ts", 0) or 0)),
            narrative=p.get("narrative_desc", "") or "",
            narrative_type=p.get("narrative_type", "") or "",
            narrative_cat=p.get("narrative_cat", "") or "",
        ))

    failed.sort(key=lambda x: x["gain"])
    med = lambda arr: sorted(arr)[len(arr) // 2] if arr else 0

    with OUT.open("w", encoding="utf-8") as f:
        f.write(f"失败CA分析报告 - {datetime.now(tz).strftime('%Y-%m-%d %H:%M')}\n")
        f.write(f"总计: {len(failed)} 个失败代币\n")
        f.write("=" * 90 + "\n\n")

        f.write("【总体特征】\n")
        f.write(f"  中位gain: {med([t['gain'] for t in failed]):.1f}%\n")
        f.write(f"  中位mcap: ${med([t['mcap'] for t in failed]):,.0f}\n")
        ath_vals = [t["ath_r"] for t in failed if t["ath_r"] > 0]
        f.write(f"  中位ATH/mcap: {med(ath_vals):.1f}x\n")
        f.write(f"  中位sig_pct: {med([t['sig_pct'] for t in failed]):.1f}%\n")
        f.write(f"  中位peak: {med([t['peak'] for t in failed]):.0f}min\n")
        vol_vals = [t["vol"] for t in failed if t["vol"] > 0]
        f.write(f"  中位vol: ${med(vol_vals):,.0f}\n")
        f.write(f"  中位dd_entry: {med([t['dd_entry'] for t in failed]):.1f}%\n")
        f.write(f"  中位dd_high: {med([t['dd_high'] for t in failed]):.1f}%\n")
        age_vals = [t["age_h"] for t in failed if t["age_h"] > 0]
        f.write(f"  中位age: {med(age_vals):.0f}h\n")

        sig_dist = Counter(t["sig_type"] for t in failed)
        f.write(f"\n  信号类型: {dict(sig_dist)}\n")
        nar_dist = Counter(t.get("narrative_cat", "?") for t in failed)
        f.write(f"  叙事类别: {dict(nar_dist)}\n")
        tag_dist = Counter()
        for t in failed:
            for tag in (t.get("risk_tags") or []):
                tag_dist[tag] += 1
        f.write(f"  风险标签: {dict(tag_dist)}\n")

        f.write(f"\n【年龄分布】\n")
        for lo, hi, label in [(0, 6, "<=6h"), (6, 12, "6-12h"), (12, 24, "12-24h"), (24, 48, "24-48h"), (48, 999, ">48h")]:
            cnt = sum(1 for t in failed if lo <= t["age_h"] < hi)
            f.write(f"  {label}: {cnt}个 ({cnt/len(failed)*100:.0f}%)\n")

        f.write(f"\n【市值分布】\n")
        for lo, hi, label in [(0, 50, "<$50K"), (50, 80, "$50-80K"), (80, 120, "$80-120K"), (120, 200, "$120-200K"), (200, 500, "$200-500K"), (500, 9999, ">$500K")]:
            cnt = sum(1 for t in failed if lo * 1000 <= t["mcap"] < hi * 1000)
            f.write(f"  {label}: {cnt}个 ({cnt/len(failed)*100:.0f}%)\n")

        f.write(f"\n\n【详细列表】(按gain升序)\n")
        f.write(f"{'Symbol':<16} {'gain':>6} {'mcap':>9} {'ATH/m':>6} {'sig%':>6} {'dd_e%':>6} {'dd_h%':>6} {'peak':>5} {'vol':>9} {'age':>5} {'type':<16} {'tags':<20} {'叙事':>6}\n")
        f.write("-" * 130 + "\n")
        for t in failed:
            et = datetime.fromtimestamp(t["event_ts"], tz).strftime("%m-%d %H:%M") if t["event_ts"] else "?"
            tags_str = ",".join(t.get("risk_tags", "") or [])
            f.write(f"{'$'+t['symbol']:<16} {t['gain']:>+5.0f}% ${t['mcap']:>7,.0f} {t['ath_r']:>5.1f}x {t['sig_pct']:>5.0f}% {t['dd_entry']:>+5.0f}% {t['dd_high']:>5.0f}% {t['peak']:>4.0f}m ${t['vol']:>7,.0f} {t['age_h']:>4.0f}h {t['sig_type']:<16} {tags_str:<20} {t.get('narrative_cat','?'):>6}\n")
            f.write(f"  CA: {t['address']} | {et}\n")
            f.write(f"  https://gmgn.ai/sol/token/{t['address']}\n")
            if t.get("narrative"):
                f.write(f"  叙事: {t['narrative'][:200]}\n")
            f.write("\n")

    print(f"Written: {OUT} ({OUT.stat().st_size:,} bytes, {len(failed)} tokens)")


if __name__ == "__main__":
    main()
