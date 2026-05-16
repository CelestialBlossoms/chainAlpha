#!/usr/bin/env python3
"""Analyze push success rate by time of day."""
import sys, csv, json
from pathlib import Path
from datetime import datetime, timezone, timedelta

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from db_client import db_op

tz = timezone(timedelta(hours=8))
CSV_PATH = ROOT / "gmgn_outputs" / "bottom_push_perf_20260516.csv"


def main():
    perf = {}
    with CSV_PATH.open("r", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            perf[r["address"]] = r

    def _run(conn):
        cur = conn.cursor()
        cur.execute("""
            WITH firsts AS (
                SELECT DISTINCT ON (address) address, symbol, signal_type,
                       current_mcap, ath_mcap, event_ts,
                       extra->>'risk_tags' as risk_tags
                FROM bottom_top100_push_records
                WHERE event_ts >= 1778860800 AND event_ts < 1778947200
                  AND COALESCE(signal_type,'') <> ''
                ORDER BY address, event_ts ASC
            ) SELECT * FROM firsts ORDER BY event_ts
        """)
        rows = cur.fetchall()

        # By hour
        hour_data = {h: {"total": 0, "succ": 0, "gains": [], "mcaps": [], "volumes": [], "peaks": []} for h in range(24)}
        for r in rows:
            addr = r[0]; et = r[5]
            dt = datetime.fromtimestamp(et, tz)
            h = dt.hour
            p = perf.get(addr, {})
            gain = float(p.get("max_gain_pct", 0) or 0)
            mcap = float(r[3] or 0)
            vol = float(p.get("volume_usd", 0) or 0)
            peak = float(p.get("time_to_peak_min", 0) or 0)
            succ = gain >= 10
            hour_data[h]["total"] += 1
            if succ:
                hour_data[h]["succ"] += 1
            hour_data[h]["gains"].append(gain)
            hour_data[h]["mcaps"].append(mcap)
            if vol > 0:
                hour_data[h]["volumes"].append(vol)
            hour_data[h]["peaks"].append(peak)

        # Buckets
        buckets = [
            ("凌晨 00-04", 0, 4),
            ("清晨 04-08", 4, 8),
            ("上午 08-12", 8, 12),
            ("下午 12-16", 12, 16),
            ("晚间 16-20", 16, 20),
            ("深夜 20-24", 20, 24),
        ]

        print(f"{'时段':<14} {'总数':>4} {'成功':>4} {'失败':>4} {'成功率':>7} {'均涨幅':>7} {'中位涨幅':>7} {'均市值':>9} {'均量':>8} {'均峰顶':>6}")
        print("-" * 85)

        bucket_rates = []
        for name, lo, hi in buckets:
            total = sum(hour_data[h]["total"] for h in range(lo, hi))
            succ = sum(hour_data[h]["succ"] for h in range(lo, hi))
            gains = []
            mcaps = []; vols = []; peaks = []
            for h in range(lo, hi):
                gains.extend(hour_data[h]["gains"])
                mcaps.extend(hour_data[h]["mcaps"])
                vols.extend(hour_data[h]["volumes"])
                peaks.extend(hour_data[h]["peaks"])
            fail = total - succ
            rate = succ / max(total, 1) * 100
            avg_gain = sum(gains) / max(len(gains), 1)
            med_gain = sorted(gains)[len(gains) // 2] if gains else 0
            avg_mcap = sum(mcaps) / max(len(mcaps), 1)
            avg_vol = sum(vols) / max(len(vols), 1) if vols else 0
            avg_peak = sum(peaks) / max(len(peaks), 1) if peaks else 0
            bucket_rates.append((name, rate, total))
            print(f"{name:<14} {total:>4} {succ:>4} {fail:>4} {rate:>6.0f}% {avg_gain:>6.1f}% {med_gain:>6.1f}% ${avg_mcap:>8,.0f} ${avg_vol:>7,.0f} {avg_peak:>5.0f}m")

        # Hourly detail
        print(f"\n=== 按小时成功率 (24h) ===")
        best_hour = None
        best_rate = 0
        for h in sorted(hour_data.keys()):
            d = hour_data[h]
            if d["total"] == 0:
                continue
            rate = d["succ"] / d["total"] * 100
            avg_g = sum(d["gains"]) / len(d["gains"])
            bar = "#" * int(rate / 5) + "-" * (20 - int(rate / 5))
            marker = " <-- BEST" if rate > best_rate else ""
            if rate > best_rate:
                best_rate = rate
                best_hour = h
            print(f"  {h:02d}:00 |{bar}| {rate:.0f}% ({d['succ']}/{d['total']}) 均涨幅{avg_g:.0f}%{marker}")

        # Best performer analysis
        print(f"\n=== 最成功的时段: {best_hour:02d}:00-{best_hour+1:02d}:00 (成功率{best_rate:.0f}%) ===")
        best_tokens = []
        for r in rows:
            dt = datetime.fromtimestamp(r[5], tz)
            if dt.hour == best_hour:
                p = perf.get(r[0], {})
                best_tokens.append({
                    "symbol": r[1], "gain": float(p.get("max_gain_pct", 0) or 0),
                    "mcap": float(r[3] or 0), "sig": r[2],
                })
        best_tokens.sort(key=lambda x: -x["gain"])
        for t in best_tokens[:10]:
            succ_str = "+" if t["gain"] >= 10 else ""
            print(f"  ${t['symbol']:<12s} gain={t['gain']:+5.0f}% mcap=${t['mcap']:,.0f} {t['sig']}")

        # Compare peak-to-current by time
        print(f"\n=== 峰值时间 vs 时段 ===")
        for name, lo, hi in buckets:
            peaks_bucket = []
            for h in range(lo, hi):
                peaks_bucket.extend(hour_data[h]["peaks"])
            if peaks_bucket:
                quick = sum(1 for p in peaks_bucket if p <= 5)
                slow = sum(1 for p in peaks_bucket if p >= 120)
                print(f"  {name}: 瞬爆(<=5min)={quick}个 持续拉升(>=2h)={slow}个")

    db_op(_run)


if __name__ == "__main__":
    main()
