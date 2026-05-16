#!/usr/bin/env python3
"""
Tag today's bottom_top100_push_records with risk labels:
  - 瞬爆: peak <= 5min or signal change_pct > 50%
  - 天花板: ATH / mcap < 1.5x
  - 大市值: mcap > 500K
  - 无量: post-signal volume < 10K
Stores tags in extra->'risk_tags' JSONB field.
"""
import sys, csv, json
from pathlib import Path
from datetime import datetime, timezone, timedelta

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from db_client import db_op

CSV_PATH = ROOT / "gmgn_outputs" / "bottom_push_perf_20260516.csv"
tz = timezone(timedelta(hours=8))


def main():
    # Load performance data
    perf = {}
    with CSV_PATH.open("r", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            perf[r["address"]] = r

    def _tag(conn):
        cur = conn.cursor()

        # Get today's first pushes
        cur.execute("""
            WITH firsts AS (
                SELECT DISTINCT ON (address) id, address, symbol, signal_type,
                       current_mcap, ath_mcap, price_change_pct, event_ts,
                       pool_total_liquidity, pool_mcap_ratio, extra
                FROM bottom_top100_push_records
                WHERE event_ts >= 1778860800 AND event_ts < 1778947200
                  AND COALESCE(signal_type,'') <> ''
                ORDER BY address, event_ts ASC
            )
            SELECT * FROM firsts ORDER BY event_ts
        """)
        rows = cur.fetchall()

        tagged = 0
        stats = {"success": {}, "failed": {}}
        all_tags_count = {}

        for r in rows:
            push_id = r[0]
            addr = r[1]
            mcap = float(r[4] or 0)
            ath = float(r[5] or 0)
            sig_pct = float(r[6] or 0)
            pool_liq = float(r[8] or 0)
            pool_ratio = float(r[9] or 0)
            extra = r[10] if isinstance(r[10], dict) else {}

            p = perf.get(addr, {})
            gain = float(p.get("max_gain_pct", 0) or 0)
            peak_min = float(p.get("time_to_peak_min", 0) or 0)
            volume = float(p.get("volume_usd", 0) or 0)
            result = "success" if gain >= 10 else "failed"

            # Tag classification
            tags = []
            ath_ratio = ath / max(1, mcap)

            # 瞬爆: peak <= 5min OR signal change_pct > 50%
            if peak_min <= 5 or sig_pct > 50:
                tags.append("瞬爆")

            # 天花板: ATH/mcap < 1.5x
            if ath_ratio < 1.5:
                tags.append("天花板")

            # 大市值: mcap > 500K
            if mcap > 500_000:
                tags.append("大市值")

            # 无量: volume < 10K
            if 0 < volume < 10_000:
                tags.append("无量")

            # Update extra with tags
            extra["risk_tags"] = tags

            cur.execute(
                "UPDATE bottom_top100_push_records SET extra = %s WHERE id = %s",
                (json.dumps(extra), push_id),
            )
            tagged += 1

            # Stats
            if result not in stats:
                stats[result] = {}
            for tag in tags:
                if tag not in all_tags_count:
                    all_tags_count[tag] = {"success": 0, "failed": 0}
                all_tags_count[tag][result] += 1
                if tag not in stats[result]:
                    stats[result][tag] = 0
                stats[result][tag] += 1

        print(f"Tagged {tagged} push records")

        # Print analysis
        print(f"\n=== 标签分布 (全部{tagged}条) ===")
        for tag, counts in sorted(all_tags_count.items(), key=lambda x: -(x[1]["success"] + x[1]["failed"])):
            total = counts["success"] + counts["failed"]
            s_pct = counts["success"] / max(total, 1) * 100
            print(f"  {tag}: 共{total}条 成功{counts['success']}({s_pct:.0f}%) 失败{counts['failed']}({100-s_pct:.0f}%)")

        # Cross-tag analysis: how many tokens have N tags
        print(f"\n=== 标签组合分析 ===")
        cur.execute("""
            SELECT extra->>'risk_tags' as tags, COUNT(*)
            FROM bottom_top100_push_records
            WHERE event_ts >= 1778860800 AND event_ts < 1778947200
            GROUP BY 1 ORDER BY 2 DESC
        """)
        for tags_json, cnt in cur.fetchall():
            tags_list = json.loads(tags_json) if tags_json else []
            label = "+".join(tags_list) if tags_list else "无标签"
            print(f"  {label}: {cnt}条")

    db_op(_tag)


if __name__ == "__main__":
    main()
