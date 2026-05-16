#!/usr/bin/env python3
"""Deep analysis of success vs failure push patterns across all dimensions."""
import sys, csv
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from db_client import db_op
from datetime import datetime, timezone, timedelta

tz = timezone(timedelta(hours=8))

CSV_PATH = ROOT / "gmgn_outputs" / "bottom_push_perf_20260516.csv"

NAR_KW = {
    "政治": ["总统","选举","特朗普","拜登","政府","政治","国会","America","USA","国家","爱国","马斯克","DOGE"],
    "动物": ["猫","狗","熊","兔","鱼","马","牛","蛇","鼠","虎","龙","狮","狼","狐","鹰","鸟","鲸","鲨","青蛙","猴子","猩猩","大象","猪","虫","宠物","动物","野兽","dog","cat","bear","bull","ape","pepe","doge","frog","龙虾","企鹅","BUFO","LOBSTER","RABBIT"],
    "应用": ["AI","人工智能","平台","应用","工具","软件","协议","DeFi","DEX","交易所","钱包","智能合约","app","bot","机器人","自动化","算法","LLM","GPT","Claude","OpenAI","Anthropic","IDE","builder","build","技术","Trading","swap"],
    "抽象": ["meme","迷因","梗","搞笑","讽刺","幽默","文化","社区","社交","病毒","信仰","哲学","艺术","音乐","情绪","vibe","梦想","抽象","幻想"],
}

def classify(desc, ntype):
    text = f"{desc} {ntype}".lower()
    scores = {}
    for c, kw in NAR_KW.items():
        scores[c] = sum(1 for k in kw if k.lower() in text)
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "其他"


def main():
    perf = {}
    with CSV_PATH.open("r", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            perf[r["address"]] = r

    def _fetch(conn):
        cur = conn.cursor()
        cur.execute("""
            WITH firsts AS (
                SELECT DISTINCT ON (address) address, symbol, signal_type, current_mcap, ath_mcap,
                       price_change_pct, event_ts, pool_total_liquidity, pool_mcap_ratio,
                       extra->>'narrative_desc' as nd, extra->>'narrative_type' as nt
                FROM bottom_top100_push_records
                WHERE event_ts >= 1778860800 AND event_ts < 1778947200
                  AND COALESCE(signal_type,'') <> ''
                ORDER BY address, event_ts ASC
            )
            SELECT * FROM firsts ORDER BY event_ts
        """)
        rows = cur.fetchall()

        all_tokens = []
        for r in rows:
            addr = r[0]
            p = perf.get(addr, {})
            gain = float(p.get("max_gain_pct", 0) or 0)
            nd = r[9] or ""
            nt_str = r[10] or ""
            all_tokens.append({
                "symbol": r[1], "address": addr, "signal_type": r[2],
                "mcap": float(r[3] or 0), "ath": float(r[4] or 0),
                "ath_ratio": float(r[4] or 0) / max(1, float(r[3] or 0)),
                "sig_pct": float(r[5] or 0),
                "max_gain": gain,
                "cur_ret": float(p.get("current_return_pct", 0) or 0),
                "dd_high": float(p.get("high_to_low_drawdown_pct", 0) or 0),
                "dd_entry": float(p.get("entry_drawdown_pct", 0) or 0),
                "peak_min": float(p.get("time_to_peak_min", 0) or 0),
                "candles": int(p.get("candles", 0) or 0),
                "pool_liq": float(r[7] or 0), "pool_ratio": float(r[8] or 0),
                "narrative": classify(nd, nt_str),
                "volume": float(p.get("volume_usd", 0) or 0),
                "result": "成功" if gain >= 10 else "失败",
                "event_ts": r[6],
            })

        success = [t for t in all_tokens if t["result"] == "成功"]
        failed = [t for t in all_tokens if t["result"] == "失败"]

        def stats(tokens, label):
            n = len(tokens)
            avg = lambda arr: sum(arr) / len(arr)
            med = lambda arr: sorted(arr)[len(arr) // 2]
            print(f"\n{'='*60}")
            print(f"{label} ({n}个)")
            print(f"{'='*60}")

            # 1. Narrative
            nd = Counter(t["narrative"] for t in tokens)
            print(f"\n--- 叙事分布 ---")
            for c in ["政治", "动物", "抽象", "应用", "其他"]:
                cnt = nd.get(c, 0)
                succ_in = sum(1 for t in tokens if t["narrative"] == c and t["result"] == "成功")
                print(f"  {c}: {cnt}个 ({cnt/n*100:.0f}%)")

            # 2. MCap & ATH
            mcaps = [t["mcap"] for t in tokens]
            aths = [t["ath"] for t in tokens]
            ratios = [t["ath_ratio"] for t in tokens]
            print(f"\n--- 市值特征 ---")
            print(f"  平均市值: ${avg(mcaps):,.0f}  中位: ${med(mcaps):,.0f}")
            print(f"  平均ATH: ${avg(aths):,.0f}  中位: ${med(aths):,.0f}")
            print(f"  平均ATH/mcap: {avg(ratios):.1f}x  中位: {med(ratios):.1f}x")
            for lo, hi, lb in [(0, 50, "<$50K"), (50, 100, "$50-100K"), (100, 200, "$100-200K"), (200, 500, "$200-500K"), (500, 999999, ">$500K")]:
                c2 = sum(1 for m in mcaps if lo * 1000 <= m < hi * 1000)
                print(f"  {lb}: {c2}个 ({c2/n*100:.0f}%)")
            for lo2, hi2, lb2 in [(0, 1.5, "<1.5x(空间小)"), (1.5, 3, "1.5-3x"), (3, 10, "3-10x"), (10, 99999, ">10x(空间大)")]:
                c3 = sum(1 for r in ratios if lo2 <= r < hi2)
                print(f"  ATH/mcap {lb2}: {c3}个 ({c3/n*100:.0f}%)")

            # 3. Signal type
            sd = Counter(t["signal_type"] for t in tokens)
            print(f"\n--- 信号类型 ---")
            for k, v in sd.most_common():
                print(f"  {k}: {v}个 ({v/n*100:.0f}%)")

            # 4. K-line & Volume
            sigs = [t["sig_pct"] for t in tokens]
            peaks = [t["peak_min"] for t in tokens]
            vols = [t["volume"] for t in tokens if t["volume"] > 0]
            dds = [t["dd_high"] for t in tokens]
            eds = [t["dd_entry"] for t in tokens]
            print(f"\n--- K线/量能特征 ---")
            print(f"  平均信号change_pct: {avg(sigs):.1f}%  中位: {med(sigs):.1f}%")
            print(f"  平均到峰顶: {avg(peaks):.0f}min  中位: {med(peaks):.0f}min")
            if vols:
                print(f"  平均后续量能: ${avg(vols):,.0f}  中位: ${med(vols):,.0f}")
            print(f"  平均高点回撤: {avg(dds):.1f}%  中位: {med(dds):.1f}%")
            print(f"  平均Entry回撤: {avg(eds):.1f}%  中位: {med(eds):.1f}%")
            for lo3, hi3, lb3 in [(0, 5, "<=5min(瞬爆)"), (5, 30, "5-30min"), (30, 120, "30-120min"), (120, 480, "2-8h(持续)"), (480, 99999, ">8h")]:
                c4 = sum(1 for p in peaks if lo3 <= p < hi3)
                print(f"  峰顶 {lb3}: {c4}个 ({c4/n*100:.0f}%)")

            # 5. Pool
            liqs = [t["pool_liq"] for t in tokens]
            pratios = [t["pool_ratio"] for t in tokens if t["pool_ratio"] > 0]
            print(f"\n--- 池子特征 ---")
            print(f"  平均流动性: ${avg(liqs):,.0f}  中位: ${med(liqs):,.0f}")
            if pratios:
                print(f"  平均池子/mcap: {avg(pratios):.1%}  中位: {med(pratios):.1%}")
            for lo5, hi5, lb5 in [(0, 0.1, "<10%(薄)"), (0.1, 0.2, "10-20%"), (0.2, 0.4, "20-40%"), (0.4, 99, ">40%(厚)")]:
                c6 = sum(1 for pr in pratios if lo5 <= pr < hi5)
                print(f"  {lb5}: {c6}个 ({c6/n*100:.0f}%)")

            # 6. Pattern
            vrev = sum(1 for t in tokens if t["dd_entry"] < -10 and t["cur_ret"] > 0)
            pure = sum(1 for t in tokens if t["dd_entry"] > -5)
            deep_back = sum(1 for t in tokens if t["dd_entry"] < -20 and t["cur_ret"] > 0)
            dead = sum(1 for t in tokens if t["dd_entry"] < -20 and t["cur_ret"] < 0)
            print(f"\n--- 走势模式 ---")
            print(f"  V反(先跌>10%再盈利): {vrev}个 ({vrev/n*100:.0f}%)")
            print(f"  纯拉(无回撤): {pure}个 ({pure/n*100:.0f}%)")
            print(f"  深跌回拉(>20%再拉): {deep_back}个 ({deep_back/n*100:.0f}%)")
            print(f"  深跌不拉(>20%且亏): {dead}个 ({dead/n*100:.0f}%)")

            return {"n": n, "avg_mcap": avg(mcaps), "med_mcap": med(mcaps),
                    "avg_ath": avg(aths), "avg_ratio": avg(ratios), "med_ratio": med(ratios),
                    "avg_sig": avg(sigs), "avg_peak": avg(peaks), "avg_dd": avg(dds), "avg_ed": avg(eds)}

        s_stats = stats(success, "【成功组】gain>=10%")
        f_stats = stats(failed, "【失败组】gain<10%")

        # Key differentiators
        print(f"\n{'='*60}")
        print(f"核心差异排序 (差异倍数越大越重要)")
        print(f"{'='*60}")
        diffs = [
            ("市值(中位)", s_stats["med_mcap"], f_stats["med_mcap"], "$"),
            ("ATH/mcap比(中位)", s_stats["med_ratio"], f_stats["med_ratio"], "x"),
            ("到峰顶(平均)", s_stats["avg_peak"], f_stats["avg_peak"], "min"),
            ("高点回撤(平均)", s_stats["avg_dd"], f_stats["avg_dd"], "%"),
            ("Entry回撤(平均)", s_stats["avg_ed"], f_stats["avg_ed"], "%"),
        ]
        ranked = []
        for name, sv, fv, unit in diffs:
            ratio_val = sv / max(fv, 0.001)
            ranked.append((name, sv, fv, unit, ratio_val))
        ranked.sort(key=lambda x: abs(x[4] - 1), reverse=True)
        for name, sv, fv, unit, ratio_val in ranked:
            direction = "成功更大" if sv > fv else "失败更大"
            print(f"  {name}: 成功{sv:,.1f}{unit} vs 失败{fv:,.1f}{unit} | {direction} | 差异{abs(ratio_val-1)*100:.0f}%")

    db_op(_fetch)


if __name__ == "__main__":
    main()
