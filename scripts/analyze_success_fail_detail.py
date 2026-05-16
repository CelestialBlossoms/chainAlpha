#!/usr/bin/env python3
"""Detailed success vs failure profiles with concrete examples."""
import sys, csv
from pathlib import Path
from datetime import datetime, timezone, timedelta

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from db_client import db_op

tz = timezone(timedelta(hours=8))
CSV = ROOT / "gmgn_outputs" / "bottom_push_perf_20260516.csv"


def main():
    perf = {}
    with CSV.open("r", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            perf[r["address"]] = r

    def _run(conn):
        cur = conn.cursor()
        cur.execute("""
            WITH firsts AS (
                SELECT DISTINCT ON (address) address, symbol, signal_type,
                       current_mcap, ath_mcap, price_change_pct, event_ts,
                       pool_total_liquidity, pool_mcap_ratio,
                       extra->>'narrative_desc' as nd,
                       extra->>'narrative_type' as nt
                FROM bottom_top100_push_records
                WHERE event_ts >= 1778860800 AND event_ts < 1778947200
                  AND COALESCE(signal_type,'') <> ''
                ORDER BY address, event_ts ASC
            ) SELECT * FROM firsts ORDER BY event_ts
        """)
        rows = cur.fetchall()

        tokens = []
        for r in rows:
            addr = r[0]; p = perf.get(addr, {})
            gain = float(p.get("max_gain_pct", 0) or 0)
            tokens.append(dict(
                symbol=r[1], address=addr, signal_type=r[2],
                mcap=float(r[3] or 0), ath=float(r[4] or 0),
                ath_ratio=float(r[4] or 0) / max(1, float(r[3] or 0)),
                sig_pct=float(r[5] or 0),
                max_gain=gain,
                cur_ret=float(p.get("current_return_pct", 0) or 0),
                dd_high=float(p.get("high_to_low_drawdown_pct", 0) or 0),
                dd_entry=float(p.get("entry_drawdown_pct", 0) or 0),
                peak_min=float(p.get("time_to_peak_min", 0) or 0),
                candles=int(p.get("candles", 0) or 0),
                pool_liq=float(r[7] or 0), pool_ratio=float(r[8] or 0),
                volume=float(p.get("volume_usd", 0) or 0),
                result="成功" if gain >= 10 else "失败",
                event_ts=r[6],
            ))

        s_all = [t for t in tokens if t["result"] == "成功"]
        f_all = [t for t in tokens if t["result"] == "失败"]
        med = lambda arr: sorted(arr)[len(arr) // 2]
        fm = lambda v: f"${v:,.0f}" if v >= 1000 else f"${v:.0f}"

        # ===== SUCCESS PROFILES =====
        print("=" * 75)
        print("【成功代币画像】105个，涨幅>=10%")
        print("=" * 75)

        # Type A: Pure pump
        pure = sorted([t for t in s_all if t["dd_entry"] > -5], key=lambda x: -x["max_gain"])
        print("\n  类型A | 纯拉升型 |", len(pure), "个 (12%)")
        print("  特征: 推送后几乎不回撤，直线上涨。市值偏大、池子厚、ATH空间充裕。")
        print("  典型:")
        for s in pure[:5]:
            et = datetime.fromtimestamp(s["event_ts"], tz).strftime("%H:%M")
            print(f"    ${s['symbol']:<12s} {et} mcap={fm(s['mcap']):>9s} ATH={s['ath_ratio']:.1f}x "
                  f"涨{s['max_gain']:+.0f}% 量{fm(s['volume'])} 顶{s['peak_min']:.0f}min {s['signal_type']}")

        # Type B: V-reversal
        vrev = sorted([t for t in s_all if t["dd_entry"] < -10 and t["cur_ret"] > 0], key=lambda x: -x["max_gain"])
        print(f"\n  类型B | V反型 | {len(vrev)} 个 (31%)")
        print("  特征: 推送后先跌10-40%，洗掉不坚定筹码后暴力拉回创新高。小市值为主。")
        print("  典型:")
        for s in vrev[:5]:
            et = datetime.fromtimestamp(s["event_ts"], tz).strftime("%H:%M")
            print(f"    ${s['symbol']:<12s} {et} mcap={fm(s['mcap']):>9s} "
                  f"先跌{s['dd_entry']:+.0f}% -> 最高涨{s['max_gain']:+.0f}% -> 现在{s['cur_ret']:+.0f}% "
                  f"量{fm(s['volume'])} {s['peak_min']:.0f}min")

        # Type C: Deep-back
        deep = sorted([t for t in s_all if t["dd_entry"] < -20 and t["cur_ret"] > 0], key=lambda x: -x["max_gain"])
        print(f"\n  类型C | 深跌回拉型 | {len(deep)} 个 (13%)")
        print("  特征: 暴跌>20%后暴力V反，高量能，极端波动。")
        print("  典型:")
        for s in deep[:5]:
            et = datetime.fromtimestamp(s["event_ts"], tz).strftime("%H:%M")
            print(f"    ${s['symbol']:<12s} {et} mcap={fm(s['mcap']):>9s} "
                  f"暴跌{s['dd_entry']:+.0f}% -> 最高涨{s['max_gain']:+.0f}% -> 现在{s['cur_ret']:+.0f}% "
                  f"量{fm(s['volume'])} {s['peak_min']:.0f}min")

        # Type D: Deep-dead (success that died later)
        dead_s = [t for t in s_all if t["dd_entry"] < -20 and t["cur_ret"] < 0]
        print(f"\n  类型D | 先涨后崩型 | {len(dead_s)} 个 (27%)")
        print("  特征: 曾经涨了>10%但后续崩回，当前亏损。说明利润要及时兑现。")

        # ===== FAILURE PROFILES =====
        print("\n\n" + "=" * 75)
        print("【失败代币画像】63个，涨幅<10%")
        print("=" * 75)

        # Type 1: Flash peak
        flash = [t for t in f_all if t["peak_min"] <= 5]
        print(f"\n  类型1 | 瞬爆入场即巅峰 | {len(flash)} 个 (46%)")
        print("  特征: 峰顶时间<=5分钟，信号发出时K线已在最高点附近。change_pct虚高但无后续。")
        print("  根因: 信号取了拉升尾端的K线close，entry价=最高价。")
        print("  典型:")
        for s in sorted(flash, key=lambda x: x["max_gain"])[:5]:
            et = datetime.fromtimestamp(s["event_ts"], tz).strftime("%H:%M")
            print(f"    ${s['symbol']:<12s} {et} mcap={fm(s['mcap']):>9s} sig={s['sig_pct']:.0f}% "
                  f"涨{s['max_gain']:+.0f}% peak={s['peak_min']:.0f}min 量{fm(s['volume'])} {s['signal_type']}")

        # Type 2: No ATH room
        no_room = [t for t in f_all if t["ath_ratio"] < 1.3 and t["mcap"] > 80000]
        print(f"\n  类型2 | 天花板型-无拉升空间 | {len(no_room)} 个 ({len(no_room)/len(f_all)*100:.0f}%)")
        print("  特征: ATH/当前市值<1.3x，ATH就在头顶，没有拉升空间。大户不愿在这个位置拉盘。")
        print("  典型:")
        for s in sorted(no_room, key=lambda x: x["ath_ratio"])[:5]:
            et = datetime.fromtimestamp(s["event_ts"], tz).strftime("%H:%M")
            print(f"    ${s['symbol']:<12s} {et} mcap={fm(s['mcap']):>9s} ATH={fm(s['ath']):>9s}({s['ath_ratio']:.1f}x) "
                  f"涨{s['max_gain']:+.1f}% {s['signal_type']}")

        # Type 3: Death volume
        dead_vol = [t for t in f_all if 0 < t["volume"] < 15000 and t["max_gain"] < 5]
        print(f"\n  类型3 | 无量型-无人跟买 | {len(dead_vol)} 个 ({len(dead_vol)/len(f_all)*100:.0f}%)")
        print("  特征: 推送后成交极度冷清，没有真金白银跟进。散户不跟、庄家不拉。")
        print("  典型:")
        for s in sorted(dead_vol, key=lambda x: x["volume"])[:5]:
            et = datetime.fromtimestamp(s["event_ts"], tz).strftime("%H:%M")
            print(f"    ${s['symbol']:<12s} {et} mcap={fm(s['mcap']):>9s} "
                  f"涨{s['max_gain']:+.1f}% 量仅{fm(s['volume'])} {s['signal_type']}")

        # Type 4: High mcap failure
        hi_mcap = [t for t in f_all if t["mcap"] > 300000]
        print(f"\n  类型4 | 大市值拉不动 | {len(hi_mcap)} 个 ({len(hi_mcap)/len(f_all)*100:.0f}%)")
        print("  特征: 市值>$300K的代币，拉升需要的资金量更大，失败率高。")
        print("  典型:")
        for s in sorted(hi_mcap, key=lambda x: -x["mcap"])[:5]:
            et = datetime.fromtimestamp(s["event_ts"], tz).strftime("%H:%M")
            print(f"    ${s['symbol']:<12s} {et} mcap={fm(s['mcap']):>9s} ATH={fm(s['ath']):>9s}({s['ath_ratio']:.1f}x) "
                  f"涨{s['max_gain']:+.1f}% {s['signal_type']}")

        # ===== RECOMMENDATIONS =====
        print("\n\n" + "=" * 75)
        print("【改进建议】基于成功/失败画像的过滤规则")
        print("=" * 75)

        print("""
  优先级1 (最高ROI): 排除瞬爆型 (46%失败)
    -> 信号触发后等2-3根K线，确认价格站稳entry上方再推送
    -> 或: 信号change_pct>50%时跳过 (已经涨完了)

  优先级2: 排除天花板型
    -> ATH/当前mcap < 1.5x 时跳过 (没有拉升空间)

  优先级3: 市值天花板
    -> mcap > $500K 时降低推送优先级或跳过

  优先级4: 量能确认
    -> 推送后如果第一根K线量能 < 前N根均量*1.5，标记为弱信号

  预期效果:
    -> 排除瞬爆型: 减少46%假突破 -> 失败率从37%降到~20%
    -> 排除天花板型: 再减少部分 -> 失败率降到~15%
    -> 排除大市值: -> 失败率降到~10%
""")

    db_op(_run)


if __name__ == "__main__":
    main()
