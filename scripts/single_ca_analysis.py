"""Single-CA deep analysis -- 30m+1h dual resolution vs 08/09 strategy docs."""
import sys, os, io, math
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.path.insert(0, r'D:\github\chainAlpha')

from db_client import db_op
from bottom_detection.top100_push_record_store import ensure_top100_push_records_table
from datetime import datetime, timezone

ADDR = "iJMcUZNW9KXVXwkTMJMXZWgGrs8EPwVUK7xxHvxpump"

# ---- Load data ----
ensure_top100_push_records_table()

def _op(conn):
    cur = conn.cursor()
    cur.execute("""SELECT symbol, signal_type, event_ts, current_mcap, ath_mcap, age_sec,
        price_change_pct, liquidity, pool_mcap_ratio, extra
        FROM bottom_top100_push_records WHERE address = %s ORDER BY event_ts""", (ADDR,))
    return [dict(zip([d[0] for d in cur.description], r)) for r in cur.fetchall()]
records = db_op(_op) or []

def load_kline(conn, table, addr, res):
    cur = conn.cursor()
    cur.execute(f"SELECT ts,open,high,low,close,volume FROM {table} WHERE address=%s AND resolution=%s ORDER BY ts", (addr, res))
    return [{"t":r[0],"o":float(r[1]),"h":float(r[2]),"l":float(r[3]),"c":float(r[4]),"v":float(r[5] or 0)} for r in cur]

k5 = db_op(lambda c: load_kline(c, "bottom_kline_cache", ADDR, "5m")) or []
k1 = db_op(lambda c: load_kline(c, "bottom_kline_cache_1m", ADDR, "1m")) or []

print(f"CA: {ADDR}  |  Records: {len(records)}  |  5m: {len(k5)} bars  |  1m: {len(k1)} bars")

# ---- Segment analysis helpers ----
BARS_30M = 6   # 6 × 5m = 30min
BARS_1H  = 12  # 12 × 5m = 1h

def segment_pct(seg):
    """Price change% over a segment: (last_close - first_open) / first_open * 100"""
    if len(seg) < 2 or seg[0]["o"] <= 0:
        return 0.0
    return (seg[-1]["c"] - seg[0]["o"]) / seg[0]["o"] * 100

def segment_vol(seg):
    """Average volume per bar in segment"""
    if not seg: return 0
    return sum(k["v"] for k in seg) / len(seg)

def segment_wick_analysis(seg):
    """Analyze candle wick patterns in a segment"""
    bodies = [abs(k["c"]-k["o"]) for k in seg]
    uppers = [k["h"]-max(k["c"],k["o"]) for k in seg]
    lowers = [min(k["c"],k["o"])-k["l"] for k in seg]
    n = len(seg)
    return {
        "avg_body": sum(bodies)/n if n else 0,
        "avg_upper": sum(uppers)/n if n else 0,
        "avg_lower": sum(lowers)/n if n else 0,
        "long_lower_bars": sum(1 for i in range(n) if lowers[i] > bodies[i]*2),
        "long_upper_bars": sum(1 for i in range(n) if uppers[i] > bodies[i]*2),
        "bull_bars": sum(1 for k in seg if k["c"] > k["o"]),
        "bear_bars": sum(1 for k in seg if k["c"] < k["o"]),
    }

def find_event_idx(klines, ets):
    for i, k in enumerate(klines):
        if k["t"] > ets:
            return i
    return len(klines)

def build_segments(bars, bar_count, n_segments):
    """Split bars into n_segments of bar_count bars each"""
    segs = []
    for i in range(n_segments):
        start = i * bar_count
        end = start + bar_count
        if end > len(bars):
            break
        segs.append(bars[start:end])
    return segs

def format_segment_row(segs, labels):
    """Print a single row: label1: +XX%  label2: +XX% ..."""
    parts = []
    for label, seg in zip(labels, segs):
        pct = segment_pct(seg)
        vol = segment_vol(seg)
        parts.append(f"{label}: {pct:>+6.1f}% (vol=${vol:,.0f})")
    return " | ".join(parts)

# ---- Analyze each signal ----
for rec in records:
    sig = rec["signal_type"]
    ets = rec["event_ts"]
    mcap = float(rec["current_mcap"])
    ath = float(rec["ath_mcap"])
    liq = float(rec["liquidity"])
    ratio = float(rec["pool_mcap_ratio"])
    age_h = float(rec["age_sec"]) / 3600
    sym = rec["symbol"]
    price_chg = float(rec["price_change_pct"] or 0)

    ts_str = datetime.fromtimestamp(ets, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"\n{'='*90}")
    print(f"  {sym} [{sig}]  @ {ts_str}")
    print(f"  MCap=${mcap:,.0f}  ATH=${ath:,.0f}  Liq=${liq:,.0f}  PoolRatio={ratio:.2f}  Age={age_h:.0f}h  SignalChg={price_chg:+.1f}%")
    print(f"{'='*90}")

    idx5 = find_event_idx(k5, ets)
    idx1 = find_event_idx(k1, ets)

    # =====================================================================
    #  PRE-STRUCTURE: 30m resolution (8 segments × 30min = 4h)
    # =====================================================================
    pre5 = k5[max(0, idx5-48):idx5]
    segs_30m = build_segments(pre5, BARS_30M, 8)  # S1..S8, each 30min
    segs_1h  = build_segments(pre5, BARS_1H, 4)   # Q1..Q4, each 1h

    print(f"\n  {'─'*85}")
    print(f"  PRE-STRUCTURE (4h before push, {len(pre5)} bars)")

    if len(segs_30m) >= 4:
        # 30m resolution
        s30_labels = [f"S{i+1}" for i in range(len(segs_30m))]
        s30_pcts = [segment_pct(s) for s in segs_30m]
        s30_vols = [segment_vol(s) for s in segs_30m]

        print(f"\n  [30m Resolution]")
        print(f"  " + format_segment_row(segs_30m, s30_labels))

        # Last 30m segment (S8) detailed analysis
        s_last = segs_30m[-1]
        s_prev = segs_30m[-2] if len(segs_30m) >= 2 else None
        w = segment_wick_analysis(s_last)
        vol_last = s30_vols[-1]
        vol_prev = s30_vols[-2] if len(s30_vols) >= 2 else vol_last
        vol_ratio_s8 = vol_last / vol_prev if vol_prev > 0 else 1

        # Detect 30m-level patterns
        s30_patterns = []
        if len(segs_30m) >= 4:
            s7, s8 = segs_30m[-2], segs_30m[-1]
            s7p, s8p = segment_pct(s7), segment_pct(s8)
            # V-reversal: S7 deep drop + S8 sharp bounce
            if s7p < -10 and s8p > 10:
                s30_patterns.append("V-reversal (S7急跌→S8急弹) -- 死猫跳风险高")
            elif s7p < -10 and s8p > 0:
                s30_patterns.append("Bottom bounce (S7急跌→S8小弹) -- 需确认S8放量")
            # Exhaustion: last 3 segments declining but volume shrinking
            if len(segs_30m) >= 3:
                s6v, s7v, s8v = s30_vols[-3], s30_vols[-2], s30_vols[-1]
                if s30_pcts[-3] < -5 and s30_pcts[-2] < -5 and s30_pcts[-1] < -5:
                    if s8v < s7v < s6v:
                        s30_patterns.append("Exhaustion selling (连续3段缩量下跌) -- 底部信号")
                    elif s8v > s7v > s6v:
                        s30_patterns.append("Accelerating dump (连续3段放量下跌) -- 恐慌未结束")
            # S8 engulfing: bullish engulfing candle cluster
            if w["bull_bars"] >= 5 and w["avg_body"] > 0:
                if vol_ratio_s8 > 1.5:
                    s30_patterns.append(f"S8放量阳线簇({w['bull_bars']}/6 bullish, vol {vol_ratio_s8:.1f}x) -- 短线资金进场")
            # S8 doji cluster: indecision
            if w["bull_bars"] in (3,) and abs(s30_pcts[-1]) < 3 and vol_ratio_s8 < 0.7:
                s30_patterns.append("S8缩量横盘 -- 方向待选，量能萎缩")

        print(f"  S8 candle: {w['bull_bars']}/6 bullish, body={w['avg_body']:.8f}, upperWick={w['avg_upper']:.8f}, lowerWick={w['avg_lower']:.8f}")
        print(f"  S8 longLowerWick: {w['long_lower_bars']}/6, longUpperWick: {w['long_upper_bars']}/6")
        print(f"  S8 Vol=${vol_last:,.0f}, S7 Vol=${vol_prev:,.0f}, S8/S7 VolRatio={vol_ratio_s8:.2f}x")
        if s30_patterns:
            for p in s30_patterns:
                print(f"  >> {p}")

        # 1h resolution (for comparison)
        s1h_labels = [f"Q{i+1}" for i in range(len(segs_1h))]
        print(f"\n  [1h Resolution (comparison)]")
        print(f"  " + format_segment_row(segs_1h, s1h_labels))

        # Q4 wick analysis
        if len(segs_1h) >= 4:
            q4w = segment_wick_analysis(segs_1h[-1])
            print(f"  Q4 candle: {q4w['bull_bars']}/12 bullish, body={q4w['avg_body']:.8f}, upperWick={q4w['avg_upper']:.8f}, lowerWick={q4w['avg_lower']:.8f}")
            print(f"  Q4 longLowerWick: {q4w['long_lower_bars']}/12, longUpperWick: {q4w['long_upper_bars']}/12")

        # Position & volatility
        all_h = [k["h"] for k in pre5]; all_l = [k["l"] for k in pre5]
        rng_h, rng_l = max(all_h), min(all_l)
        pos = (pre5[-1]["c"] - rng_l) / (rng_h - rng_l) * 100 if rng_h > rng_l else 50
        rets = [(pre5[i]["c"]-pre5[i-1]["c"])/pre5[i-1]["c"] for i in range(1,len(pre5)) if pre5[i-1]["c"]>0]
        volatility = math.sqrt(sum(r*r for r in rets)/len(rets))*100 if rets else 0

        print(f"\n  Position: {pos:.0f}% (0=4h最低)  |  Volatility: {volatility:.2f}%")

    # =====================================================================
    #  POST-4h WALK-OFF: 30m resolution
    # =====================================================================
    post5 = k5[idx5:idx5+48]
    if len(post5) >= 6:  # need at least 1 full 30m segment
        baseline = k5[idx5-1]["c"] if idx5 > 0 else post5[0]["o"]
        if baseline <= 0: baseline = post5[0]["o"]

        peak = max(b["h"] for b in post5)
        trough = min(b["l"] for b in post5)
        peak_pct = (peak - baseline) / baseline * 100
        trough_pct = (trough - baseline) / baseline * 100
        final_pct = (post5[-1]["c"] - baseline) / baseline * 100

        post_segs_30m = build_segments(post5, BARS_30M, min(8, len(post5)//BARS_30M))
        post_segs_1h  = build_segments(post5, BARS_1H,  4)

        print(f"\n  {'─'*85}")
        print(f"  POST-4h WALK-OFF ({len(post5)} bars after push)")

        if post_segs_30m:
            p30_labels = [f"P{i+1}" for i in range(len(post_segs_30m))]
            print(f"\n  [30m Resolution]")
            print(f"  " + format_segment_row(post_segs_30m, p30_labels))

            # Key 30m-level observations
            p_pcts = [segment_pct(s) for s in post_segs_30m]
            p_vols = [segment_vol(s) for s in post_segs_30m]

            # Find when the peak occurred
            peak_seg = max(range(len(post_segs_30m)), key=lambda i: max(k["h"] for k in post_segs_30m[i]))
            trough_seg = min(range(len(post_segs_30m)), key=lambda i: min(k["l"] for k in post_segs_30m[i]))

            print(f"  Peak in P{peak_seg+1} ({peak_pct:+.1f}%), Trough in P{trough_seg+1} ({trough_pct:+.1f}%)")

            # Post-30m volume vs pre-last-30m
            post_vol_total = sum(k["v"] for k in post5[:6]) / 6  # first 30m avg vol
            pre_end_vol = sum(k["v"] for k in pre5[-6:]) / max(1, len(pre5[-6:]))  # last 30m before push
            post_vol_ratio = post_vol_total / pre_end_vol if pre_end_vol > 0 else 1
            print(f"  Post30m Vol=${post_vol_total:,.0f} vs Pre30m Vol=${pre_end_vol:,.0f} (ratio={post_vol_ratio:.2f}x)")

        if post_segs_1h:
            p1h_labels = [f"H{i+1}" for i in range(len(post_segs_1h))]
            print(f"\n  [1h Resolution (comparison)]")
            print(f"  " + format_segment_row(post_segs_1h, p1h_labels))

        # Walk-off classification
        if peak_pct >= 50:
            wo_class = "暴涨"
            wo_wr = 100
        elif peak_pct >= 30 and final_pct > 15:
            wo_class = "强涨守住"
            wo_wr = 100
        elif peak_pct >= 20 and final_pct < -10:
            wo_class = "冲高急跌"
            wo_wr = 100
        elif peak_pct >= 30 and final_pct < -5:
            wo_class = "暴涨回吐"
            wo_wr = 100
        elif trough_pct < -15 and final_pct < -5:
            wo_class = "持续阴跌"
            wo_wr = 20 if sig == "new_revival" else 26
        elif peak_pct >= 20 and final_pct > 5:
            wo_class = "稳健上涨"
            wo_wr = 80
        elif 10 <= peak_pct < 20 and final_pct > 0:
            wo_class = "温和上涨"
            wo_wr = 67 if sig == "new_revival" else 33
        elif abs(peak_pct) < 10 and abs(trough_pct) < 10:
            wo_class = "横盘震荡"
            wo_wr = 0
        else:
            wo_class = f"其他(final={final_pct:+.1f}%)"
            wo_wr = 0

        wr20_hit = peak_pct >= 20
        wr50_hit = peak_pct >= 50

        print(f"\n  => Post-4h: {wo_class} (百科WR20={wo_wr}%)")
        print(f"  => Actual: Peak={peak_pct:+.1f}%  Trough={trough_pct:+.1f}%  Final={final_pct:+.1f}%")
        print(f"  => WR20={'YES' if wr20_hit else 'NO'}  WR50={'YES' if wr50_hit else 'NO'}")

    # =====================================================================
    #  1m MICRO-STRUCTURE
    # =====================================================================
    post1 = k1[idx1:idx1+60]
    pre1 = k1[max(0, idx1-30):idx1]

    if len(post1) >= 5:
        bl = post1[0]["o"] if post1[0]["o"] > 0 else (k1[idx1-1]["c"] if idx1 > 0 else 0)
        if bl <= 0: bl = post1[0]["c"]

        # 5min
        b5 = post1[:5]; chg5 = (b5[-1]["c"] - bl) / bl * 100
        post5v = sum(b["v"] for b in b5) / 5
        pre30v = sum(b["v"] for b in pre1) / max(len(pre1), 1) if pre1 else post5v
        vr5 = post5v / pre30v if pre30v > 0 else 1

        # Direction
        ups5 = sum(1 for i in range(1, 5) if b5[i]["c"] > b5[i-1]["c"])
        direction = "涨" if ups5 >= 4 else ("跌" if ups5 <= 1 else "震荡")

        # Deepest dip + recovery
        deepest = min(k["l"] for k in post1)
        deepest_pct = (deepest - bl) / bl * 100
        deepest_i = min(range(len(post1)), key=lambda i: post1[i]["l"])
        deepest_min = deepest_i + 1

        rec10 = (post1[9]["c"] - bl) / bl * 100 if len(post1) >= 10 else None
        rec30 = (post1[29]["c"] - bl) / bl * 100 if len(post1) >= 30 else None
        rec60 = (post1[59]["c"] - bl) / bl * 100 if len(post1) >= 60 else None

        # 1m candle cluster patterns (first 5 bars)
        fb = post1[0]
        fb_body = abs(fb["c"] - fb["o"])
        fb_upper = fb["h"] - max(fb["c"], fb["o"])
        fb_lower = min(fb["c"], fb["o"]) - fb["l"]
        fb_bull = fb["c"] > fb["o"]
        fb_engulf = fb_bull and fb["c"] > bl and fb["o"] < bl

        print(f"\n  {'─'*85}")
        print(f"  1m MICRO-STRUCTURE ({len(post1)} bars after push)")
        print(f"  Post-5min: {chg5:+.1f}%  Direction={direction}  VolRatio={vr5:.2f}x")
        print(f"  Bar1: {'Bull' if fb_bull else 'Bear'} body={fb_body:.8f} upper={fb_upper:.8f} lower={fb_lower:.8f} {'ENGULFING' if fb_engulf else ''}")
        print(f"  Deepest dip: {deepest_pct:+.1f}% @ {deepest_min}min")
        if rec10 is not None: print(f"  Recovery:  10min={rec10:+.1f}%  30min={rec30:+.1f}%  60min={rec60:+.1f}%")

        # ---- Dip-then-pump verdict (09 strategy) ----
        if chg5 < -8:
            dip_verdict = "DEATH SIGNAL (5min > 8% drop)"
        elif chg5 < -2 and vr5 and 0.5 <= vr5 <= 2:
            dip_verdict = "ENTRY SIGNAL (dip + normal vol) -- 84% prob to +20%"
        elif chg5 > -3 and vr5 and 0.5 <= vr5 <= 2:
            dip_verdict = "GREAT (minimal dip + normal vol = likely >50% pump)"
        elif vr5 and vr5 > 2 and chg5 < -2:
            dip_verdict = "CAUTION (panic volume on dip -- wait)"
        elif rec30 is not None and rec30 > 0:
            dip_verdict = "CONFIRMED PUMP (30min already positive = 100% big pump)"
        elif chg5 >= 2 and vr5 and vr5 > 4:
            dip_verdict = "TOP SIGNAL (instant pump + massive vol -- likely peak)"
        else:
            dip_verdict = "unclear"

        print(f"  => 09 Strategy: {dip_verdict}")

    # =====================================================================
    #  DEATH SIGNAL CHECK (09 strategy §3)
    # =====================================================================
    print(f"\n  {'─'*85}")
    print(f"  DEATH SIGNAL CHECK (09 §3)")
    red_flags = []
    q4c = s1h_labels and segment_pct(segs_1h[-1]) if len(segs_1h)>=4 else 0

    if q4c > 15 and pos > 60:
        red_flags.append(f"Q4 +{q4c:.0f}% + pos={pos:.0f}% = dead cat bounce (pushed at rebound peak)")
    if q4c > 50:
        red_flags.append(f"Q4 +{q4c:.0f}% = extreme pump, almost guaranteed death")
    if 'chg5' in dir() and chg5 < -8:
        red_flags.append(f"5min drop {chg5:+.1f}% > 8% = death pattern")
    if 'vr5' in dir() and vr5 and vr5 < 0.5 and 'chg5' in dir() and chg5 < -2:
        red_flags.append(f"Shrink volume {vr5:.2f}x on dip = no buyers")
    if price_chg > 30:
        red_flags.append(f"Signal price change already +{price_chg:.0f}% = already ran up")
    if 'post_vol_ratio' in dir() and post_vol_ratio < 0.5:
        red_flags.append(f"Post-push volume collapse ({post_vol_ratio:.2f}x)")

    if red_flags:
        for f in red_flags: print(f"  X {f}")
    else:
        print(f"  No death flags -- signal looks clean")

    # =====================================================================
    #  08 ENCYCLOPEDIA MATCH
    # =====================================================================
    print(f"\n  {'─'*85}")
    print(f"  08 ENCYCLOPEDIA MATCH")

    # 1h-based structure classification (for matching against encyclopedia's Q1-Q4 stats)
    if len(segs_1h) >= 4:
        q1c, q2c, q3c, q4c = [segment_pct(s) for s in segs_1h[:4]]

        if pos < 30 and q4c < -15:
            pre_1h = "底部持续下跌 ★★★★★"
        elif pos < 35 and abs(q4c) < 5 and (q2c < -5 or q3c < -5):
            pre_1h = "底部横盘 ★★★★"
        elif pos < 40 and q4c > 15:
            pre_1h = "底部反弹启动(死猫跳风险) ★★" if q3c < -10 else "底部反弹启动 ★★"
        elif q1c < -10 and q4c > -5:
            pre_1h = "高点下跌回落中 ★★★★★"
        elif (q1c > 20 or q2c > 20) and pos > 60:
            pre_1h = "强势拉升后高位 ★★★"
        elif pos > 60 and q4c > 15:
            pre_1h = "高位加速拉升 ★★★★★"
        elif q3c > 0 and q4c > 0:
            pre_1h = "持续拉升中 ★★★"
        elif q4c < -5:
            pre_1h = "持续下跌中"
        else:
            pre_1h = "其他结构"

        print(f"  1h Structure: {pre_1h}")

    # 30m-based structure classification (finer granularity)
    if len(segs_30m) >= 8:
        s5, s6, s7, s8 = [segment_pct(s) for s in segs_30m[4:8]]
        # 30m-level classification - looking at the last 2 hours in 30m steps
        if pos < 30 and s8 < -10:
            pre_30m = "底部急跌中 (S8仍在加速跌, 未触底)"
        elif pos < 30 and s7 < -10 and s8 > 5:
            pre_30m = "底部V反启动 (S7急跌→S8急弹, 反弹已开始)"
        elif pos < 30 and s7 < -10 and abs(s8) < 3:
            pre_30m = "底部企稳 (S7急跌→S8缩量横盘, 卖压衰竭)"
        elif pos < 35 and abs(s8) < 3 and s7 < -5:
            pre_30m = "下跌末端横盘 (S7跌→S8止跌, 底部正在形成)"
        elif pos > 60 and s7 > 10 and s8 > 5:
            pre_30m = "高位加速 (S7+S8连续拉升, 越快越危险)"
        elif pos > 60 and s7 > 10 and s8 < -3:
            pre_30m = "高位回落开始 (S7拉升→S8转跌, 可能见顶)"
        elif pos > 60 and abs(s8) < 3 and s7 < 0:
            pre_30m = "高位横盘 (涨不动了, 方向待选)"
        elif s7 < -5 and s8 > 5:
            pre_30m = "30m级V反 (S7急跌→S8急弹, 1h分辨率看不出)"
        else:
            pre_30m = "混合走势"

        print(f"  30m Structure: {pre_30m}")

    # Match against encyclopedia
    pre_clean = pre_1h.split(" ★")[0] if "★" in pre_1h else pre_1h
    expected_map = {
        ("new_revival","底部持续下跌"): ("暴涨59% WR20=84%", 84),
        ("new_revival","持续拉升中"): ("三等分:暴涨33%/冲高急跌30%/阴跌30% WR20=74%", 74),
        ("new_revival","底部横盘"): ("暴涨42% WR20=74%", 74),
        ("new_revival","底部反弹启动"): ("阴跌39% WR20=67%", 67),
        ("new_revival","高点下跌回落中"): ("暴涨31%+温和上涨31% WR20=77%", 77),
        ("new_revival","强势拉升后高位"): ("阴跌40% WR20=80%", 80),
        ("abnormal","持续拉升中"): ("仅30%值得做(暴涨21%+强涨守住9%) WR20=65%", 65),
        ("abnormal","底部反弹启动"): ("abnormal最优 基线WR20=80%", 80),
        ("abnormal","底部持续下跌"): ("WR20=57% 远不如new_revival", 57),
        ("abnormal","高位加速拉升"): ("WR20=83%", 83),
    }

    key = (sig, pre_clean)
    if key in expected_map:
        pat, wr = expected_map[key]
        print(f"\n  百科预期 ({sig}, {pre_clean}): {pat}")
        print(f"  实际: Peak={peak_pct:+.1f}%  WR20={'YES' if wr20_hit else 'NO'}  WR50={'YES' if wr50_hit else 'NO'}")
        if wr >= 80 and not wr20_hit:
            print(f"  => MISMATCH: 百科WR20>={wr}% but signal failed")
        elif wr < 60 and wr20_hit:
            print(f"  => BEAT: 百科仅WR20={wr}% but signal hit")
        else:
            print(f"  => MATCHES expectation")
    else:
        print(f"\n  百科: no entry for ({sig}, {pre_clean})")

    # =====================================================================
    #  FINAL VERDICT
    # =====================================================================
    print(f"\n  {'─'*85}")
    print(f"  FINAL VERDICT")

    score = 0
    reasons = []
    if wr20_hit: score += 3; reasons.append("WR20 hit")
    if wr50_hit: score += 3; reasons.append("WR50 hit")
    if pre_clean in ("底部持续下跌","高点下跌回落中","底部横盘"):
        score += 2; reasons.append(f"Top-tier pre-structure ({pre_clean})")
    if pre_clean in ("高位加速拉升",) and sig == "abnormal":
        score += 2; reasons.append("abnormal high-acceleration")
    if 'chg5' in dir() and chg5 and -3 <= chg5 <= 5:
        score += 2; reasons.append("Post-5min in optimal dip range")
    if 'vr5' in dir() and vr5 and 0.5 <= vr5 <= 2:
        score += 1; reasons.append("Normal post-5min volume")
    if red_flags:
        score -= len(red_flags) * 2
        reasons.append(f"{len(red_flags)} death flags")

    # 30m bonus/penalty
    if 's30_patterns' in dir():
        for p in s30_patterns:
            if "Exhaustion" in p: score += 1; reasons.append("30m: exhaustion selling")
            if "V-reversal" in p: score -= 1; reasons.append("30m: V-reversal risk")
            if "Accelerating dump" in p: score -= 1; reasons.append("30m: accelerating dump")

    print(f"  Score: {score}/10")
    print(f"  Reasons: {' | '.join(reasons)}")

    if score >= 8:
        print(f"  Verdict: STRONG BUY -- optimal pattern match across all resolutions")
    elif score >= 5:
        print(f"  Verdict: WATCH -- decent pattern, wait for 30min confirmation")
    elif score >= 2:
        print(f"  Verdict: RISKY -- multiple concerns, small position only if any")
    else:
        print(f"  Verdict: SKIP -- death flags or no clear edge")

print(f"\n{'='*90}")
print("Done.")
