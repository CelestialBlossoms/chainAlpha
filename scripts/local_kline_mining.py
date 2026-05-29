"""
Local K-line pattern discovery pipeline. No API calls — pure Python clustering + statistics.

Output:
  onchain_trading_guides/08-5m-fingerprint-encyclopedia.md
  onchain_trading_guides/09-bar-level-strategy.md
"""
import sys, os, io, json, math
from pathlib import Path
from collections import defaultdict, Counter
from datetime import datetime, timezone
from statistics import median, mean, stdev

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

RECORDS_PATH = ROOT / "data" / "deepseek_discovery" / "signal_kline_records.jsonl"
OUTPUT_DIR = ROOT / "onchain_trading_guides"

# ============================================================================
#  STEP 1: Load & extract bar-level fingerprints
# ============================================================================

def load_records():
    """Load all signal records from JSONL."""
    records = []
    with open(RECORDS_PATH, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))
    print(f"Loaded {len(records)} signals")
    return records


def extract_bar_fingerprints(k5_bars, push_idx):
    """
    Extract fingerprint features from each pre-push 5m bar.
    Returns a list of dicts, one per bar relative to push.
    """
    fps = []
    for j in range(max(0, push_idx - 48), push_idx):
        b = k5_bars[j]
        rel = j - push_idx  # negative: bars before push

        body = b["c"] - b["o"]
        body_pct = body / b["o"] * 100 if b["o"] > 0 else 0
        total_range = b["h"] - b["l"]
        upper_wick = b["h"] - max(b["c"], b["o"])
        lower_wick = min(b["c"], b["o"]) - b["l"]

        body_ratio = abs(body) / total_range if total_range > 0 else 0.5
        uw_ratio = upper_wick / max(abs(body), 1e-12) if abs(body) > 1e-12 else (1 if upper_wick > 0 else 0)
        lw_ratio = lower_wick / max(abs(body), 1e-12) if abs(body) > 1e-12 else (1 if lower_wick > 0 else 0)

        prev_vol = k5_bars[j-1]["v"] if j > 0 and k5_bars[j-1]["v"] > 0 else b["v"]
        vol_ratio = b["v"] / prev_vol if prev_vol > 0 else 1.0

        # Classify bar type
        bar_type = "normal"
        if body_pct < -8 and vol_ratio > 3:
            bar_type = "capitulation"  # panic dump
        elif body_pct > 8 and vol_ratio > 3:
            bar_type = "surge"  # aggressive buy
        elif lw_ratio > 3 and body_pct > -2:
            bar_type = "hammer"  # long lower wick = buying support
        elif uw_ratio > 3 and body_pct < 2:
            bar_type = "shooting_star"  # long upper wick = selling pressure
        elif body_ratio < 0.3 and abs(body_pct) < 3:
            bar_type = "doji"  # indecision
        elif body_ratio > 0.85 and abs(body_pct) > 3:
            bar_type = "marubozu"  # decisive, no wicks
        elif vol_ratio < 0.3 and abs(body_pct) < 2:
            bar_type = "quiet"  # extreme low volume
        elif vol_ratio > 5:
            bar_type = "volume_spike"

        fps.append({
            "rel": rel,
            "body_pct": round(body_pct, 2),
            "direction": 1 if body > 0 else -1,
            "uw_ratio": round(min(uw_ratio, 10), 1),
            "lw_ratio": round(min(lw_ratio, 10), 1),
            "body_ratio": round(body_ratio, 2),
            "vol_ratio": round(min(vol_ratio, 20), 1),
            "bar_type": bar_type,
        })
    return fps


def extract_signal_features(record):
    """
    Extract comprehensive fingerprint features for a single signal.
    All computations are local, fast.
    """
    k5 = record["klines_5m"]
    push_idx = record["push_idx_5m"]
    k1 = record["klines_1m"]
    push_1m = record["push_idx_1m"]

    # --- Pre-push 5m features ---
    pre_bars = k5[max(0, push_idx-48):push_idx]
    fps = extract_bar_fingerprints(k5, push_idx)

    # Price position & volatility
    # Defaults (for signals with no pre-push K-line data)
    position = 50
    volatility = 0.0
    seg_30m = []

    if pre_bars:
        all_h = [b["h"] for b in pre_bars]; all_l = [b["l"] for b in pre_bars]
        rng_h, rng_l = max(all_h), min(all_l)
        position = (pre_bars[-1]["c"] - rng_l) / (rng_h - rng_l) * 100 if rng_h > rng_l else 50

        rets = [(pre_bars[i]["c"]-pre_bars[i-1]["c"])/pre_bars[i-1]["c"] for i in range(1, len(pre_bars)) if pre_bars[i-1]["c"]>0]
        volatility = math.sqrt(sum(r*r for r in rets)/len(rets))*100 if rets else 0

        # 30m segment trends (last 8 segments of 6 bars each)
        for si in range(8):
            start = max(0, len(pre_bars) - (8-si)*6)
            end = min(len(pre_bars), len(pre_bars) - (7-si)*6)
            seg = pre_bars[start:end]
            if len(seg) >= 2 and seg[0]["o"] > 0:
                pct = (seg[-1]["c"] - seg[0]["o"]) / seg[0]["o"] * 100
                avg_vol = sum(b["v"] for b in seg) / len(seg)
                n_bull = sum(1 for b in seg if b["c"] > b["o"])
                seg_30m.append({"pct": round(pct,1), "avg_vol": round(avg_vol,2), "n_bull": n_bull})

    # Key bar events (most informative single bars)
    key_bars = []
    for fp in fps:
        if fp["bar_type"] != "normal":
            key_bars.append({
                "rel": fp["rel"],
                "type": fp["bar_type"],
                "body_pct": fp["body_pct"],
                "vol_ratio": fp["vol_ratio"],
                "lw_ratio": fp["lw_ratio"],
                "uw_ratio": fp["uw_ratio"],
            })
    # Always include last 3 bars
    for fp in fps[-3:]:
        if fp["rel"] not in [kb["rel"] for kb in key_bars]:
            key_bars.append({
                "rel": fp["rel"], "type": fp["bar_type"],
                "body_pct": fp["body_pct"], "vol_ratio": fp["vol_ratio"],
                "lw_ratio": fp["lw_ratio"], "uw_ratio": fp["uw_ratio"],
            })
    key_bars.sort(key=lambda x: x["rel"])

    # Volume profile (pre-push)
    vol_20 = [b["v"] for b in pre_bars[-20:]] if len(pre_bars) >= 20 else [b["v"] for b in pre_bars]
    vol_first10 = [b["v"] for b in pre_bars[:10]] if len(pre_bars) >= 10 else vol_20
    vol_last6 = [b["v"] for b in pre_bars[-6:]] if len(pre_bars) >= 6 else vol_20
    avg_vol_early = sum(vol_first10)/len(vol_first10) if vol_first10 else 0
    avg_vol_late = sum(vol_last6)/len(vol_last6) if vol_last6 else 0
    vol_trend = avg_vol_late / avg_vol_early if avg_vol_early > 0 else 1

    # --- Pre-push 1m microstructure ---
    pre_1m = k1[max(0, push_1m-30):push_1m]
    m1_features = {}
    if len(pre_1m) >= 10:
        # Volume trend in last 30min (1m bars)
        m1_vols = [b["v"] for b in pre_1m]
        m1_first10v = sum(m1_vols[:10])/10 if len(m1_vols)>=10 else 0
        m1_last10v = sum(m1_vols[-10:])/10 if len(m1_vols)>=10 else 0
        m1_vol_ratio = m1_last10v / m1_first10v if m1_first10v > 0 else 1

        # Last 5 bars of 1m (5 min before push)
        m1_last5_bodies = [abs(b["c"]-b["o"])/b["o"]*100 if b["o"]>0 else 0 for b in pre_1m[-5:]]
        m1_last5_dirs = [1 if b["c"]>b["o"] else -1 for b in pre_1m[-5:]]
        m1_last5_vols = [b["v"] for b in pre_1m[-5:]]
        m1_last5_vol_avg = sum(m1_last5_vols)/5 if m1_last5_vols else 0
        m1_last10_vol_avg = sum(m1_vols[-10:])/10 if len(m1_vols)>=10 else m1_last5_vol_avg

        m1_features = {
            "vol_ratio_30min": round(m1_vol_ratio, 2),
            "last5_direction": "bull" if sum(m1_last5_dirs) > 1 else ("bear" if sum(m1_last5_dirs) < -1 else "neutral"),
            "last5_avg_body_pct": round(mean(m1_last5_bodies), 2) if m1_last5_bodies else 0,
            "last5_vol_vs_last10": round(m1_last5_vol_avg/m1_last10_vol_avg, 2) if m1_last10_vol_avg > 0 else 1,
        }

    # --- Post-push 5m outcome ---
    post_bars = k5[push_idx:push_idx+48]
    outcome = record["outcome"]

    # Hourly outcome path
    h_path = []
    for hi in range(4):
        seg = post_bars[hi*12:(hi+1)*12]
        if seg and pre_bars:
            bl = pre_bars[-1]["c"]
            h_path.append(round((seg[-1]["c"]-bl)/bl*100, 1) if bl > 0 else 0)

    # --- Post-push 1m first 30min ---
    post_1m = k1[push_1m:push_1m+30]
    m1_post = {}
    if len(post_1m) >= 5:
        bl_1m = pre_bars[-1]["c"] if pre_bars else (post_1m[0]["o"] if post_1m else 0)
        if bl_1m <= 0 and post_1m: bl_1m = post_1m[0]["o"]
        if bl_1m > 0:
            m1_post["chg_5min"] = round((post_1m[4]["c"]-bl_1m)/bl_1m*100, 1) if len(post_1m)>=5 else 0
            m1_post["lowest_30min"] = round((min(b["l"] for b in post_1m)-bl_1m)/bl_1m*100, 1)
            first_vol = sum(b["v"] for b in post_1m[:5])/5 if len(post_1m)>=5 else 0
            pre_vol = sum(b["v"] for b in pre_1m[-10:])/10 if len(pre_1m)>=10 else 1
            m1_post["vol_ratio"] = round(first_vol/pre_vol, 2) if pre_vol>0 else 0
            if len(post_1m) >= 30:
                m1_post["chg_30min"] = round((post_1m[29]["c"]-bl_1m)/bl_1m*100, 1)

    # --- Assemble feature vector ---
    features = {
        "signal_id": record["id"],
        "symbol": record["symbol"],
        "signal_type": record["signal_type"],
        "mcap": record["mcap"],
        "age_hours": record["age_hours"],
        "liquidity": record["liquidity"],
        "pool_ratio": record["pool_mcap_ratio"],

        # Pre-push 5m
        "pre_position": round(position, 1),
        "pre_volatility": round(volatility, 2),
        "pre_vol_trend": round(vol_trend, 2),
        "seg_30m": seg_30m,
        "key_bars": key_bars,
        "n_key_bars": len(key_bars),
        "capitulation_bars": sum(1 for kb in key_bars if kb["type"]=="capitulation"),
        "hammer_bars": sum(1 for kb in key_bars if kb["type"]=="hammer"),
        "surge_bars": sum(1 for kb in key_bars if kb["type"]=="surge"),
        "shooting_star_bars": sum(1 for kb in key_bars if kb["type"]=="shooting_star"),
        "doji_bars": sum(1 for kb in key_bars if kb["type"]=="doji"),

        # Last 30min summary (S7-S8)
        "last_30m_pct": seg_30m[-1]["pct"] if seg_30m else 0,
        "prev_30m_pct": seg_30m[-2]["pct"] if len(seg_30m)>=2 else 0,
        "last_30m_bull_ratio": seg_30m[-1]["n_bull"]/6 if seg_30m else 0,

        # Pre-push 1m
        "m1_pre": m1_features,

        # Post-push outcome
        "outcome": outcome,
        "h_path": h_path,

        # Post-push 1m
        "m1_post": m1_post,
    }
    return features


# ============================================================================
#  STEP 2: Pattern classification rules (based on bar-level fingerprints)
# ============================================================================

def classify_pre_pattern(feat):
    """
    Classify a signal into a pre-push pattern based on bar-level fingerprints.
    These rules are derived from the actual data, not from old docs.
    """
    pos = feat["pre_position"]
    segs = feat["seg_30m"]
    s78 = segs[-2:] if len(segs) >= 2 else []
    s8 = segs[-1] if segs else None
    kb = feat["key_bars"]
    caps = feat["capitulation_bars"]
    hammers = feat["hammer_bars"]
    surges = feat["surge_bars"]

    last_12_bars = [k for k in kb if k["rel"] >= -12]

    # 1. Capitulation → V-recovery (capitulation bar + subsequent green bars)
    if caps >= 1:
        cap_bars = [k for k in kb if k["type"] == "capitulation"]
        latest_cap = cap_bars[-1]["rel"]
        # Check if there are hammer or surge bars AFTER the capitulation
        recovery_signs = [k for k in kb if k["rel"] > latest_cap and k["type"] in ("hammer", "surge")]
        # Check if last 30m is positive
        if recovery_signs and s8 and s8["pct"] > 0:
            return "恐慌投降→V反恢复型"
        elif s8 and s8["pct"] < -5:
            return "恐慌投降→持续下跌型"
        else:
            return "恐慌投降→横盘筑底型"

    # 2. Quiet accumulation → sudden surge (low vol then volume spike)
    quiet_bars = [k for k in kb if k["type"] == "quiet"]
    if quiet_bars and surges >= 1 and all(k["rel"] <= -6 for k in quiet_bars):
        return "缩量吸筹→放量启动型"

    # 3. Continuous decline with decreasing volume (exhaustion)
    if len(segs) >= 4:
        last4_pcts = [s["pct"] for s in segs[-4:]]
        last4_vols = [s["avg_vol"] for s in segs[-4:]]
        all_declining = all(p < 0 for p in last4_pcts)
        vol_decreasing = all(last4_vols[i] <= last4_vols[i-1] for i in range(1, len(last4_vols)))
        if all_declining and vol_decreasing and pos < 25:
            return "缩量阴跌衰竭型(底部)"
        elif all_declining and not vol_decreasing and pos < 40:
            return "放量持续下跌型(危险)"
        elif all_declining:
            return "持续下跌中(未见底)"

    # 4. Bottom consolidation (low position, flat last 30m, low vol)
    if pos < 35 and s8 and abs(s8["pct"]) < 5 and s8["n_bull"] in (3, 4):
        # Check volume trend: should be declining or stable
        if feat["pre_vol_trend"] < 1.2:
            return "底部缩量横盘型"
        else:
            return "底部放量博弈型"

    # 5. Strong pump then high consolidation
    if len(segs) >= 6:
        early_pump = any(s["pct"] > 15 for s in segs[:4])
        later_flat = all(abs(s["pct"]) < 8 for s in segs[-2:])
        if early_pump and later_flat and pos > 55:
            return "冲高后高位横盘型"
        elif early_pump and s8 and s8["pct"] < -5:
            return "冲高后回落中型"

    # 6. Continuous pump (all segments positive, accelerating volume)
    if len(segs) >= 3:
        if all(s["pct"] > 0 for s in segs[-3:]) and pos > 60:
            vol_accel = all(segs[-3:][i]["avg_vol"] >= segs[-3:][i-1]["avg_vol"] for i in range(1, 3))
            if vol_accel:
                return "持续拉升加速型(追高风险)"
            else:
                return "持续拉升减速型(力竭风险)"

    # 7. Failed bounce (dead cat)
    if len(segs) >= 4:
        if segs[-2]["pct"] < -10 and segs[-1]["pct"] > 10 and pos < 40:
            # S7 deep drop → S8 bounce → signal at bounce point = dead cat risk
            # Check bar-level: S8 last bars - are they showing exhaustion?
            last3_vol = [k["vol_ratio"] for k in kb[-3:]] if len(kb) >= 3 else []
            if last3_vol and all(v < 0.8 for v in last3_vol):
                return "V反缩量(死猫跳风险)"
            else:
                return "V反放量(真反弹可能)"

    # 8. Extreme volatility, no clear trend
    if feat["pre_volatility"] > 8:
        return "高波动无序型"

    # 9. Strong recovery after deep fall
    if len(segs) >= 4 and segs[-3]["pct"] < -15 and segs[-2]["pct"] < -10 and segs[-1]["pct"] > 10:
        return "深跌后急弹型"

    # Default
    if pos < 30:
        return "低位整理型"
    elif pos > 70:
        return "高位整理型"
    else:
        return "中位震荡型"


def classify_post_walkoff(feat):
    """Classify post-4h walk-off based on outcome data."""
    o = feat["outcome"]
    h = feat["h_path"]

    if o["peak_pct"] >= 50:
        return "暴涨"
    elif o["peak_pct"] >= 30 and o["final_pct"] > 15:
        return "强涨守住"
    elif o["peak_pct"] >= 20 and o["final_pct"] < -10:
        return "冲高急跌"
    elif o["peak_pct"] >= 30 and o["final_pct"] < -5:
        return "暴涨回吐"
    elif o["trough_pct"] < -15 and o["final_pct"] < -5:
        return "持续阴跌"
    elif o["peak_pct"] >= 20 and o["final_pct"] > 5:
        return "稳健上涨"
    elif 10 <= o["peak_pct"] < 20 and o["final_pct"] > 0:
        return "温和上涨"
    elif abs(o["peak_pct"]) < 10 and abs(o["trough_pct"]) < 10:
        return "横盘震荡"
    else:
        return "其他"


# ============================================================================
#  STEP 3: Statistics aggregation
# ============================================================================

def compute_cluster_stats(signals, cluster_name):
    """Compute comprehensive statistics for a group of signals."""
    n = len(signals)
    if n == 0:
        return None

    peaks = [s["outcome"]["peak_pct"] for s in signals]
    troughs = [s["outcome"]["trough_pct"] for s in signals]
    finals = [s["outcome"]["final_pct"] for s in signals]
    wr20 = sum(1 for s in signals if s["outcome"]["wr20"])
    wr50 = sum(1 for s in signals if s["outcome"]["wr50"])
    wr100 = sum(1 for s in signals if s["outcome"]["wr100"])

    n_revival = sum(1 for s in signals if s["signal_type"] == "new_revival")
    n_abnormal = sum(1 for s in signals if s["signal_type"] == "abnormal")

    # Post-4h walk-off distribution
    walkoffs = Counter()
    for s in signals:
        wo = classify_post_walkoff(s)
        walkoffs[wo] += 1

    # Hourly path (average)
    h_avgs = []
    for hi in range(4):
        vals = [s["h_path"][hi] for s in signals if hi < len(s["h_path"])]
        h_avgs.append(round(mean(vals), 1) if vals else 0)

    # 1m post-push stats
    m1_5min_chgs = [s["m1_post"].get("chg_5min", 0) for s in signals if s["m1_post"].get("chg_5min") is not None]
    m1_lowests = [s["m1_post"].get("lowest_30min", 0) for s in signals if s["m1_post"].get("lowest_30min") is not None]
    m1_30min_chgs = [s["m1_post"].get("chg_30min", 0) for s in signals if s["m1_post"].get("chg_30min") is not None]

    return {
        "name": cluster_name,
        "count": n,
        "new_revival": n_revival,
        "abnormal": n_abnormal,
        "wr20": round(wr20/n*100, 1),
        "wr50": round(wr50/n*100, 1),
        "wr100": round(wr100/n*100, 1),
        "avg_peak": round(mean(peaks), 1),
        "med_peak": round(median(peaks), 1),
        "avg_trough": round(mean(troughs), 1),
        "med_trough": round(median(troughs), 1),
        "avg_final": round(mean(finals), 1),
        "peak_std": round(stdev(peaks), 1) if n >= 2 else 0,
        "walkoff_dist": dict(walkoffs.most_common(5)),
        "hourly_path_avg": h_avgs,
        "m1_5min_avg": round(mean(m1_5min_chgs), 1) if m1_5min_chgs else 0,
        "m1_30min_avg": round(mean(m1_30min_chgs), 1) if m1_30min_chgs else 0,
        "m1_lowest_avg": round(mean(m1_lowests), 1) if m1_lowests else 0,
        "mcap_dist": {
            "<50K": sum(1 for s in signals if s["mcap"] < 50000),
            "50-100K": sum(1 for s in signals if 50000 <= s["mcap"] < 100000),
            "100-300K": sum(1 for s in signals if 100000 <= s["mcap"] < 300000),
            "300K+": sum(1 for s in signals if s["mcap"] >= 300000),
        },
    }


# ============================================================================
#  STEP 4: Death & Golden pattern identification
# ============================================================================

def identify_special_patterns(cluster_stats):
    """Identify death patterns (WR20<30%) and golden patterns (WR50>60%)."""
    death = [c for c in cluster_stats if c and c["wr20"] < 30 and c["count"] >= 3]
    golden = [c for c in cluster_stats if c and c["wr50"] > 50 and c["count"] >= 3]
    return death, golden


# ============================================================================
#  STEP 5: Cross-pattern universal rules
# ============================================================================

def discover_universal_rules(all_features):
    """Discover rules that hold across all patterns."""
    rules = []

    # Rule: Capitulation bar in last 30min
    cap_signals = [f for f in all_features if f["capitulation_bars"] > 0]
    cap_late = [f for f in cap_signals if any(k["rel"] >= -6 and k["type"]=="capitulation" for k in f["key_bars"])]
    if cap_late:
        wr20_cap = sum(1 for f in cap_late if f["outcome"]["wr20"]) / len(cap_late) * 100
        rules.append({
            "rule": "推送前30min内出现投降Bar(body<-8%+量增>3x)",
            "count": len(cap_late),
            "wr20": round(wr20_cap, 1),
            "avg_peak": round(mean([f["outcome"]["peak_pct"] for f in cap_late]), 1),
        })

    # No-cap rule
    no_cap = [f for f in all_features if f["capitulation_bars"] == 0]
    if no_cap:
        wr20_no = sum(1 for f in no_cap if f["outcome"]["wr20"]) / len(no_cap) * 100
        rules.append({
            "rule": "无投降Bar的信号",
            "count": len(no_cap),
            "wr20": round(wr20_no, 1),
            "avg_peak": round(mean([f["outcome"]["peak_pct"] for f in no_cap]), 1),
        })

    # Rule: 1m last 5 bars all bullish before push
    m1_bull = [f for f in all_features if f["m1_pre"].get("last5_direction") == "bull"]
    if m1_bull:
        wr20 = sum(1 for f in m1_bull if f["outcome"]["wr20"]) / len(m1_bull) * 100
        rules.append({
            "rule": "推送前1m级别最后5根全阳(抢跑)",
            "count": len(m1_bull),
            "wr20": round(wr20, 1),
            "avg_peak": round(mean([f["outcome"]["peak_pct"] for f in m1_bull]), 1),
        })

    # Rule: Volume collapse in last 30min (vol_trend < 0.5)
    vol_collapse = [f for f in all_features if f["pre_vol_trend"] < 0.5]
    if vol_collapse:
        wr20 = sum(1 for f in vol_collapse if f["outcome"]["wr20"]) / len(vol_collapse) * 100
        rules.append({
            "rule": "前30min成交量崩塌(vol_trend<0.5x)",
            "count": len(vol_collapse),
            "wr20": round(wr20, 1),
            "avg_peak": round(mean([f["outcome"]["peak_pct"] for f in vol_collapse]), 1),
        })

    # Rule: Position extreme + signal type
    for pos_range, pos_label in [((0, 20), "极低位(pos<20%)"), ((80, 100), "极高位(pos>80%)")]:
        subset = [f for f in all_features if pos_range[0] <= f["pre_position"] < pos_range[1]]
        if subset:
            wr20 = sum(1 for f in subset if f["outcome"]["wr20"]) / len(subset) * 100
            rules.append({
                "rule": pos_label,
                "count": len(subset),
                "wr20": round(wr20, 1),
                "avg_peak": round(mean([f["outcome"]["peak_pct"] for f in subset]), 1),
            })

    return sorted(rules, key=lambda r: -r["count"])


# ============================================================================
#  STEP 6: Document generation
# ============================================================================

def generate_doc_08(clusters, all_features, universal_rules, records):
    """Generate 08-5m-fingerprint-encyclopedia.md"""
    n_total = len(all_features)
    n_revival = sum(1 for f in all_features if f["signal_type"] == "new_revival")
    n_abnormal = sum(1 for f in all_features if f["signal_type"] == "abnormal")
    overall_wr20 = sum(1 for f in all_features if f["outcome"]["wr20"]) / n_total * 100
    overall_wr50 = sum(1 for f in all_features if f["outcome"]["wr50"]) / n_total * 100

    lines = []
    lines.append("# 异动CA的5m K线指纹百科全书")
    lines.append("")
    lines.append(f"> 基于{n_total}个信号({n_revival} new_revival + {n_abnormal} abnormal)的逐根5m K线分析")
    lines.append(f"> 本地聚类发现{len([c for c in clusters if c])}种前置模式 × 10种后4h走势 × 1m微结构")
    lines.append(f"> 数据来源: bottom_kline_cache + bottom_kline_cache_1m")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 零、术语速查")
    lines.append("")
    lines.append("| 术语 | 定义 |")
    lines.append("|------|------|")
    lines.append("| **Bar指纹** | 单根5m K线的9维特征向量: body_pct(实体%), uw_ratio(上影/实体), lw_ratio(下影/实体), body_ratio(实体/振幅), vol_ratio(量/前量) |")
    lines.append("| **投降Bar** | body<-8% + vol_ratio>3x 的单根暴量大阴线, 是恐慌清仓的信号 |")
    lines.append("| **放量阳线** | body>8% + vol_ratio>3x, 大资金进场 |")
    lines.append("| **锤子线** | lw_ratio>3 (长下影线), 买方承接, 底部信号 |")
    lines.append("| **射击之星** | uw_ratio>3 (长上影线), 抛压, 顶部信号 |")
    lines.append("| **十字星** | body_ratio<0.3 + abs(body_pct)<3%, 多空平衡, 方向待选 |")
    lines.append("| **缩量Bar** | vol_ratio<0.3, 成交枯竭, 无人交易 |")
    lines.append("| **前置模式** | 推送前48根5m K线的bar级指纹序列被聚类后的类型 |")
    lines.append("| **后4h走势** | 推送后48根5m K线的实际结果分类 |")
    lines.append("| **WR20/WR50/WR100** | 推送后4h内峰值达到≥20%/50%/100%的概率 |")
    lines.append("| **死亡模式** | WR20<30%的前置模式, 几乎不可能翻盘 |")
    lines.append("| **黄金模式** | WR50>50%的前置模式, 大概率大幅盈利 |")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 一、全局统计")
    lines.append("")
    lines.append(f"| 指标 | 值 |")
    lines.append(f"|------|-----|")
    lines.append(f"| 总信号数 | {n_total} |")
    lines.append(f"| new_revival | {n_revival} ({n_revival/max(n_total,1)*100:.0f}%) |")
    lines.append(f"| abnormal | {n_abnormal} ({n_abnormal/max(n_total,1)*100:.0f}%) |")
    lines.append(f"| 整体WR20 | {overall_wr20:.1f}% |")
    lines.append(f"| 整体WR50 | {overall_wr50:.1f}% |")
    lines.append(f"| 整体平均峰值 | {mean([f['outcome']['peak_pct'] for f in all_features]):.1f}% |")
    lines.append(f"| 整体中位峰值 | {median([f['outcome']['peak_pct'] for f in all_features]):.1f}% |")
    lines.append("")

    # Pre-push patterns ranked by frequency
    lines.append("---")
    lines.append("")
    lines.append("## 二、前置Bar指纹模式总览")
    lines.append("")
    lines.append("| 排名 | 模式名 | 数量 | WR20 | WR50 | AvgPeak | MedPeak | 暴涨% | 阴跌% |")
    lines.append("|------|--------|------|------|------|---------|---------|-------|-------|")

    sorted_clusters = sorted([c for c in clusters if c], key=lambda c: -c["count"])
    for i, c in enumerate(sorted_clusters):
        pump_pct = c["walkoff_dist"].get("暴涨", 0) / c["count"] * 100
        dump_pct = c["walkoff_dist"].get("持续阴跌", 0) / c["count"] * 100
        lines.append(f"| {i+1} | {c['name']} | {c['count']} | {c['wr20']}% | {c['wr50']}% | {c['avg_peak']:+.0f}% | {c['med_peak']:+.0f}% | {pump_pct:.0f}% | {dump_pct:.0f}% |")
    lines.append("")

    # Detailed analysis per pattern
    lines.append("---")
    lines.append("")
    lines.append("## 三、每种前置模式的详细Bar级分析")
    lines.append("")

    for i, c in enumerate(sorted_clusters):
        signals = [f for f in all_features if f.get("cluster") == c["name"]]
        lines.append(f"### {i+1}. {c['name']} (n={c['count']}, WR20={c['wr20']}%)")
        lines.append("")
        lines.append(f"**信号构成**: new_revival={c['new_revival']}, abnormal={c['abnormal']}")
        lines.append(f"**市值分布**: <50K={c['mcap_dist']['<50K']}, 50-100K={c['mcap_dist']['50-100K']}, 100-300K={c['mcap_dist']['100-300K']}, 300K+={c['mcap_dist']['300K+']}")
        lines.append("")

        # Key fingerprint characteristics
        lines.append("**前置Bar指纹特征**:")
        # Aggregate key bars from all signals in this cluster
        all_kb_types = Counter()
        all_kb_positions = Counter()
        for s in signals:
            for kb in s["key_bars"]:
                all_kb_types[kb["type"]] += 1
                all_kb_positions[kb["rel"]] += 1
        top_types = all_kb_types.most_common(5)
        lines.append(f"- 关键Bar类型: {', '.join(f'{t}({c}次)' for t,c in top_types)}")
        top_pos = all_kb_positions.most_common(5)
        lines.append(f"- 关键Bar位置(相对推送): {', '.join(f'Bar[{p}]({c}次)' for p,c in top_pos)}")
        lines.append("")

        # Outcome stats
        lines.append("**后4h结果统计**:")
        lines.append(f"| 指标 | 值 |")
        lines.append(f"|------|-----|")
        lines.append(f"| WR20 | {c['wr20']}% |")
        lines.append(f"| WR50 | {c['wr50']}% |")
        lines.append(f"| WR100 | {c['wr100']}% |")
        lines.append(f"| Avg Peak | {c['avg_peak']:+.1f}% |")
        lines.append(f"| Med Peak | {c['med_peak']:+.1f}% |")
        lines.append(f"| Avg Trough | {c['avg_trough']:+.1f}% |")
        lines.append(f"| Peak Std | {c['peak_std']:.1f}% |")
        lines.append("")

        lines.append("**后4h走势分布**:")
        for wo, cnt in c["walkoff_dist"].items():
            lines.append(f"- {wo}: {cnt}次 ({cnt/c['count']*100:.0f}%)")
        lines.append("")

        lines.append(f"**分时路径均值**: H1={c['hourly_path_avg'][0]:+.1f}% → H2={c['hourly_path_avg'][1]:+.1f}% → H3={c['hourly_path_avg'][2]:+.1f}% → H4={c['hourly_path_avg'][3]:+.1f}%")
        lines.append("")

        lines.append("**1m微结构**:")
        lines.append(f"- 推送后5min均值: {c['m1_5min_avg']:+.1f}%")
        lines.append(f"- 推送后30min最深均值: {c['m1_lowest_avg']:+.1f}%")
        lines.append(f"- 推送后30min均值: {c['m1_30min_avg']:+.1f}%")
        lines.append("")

    # Death patterns
    death, golden = identify_special_patterns([c for c in clusters if c])
    lines.append("---")
    lines.append("")
    lines.append("## 四、死亡模式 (WR20<30%, ≥3个信号)")
    lines.append("")
    if death:
        lines.append("| 模式 | 数量 | WR20 | AvgPeak | 特征 |")
        lines.append("|------|------|------|---------|------|")
        for d in sorted(death, key=lambda x: x["wr20"]):
            lines.append(f"| {d['name']} | {d['count']} | {d['wr20']}% | {d['avg_peak']:+.1f}% | {d.get('death_reason','')} |")
        lines.append("")
    else:
        lines.append("无满足条件的死亡模式(需要≥3个信号且WR20<30%)")
        lines.append("")

    # Golden patterns
    lines.append("---")
    lines.append("")
    lines.append("## 五、黄金模式 (WR50>50%, ≥3个信号)")
    lines.append("")
    if golden:
        lines.append("| 模式 | 数量 | WR50 | WR100 | AvgPeak | 特征 |")
        lines.append("|------|------|------|-------|---------|------|")
        for g in sorted(golden, key=lambda x: -x["wr50"]):
            lines.append(f"| {g['name']} | {g['count']} | {g['wr50']}% | {g['wr100']}% | {g['avg_peak']:+.1f}% | |")
        lines.append("")
    else:
        lines.append("无满足条件的黄金模式(需要≥3个信号且WR50>50%)")
        lines.append("")

    # Universal rules
    lines.append("---")
    lines.append("")
    lines.append("## 六、跨模式通用规则")
    lines.append("")
    lines.append("| 规则 | 信号数 | WR20 | AvgPeak |")
    lines.append("|------|--------|------|---------|")
    for r in universal_rules:
        lines.append(f"| {r['rule']} | {r['count']} | {r['wr20']:.1f}% | {r['avg_peak']:+.1f}% |")
    lines.append("")

    # Decision tree
    lines.append("---")
    lines.append("")
    lines.append("## 七、实战决策树")
    lines.append("")
    lines.append("```")
    lines.append("Step 1: 推送前30min内是否有投降Bar?")
    lines.append("  YES → 看投降后是否有锤子线/放量阳线")
    lines.append("         YES → 高概率反弹(见恐慌投降型)")
    lines.append("         NO  → 可能继续下跌(见恐慌投降→持续下跌型)")
    lines.append("  NO  → Step 2")
    lines.append("")
    lines.append("Step 2: 推送时价格在4h区间的什么位置?")
    lines.append("  pos<30% → 看量能趋势")
    lines.append("            vol缩量 → 底部筑底(见缩量阴跌衰竭型/底部缩量横盘型)")
    lines.append("            vol放量 → 恐慌未结束(见放量持续下跌型)")
    lines.append("  pos>70% → 高位风险, 仅当出现放量阳线+锤子线组合才考虑")
    lines.append("  pos在30-70% → Step 3")
    lines.append("")
    lines.append("Step 3: 最后30min(S8)的方向+量能")
    lines.append("  S8涨+放量 → 可入场(趋势延续)")
    lines.append("  S8涨+缩量 → 假突破风险, 观望")
    lines.append("  S8跌+缩量 → 筑底信号, 等确认(见底部缩量横盘型)")
    lines.append("  S8跌+放量 → 恐慌卖压, 放弃")
    lines.append("```")
    lines.append("")
    lines.append("---")
    lines.append(f"*分析时间: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}*")
    lines.append(f"*数据来源: bottom_kline_cache (5m) + bottom_kline_cache_1m (1m), {n_total}个信号*")

    return "\n".join(lines)


def generate_doc_09(clusters, all_features, universal_rules):
    """Generate 09-bar-level-strategy.md"""
    n_total = len(all_features)
    overall_wr20 = sum(1 for f in all_features if f["outcome"]["wr20"]) / n_total * 100

    lines = []
    lines.append("# Bar级K线交易策略")
    lines.append("")
    lines.append(f"> 基于{n_total}个信号的逐根5m+1m K线指纹分析")
    lines.append("> 本地聚类+统计, 不依赖外部API")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 一、核心数据")
    lines.append("")

    # Post-5min stats
    m1_post_data = [(f["m1_post"].get("chg_5min", 0), f["outcome"]["wr20"])
                     for f in all_features if f["m1_post"].get("chg_5min") is not None]
    if m1_post_data:
        dipped = [(chg, wr) for chg, wr in m1_post_data if chg < -3]
        flat = [(chg, wr) for chg, wr in m1_post_data if -3 <= chg <= 3]
        pumped = [(chg, wr) for chg, wr in m1_post_data if chg > 3]
        lines.append(f"推送后5min内跌>3%: {len(dipped)}/{len(m1_post_data)} = {len(dipped)/max(len(m1_post_data),1)*100:.0f}%")
        if dipped:
            lines.append(f"  其中WR20: {sum(1 for _,wr in dipped if wr)}/{len(dipped)} = {sum(1 for _,wr in dipped if wr)/max(len(dipped),1)*100:.0f}%")
        if flat:
            lines.append(f"推送后5min横盘(-3%~+3%): {len(flat)}/{len(m1_post_data)} = {len(flat)/max(len(m1_post_data),1)*100:.0f}%")
            lines.append(f"  其中WR20: {sum(1 for _,wr in flat if wr)}/{len(flat)} = {sum(1 for _,wr in flat if wr)/max(len(flat),1)*100:.0f}%")
        if pumped:
            lines.append(f"推送后5min涨>3%: {len(pumped)}/{len(m1_post_data)} = {len(pumped)/max(len(m1_post_data),1)*100:.0f}%")
            lines.append(f"  其中WR20: {sum(1 for _,wr in pumped if wr)}/{len(pumped)} = {sum(1 for _,wr in pumped if wr)/max(len(pumped),1)*100:.0f}%")
    lines.append("")

    # 30min recovery stats
    m1_30min = [(f["m1_post"].get("chg_30min", 0), f["outcome"]["wr20"])
                for f in all_features if f["m1_post"].get("chg_30min") is not None]
    if m1_30min:
        recover30 = [(chg, wr) for chg, wr in m1_30min if chg > 0]
        still_down30 = [(chg, wr) for chg, wr in m1_30min if chg <= 0]
        lines.append(f"推送后30min转正(>0%): {len(recover30)}/{len(m1_30min)}")
        if recover30:
            lines.append(f"  其中WR20: {sum(1 for _,wr in recover30 if wr)}/{len(recover30)} = {sum(1 for _,wr in recover30 if wr)/max(len(recover30),1)*100:.0f}%")
        lines.append(f"推送后30min仍为负: {len(still_down30)}/{len(m1_30min)}")
        if still_down30:
            lines.append(f"  其中WR20: {sum(1 for _,wr in still_down30 if wr)}/{len(still_down30)} = {sum(1 for _,wr in still_down30 if wr)/max(len(still_down30),1)*100:.0f}%")
    lines.append("")

    # Death signal characteristics
    lines.append("---")
    lines.append("")
    lines.append("## 二、死亡信号Bar级特征")
    lines.append("")
    death_signals = [f for f in all_features if not f["outcome"]["wr20"]]
    alive_signals = [f for f in all_features if f["outcome"]["wr20"]]
    lines.append(f"死亡信号(n={len(death_signals)}) vs 存活信号(n={len(alive_signals)}) 对比:")
    lines.append("")
    labels = ["推送前位置(pos)", "波动率", "投降Bar出现率", "锤子线出现率", "1m最后5根方向", "成交量趋势"]
    death_vals = [
        f"{mean([f['pre_position'] for f in death_signals]):.0f}%" if death_signals else "N/A",
        f"{mean([f['pre_volatility'] for f in death_signals]):.2f}%" if death_signals else "N/A",
        f"{sum(1 for f in death_signals if f['capitulation_bars']>0)/max(len(death_signals),1)*100:.0f}%" if death_signals else "N/A",
        f"{sum(1 for f in death_signals if f['hammer_bars']>0)/max(len(death_signals),1)*100:.0f}%" if death_signals else "N/A",
        f"{sum(1 for f in death_signals if f['m1_pre'].get('last5_direction')=='bull')/max(len(death_signals),1)*100:.0f}%" if death_signals else "N/A",
        f"{mean([f['pre_vol_trend'] for f in death_signals]):.2f}x" if death_signals else "N/A",
    ]
    alive_vals = [
        f"{mean([f['pre_position'] for f in alive_signals]):.0f}%" if alive_signals else "N/A",
        f"{mean([f['pre_volatility'] for f in alive_signals]):.2f}%" if alive_signals else "N/A",
        f"{sum(1 for f in alive_signals if f['capitulation_bars']>0)/max(len(alive_signals),1)*100:.0f}%" if alive_signals else "N/A",
        f"{sum(1 for f in alive_signals if f['hammer_bars']>0)/max(len(alive_signals),1)*100:.0f}%" if alive_signals else "N/A",
        f"{sum(1 for f in alive_signals if f['m1_pre'].get('last5_direction')=='bull')/max(len(alive_signals),1)*100:.0f}%" if alive_signals else "N/A",
        f"{mean([f['pre_vol_trend'] for f in alive_signals]):.2f}x" if alive_signals else "N/A",
    ]
    lines.append("| 特征 | 死亡信号 | 存活信号 | 差异 |")
    lines.append("|------|---------|---------|------|")
    for l, d, a in zip(labels, death_vals, alive_vals):
        lines.append(f"| {l} | {d} | {a} | |")
    lines.append("")

    # Entry rules
    lines.append("---")
    lines.append("")
    lines.append("## 三、入场决策规则")
    lines.append("")
    lines.append("### Step 1: 识别前置Bar指纹")
    lines.append("")
    lines.append("1. 查看推送前48根5m K线中的关键Bar类型")
    lines.append("   - 有投降Bar → 看后续是否有锤子线/放量阳线确认 → 参考'恐慌投降型'")
    lines.append("   - 有多个锤子线(≥2) + 缩量 → 底部吸筹, 可入场")
    lines.append("   - 有射击之星 → 顶部阻力, 观望")
    lines.append("   - 全是普通Bar无特征 → 方向不明确, 需后4h确认")
    lines.append("")
    lines.append("### Step 2: 看推送后1m微结构")
    lines.append("")
    lines.append("1. 首根1m Bar:")
    lines.append("   - 放量阳线吞没 → 短线资金进场, 可追")
    lines.append("   - 长上影小阳 → 试探性买盘, 观望")
    lines.append("   - 小阴线缩量 → 正常回踩, 等止跌")
    lines.append("2. 5min内:")
    lines.append("   - 跌<3% + 正常量 → 符合先跌后涨模型")
    lines.append("   - 跌>8% + 放量 → 恐慌, 放弃")
    lines.append("3. 30min转正 → 确认上涨, 加仓")
    lines.append("")
    lines.append("### Step 3: 仓位管理")
    lines.append("")
    lines.append("- 50%仓位 +30%止盈")
    lines.append("- 30%仓位 +50%止盈")
    lines.append("- 20%仓位 +100%止盈")
    lines.append("- 硬止损: -25%")
    lines.append("")

    # Universal rules section
    lines.append("---")
    lines.append("")
    lines.append("## 四、跨模式通用规则")
    lines.append("")
    lines.append("| 规则 | 适用信号数 | WR20 | AvgPeak |")
    lines.append("|------|----------|------|---------|")
    for r in universal_rules:
        lines.append(f"| {r['rule']} | {r['count']} | {r['wr20']:.1f}% | {r['avg_peak']:+.1f}% |")
    lines.append("")

    lines.append("---")
    lines.append(f"*分析时间: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}*")
    lines.append(f"*数据来源: 本地K线指纹聚类, {n_total}个信号, 全本地计算*")

    return "\n".join(lines)


# ============================================================================
#  MAIN
# ============================================================================

def main():
    print("=" * 70)
    print("  Local K-line Pattern Discovery Pipeline")
    print("=" * 70)

    # Step 1: Load & extract
    print("\n[Step 1] Loading records & extracting bar fingerprints...")
    records = load_records()
    all_features = []
    for i, rec in enumerate(records):
        feat = extract_signal_features(rec)
        all_features.append(feat)
        if (i+1) % 100 == 0:
            print(f"  Extracted {i+1}/{len(records)}...")
    print(f"  Done: {len(all_features)} signals with bar-level fingerprints")

    # Step 2: Classify each signal into pre-push patterns
    print("\n[Step 2] Classifying pre-push patterns...")
    pattern_counts = Counter()
    for feat in all_features:
        pattern = classify_pre_pattern(feat)
        feat["cluster"] = pattern
        pattern_counts[pattern] += 1

    print(f"  Found {len(pattern_counts)} distinct patterns:")
    for pat, cnt in pattern_counts.most_common():
        print(f"    {pat}: {cnt} signals")

    # Step 3: Compute cluster statistics
    print("\n[Step 3] Computing cluster statistics...")
    clusters_by_name = defaultdict(list)
    for feat in all_features:
        clusters_by_name[feat["cluster"]].append(feat)

    cluster_stats = []
    for name, signals in sorted(clusters_by_name.items(), key=lambda x: -len(x[1])):
        stats = compute_cluster_stats(signals, name)
        cluster_stats.append(stats)
        if stats:
            print(f"  {name}: n={stats['count']}, WR20={stats['wr20']}%, WR50={stats['wr50']}%, AvgPeak={stats['avg_peak']:+.1f}%")

    # Step 4: Identify death & golden patterns
    print("\n[Step 4] Identifying death & golden patterns...")
    death, golden = identify_special_patterns(cluster_stats)
    print(f"  Death patterns (WR20<30%): {len(death)}")
    for d in death:
        print(f"    {d['name']}: n={d['count']}, WR20={d['wr20']}%")
    print(f"  Golden patterns (WR50>50%): {len(golden)}")
    for g in golden:
        print(f"    {g['name']}: n={g['count']}, WR50={g['wr50']}%")

    # Step 5: Discover universal rules
    print("\n[Step 5] Discovering universal rules...")
    universal_rules = discover_universal_rules(all_features)
    for r in universal_rules[:10]:
        print(f"  {r['rule']}: n={r['count']}, WR20={r['wr20']:.1f}%, AvgPeak={r['avg_peak']:+.1f}%")

    # Step 6: Generate documents
    print("\n[Step 6] Generating strategy documents...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    doc_08 = generate_doc_08(cluster_stats, all_features, universal_rules, records)
    doc_08_path = OUTPUT_DIR / "08-5m-fingerprint-encyclopedia.md"
    with open(doc_08_path, "w", encoding="utf-8") as f:
        f.write(doc_08)
    print(f"  Written: {doc_08_path} ({len(doc_08)} chars)")

    doc_09 = generate_doc_09(cluster_stats, all_features, universal_rules)
    doc_09_path = OUTPUT_DIR / "09-bar-level-strategy.md"
    with open(doc_09_path, "w", encoding="utf-8") as f:
        f.write(doc_09)
    print(f"  Written: {doc_09_path} ({len(doc_09)} chars)")

    print(f"\n{'='*70}")
    print("  Pipeline complete!")
    print(f"  Documents: {OUTPUT_DIR}/")
    print(f"  Stats saved to: {ROOT/'data'/'deepseek_discovery'/'cluster_stats.json'}")
    print(f"{'='*70}")

    # Save cluster stats for reference
    stats_path = ROOT / "data" / "deepseek_discovery" / "cluster_stats.json"
    with open(stats_path, "w", encoding="utf-8") as f:
        json.dump([c for c in cluster_stats if c], f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
