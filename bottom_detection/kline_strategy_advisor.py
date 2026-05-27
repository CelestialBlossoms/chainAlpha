"""
Real-time kline strategy advisor.
Called at push time: analyzes pre-signal klines, matches against historical patterns,
attaches strategy recommendation to the push signal.

Usage (in bottom_accumulation_monitor.py):
    from bottom_detection.kline_strategy_advisor import analyze_push_signal
    strategy = analyze_push_signal(address, event_ts, signal_type)
    extra["strategy"] = strategy
"""
import sys
from collections import defaultdict
from statistics import median
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from db_client import db_op


# ---- Historical baseline stats (pre-computed from our analysis) ----
# These are from 08-kline-journey-encyclopedia.md cache-based analysis

STRUCTURE_PROFILE = {
    "new_revival": {
        "底部持续下跌": {"wr20": 84, "wr50": 66, "pump_pct": 59, "dump_pct": 12, "label": "最优结构", "stars": 5},
        "底部横盘":     {"wr20": 74, "wr50": 53, "pump_pct": 42, "dump_pct": 21, "label": "良好", "stars": 4},
        "高点下跌回落中": {"wr20": 77, "wr50": 54, "pump_pct": 31, "dump_pct": 23, "label": "V字反转", "stars": 5},
        "持续拉升中":    {"wr20": 74, "wr50": 46, "pump_pct": 33, "dump_pct": 30, "label": "分化严重", "stars": 3},
        "底部反弹启动":  {"wr20": 67, "wr50": 22, "pump_pct": 17, "dump_pct": 39, "label": "死猫跳风险", "stars": 2},
        "底部反弹启动(死猫跳风险)": {"wr20": 0, "wr50": 0, "pump_pct": 0, "dump_pct": 100, "label": "极度危险！反弹尖上=必死", "stars": 1},
        "强势拉升后高位": {"wr20": 80, "wr50": 50, "pump_pct": 30, "dump_pct": 40, "label": "高位追涨", "stars": 3},
    },
    "abnormal": {
        "底部反弹启动":  {"wr20": 80, "wr50": 20, "pump_pct": 20, "dump_pct": 40, "label": "abnormal最优", "stars": 4},
        "高位加速拉升":  {"wr20": 83, "wr50": 67, "pump_pct": 33, "dump_pct": 0,  "label": "突破型", "stars": 5},
        "持续拉升中":    {"wr20": 65, "wr50": 34, "pump_pct": 21, "dump_pct": 19, "label": "abnormal主力", "stars": 2},
        "底部持续下跌":  {"wr20": 57, "wr50": 27, "pump_pct": 14, "dump_pct": 0,  "label": "一般", "stars": 2},
    },
}

POST5MIN_PROFILE = {
    "跌+正常量": {"post4h": "42%稳健上涨+33%冲高急跌", "action": "可入场"},
    "涨+正常量": {"post4h": "40%暴涨+28%冲高急跌", "action": "可追"},
    "震荡+缩量": {"post4h": "71%稳健上涨+29%暴涨", "action": "最优"},
    "涨+放量":   {"post4h": "风险高", "action": "观望"},
    "跌+放量":   {"post4h": "恐慌盘", "action": "观望"},
    "天量(>4x)": {"post4h": "顶部信号", "action": "放弃"},
}

MCAP_BASELINE = {
    "new_revival": {"<50K": 77, "50-100K": 71, "100-300K": 77},
    "abnormal":    {"<50K": 50, "50-100K": 61, "100-300K": 66, "300K+": 60},
}


def classify_pre_structure(candles_5m, sig_ts):
    """Classify pre-signal 5m kline structure (4h window)."""
    if len(candles_5m) < 48:
        return "数据不足", {}

    # Find signal
    sig_idx = 0; md = float('inf')
    for i, c in enumerate(candles_5m):
        d = abs(c["t"] - sig_ts)
        if d < md: md = d; sig_idx = i
    if sig_idx < 48:
        return "数据不足", {}

    pw = candles_5m[:sig_idx+1][-48:]
    qs = 12
    q4 = pw[-qs:]; q3 = pw[-2*qs:-qs]; q2 = pw[-3*qs:-2*qs]; q1 = pw[-4*qs:-3*qs]

    def qc(q):
        return (q[-1]["c"] - q[0]["c"]) / q[0]["c"] * 100 if len(q) >= 2 and q[0]["c"] > 0 else 0

    q1c, q2c, q3c, q4c = qc(q1), qc(q2), qc(q3), qc(q4)
    sig_p = candles_5m[sig_idx]["c"]
    ph = max(c["h"] for c in pw); pl = min(c["l"] for c in pw)
    sig_pos = (sig_p - pl) / (ph - pl) * 100 if ph > pl else 50

    q4_rng = max(c["h"] for c in q4) - min(c["l"] for c in q4)
    q4_rng_pct = q4_rng / (ph - pl) * 100 if ph > pl else 100
    is_consolidating = q4_rng_pct < 35 and abs(q4c) < 15
    peak_i = max(range(len(pw)), key=lambda i: pw[i]["h"])
    trough_i = min(range(len(pw)), key=lambda i: pw[i]["l"])
    peak_first = peak_i < trough_i
    pre_total = (pw[-1]["c"] - pw[0]["c"]) / pw[0]["c"] * 100 if pw[0]["c"] > 0 else 0

    # Dead cat bounce detection: Q3 deep drop + Q4 strong bounce (signal at bounce peak)
    if q3c < -10 and q4c > 15:
        label = "底部反弹启动(死猫跳风险)"
    elif sig_pos < 35 and q4c < -10:
        label = "底部持续下跌"
    elif sig_pos < 35 and abs(q4c) < 10 and is_consolidating:
        label = "底部横盘"
    elif sig_pos < 35 and q4c >= 10:
        label = "底部反弹启动"
    elif peak_first and sig_pos < 55 and pre_total < -10:
        label = "高点下跌回落中"
    elif not peak_first and sig_pos > 55 and pre_total > 10:
        label = "持续拉升中"
    elif sig_pos > 70 and q4c > 10:
        label = "高位加速拉升"
    elif pre_total > 30 and sig_pos > 50:
        label = "强势拉升后高位"
    elif pre_total < -20:
        label = "持续下跌中"
    else:
        label = "其他"

    return label, {
        "q1": q1c, "q2": q2c, "q3": q3c, "q4": q4c,
        "sig_pos": sig_pos,
        "pre_4h_range": f"{pl:.8f} ~ {ph:.8f}",
    }


def classify_post_5min(candles_1m, sig_ts, sig_price):
    """Classify post-signal 5min 1m kline pattern."""
    if len(candles_1m) < 30:
        return "数据不足", {}

    sig_idx = 0; md = float('inf')
    for i, c in enumerate(candles_1m):
        d = abs(c["t"] - sig_ts)
        if d < md: md = d; sig_idx = i

    pre_vols = [c["v"] for c in candles_1m[max(0, sig_idx-30):sig_idx+1]]
    pre_vol_avg = sum(pre_vols) / len(pre_vols) if pre_vols else 1

    post5 = candles_1m[sig_idx:sig_idx+5]
    if len(post5) < 3:
        return "数据不足", {}

    post5_chg = (post5[-1]["c"] - sig_price) / sig_price * 100 if sig_price > 0 else 0
    ups = sum(1 for i in range(1, len(post5)) if post5[i]["c"] > post5[i-1]["c"])
    up_ratio = ups / max(1, len(post5)-1)
    post_vol_avg = sum(c["v"] for c in post5) / len(post5)
    vol_ratio = post_vol_avg / pre_vol_avg if pre_vol_avg > 0 else 1

    if up_ratio > 0.6 and vol_ratio < 2:
        label = "涨+正常量"
    elif up_ratio > 0.6 and vol_ratio >= 2:
        label = "涨+放量"
    elif up_ratio < 0.4 and vol_ratio < 2:
        label = "跌+正常量"
    elif up_ratio < 0.4 and vol_ratio >= 2:
        label = "跌+放量"
    elif vol_ratio >= 2:
        label = "震荡+放量"
    elif vol_ratio < 0.5:
        label = "震荡+缩量"
    else:
        label = "震荡+平量"

    return label, {
        "chg_5min": post5_chg,
        "up_ratio": up_ratio,
        "vol_ratio": vol_ratio,
    }


def get_mcap_label(mcap):
    if mcap < 50000: return "<50K"
    if mcap < 100000: return "50-100K"
    if mcap < 300000: return "100-300K"
    return "300K+"


def analyze_push_signal(address, event_ts, signal_type, mcap=0):
    """
    Analyze a push signal in real-time.
    Returns a strategy dict to attach to the push extra/notification.

    Call this BEFORE pushing the signal to TG/frontend.
    """
    # Load klines
    def _op(conn):
        cur = conn.cursor()
        # 5m klines
        cur.execute("""SELECT ts, open, high, low, close, volume
            FROM bottom_kline_cache
            WHERE address=%s AND chain='sol' AND resolution='5m' ORDER BY ts""", (address,))
        k5 = [{"t": int(r[0]), "o": float(r[1]), "h": float(r[2]),
               "l": float(r[3]), "c": float(r[4]), "v": float(r[5] or 0)} for r in cur]

        # 1m klines
        cur.execute("""SELECT ts, open, high, low, close, volume
            FROM bottom_kline_cache_1m
            WHERE address=%s AND chain='sol' AND resolution='1m' ORDER BY ts""", (address,))
        k1 = [{"t": int(r[0]), "o": float(r[1]), "h": float(r[2]),
               "l": float(r[3]), "c": float(r[4]), "v": float(r[5] or 0)} for r in cur]
        return k5, k1

    try:
        k5, k1 = db_op(_op)
    except Exception:
        return {"error": "kline_load_failed", "ready": False}

    sig_ts = int(event_ts)

    # Find signal price
    sig_p = 0
    for c in k5:
        if abs(c["t"] - sig_ts) < 300:
            sig_p = c["c"]
            break
    if sig_p <= 0 and k5:
        sig_p = k5[-1]["c"]

    # 1. Pre-signal structure
    pre_label, pre_detail = classify_pre_structure(k5, sig_ts)

    # 2. Post-5min pattern (only if enough time has passed)
    post_label, post_detail = classify_post_5min(k1, sig_ts, sig_p)

    # 2.5 Post-signal journey: current drawdown + recovery analysis
    post_journey = _analyze_post_journey(k5, sig_ts, sig_p, signal_type)

    # 3. Look up historical profile
    profile = STRUCTURE_PROFILE.get(signal_type, {}).get(pre_label, {})
    mcap_label = get_mcap_label(mcap)
    baseline_wr = MCAP_BASELINE.get(signal_type, {}).get(mcap_label, 0)

    # 4. Build strategy recommendation
    risk_level = "low"
    if profile.get("dump_pct", 0) >= 30:
        risk_level = "high"
    elif profile.get("dump_pct", 0) >= 20:
        risk_level = "medium"

    entry_advice = "观望"
    if profile.get("stars", 0) >= 4:
        entry_advice = "等5min确认后可入场"
    elif profile.get("stars", 0) >= 3:
        entry_advice = "需等后4h二次确认"
    elif profile.get("stars", 0) >= 2:
        entry_advice = "高风险，建议观察"

    # Specific red flags
    warnings = []
    if signal_type == "new_revival" and pre_label == "底部反弹启动":
        warnings.append("死猫跳风险！Q3暴跌+Q4反弹=反弹尖上，历史同类Peak<2%")
    if signal_type == "abnormal" and "急拉" in pre_label:
        warnings.append("abnormal+急拉=WR20仅36%")

    return {
        "ready": True,
        "signal_type": signal_type,
        "pre_structure": pre_label,
        "pre_detail": pre_detail,
        "post_5min": post_label,
        "post_detail": post_detail,
        "post_journey": post_journey,
        "profile": profile,
        "baseline_wr20": baseline_wr,
        "risk_level": risk_level,
        "entry_advice": entry_advice,
        "warnings": warnings,
        "summary": _build_summary(signal_type, pre_label, profile, post_label, post_journey, warnings),
    }


def _analyze_post_journey(k5, sig_ts, sig_p, signal_type):
    """Analyze what happened after the signal: drawdown, recovery, current state."""
    sig_idx = 0; md = float('inf')
    for i, c in enumerate(k5):
        d = abs(c["t"] - sig_ts)
        if d < md: md = d; sig_idx = i

    post = k5[sig_idx:]
    if len(post) < 4:
        return {"status": "刚推送，数据不足"}

    hours_since = (k5[-1]["t"] - sig_ts) / 3600
    cur_p = post[-1]["c"]
    cur_return = (cur_p - sig_p) / sig_p * 100

    # Find lowest point after signal
    lowest_p = sig_p
    lowest_i = 0
    for i, c in enumerate(post):
        if c["l"] < lowest_p:
            lowest_p = c["l"]
            lowest_i = i
    max_dd = (lowest_p - sig_p) / sig_p * 100
    dd_time_min = lowest_i * 5

    # Recovery from lowest
    after_low = post[lowest_i:]
    recovery_high = max(c["h"] for c in after_low) if after_low else lowest_p
    recovery_gain = (recovery_high - lowest_p) / lowest_p * 100 if lowest_p > 0 else 0
    recovery_from_sig = (recovery_high - sig_p) / sig_p * 100

    # Peak after signal
    peak_high = max(c["h"] for c in post)
    peak_gain = (peak_high - sig_p) / sig_p * 100
    peak_i = max(range(len(post)), key=lambda i: post[i]["h"])
    peak_time_min = peak_i * 5

    # Phase: are we in "dropping" or "recovering"?
    phase = "unknown"
    if cur_return < max_dd * 0.7 and cur_return < -5:
        phase = "仍在下跌中"
    elif recovery_gain > 10 and cur_return > max_dd:
        phase = "正在反弹中"
    elif abs(cur_return) < 5:
        phase = "横盘整理"
    elif cur_return > 5:
        phase = "已回升"
    else:
        phase = "下跌后企稳"

    # Historical recovery reference (from 08 doc)
    recovery_ref = {}
    if signal_type == "new_revival":
        recovery_ref = {
            "深跌反弹型占比": "66%",
            "深跌死亡型占比": "12%",
            "中位反弹幅度": "+101%",
            "中位最大回撤": "-70%",
        }
    elif signal_type == "abnormal":
        recovery_ref = {
            "深跌反弹型占比": "49%",
            "深跌死亡型占比": "18%",
            "中位反弹幅度": "+54%",
            "中位最大回撤": "-46%",
        }

    return {
        "hours_since": round(hours_since, 1),
        "cur_return": round(cur_return, 1),
        "max_dd": round(max_dd, 1),
        "dd_time_min": dd_time_min,
        "recovery_gain": round(recovery_gain, 1),
        "recovery_from_sig": round(recovery_from_sig, 1),
        "peak_gain": round(peak_gain, 1),
        "peak_time_min": peak_time_min,
        "phase": phase,
        "history_ref": recovery_ref,
    }


def _build_summary(stype, pre, profile, post, journey, warnings):
    stars = "★" * profile.get("stars", 0)
    wr = profile.get("wr20", "?")
    label = profile.get("label", "")
    pump = profile.get("pump_pct", "?")
    dump = profile.get("dump_pct", "?")

    lines = [
        f"[{stype}] 前置={pre} {stars}",
        f"历史WR20={wr}% | 暴涨{pump}% | 阴跌{dump}% | {label}",
    ]
    if post and post != "数据不足":
        action = POST5MIN_PROFILE.get(post, {}).get("action", "?")
        lines.append(f"后5min={post} → {action}")
    if journey and isinstance(journey, dict) and journey.get("hours_since", 0) > 0:
        j = journey
        lines.append(f"已过{j['hours_since']}h | 当前{j['cur_return']:+.1f}% | 最深{j['max_dd']:+.1f}%({j['dd_time_min']}min)")
        lines.append(f"阶段={j['phase']} | 从底部反弹{j['recovery_gain']:+.1f}% | 峰值{j['peak_gain']:+.1f}%({j['peak_time_min']}min)")
        if j.get("history_ref"):
            ref = j["history_ref"]
            lines.append(f"历史: 反弹型{ref.get('深跌反弹型占比','?')} | 死亡型{ref.get('深跌死亡型占比','?')} | 中位反弹{ref.get('中位反弹幅度','?')}")
    for w in warnings:
        lines.append(f"⚠️ {w}")
    return "\n".join(lines)


# ---- Quick test ----
if __name__ == "__main__":
    # Test on Peigengoo
    addr = "roqPTEPKShP5bgfipqm3Y6qZnZU1WLCb1qjTGFspump"
    ets = 1779908377  # 2026-05-28 02:59:37 CST
    result = analyze_push_signal(addr, ets, "new_revival", 69776)
    import json
    print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
