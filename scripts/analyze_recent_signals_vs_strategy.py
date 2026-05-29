"""
Analyze yesterday & today's push records vs 09-dip-then-pump-strategy.md
and 08-kline-journey-encyclopedia.md patterns.

Computes win rates and checks pattern conformance.
"""
import sys
import os
import io

# Fix Unicode output on Windows GBK terminals
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timezone, timedelta
from collections import defaultdict
import json

from db_client import db_op
from bottom_detection.top100_push_record_store import ensure_top100_push_records_table

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
TODAY = "2026-05-29"
YESTERDAY = "2026-05-28"

# Timestamp range (Unix seconds)
START_TS = int(datetime(2026, 5, 28, 0, 0, 0, tzinfo=timezone.utc).timestamp())
END_TS = int(datetime(2026, 5, 30, 0, 0, 0, tzinfo=timezone.utc).timestamp())


def fetch_push_records():
    """Fetch push records for yesterday and today."""
    ensure_top100_push_records_table()

    def _op(conn):
        cur = conn.cursor()
        cur.execute("""
            SELECT id, pushed_at, event_ts, chain, source, address, symbol,
                   signal_type, abnormal_rule, current_mcap, first_signal_mcap,
                   first_signal_change_pct, price_change_pct, ath_mcap,
                   liquidity, pool_mcap_ratio, age_sec, text, extra
            FROM bottom_top100_push_records
            WHERE event_ts >= %s AND event_ts < %s
              AND signal_type IN ('new_revival', 'abnormal')
            ORDER BY event_ts DESC
        """, (START_TS, END_TS))
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in rows]

    return db_op(_op) or []


def fetch_kline_5m(address, event_ts):
    """Fetch 5m K-line data: 48 bars before event (4h) + 48 bars after (4h).
    Note: bottom_kline_cache.ts is in Unix SECONDS (not ms)."""
    def _op(conn):
        cur = conn.cursor()
        start_ts = event_ts - 4 * 3600  # 4h before, seconds
        end_ts = event_ts + 4 * 3600    # 4h after, seconds
        cur.execute("""
            SELECT ts, open, high, low, close, volume
            FROM bottom_kline_cache
            WHERE address = %s
              AND ts >= %s AND ts <= %s
              AND resolution = '5m'
            ORDER BY ts ASC
        """, (address, start_ts, end_ts))
        rows = cur.fetchall()
        return [
            {"ts": r[0], "open": float(r[1]), "high": float(r[2]),
             "low": float(r[3]), "close": float(r[4]), "volume": float(r[5])}
            for r in rows
        ]
    return db_op(_op) or []


def fetch_kline_1m(address, event_ts):
    """Fetch 1m K-line: 60 bars before + 120 bars after.
    Note: bottom_kline_cache_1m.ts is in Unix SECONDS (not ms)."""
    def _op(conn):
        cur = conn.cursor()
        start_ts = event_ts - 3600   # 1h before, seconds
        end_ts = event_ts + 7200     # 2h after, seconds
        cur.execute("""
            SELECT ts, open, high, low, close, volume
            FROM bottom_kline_cache_1m
            WHERE address = %s
              AND ts >= %s AND ts <= %s
            ORDER BY ts ASC
        """, (address, start_ts, end_ts))
        rows = cur.fetchall()
        return [
            {"ts": r[0], "open": float(r[1]), "high": float(r[2]),
             "low": float(r[3]), "close": float(r[4]), "volume": float(r[5])}
            for r in rows
        ]
    return db_op(_op) or []


# ---------------------------------------------------------------------------
# 5m Pre-structure classification (from 08-kline-journey-encyclopedia.md)
# ---------------------------------------------------------------------------

def classify_pre_structure_5m(klines_5m, event_ts):
    """
    Classify the 4h pre-structure into one of the 11 types.
    Uses the 48 bars (4h) before the event.
    Returns: (structure_name, q1_pct, q2_pct, q3_pct, q4_pct, position_pct, volatility)
    """
    pre_bars = [k for k in klines_5m if k["ts"] <= event_ts]

    if len(pre_bars) < 12:
        return ("insufficient_data", 0, 0, 0, 0, 50, 0)

    n = len(pre_bars)
    # Split into 4 quarters (~12 bars each, ~1h each)
    q_size = max(1, n // 4)
    q1 = pre_bars[:q_size]
    q2 = pre_bars[q_size:2*q_size]
    q3 = pre_bars[2*q_size:3*q_size]
    q4 = pre_bars[3*q_size:]

    def q_pct(q):
        if len(q) < 2:
            return 0.0
        return (q[-1]["close"] - q[0]["open"]) / q[0]["open"] * 100 if q[0]["open"] > 0 else 0.0

    q1_pct = q_pct(q1)
    q2_pct = q_pct(q2)
    q3_pct = q_pct(q3)
    q4_pct = q_pct(q4)

    # Position: where is the last price relative to the whole range?
    all_highs = [k["high"] for k in pre_bars]
    all_lows = [k["low"] for k in pre_bars]
    range_high = max(all_highs)
    range_low = min(all_lows)
    last_close = pre_bars[-1]["close"]
    if range_high > range_low:
        position_pct = (last_close - range_low) / (range_high - range_low) * 100
    else:
        position_pct = 50

    # Volatility: std of close-to-close returns
    if len(pre_bars) >= 3:
        returns = [(pre_bars[i]["close"] - pre_bars[i-1]["close"]) / pre_bars[i-1]["close"]
                   for i in range(1, len(pre_bars)) if pre_bars[i-1]["close"] > 0]
        import math
        vol = math.sqrt(sum(r*r for r in returns) / len(returns)) * 100 if returns else 0
    else:
        vol = 0

    # Classify
    total_change = (pre_bars[-1]["close"] - pre_bars[0]["open"]) / pre_bars[0]["open"] * 100 if pre_bars[0]["open"] > 0 else 0

    # "底部持续下跌" (bottom continuous decline): pos<30%, Q4 accelerating down (<-15%)
    if position_pct < 30 and q4_pct < -15:
        return ("底部持续下跌", q1_pct, q2_pct, q3_pct, q4_pct, position_pct, vol)

    # "底部横盘" (bottom consolidation): pos<35%, Q4 flat (|Q4|<5%), preceded by decline
    if position_pct < 35 and abs(q4_pct) < 5 and (q2_pct < -5 or q3_pct < -5):
        return ("底部横盘", q1_pct, q2_pct, q3_pct, q4_pct, position_pct, vol)

    # "底部反弹启动" (bottom bounce start): pos<40%, Q4 strong bounce (>15%)
    if position_pct < 40 and q4_pct > 15:
        return ("底部反弹启动", q1_pct, q2_pct, q3_pct, q4_pct, position_pct, vol)

    # "高点下跌回落中" (falling from high): total_change negative, Q4 starting to stabilize
    if total_change < -10 and q4_pct > -5:
        return ("高点下跌回落中", q1_pct, q2_pct, q3_pct, q4_pct, position_pct, vol)

    # "强势拉升后高位" (strong pump then high): Q1/Q2 strong pump, position high
    if (q1_pct > 20 or q2_pct > 20) and position_pct > 60:
        return ("强势拉升后高位", q1_pct, q2_pct, q3_pct, q4_pct, position_pct, vol)

    # "高位加速拉升" (high-position accelerating): pos>60%, Q4 accelerating up
    if position_pct > 60 and q4_pct > 15:
        return ("高位加速拉升", q1_pct, q2_pct, q3_pct, q4_pct, position_pct, vol)

    # "持续拉升中" (continuous pumping): overall up, Q3/Q4 positive
    if total_change > 10 and q3_pct > 0 and q4_pct > 0:
        return ("持续拉升中", q1_pct, q2_pct, q3_pct, q4_pct, position_pct, vol)

    # "持续下跌中" (continuous decline): overall down
    if total_change < -10 and q4_pct < -5:
        return ("持续下跌中", q1_pct, q2_pct, q3_pct, q4_pct, position_pct, vol)

    # "下跌后横盘筑底" (decline then consolidation bottom): declined then flat at bottom
    if (q1_pct < -5 or q2_pct < -5) and abs(q4_pct) < 5 and position_pct < 40:
        return ("下跌后横盘筑底", q1_pct, q2_pct, q3_pct, q4_pct, position_pct, vol)

    # "长期横盘震荡" (long consolidation): all Qs flat
    if all(abs(q) < 8 for q in [q1_pct, q2_pct, q3_pct, q4_pct]):
        return ("长期横盘震荡", q1_pct, q2_pct, q3_pct, q4_pct, position_pct, vol)

    return ("其他结构", q1_pct, q2_pct, q3_pct, q4_pct, position_pct, vol)


# ---------------------------------------------------------------------------
# Post-4h walk-off classification (from 08-kline-journey-encyclopedia.md)
# ---------------------------------------------------------------------------

def classify_post_walkoff(klines_5m, event_ts):
    """
    Classify the 4h post-event walk-off into one of the 12 types.
    Returns: (walkoff_type, peak_pct, trough_pct, final_close_pct, hourly_path)
    """
    post_bars = [k for k in klines_5m if k["ts"] > event_ts]

    if len(post_bars) < 4:
        return ("insufficient_data", 0, 0, 0, [])

    # Use event's close price (or last pre-bar close) as baseline
    pre_last = [k for k in klines_5m if k["ts"] <= event_ts]
    if not pre_last:
        return ("no_baseline", 0, 0, 0, [])
    baseline = pre_last[-1]["close"]
    if baseline <= 0:
        return ("invalid_baseline", 0, 0, 0, [])

    closes = [b["close"] for b in post_bars]
    peak = max(b["high"] for b in post_bars)
    trough = min(b["low"] for b in post_bars)
    peak_pct = (peak - baseline) / baseline * 100
    trough_pct = (trough - baseline) / baseline * 100
    final_close = closes[-1]
    final_pct = (final_close - baseline) / baseline * 100

    # Hourly path: split into 4 segments of ~12 bars each
    n = len(post_bars)
    seg_size = max(1, n // 4)
    hourly_path = []
    for i in range(4):
        seg = post_bars[i*seg_size:(i+1)*seg_size]
        if seg:
            seg_close = seg[-1]["close"]
            hourly_path.append((seg_close - baseline) / baseline * 100)

    # Classification rules from encyclopedia:
    # 暴涨: peak >= 50%
    if peak_pct >= 50:
        return ("暴涨", peak_pct, trough_pct, final_pct, hourly_path)

    # 强涨守住: peak >= 30% AND final > 15%
    if peak_pct >= 30 and final_pct > 15:
        return ("强涨守住", peak_pct, trough_pct, final_pct, hourly_path)

    # 冲高急跌: peak >= 20% AND final < -10%
    if peak_pct >= 20 and final_pct < -10:
        return ("冲高急跌", peak_pct, trough_pct, final_pct, hourly_path)

    # 暴涨回吐: peak >= 30% AND final < -5%
    if peak_pct >= 30 and final_pct < -5:
        return ("暴涨回吐", peak_pct, trough_pct, final_pct, hourly_path)

    # 持续阴跌: trough < -15% AND final < -5%
    if trough_pct < -15 and final_pct < -5:
        return ("持续阴跌", peak_pct, trough_pct, final_pct, hourly_path)

    # 深度下跌: trough < -30% AND final < -15%
    if trough_pct < -30 and final_pct < -15:
        return ("深度下跌", peak_pct, trough_pct, final_pct, hourly_path)

    # 先涨后跌: 0-1h up > 10%, then down
    if len(hourly_path) >= 2 and hourly_path[0] > 10 and hourly_path[1] < -5:
        return ("先涨后跌", peak_pct, trough_pct, final_pct, hourly_path)

    # 先跌后涨: 0-1h down < -10%, then up
    if len(hourly_path) >= 2 and hourly_path[0] < -10 and hourly_path[1] > 5:
        return ("先跌后涨", peak_pct, trough_pct, final_pct, hourly_path)

    # 稳健上涨: peak >= 20% AND final > 5%
    if peak_pct >= 20 and final_pct > 5:
        return ("稳健上涨", peak_pct, trough_pct, final_pct, hourly_path)

    # 温和上涨: 10% <= peak < 20% AND final > 0
    if 10 <= peak_pct < 20 and final_pct > 0:
        return ("温和上涨", peak_pct, trough_pct, final_pct, hourly_path)

    # 横盘震荡: |peak| < 10% AND |trough| < 10%
    if abs(peak_pct) < 10 and abs(trough_pct) < 10:
        return ("横盘震荡", peak_pct, trough_pct, final_pct, hourly_path)

    # Default: mild decline or unclear
    if final_pct < 0:
        return ("持续阴跌", peak_pct, trough_pct, final_pct, hourly_path)
    return ("温和上涨", peak_pct, trough_pct, final_pct, hourly_path)


# ---------------------------------------------------------------------------
# Dip-then-pump analysis (from 09-dip-then-pump-strategy.md)
# ---------------------------------------------------------------------------

def analyze_dip_then_pump(klines_1m, event_ts):
    """
    Analyze the 1m K-line for dip-then-pump pattern.
    Returns dict with key metrics from strategy doc.
    """
    post_1m = [k for k in klines_1m if k["ts"] > event_ts]
    pre_1m = [k for k in klines_1m if k["ts"] <= event_ts]

    result = {
        "has_1m_data": len(post_1m) >= 5,
        "first_5min_pct": None,
        "first_5min_vol_ratio": None,
        "dip_3pct_in_25min": False,
        "deepest_dip_pct": 0,
        "time_to_bottom_min": None,
        "recovery_30min_pct": None,
        "recovery_60min_pct": None,
        "dip_then_pump_verdict": "no_data",
        "death_signal_risk": "unknown",
    }

    if len(post_1m) < 5:
        return result

    baseline = post_1m[0]["open"]
    if baseline <= 0:
        return result

    # First 5 min (bars 0-4)
    bars_5 = post_1m[:5]
    close_5min = bars_5[-1]["close"]
    result["first_5min_pct"] = (close_5min - baseline) / baseline * 100

    # Volume ratio: post-5min avg vol / pre-30min avg vol
    pre_30_bars = pre_1m[-30:] if len(pre_1m) >= 30 else pre_1m
    post_5_vol = sum(b["volume"] for b in bars_5) / max(len(bars_5), 1)
    pre_30_vol = sum(b["volume"] for b in pre_30_bars) / max(len(pre_30_bars), 1)
    if pre_30_vol > 0:
        result["first_5min_vol_ratio"] = post_5_vol / pre_30_vol

    # Did it dip >3% within 25 min?
    bars_25 = post_1m[:25]
    trough_25 = min(b["low"] for b in bars_25)
    dip_25 = (trough_25 - baseline) / baseline * 100
    result["dip_3pct_in_25min"] = dip_25 < -3

    # Deepest dip overall
    all_trough = min(b["low"] for b in post_1m)
    result["deepest_dip_pct"] = (all_trough - baseline) / baseline * 100

    # Time to bottom
    for i, b in enumerate(post_1m):
        if b["low"] <= all_trough:
            result["time_to_bottom_min"] = i + 1  # 1-indexed minutes
            break

    # Recovery at 30min
    if len(post_1m) >= 30:
        result["recovery_30min_pct"] = (post_1m[29]["close"] - baseline) / baseline * 100

    # Recovery at 60min
    if len(post_1m) >= 60:
        result["recovery_60min_pct"] = (post_1m[59]["close"] - baseline) / baseline * 100

    # ---- Dip-then-pump verdict ----
    first5 = result["first_5min_pct"]
    vol_ratio = result["first_5min_vol_ratio"]
    depth = result["deepest_dip_pct"]
    recovery_30 = result["recovery_30min_pct"]

    if first5 is None:
        return result

    # Death signal check (from strategy doc Section 3)
    if first5 < -8:
        result["death_signal_risk"] = "high"
    elif vol_ratio is not None and vol_ratio > 2 and first5 < -2:
        result["death_signal_risk"] = "medium (panic volume)"
    elif vol_ratio is not None and vol_ratio < 0.5:
        result["death_signal_risk"] = "medium (shrink volume)"
    else:
        result["death_signal_risk"] = "low"

    # Entry check (from strategy doc Section 4)
    is_good_entry = (
        first5 < -2
        and vol_ratio is not None
        and 0.5 <= vol_ratio <= 2
        and depth > -30
    )

    is_great_entry = (
        first5 > -3
        and vol_ratio is not None
        and 0.5 <= vol_ratio <= 2
    )

    if recovery_30 is not None and recovery_30 > 0:
        result["dip_then_pump_verdict"] = "confirmed_pump"
    elif is_great_entry:
        result["dip_then_pump_verdict"] = "likely_big_pump"
    elif is_good_entry:
        result["dip_then_pump_verdict"] = "entry_signal"
    elif first5 < -8:
        result["dip_then_pump_verdict"] = "likely_death"
    else:
        result["dip_then_pump_verdict"] = "unclear"

    return result


# ---------------------------------------------------------------------------
# WR20 computation
# ---------------------------------------------------------------------------

def compute_wr20(klines_5m, event_ts):
    """WR20: did price reach +20% within 4h after push?"""
    post_bars = [k for k in klines_5m if k["ts"] > event_ts]
    if not post_bars:
        return False, 0

    pre_last = [k for k in klines_5m if k["ts"] <= event_ts]
    if not pre_last:
        return False, 0
    baseline = pre_last[-1]["close"]
    if baseline <= 0:
        return False, 0

    peak = max(b["high"] for b in post_bars)
    peak_pct = (peak - baseline) / baseline * 100
    return peak_pct >= 20, peak_pct


def compute_wr50(klines_5m, event_ts):
    """WR50: did price reach +50% within 4h?"""
    post_bars = [k for k in klines_5m if k["ts"] > event_ts]
    if not post_bars:
        return False, 0

    pre_last = [k for k in klines_5m if k["ts"] <= event_ts]
    if not pre_last:
        return False, 0
    baseline = pre_last[-1]["close"]
    if baseline <= 0:
        return False, 0

    peak = max(b["high"] for b in post_bars)
    peak_pct = (peak - baseline) / baseline * 100
    return peak_pct >= 50, peak_pct


# ---------------------------------------------------------------------------
# Match against encyclopedia expected patterns
# ---------------------------------------------------------------------------

def check_encyclopedia_match(signal_type, pre_structure, post_walkoff, wr20_hit, peak_pct):
    """
    Check if the actual outcome matches what the encyclopedia predicts.
    Returns (matches, expected_behavior, actual_behavior, notes)
    """
    matches = True
    notes = []

    # Encyclopedia WR20 expectations per structure (Section 2 & 3)
    wr20_expectations = {
        ("new_revival", "底部持续下跌"): 84,
        ("new_revival", "持续拉升中"): 74,
        ("new_revival", "底部横盘"): 74,
        ("new_revival", "底部反弹启动"): 67,
        ("new_revival", "高点下跌回落中"): 77,
        ("new_revival", "强势拉升后高位"): 80,
        ("new_revival", "其他结构"): 77,
        ("abnormal", "持续拉升中"): 65,
        ("abnormal", "底部持续下跌"): 57,
        ("abnormal", "底部反弹启动"): 80,
        ("abnormal", "高位加速拉升"): 83,
    }

    # Walk-off WR20 expectations (Section 4)
    walkoff_wr20 = {
        "暴涨": 100,
        "强涨守住": 100,
        "冲高急跌": 100,
        "暴涨回吐": 100,
        "稳健上涨": 80,
        "持续阴跌": 20 if signal_type == "new_revival" else 26,
    }

    # Check pre-structure WR20
    key = (signal_type, pre_structure)
    if key in wr20_expectations:
        expected_wr = wr20_expectations[key]
        if expected_wr >= 80 and not wr20_hit:
            notes.append(f"预期WR20={expected_wr}%但实际未达+20%")
            matches = False
        elif expected_wr < 60 and wr20_hit:
            notes.append(f"预期WR20={expected_wr}%但实际达到+20%(超预期)")

    # Check walk-off WR20
    if post_walkoff in walkoff_wr20:
        expected_wr = walkoff_wr20[post_walkoff]
        if expected_wr == 100 and not wr20_hit:
            notes.append(f"后4h走势'{post_walkoff}'预期WR20=100%但实际未达")
            matches = False
        if post_walkoff == "持续阴跌" and wr20_hit:
            notes.append(f"后4h持续阴跌但实际达到+20%(罕见逆转)")
            matches = False

    return matches, notes


# ---------------------------------------------------------------------------
# Main analysis
# ---------------------------------------------------------------------------

def main():
    print("=" * 80)
    print(f"  异动信号 vs 策略文档 对比分析")
    print(f"  日期: {YESTERDAY} ~ {TODAY}")
    print(f"  参考文档: 09-dip-then-pump-strategy.md + 08-kline-journey-encyclopedia.md")
    print("=" * 80)

    records = fetch_push_records()
    print(f"\n查询到 {len(records)} 条推送记录 (new_revival + abnormal)")

    if not records:
        print("\n[!] 没有找到相关推送记录，请检查数据库连接和数据范围")
        return

    # Separate by signal type
    revival = [r for r in records if r["signal_type"] == "new_revival"]
    abnormal = [r for r in records if r["signal_type"] == "abnormal"]
    print(f"  new_revival: {len(revival)} 条")
    print(f"  abnormal:    {len(abnormal)} 条")

    # Process each record
    results = []
    for i, rec in enumerate(records):
        addr = rec["address"]
        symbol = rec["symbol"]
        sig = rec["signal_type"]
        event_ts = rec["event_ts"]
        mcap = float(rec["current_mcap"] or 0)

        # Fetch K-line data
        klines_5m = fetch_kline_5m(addr, event_ts)
        klines_1m = fetch_kline_1m(addr, event_ts)

        # Pre-structure classification
        pre_structure, q1, q2, q3, q4, pos, vol = classify_pre_structure_5m(klines_5m, event_ts)

        # Post walk-off classification
        post_walkoff, peak_pct, trough_pct, final_pct, hourly = classify_post_walkoff(klines_5m, event_ts)

        # WR20 / WR50
        wr20_hit, peak_actual = compute_wr20(klines_5m, event_ts)
        wr50_hit, _ = compute_wr50(klines_5m, event_ts)

        # Dip-then-pump analysis
        dip = analyze_dip_then_pump(klines_1m, event_ts)

        # Encyclopedia match
        matches_encyc, match_notes = check_encyclopedia_match(
            sig, pre_structure, post_walkoff, wr20_hit, peak_actual
        )

        results.append({
            "symbol": symbol,
            "address": addr,
            "signal_type": sig,
            "mcap": mcap,
            "pre_structure": pre_structure,
            "q1": q1, "q2": q2, "q3": q3, "q4": q4,
            "position_pct": pos,
            "post_walkoff": post_walkoff,
            "peak_pct": peak_actual,
            "trough_pct": trough_pct,
            "final_pct": final_pct,
            "wr20": wr20_hit,
            "wr50": wr50_hit,
            "dip_analysis": dip,
            "encyc_match": matches_encyc,
            "encyc_notes": match_notes,
            "klines_5m_count": len(klines_5m),
            "klines_1m_count": len(klines_1m),
        })

        if (i + 1) % 20 == 0:
            print(f"  处理进度: {i+1}/{len(records)}")

    print(f"\n处理完成，共 {len(results)} 条有效分析")

    # -------------------------------------------------------------------
    # Report 1: Signal type summary
    # -------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("  PART 1: 信号类型 × 胜率总览")
    print("=" * 80)

    for sig_type in ["new_revival", "abnormal"]:
        sig_results = [r for r in results if r["signal_type"] == sig_type]
        if not sig_results:
            continue
        wr20_count = sum(1 for r in sig_results if r["wr20"])
        wr50_count = sum(1 for r in sig_results if r["wr50"])
        avg_peak = sum(r["peak_pct"] for r in sig_results) / len(sig_results)
        med_peak = sorted([r["peak_pct"] for r in sig_results])[len(sig_results)//2]

        print(f"\n--- {sig_type} (n={len(sig_results)}) ---")
        print(f"  WR20 (>=+20%): {wr20_count}/{len(sig_results)} = {wr20_count/len(sig_results)*100:.0f}%")
        print(f"  WR50 (>=+50%): {wr50_count}/{len(sig_results)} = {wr50_count/len(sig_results)*100:.0f}%")
        print(f"  Avg Peak:     {avg_peak:+.1f}%")
        print(f"  Med Peak:     {med_peak:+.1f}%")

        # Compare with encyclopedia baseline
        if sig_type == "new_revival":
            baseline_wr20 = 66  # rough avg from encyclopedia
        else:
            baseline_wr20 = 60
        actual_wr20 = wr20_count / len(sig_results) * 100
        diff = actual_wr20 - baseline_wr20
        print(f"  百科基线WR20: ~{baseline_wr20}% → 实际: {actual_wr20:.0f}% → {'↑优于' if diff > 0 else '↓低于'}基线 {abs(diff):.0f}pp")

    # -------------------------------------------------------------------
    # Report 2: Pre-structure distribution & win rates
    # -------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("  PART 2: 前置结构 × 胜率 (vs 百科第八章)")
    print("=" * 80)

    for sig_type in ["new_revival", "abnormal"]:
        sig_results = [r for r in results if r["signal_type"] == sig_type]
        if not sig_results:
            continue

        print(f"\n--- {sig_type} 前置结构分布 ---")
        structures = defaultdict(list)
        for r in sig_results:
            structures[r["pre_structure"]].append(r)

        # Sort by frequency
        for struct, items in sorted(structures.items(), key=lambda x: -len(x[1])):
            n = len(items)
            wr20 = sum(1 for r in items if r["wr20"]) / n * 100
            wr50 = sum(1 for r in items if r["wr50"]) / n * 100
            avg_peak = sum(r["peak_pct"] for r in items) / n
            med_peak = sorted([r["peak_pct"] for r in items])[n//2]
            print(f"  {struct:16s}  n={n:3d}  WR20={wr20:.0f}%  WR50={wr50:.0f}%  MedPeak={med_peak:+.0f}%  AvgPeak={avg_peak:+.0f}%")

    # -------------------------------------------------------------------
    # Report 3: Post-4h walk-off distribution & win rates
    # -------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("  PART 3: 后4h走势 × 胜率 (vs 百科第四章)")
    print("=" * 80)

    walkoffs = defaultdict(list)
    for r in results:
        key = (r["signal_type"], r["post_walkoff"])
        walkoffs[key].append(r)

    for (sig, wo), items in sorted(walkoffs.items(), key=lambda x: -len(x[1])):
        n = len(items)
        wr20 = sum(1 for r in items if r["wr20"]) / n * 100
        med_peak = sorted([r["peak_pct"] for r in items])[n//2]
        print(f"  {sig:14s} + {wo:14s}  n={n:3d}  WR20={wr20:.0f}%  MedPeak={med_peak:+.0f}%")

    # -------------------------------------------------------------------
    # Report 4: Dip-then-pump analysis (vs 09 strategy document)
    # -------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("  PART 4: 先跌后涨分析 (vs 09策略文档)")
    print("=" * 80)

    dip_results = [r for r in results if r["dip_analysis"]["has_1m_data"]]
    print(f"  有1m K线数据: {len(dip_results)}/{len(results)}")

    # Dip within 25min
    dipped = [r for r in dip_results if r["dip_analysis"]["dip_3pct_in_25min"]]
    print(f"  25min内跌>3%: {len(dipped)}/{len(dip_results)} = {len(dipped)/max(len(dip_results),1)*100:.0f}%")
    print(f"    百科基线: 94% (144/154)")

    # Outcomes after dip
    if dipped:
        dip_wr20 = sum(1 for r in dipped if r["wr20"])
        dip_wr50 = sum(1 for r in dipped if r["wr50"])
        dip_double = sum(1 for r in dipped if r["peak_pct"] >= 100)
        dip_dead = sum(1 for r in dipped if r["peak_pct"] < 20 and not r["wr20"])

        print(f"\n  跌后结果:")
        print(f"    涨超+100%:  {dip_double}/{len(dipped)} = {dip_double/len(dipped)*100:.0f}% (基线32%)")
        print(f"    涨超+50%:   {dip_wr50}/{len(dipped)} = {dip_wr50/len(dipped)*100:.0f}%")
        print(f"    涨超+30%:   {sum(1 for r in dipped if r['peak_pct']>=30)}/{len(dipped)} = {sum(1 for r in dipped if r['peak_pct']>=30)/len(dipped)*100:.0f}% (基线62%)")
        print(f"    涨超+20%:   {dip_wr20}/{len(dipped)} = {dip_wr20/len(dipped)*100:.0f}% (基线76%)")
        print(f"    不反弹(<20%): {dip_dead}/{len(dipped)} = {dip_dead/len(dipped)*100:.0f}% (基线24%)")

    # Death signal check
    death_high = [r for r in dip_results if r["dip_analysis"]["death_signal_risk"] == "high"]
    print(f"\n  死亡信号风险(5min跌>8%): {len(death_high)}个")
    if death_high:
        death_wr20 = sum(1 for r in death_high if r["wr20"])
        print(f"    其中实际达到WR20: {death_wr20}/{len(death_high)} (基线预期: 大概率死亡)")

    # Good entry signals
    good_entries = [r for r in dip_results if r["dip_analysis"]["dip_then_pump_verdict"] in ("entry_signal", "likely_big_pump", "confirmed_pump")]
    print(f"\n  符合入场规则: {len(good_entries)}个")
    if good_entries:
        entry_wr20 = sum(1 for r in good_entries if r["wr20"])
        print(f"    其中实际WR20胜率: {entry_wr20}/{len(good_entries)} = {entry_wr20/len(good_entries)*100:.0f}% (基线84%)")

    # -------------------------------------------------------------------
    # Report 5: Encyclopedia pattern match rate
    # -------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("  PART 5: 百科模式匹配度")
    print("=" * 80)

    matched = [r for r in results if r["encyc_match"]]
    mismatched = [r for r in results if not r["encyc_match"] and r["encyc_notes"]]
    print(f"  完全匹配百科预测: {len(matched)}/{len(results)} = {len(matched)/max(len(results),1)*100:.0f}%")
    print(f"  与百科预测不符:   {len(mismatched)}/{len(results)}")

    if mismatched:
        print(f"\n  不符案例:")
        for r in mismatched[:15]:
            print(f"    {r['symbol']:12s} [{r['signal_type']:12s}] {r['pre_structure']} → {r['post_walkoff']}  peak={r['peak_pct']:+.0f}%")
            for note in r["encyc_notes"]:
                print(f"      ⚠ {note}")

    # -------------------------------------------------------------------
    # Report 6: Best opportunities today
    # -------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("  PART 6: 最优交易机会 (百科五星结构)")
    print("=" * 80)

    five_star_structures = {
        "new_revival": ["底部持续下跌", "高点下跌回落中"],
        "abnormal": ["高位加速拉升"],
    }

    for sig_type, good_structs in five_star_structures.items():
        candidates = [
            r for r in results
            if r["signal_type"] == sig_type and r["pre_structure"] in good_structs
        ]
        if candidates:
            print(f"\n  {sig_type} 五星结构 ({good_structs}):")
            for r in candidates:
                wr20_mark = "✓" if r["wr20"] else "✗"
                print(f"    {r['symbol']:12s}  {r['pre_structure']:16s} → {r['post_walkoff']:10s}  "
                      f"Peak={r['peak_pct']:+.0f}%  WR20={wr20_mark}  "
                      f"Q4={r['q4']:+.0f}%  pos={r['position_pct']:.0f}%")

    # Also show walk-off-based opportunities
    print(f"\n  后4h=暴涨/冲高急跌/强涨守住 (百科WR20=100%):")
    for r in results:
        if r["post_walkoff"] in ("暴涨", "冲高急跌", "强涨守住", "暴涨回吐"):
            wr20_mark = "✓WR20" if r["wr20"] else "✗"
            print(f"    {r['symbol']:12s} [{r['signal_type']:12s}] {r['pre_structure']:16s} → {r['post_walkoff']:10s}  "
                  f"Peak={r['peak_pct']:+.0f}%  {wr20_mark}")


    # -------------------------------------------------------------------
    # Report 7: Detailed signal-by-signal table
    # -------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("  PART 7: 逐信号明细")
    print("=" * 80)

    print(f"\n{'Symbol':<14s} {'Type':<14s} {'Pre-Structure':<18s} {'Q4':>6s} {'Pos':>5s} {'Post-4h':<14s} {'Peak':>7s} {'Trough':>7s} {'Final':>7s} {'WR20':>5s} {'WR50':>5s} {'DipVerdict':<18s} {'EncMatch':>8s}")
    print("-" * 145)

    for r in sorted(results, key=lambda x: -x["peak_pct"]):
        dip_v = r["dip_analysis"]["dip_then_pump_verdict"]
        enc = "✓" if r["encyc_match"] else "✗"
        wr20 = "✓" if r["wr20"] else "✗"
        wr50 = "✓" if r["wr50"] else "✗"
        print(f"{r['symbol']:<14s} {r['signal_type']:<14s} {r['pre_structure']:<18s} {r['q4']:>+5.0f}% {r['position_pct']:>4.0f}% {r['post_walkoff']:<14s} {r['peak_pct']:>+6.1f}% {r['trough_pct']:>+6.1f}% {r['final_pct']:>+6.1f}% {wr20:>5s} {wr50:>5s} {dip_v:<18s} {enc:>8s}")

    print("\n" + "=" * 80)
    print("  分析完成")
    print("=" * 80)


if __name__ == "__main__":
    main()
