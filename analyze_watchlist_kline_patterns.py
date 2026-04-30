"""
Analyze bottom_watchlist_tokens 1h K-line patterns to identify common
"golden dog" (金狗) pump lifecycle signatures.

Usage:
    D:/software/anaconda/envs/py312/python.exe analyze_watchlist_kline_patterns.py
    D:/software/anaconda/envs/py312/python.exe analyze_watchlist_kline_patterns.py --lookback-days 14
    D:/software/anaconda/envs/py312/python.exe analyze_watchlist_kline_patterns.py --ca <address>
"""
import json
import os
import re
import shutil
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from db_client import db_op

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CHAIN = "sol"
KLINE_RESOLUTION = "1h"
LOOKBACK_DAYS = int(os.getenv("KLINE_PATTERN_LOOKBACK_DAYS", "14"))
LOOKBACK_SEC = LOOKBACK_DAYS * 24 * 3600
RATE_LIMIT_DELAY = float(os.getenv("KLINE_PATTERN_RATE_DELAY", "2.5"))
OUTPUT_DIR = Path(os.getenv("KLINE_PATTERN_OUTPUT_DIR", str(ROOT_DIR / "kline_analysis_output")))

# Pump phase thresholds
ACCUMULATION_MAX_PRICE_RATIO = 1.5    # price <= 1.5x start = accumulation zone
BREAKOUT_MIN_GAIN_PCT = 30            # single-hour gain >= 30% = breakout signal
CLIMAX_VOLUME_MULTIPLIER = 3.0        # volume >= 3x avg = climax volume
DISTRIBUTION_MIN_DRAWDOWN_PCT = 25    # drop from peak >= 25% = distribution


def to_float(value, default=0.0):
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def to_int(value, default=0):
    try:
        if value in (None, ""):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def now_ts():
    return int(time.time())


def gmgn_prefix():
    exe = shutil.which("gmgn-cli") or shutil.which("gmgn-cli.cmd") or shutil.which("gmgn-cli.ps1")
    if not exe:
        return ["gmgn-cli"]
    if exe.lower().endswith(".ps1"):
        return ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", exe]
    return [exe]


def run_gmgn(args, timeout=90):
    cmd = [*gmgn_prefix(), *args, "--raw"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", timeout=timeout)
    except Exception as exc:
        print(f"  gmgn exception: {exc}")
        return None
    if result.returncode != 0:
        err = (result.stderr or result.stdout or "").strip()
        if "429" in err:
            print(f"  [RATE LIMITED] waiting 30s...")
            time.sleep(30)
            return None
        print(f"  gmgn failed rc={result.returncode}: {err[:200]}")
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return None


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------

def fetch_watchlist_tokens():
    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            "SELECT ca, source, peak_mcap, last_mcap, create_at, added_at, note "
            "FROM bottom_watchlist_tokens ORDER BY added_at DESC"
        )
        return [
            {
                "ca": row[0],
                "source": row[1],
                "peak_mcap": to_float(row[2]),
                "last_mcap": to_float(row[3]),
                "create_at": row[4],
                "added_at": row[5],
                "note": row[6] or "",
            }
            for row in cur.fetchall()
        ]
    return db_op(_op)


def fetch_token_info(address):
    data = run_gmgn(["token", "info", "--chain", CHAIN, "--address", address], timeout=60)
    if isinstance(data, dict):
        return {
            "symbol": data.get("symbol", "?"),
            "name": data.get("name", ""),
            "price": to_float(data.get("price")),
            "mcap": to_float(data.get("price")) * to_float(data.get("circulating_supply")),
            "liquidity": to_float(data.get("liquidity")),
            "holder_count": to_int(data.get("holder_count")),
            "created_ts": to_int(data.get("creation_timestamp") or data.get("open_timestamp")),
        }
    return {}


def fetch_1h_kline(address, lookback_sec=None):
    lookback = lookback_sec or LOOKBACK_SEC
    end = now_ts()
    start = end - lookback
    data = run_gmgn(
        [
            "market", "kline", "--chain", CHAIN, "--address", address,
            "--resolution", KLINE_RESOLUTION,
            "--from", str(start), "--to", str(end),
        ],
        timeout=90,
    )
    if not data:
        return []
    rows = data.get("list") or data.get("data", {}).get("list") or []
    candles = []
    for row in (rows if isinstance(rows, list) else []):
        if not isinstance(row, dict):
            continue
        raw_ts = to_int(row.get("time") or row.get("timestamp") or row.get("t"))
        ts = raw_ts // 1000 if raw_ts > 10_000_000_000 else raw_ts
        close = to_float(row.get("close") or row.get("c"))
        if ts <= 0 or close <= 0:
            continue
        candles.append({
            "ts": ts,
            "open": to_float(row.get("open") or row.get("o"), close),
            "high": to_float(row.get("high") or row.get("h"), close),
            "low": to_float(row.get("low") or row.get("l"), close),
            "close": close,
            "volume": to_float(row.get("volume") or row.get("v")),
        })
    candles.sort(key=lambda c: c["ts"])
    return candles


# ---------------------------------------------------------------------------
# K-line pattern analysis
# ---------------------------------------------------------------------------

def analyze_kline_pattern(candles, token_info):
    """Extract pattern fingerprints from 1h K-line data."""
    if len(candles) < 6:
        return {"error": f"only {len(candles)} candles", "quality": "insufficient"}

    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    volumes = [c["volume"] for c in candles]
    opens = [c["open"] for c in candles]

    first_price = closes[0]
    peak_price = max(highs)
    peak_idx = highs.index(peak_price)
    current_price = closes[-1]
    lowest_price = min(lows)
    lowest_idx = lows.index(lowest_price)

    total_gain = (peak_price - first_price) / first_price * 100 if first_price > 0 else 0
    total_range = (peak_price - lowest_price) / lowest_price * 100 if lowest_price > 0 else 0
    drawdown_from_peak = (peak_price - current_price) / peak_price * 100 if peak_price > 0 else 0
    drawdown_from_peak_max = (peak_price - min(lows[peak_idx:])) / peak_price * 100 if peak_price > 0 else 0

    avg_volume = sum(volumes) / len(volumes) if volumes else 0
    max_volume = max(volumes) if volumes else 0
    max_vol_idx = volumes.index(max_volume) if max_volume > 0 else 0
    vol_at_peak = volumes[peak_idx] if peak_idx < len(volumes) else 0
    volume_ratio = max_volume / avg_volume if avg_volume > 0 else 0

    # Per-bar changes
    hour_changes = []
    for i in range(1, len(closes)):
        pct = (closes[i] - closes[i - 1]) / closes[i - 1] * 100 if closes[i - 1] > 0 else 0
        hour_changes.append(pct)

    green_bars = sum(1 for c in candles if c["close"] > c["open"])
    red_bars = sum(1 for c in candles if c["close"] < c["open"])
    green_ratio = green_bars / len(candles) if candles else 0

    max_hour_gain = max(hour_changes) if hour_changes else 0
    max_hour_loss = min(hour_changes) if hour_changes else 0

    # Consecutive green streaks
    streaks = []
    current_streak = 0
    for chg in hour_changes:
        if chg > 0:
            current_streak += 1
        else:
            if current_streak > 0:
                streaks.append(current_streak)
            current_streak = 0
    if current_streak > 0:
        streaks.append(current_streak)
    max_green_streak = max(streaks) if streaks else 0

    # Volume trend: volume in first 25% vs last 25%
    qtr = max(1, len(volumes) // 4)
    early_vol = sum(volumes[:qtr]) / qtr if qtr > 0 else 0
    late_vol = sum(volumes[-qtr:]) / qtr if qtr > 0 else 0
    vol_trend = (late_vol - early_vol) / early_vol if early_vol > 0 else 0

    # Time to peak (hours)
    hours_to_peak = peak_idx
    hours_since_peak = len(candles) - peak_idx - 1

    # ---- Phase classification ----
    phases = []

    # Accumulation: early low-volatility period before main move
    pre_peak = candles[:peak_idx + 1] if peak_idx > 0 else candles
    pre_vol_avg = sum(c["volume"] for c in pre_peak) / len(pre_peak) if pre_peak else 0
    if peak_idx > 4:
        early_prices = closes[:peak_idx]
        if max(early_prices) / min(early_prices) <= ACCUMULATION_MAX_PRICE_RATIO if min(early_prices) > 0 else False:
            phases.append("accumulation")

    # Breakout: sharp price move with volume spike
    if max_hour_gain >= BREAKOUT_MIN_GAIN_PCT:
        breakout_bars = sum(1 for chg in hour_changes if chg >= BREAKOUT_MIN_GAIN_PCT)
        phases.append(f"breakout_{breakout_bars}bars")

    # Climax: volume spike near peak
    if max_vol_idx >= peak_idx - 2 and max_vol_idx <= peak_idx + 2 and volume_ratio >= CLIMAX_VOLUME_MULTIPLIER:
        phases.append("volume_climax")

    # Distribution: decline from peak
    if drawdown_from_peak_max >= DISTRIBUTION_MIN_DRAWDOWN_PCT:
        phases.append("distribution")

    # V-shape recovery
    if peak_idx > 0 and hours_since_peak > 4 and drawdown_from_peak <= 20:
        phases.append("v_recovery")

    # Dead cat bounce
    if drawdown_from_peak_max > 50 and drawdown_from_peak < drawdown_from_peak_max * 0.5:
        phases.append("dead_cat_bounce")

    # ---- Pattern fingerprint ----
    pattern_type = classify_pattern(
        total_gain, drawdown_from_peak_max, hours_to_peak, max_hour_gain, volume_ratio, phases
    )

    return {
        "candle_count": len(candles),
        "first_price": first_price,
        "peak_price": peak_price,
        "peak_idx": peak_idx,
        "current_price": current_price,
        "lowest_price": lowest_price,
        "lowest_idx": lowest_idx,
        "total_gain_pct": round(total_gain, 1),
        "total_range_pct": round(total_range, 1),
        "drawdown_from_peak_pct": round(drawdown_from_peak, 1),
        "drawdown_from_peak_max_pct": round(drawdown_from_peak_max, 1),
        "max_hour_gain_pct": round(max_hour_gain, 1),
        "max_hour_loss_pct": round(max_hour_loss, 1),
        "hours_to_peak": hours_to_peak,
        "hours_since_peak": hours_since_peak,
        "green_ratio": round(green_ratio, 2),
        "max_green_streak": max_green_streak,
        "volume_ratio": round(volume_ratio, 1),
        "vol_trend": round(vol_trend, 2),
        "phases": phases,
        "pattern_type": pattern_type,
        "quality": "good" if len(candles) >= 12 else "limited",
        "peak_mcap": token_info.get("peak_mcap", 0),
        "last_mcap": token_info.get("last_mcap", 0),
    }


def classify_pattern(total_gain, max_drawdown, hours_to_peak, max_hour_gain, volume_ratio, phases):
    """Classify the overall pump pattern."""
    is_accumulation = "accumulation" in phases
    is_breakout = any(p.startswith("breakout") for p in phases)
    is_climax = "volume_climax" in phases
    is_distribution = "distribution" in phases
    is_recovery = "v_recovery" in phases or "dead_cat_bounce" in phases

    if total_gain > 500 and is_breakout and is_climax:
        if is_distribution and not is_recovery:
            return "classic_pump_dump"
        elif is_recovery:
            return "pump_dump_recovery"
        return "mega_pump"

    if total_gain > 200 and is_accumulation and is_breakout:
        if max_drawdown < 30:
            return "steady_climb"
        return "volatile_climb"

    if total_gain > 100 and hours_to_peak <= 6:
        return "flash_pump"

    if total_gain > 50 and is_accumulation and max_drawdown < 25:
        return "slow_grind_up"

    if max_drawdown > 70:
        return "rug_pull"

    if total_gain < 30 and abs(total_gain) < 30:
        if volume_ratio > 5:
            return "chop_with_volume"
        return "sideways_chop"

    return "complex"


# ---------------------------------------------------------------------------
# Cross-token pattern analysis
# ---------------------------------------------------------------------------

def analyze_pattern_clusters(results):
    """Group tokens by pattern type and extract common characteristics."""
    by_pattern = defaultdict(list)
    for r in results:
        ptype = r.get("pattern", {}).get("pattern_type", "unknown")
        by_pattern[ptype].append(r)

    print(f"\n{'=' * 80}")
    print("  PATTERN CLUSTERS")
    print(f"{'=' * 80}")

    summaries = {}
    for ptype, tokens in sorted(by_pattern.items(), key=lambda x: -len(x[1])):
        gains = [t["pattern"]["total_gain_pct"] for t in tokens]
        drawdowns = [t["pattern"]["drawdown_from_peak_max_pct"] for t in tokens]
        hours = [t["pattern"]["hours_to_peak"] for t in tokens]
        vol_ratios = [t["pattern"]["volume_ratio"] for t in tokens]
        hour_gains = [t["pattern"]["max_hour_gain_pct"] for t in tokens]

        def avg(lst):
            return sum(lst) / len(lst) if lst else 0

        summary = {
            "count": len(tokens),
            "avg_gain": avg(gains),
            "avg_drawdown": avg(drawdowns),
            "avg_hours_to_peak": avg(hours),
            "avg_volume_ratio": avg(vol_ratios),
            "avg_max_hour_gain": avg(hour_gains),
            "tokens": [(t["token"]["symbol"], t["token"]["ca"][:12]) for t in tokens],
        }
        summaries[ptype] = summary

        print(f"\n  [{ptype}] x{len(tokens)} tokens")
        print(f"    Avg gain: {summary['avg_gain']:+.0f}%")
        print(f"    Avg drawdown from peak: {summary['avg_drawdown']:.0f}%")
        print(f"    Avg hours to peak: {summary['avg_hours_to_peak']:.1f}h")
        print(f"    Avg volume ratio: {summary['avg_volume_ratio']:.1f}x")
        print(f"    Avg max hour gain: {summary['avg_max_hour_gain']:+.0f}%")
        print(f"    Examples: {', '.join(f'${s}({c})' for s, c in summary['tokens'][:5])}")

    return summaries


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def format_ts(ts):
    if ts <= 0:
        return "N/A"
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def print_token_report(result):
    t = result["token"]
    p = result["pattern"]
    info = result.get("info", {})

    print(f"\n{'─' * 70}")
    print(f"  ${t['symbol']} ({t['ca'][:16]}...)")
    print(f"  来源: {t['source']} | 创建: {format_ts(info.get('created_ts', 0))}")
    print(f"  Peak MCap: ${t['peak_mcap']:,.0f} | Last MCap: ${t['last_mcap']:,.0f}")
    if p.get("error"):
        print(f"  [SKIP] {p['error']} (candles={p.get('candle_count', 0)})")
        return

    print(f"  蜡烛数: {p['candle_count']} | 质量: {p['quality']}")
    print(f"  首价: {p['first_price']:.12f} → 峰值: {p['peak_price']:.12f} → 现价: {p['current_price']:.12f}")
    print(f"  总涨幅: {p['total_gain_pct']:+.1f}% | 波动范围: {p['total_range_pct']:.0f}%")
    print(f"  峰值回撤(最大): {p['drawdown_from_peak_max_pct']:.1f}% | 峰值回撤(当前): {p['drawdown_from_peak_pct']:.1f}%")
    print(f"  最大单时涨幅: {p['max_hour_gain_pct']:+.1f}% | 最大单时跌幅: {p['max_hour_loss_pct']:+.1f}%")
    print(f"  到峰值: {p['hours_to_peak']}h | 峰值后: {p['hours_since_peak']}h")
    print(f"  阳线比: {p['green_ratio']:.0%} | 最长连阳: {p['max_green_streak']}根")
    print(f"  量比: {p['volume_ratio']:.1f}x | 量趋势: {p['vol_trend']:+.2f}")
    print(f"  阶段: {', '.join(p['phases']) if p['phases'] else '无明显阶段'}")
    print(f"  走势分类: >>> {p['pattern_type']} <<<")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Analyze 1h K-line patterns for watchlist tokens")
    parser.add_argument("--ca", type=str, default="", help="Analyze a single CA address")
    parser.add_argument("--lookback-days", type=int, default=LOOKBACK_DAYS, help="K-line lookback in days")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of tokens to analyze")
    parser.add_argument("--output-json", type=str, default="", help="Save raw results to JSON file")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if args.ca:
        tokens = [{"ca": args.ca, "source": "manual", "peak_mcap": 0, "last_mcap": 0, "note": ""}]
    else:
        tokens = fetch_watchlist_tokens()
        print(f"Loaded {len(tokens)} tokens from bottom_watchlist_tokens")

    if args.limit > 0:
        tokens = tokens[:args.limit]

    results = []
    success = 0
    skipped = 0

    for i, token in enumerate(tokens):
        ca = token["ca"]
        print(f"\n[{i + 1}/{len(tokens)}] Fetching {ca[:16]}...")

        # Token info
        info = fetch_token_info(ca)
        token["symbol"] = info.get("symbol", "?")
        token["name"] = info.get("name", "")
        token["holder_count"] = info.get("holder_count", 0)

        # K-line
        candles = fetch_1h_kline(ca, args.lookback_days * 24 * 3600)
        pattern = analyze_kline_pattern(candles, token)

        result = {"token": token, "info": info, "pattern": pattern, "candles": candles}
        results.append(result)

        if pattern.get("error"):
            skipped += 1
        else:
            success += 1

        print_token_report(result)

        if i < len(tokens) - 1:
            time.sleep(RATE_LIMIT_DELAY)

    # Cross-token analysis
    print(f"\n{'=' * 80}")
    print(f"  SUMMARY: {success} analyzed, {skipped} skipped ({len(tokens)} total)")
    print(f"{'=' * 80}")

    valid = [r for r in results if not r["pattern"].get("error")]
    if valid:
        analyze_pattern_clusters(valid)

        # Print pattern type reference
        print(f"\n{'=' * 80}")
        print("  PATTERN TYPE REFERENCE (for future matching)")
        print(f"{'=' * 80}")
        print("""
  classic_pump_dump    : 积累 → 爆拉(>500%) → 放量顶点 → 派发出货 → 无反弹
  mega_pump            : 爆拉(>500%) + 放量顶点，其后走势待定
  pump_dump_recovery   : 爆拉 → 派发 → 但后来反弹
  steady_climb         : 积累 → 突破(>200%) → 回撤小(<30%)
  volatile_climb       : 积累 → 突破(>200%) → 但波动大
  flash_pump           : 6h内急速拉升(>100%)
  slow_grind_up        : 缓慢积累上行(>50%)，回撤小
  rug_pull             : 跌幅>70%，典型归零
  sideways_chop        : 横盘震荡，无明显方向
  chop_with_volume     : 横盘但有异常放量
  complex              : 混合走势，无法单一归类
""")

        # Output JSON
        output_path = args.output_json or str(OUTPUT_DIR / "kline_patterns.json")
        serializable = []
        for r in results:
            serializable.append({
                "ca": r["token"]["ca"],
                "symbol": r["token"].get("symbol", "?"),
                "source": r["token"].get("source", ""),
                "peak_mcap": r["token"].get("peak_mcap", 0),
                "last_mcap": r["token"].get("last_mcap", 0),
                "pattern": {k: v for k, v in r["pattern"].items() if k not in ("candles_raw",)},
                "candle_count": len(r.get("candles", [])),
            })
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(serializable, f, ensure_ascii=False, indent=2, default=str)
        print(f"\nResults saved to {output_path}")


if __name__ == "__main__":
    main()
