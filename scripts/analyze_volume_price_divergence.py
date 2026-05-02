"""
Analyze volume-price relationship patterns on golden dog K-line data.
Focus on:
  1. 缩量下跌 → selling exhaustion / bottom signal
  2. 放量滞跌 → volume up but price drop smaller = absorption (bullish divergence)
  3. 等量等幅 → steady accumulation/distribution zone

Usage:
    D:/software/anaconda/envs/py312/python.exe analyze_volume_price_divergence.py
"""
import json
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT_DIR))

# N-type golden dog tokens
N_TOKENS = [
    ("BBC",    "hpYnu1Ld3FaVsCdPYvE7GvC2VSQ39V4VhwEG5xRnM3pF"),
    ("STJUDE", "E8syR4zsgQG2zo9Yw3bFeoNFLxkkBxkLPKSoMhVTpump"),
    ("SCAM",   "6AVAUKa9uxQpruHZyjYFcSjjpXgcnPeQtg3rX4BNLjdD"),
    ("ewon",   "12eM87tTACWpgnwUjBqB89sA3VKqDrvssgxHRxVLpump"),
    ("FOFAR",  "Ha5Z2DfRv6Ar2nAeMj2xqjTQSfBGaWBN2f5x6ZqXpump"),
    ("HENRY",  "CJUrENDAuSm4Fxxz33sf2NyRLYqcbP5GQLxk8AwEpump"),
    # Add a known pump-and-dump for comparison
    ("Goblin", "3KHMZhpthXuiCcgfTv7vVu9PpEz64KAEURFwi6Lopump"),
]

RESOLUTION = "4h"
LOOKBACK_DAYS = 90
RATE_DELAY = 2.0


def to_f(v, d=0.0):
    try:
        if v in (None, ""): return d
        return float(v)
    except: return d


def now_ts():
    return int(time.time())


def run_gmgn(args, timeout=90):
    exe = shutil.which("gmgn-cli") or "gmgn-cli"
    cmd = [exe, *args, "--raw"]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if r.returncode != 0: return None
    try: return json.loads(r.stdout)
    except: return None


def fetch_kline(address, resolution="4h"):
    end = now_ts()
    start = end - LOOKBACK_DAYS * 24 * 3600
    data = run_gmgn([
        "market", "kline", "--chain", "sol", "--address", address,
        "--resolution", resolution,
        "--from", str(start), "--to", str(end),
    ])
    if not data: return []
    rows = data.get("list") or data.get("data", {}).get("list") or []
    candles = []
    for row in (rows if isinstance(rows, list) else []):
        if not isinstance(row, dict): continue
        raw_ts = int(float(str(row.get("time") or row.get("timestamp") or row.get("t") or 0)))
        ts = raw_ts // 1000 if raw_ts > 10_000_000_000 else raw_ts
        close = to_f(row.get("close") or row.get("c"))
        if ts <= 0 or close <= 0: continue
        candles.append({
            "ts": ts,
            "open": to_f(row.get("open") or row.get("o"), close),
            "high": to_f(row.get("high") or row.get("h"), close),
            "low": to_f(row.get("low") or row.get("l"), close),
            "close": close,
            "volume": to_f(row.get("volume") or row.get("v")),
        })
    candles.sort(key=lambda c: c["ts"])
    return candles


def filter_outliers(candles):
    """Remove single-bar volume spikes (5x avg) that skew analysis."""
    if len(candles) < 10: return candles
    vols = [c["volume"] for c in candles]
    avg_v = sum(vols) / len(vols)
    return [c for c in candles if c["volume"] <= avg_v * 5]


def analyze_bar(prev, cur):
    """Analyze a single bar transition."""
    price_chg = (cur["close"] - prev["close"]) / prev["close"] * 100 if prev["close"] > 0 else 0
    vol_chg = (cur["volume"] - prev["volume"]) / prev["volume"] * 100 if prev["volume"] > 0 else 0
    return price_chg, vol_chg


def detect_patterns(candles):
    """
    Scan candles for volume-price relationship patterns.
    Returns list of detected pattern events with bar indices.
    """
    if len(candles) < 3:
        return {"events": [], "stats": {}}

    events = []
    closes = [c["close"] for c in candles]
    volumes = [c["volume"] for c in candles]
    avg_vol = sum(volumes) / len(volumes) if volumes else 1

    for i in range(1, len(candles)):
        prev = candles[i - 1]
        cur = candles[i]
        price_chg = (cur["close"] - prev["close"]) / prev["close"] * 100
        vol_chg = (cur["volume"] - prev["volume"]) / prev["volume"] * 100 if prev["volume"] > 0 else 0
        vol_ratio = cur["volume"] / avg_vol if avg_vol > 0 else 1

        event = None

        # Pattern 1: 缩量下跌 — vol down >30% AND price down
        if vol_chg <= -30 and price_chg < 0:
            event = {
                "type": "缩量下跌",
                "bar": i,
                "price_chg": round(price_chg, 2),
                "vol_chg": round(vol_chg, 1),
                "vol_ratio": round(vol_ratio, 2),
                "close": cur["close"],
                "significance": "high" if abs(price_chg) < 5 else "medium",
            }

        # Pattern 2: 放量滞跌 — vol expansion (>50%) but price drop <5% (absorption)
        elif vol_chg >= 50 and -5 <= price_chg < 0:
            event = {
                "type": "放量滞跌(吸筹信号)",
                "bar": i,
                "price_chg": round(price_chg, 2),
                "vol_chg": round(vol_chg, 1),
                "vol_ratio": round(vol_ratio, 2),
                "close": cur["close"],
                "significance": "high",
            }

        # Pattern 3: 放量滞涨 — vol expansion but price up <5% (distribution)
        elif vol_chg >= 50 and 0 < price_chg <= 5:
            event = {
                "type": "放量滞涨(派发信号)",
                "bar": i,
                "price_chg": round(price_chg, 2),
                "vol_chg": round(vol_chg, 1),
                "vol_ratio": round(vol_ratio, 2),
                "close": cur["close"],
                "significance": "medium",
            }

        # Pattern 4: 等量等幅 — consecutive bars similar vol AND similar price change
        if i >= 2:
            prev2 = candles[i - 2]
            prev_price_chg = (prev["close"] - prev2["close"]) / prev2["close"] * 100
            vol_similar = abs(vol_chg) <= 25  # vol within 25% of prev
            price_similar = abs(price_chg - prev_price_chg) <= 3  # price chg within 3% of prev
            both_down = price_chg < 0 and prev_price_chg < 0
            both_up = price_chg > 0 and prev_price_chg > 0

            if vol_similar and price_similar:
                direction = "同步下跌" if both_down else ("同步上涨" if both_up else "同步横盘")
                event = {
                    "type": f"等量等幅({direction})",
                    "bar": i,
                    "price_chg": round(price_chg, 2),
                    "prev_price_chg": round(prev_price_chg, 2),
                    "vol_chg": round(vol_chg, 1),
                    "close": cur["close"],
                    "significance": "medium" if both_down else "low",
                }

        # Pattern 5: Volume climax — bar volume > 4x avg
        if vol_ratio >= 4.0:
            direction = "放量拉升" if price_chg > 5 else ("放量砸盘" if price_chg < -5 else "放量换手")
            event = {
                "type": f"量能顶点({direction})",
                "bar": i,
                "price_chg": round(price_chg, 2),
                "vol_ratio": round(vol_ratio, 2),
                "close": cur["close"],
                "significance": "high",
            }

        if event:
            events.append(event)

    # Global stats
    prices_up = sum(1 for i in range(1, len(candles)) if candles[i]["close"] > candles[i-1]["close"])
    prices_down = sum(1 for i in range(1, len(candles)) if candles[i]["close"] < candles[i-1]["close"])

    up_vols = []
    down_vols = []
    for i in range(1, len(candles)):
        if candles[i]["close"] > candles[i-1]["close"]:
            up_vols.append(candles[i]["volume"])
        elif candles[i]["close"] < candles[i-1]["close"]:
            down_vols.append(candles[i]["volume"])

    avg_up_vol = sum(up_vols) / len(up_vols) if up_vols else 0
    avg_down_vol = sum(down_vols) / len(down_vols) if down_vols else 0
    vol_asymmetry = (avg_up_vol - avg_down_vol) / avg_down_vol if avg_down_vol > 0 else 0

    # Divergence score: many 放量滞跌 events = bullish accumulation
    absorption_events = [e for e in events if "吸筹" in e["type"]]
    exhaustion_events = [e for e in events if e["type"] == "缩量下跌" and e["significance"] == "high"]
    climax_events = [e for e in events if "量能顶点" in e["type"]]
    equal_events = [e for e in events if "等量等幅" in e["type"]]

    absorption_score = len(absorption_events) * 3 + len(exhaustion_events) * 2
    climax_count = len(climax_events)

    return {
        "events": events,
        "stats": {
            "total_bars": len(candles),
            "up_bars": prices_up,
            "down_bars": prices_down,
            "avg_up_vol": round(avg_up_vol, 2),
            "avg_down_vol": round(avg_down_vol, 2),
            "vol_asymmetry": round(vol_asymmetry, 2),
            "vol_asymmetry_desc": (
                "上涨放量>下跌 (多头主导)" if vol_asymmetry > 0.3
                else "下跌放量>上涨 (空头主导)" if vol_asymmetry < -0.3
                else "量能均衡"
            ),
            "absorption_events": len(absorption_events),
            "exhaustion_events": len(exhaustion_events),
            "climax_events": climax_count,
            "equal_volume_events": len(equal_events),
            "absorption_score": absorption_score,
            "phase_interpretation": interpret_phase(absorption_score, climax_count, vol_asymmetry),
        },
    }


def interpret_phase(absorption_score, climax_count, vol_asymmetry):
    """Interpret what phase the token is in based on volume patterns."""
    if absorption_score >= 6 and vol_asymmetry > 0:
        return "底部吸筹阶段 — 多次出现缩量下跌衰竭+放量滞跌，上涨时放量，典型底部积累特征"
    elif absorption_score >= 3 and climax_count == 0:
        return "疑似底部积累 — 有吸筹信号但尚未放量突破，关注后续放量方向"
    elif climax_count >= 2 and vol_asymmetry < -0.3:
        return "派发/砸盘阶段 — 多次量能顶点+下跌放量，已有出货迹象"
    elif climax_count >= 1 and absorption_score >= 3:
        return "拉升后换手 — 量能顶点后出现吸筹信号，可能是洗盘换手"
    elif climax_count == 0 and absorption_score == 0:
        return "无明显量价信号 — 缩量横盘或死币状态"
    return "混合信号 — 需结合价格走势综合判断"


def find_phase_zones(candles, events):
    """Group consecutive bars into price-volume phase zones."""
    if len(candles) < 5: return []

    zones = []
    closes = [c["close"] for c in candles]
    volumes = [c["volume"] for c in candles]

    # Find accumulation zones: consecutive bars with vol < avg AND price range < 10%
    avg_vol = sum(volumes) / len(volumes) if volumes else 1
    i = 0
    while i < len(candles):
        if volumes[i] < avg_vol * 0.7:
            zone_start = i
            zone_prices = [closes[i]]
            zone_vols = [volumes[i]]
            j = i + 1
            while j < len(candles) and volumes[j] < avg_vol:
                zone_prices.append(closes[j])
                zone_vols.append(volumes[j])
                j += 1
            if j - i >= 3:
                price_range = (max(zone_prices) - min(zone_prices)) / min(zone_prices) * 100
                if price_range < 10:
                    zones.append({
                        "type": "缩量横盘区(潜在底部)",
                        "bars": f"{i}-{j-1}",
                        "length": j - i,
                        "price_range_pct": round(price_range, 1),
                        "avg_vol_ratio": round(sum(zone_vols) / len(zone_vols) / avg_vol, 2),
                        "entry_price": closes[i],
                        "exit_price": closes[j-1],
                    })
            i = j
        else:
            i += 1

    return zones


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def format_ts(ts):
    return datetime.fromtimestamp(ts).strftime("%m-%d %H:%M") if ts > 0 else "?"


def summarize_for_tg(result):
    """Generate a concise summary suitable for TG bot integration."""
    stats = result["stats"]
    zones = result.get("zones", [])
    events = result.get("events", [])

    lines = []
    lines.append(f"量价关系分析 ({result['resolution']}, {result['candle_count']}根)")
    lines.append(f"- 量能不对称: {stats['vol_asymmetry_desc']} (上涨均量={stats['avg_up_vol']:.1f}, 下跌均量={stats['avg_down_vol']:.1f})")
    lines.append(f"- 吸筹信号: {stats['absorption_events']}次放量滞跌 + {stats['exhaustion_events']}次缩量下跌衰竭")
    lines.append(f"- 量能顶点: {stats['climax_events']}次 | 等量等幅: {stats['equal_volume_events']}次")
    lines.append(f"- 阶段判断: {stats['phase_interpretation']}")

    if zones:
        lines.append(f"- 缩量横盘区: {len(zones)}个")
        for z in zones[:3]:
            lines.append(f"  第{z['bars']}根 ({z['length']}根), 波动{z['price_range_pct']}%, 均量比{z['avg_vol_ratio']}x")

    if events:
        lines.append(f"- 关键量价事件 (最近10条):")
        for e in events[-10:]:
            icon = {"high": "★", "medium": "·", "low": " "}[e["significance"]]
            lines.append(f"  {icon} 第{e['bar']}根 [{e['type']}] 价格变化{e['price_chg']:+.2f}% 量{e.get('vol_chg', e.get('vol_ratio', 0))}")

    return "\n".join(lines)


def main():
    results = []

    for sym, ca in N_TOKENS:
        print(f"\n{'='*80}")
        print(f"  {sym} ({ca[:16]}...)")
        print(f"{'='*80}")

        candles = fetch_kline(ca, RESOLUTION)
        if len(candles) < 6:
            print(f"  SKIP: only {len(candles)} candles")
            continue

        analysis = detect_patterns(candles)
        zones = find_phase_zones(candles, analysis["events"])

        closes = [c["close"] for c in candles]
        first, peak, low = closes[0], max(c["high"] for c in candles), min(c["low"] for c in candles)
        cur = closes[-1]

        result = {
            "symbol": sym,
            "ca": ca,
            "resolution": RESOLUTION,
            "candle_count": len(candles),
            "first_price": first,
            "peak_price": peak,
            "lowest_price": low,
            "current_price": cur,
            "events": analysis["events"],
            "stats": analysis["stats"],
            "zones": zones,
        }

        # Print detailed analysis
        print(f"  价格: {first:.10f} → 低{low:.10f} → 高{peak:.10f} → 现{cur:.10f}")
        print(f"  蜡烛: {analysis['stats']['total_bars']} | 阳{analysis['stats']['up_bars']} 阴{analysis['stats']['down_bars']}")
        print(f"  上涨均量: {analysis['stats']['avg_up_vol']:.1f} | 下跌均量: {analysis['stats']['avg_down_vol']:.1f}")
        print(f"  量能不对称: {analysis['stats']['vol_asymmetry_desc']} ({analysis['stats']['vol_asymmetry']:+.2f})")
        print(f"  吸筹评分: {analysis['stats']['absorption_score']}")
        print(f"  阶段: {analysis['stats']['phase_interpretation']}")

        if zones:
            print(f"\n  [缩量横盘区]")
            for z in zones:
                print(f"    第{z['bars']}根 ({z['length']}根) 波动{z['price_range_pct']}% 均量比{z['avg_vol_ratio']}x")

        # Count event types
        from collections import Counter
        event_counts = Counter(e["type"] for e in analysis["events"])
        print(f"\n  [量价事件统计]")
        for etype, count in event_counts.most_common():
            print(f"    {etype}: {count}次")

        # Show key events
        key_events = [e for e in analysis["events"] if e["significance"] == "high"]
        if key_events:
            print(f"\n  [关键量价事件]")
            for e in key_events:
                print(f"    第{e['bar']}根 [{e['type']}] 价格{e['price_chg']:+.2f}% 量变{e.get('vol_chg', 0):+.0f}%")

        results.append(result)

        if sym != N_TOKENS[-1][0]:
            time.sleep(RATE_DELAY)

    # Cross-token summary
    print(f"\n{'='*80}")
    print(f"  量价关系规律总结 (基于 {len(results)} 个金狗代币)")
    print(f"{'='*80}")

    print(f"\n{'代币':<12} {'吸筹评分':>6} {'放量滞跌':>6} {'缩量衰竭':>6} {'量顶点':>5} {'量不对称':>8} {'阶段'}")
    print("-" * 100)
    for r in results:
        s = r["stats"]
        print(f"{r['symbol']:<12} {s['absorption_score']:>5}  {s['absorption_events']:>5}  {s['exhaustion_events']:>5}  {s['climax_events']:>4}  {s['vol_asymmetry']:>7.2f}  {s['phase_interpretation'][:40]}")

    print(f"""
================================================================================
  量价关系核心规律 (可用于TG bot实时判断)
================================================================================

  ┌─────────────────────────────────────────────────────────────────────┐
  │ 1. 缩量下跌 (vol↓>30% + price↓)                                    │
  │    含义: 卖盘衰竭，没人愿意在这个价位卖了                            │
  │    金狗表现: 底部区域出现3-5次缩量下跌 = 筑底信号                    │
  │                                                                     │
  │ 2. 放量滞跌 (vol↑>50% + price↓<5%)                                 │
  │    含义: 有大资金在接盘吸收卖压，跌幅被托住                          │
  │    金狗表现: CTO团队建仓的典型手法，出现在底部后段                    │
  │    评分: 每个+3分吸筹评分                                           │
  │                                                                     │
  │ 3. 等量等幅 (连续2根vol±25% + priceΔ±3%)                           │
  │    含义: 市场进入均衡状态，多空力量相当                              │
  │    同步下跌型: 可能是程序化出货/机器人砸盘                           │
  │    同步上涨型: 可能是程序化拉盘/机器人买入                           │
  │                                                                     │
  │ 4. 量能不对称 (上涨均量 vs 下跌均量)                                 │
  │    vol_asymmetry > 0.3: 多头主导，上涨时放量 = 健康                  │
  │    vol_asymmetry < -0.3: 空头主导，下跌时放量 = 危险                 │
  │                                                                     │
  │ 5. 量能顶点 (单根量>4x均量)                                         │
  │    配合+5%以上涨幅: 突破确认，可能开启主升浪                         │
  │    配合-5%以下跌幅: 砸盘出货，顶部确认                               │
  │    配合±5%以内: 换手/洗盘，方向待定                                 │
  └─────────────────────────────────────────────────────────────────────┘

  吸筹评分算法:
    放量滞跌 ×3 + 缩量下跌衰竭 ×2  + 量能不对称加分
    >=6: 明确底部吸筹
    3-5: 疑似吸筹
    <3: 无明显吸筹信号
""")

    # Save JSON
    out_path = ROOT_DIR / "kline_analysis_output" / "volume_price_patterns.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    serializable = []
    for r in results:
        serializable.append({
            "symbol": r["symbol"],
            "ca": r["ca"],
            "stats": r["stats"],
            "zone_count": len(r.get("zones", [])),
            "event_summary": {k: v for k, v in Counter(e["type"] for e in r["events"]).most_common()},
        })
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(serializable, f, ensure_ascii=False, indent=2, default=str)
    print(f"\nResults saved to {out_path}")


if __name__ == "__main__":
    main()
