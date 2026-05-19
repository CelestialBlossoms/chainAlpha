#!/usr/bin/env python3
"""
Live tracking script: monitor a pushed token for 3 hours.
After tracking, classify as: Dead Cat Bounce / Real Anomaly / V-Reversal

Usage:
    python scripts/track_push_live.py <CA> [--push-time "2026-05-19 14:30"]
    python scripts/track_push_live.py <CA> --interval 5 --duration 180
"""
import sys, time, json, argparse
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import deque

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

KLINE_URL = "https://dquery.sintral.io/u-kline/v1/k-line/candles"
BINANCE_DYNAMIC = "https://web3.binance.com/bapi/defi/v4/public/wallet-direct/buw/wallet/market/token/dynamic/info/ai"
HEADERS = {"Accept-Encoding": "identity", "User-Agent": "binance-web3/1.1 (Skill)"}
TZ = timezone(timedelta(hours=8))

# Classification thresholds (from our 300+ sample analysis)
DCB_1M_VOL_COLLAPSE = 0.5     # 1m post/pre volume ratio < 0.5 = DCB
V_REV_DD_MIN = 10              # DD > 10% for V-reversal
V_REV_RECOVERY_VOL = 1.3       # Recovery volume > 1.3x pre
REAL_ANOMALY_VOL_MIN = 0.8     # Volume sustains > 0.8x


def fm(v):
    if v >= 1e6: return "${:.2f}M".format(v / 1e6)
    if v >= 1e3: return "${:.0f}K".format(v / 1e3)
    return "${:.0f}".format(v)


def fp(v):
    return "{:+.1f}%".format(v)


def fetch_kline(addr, resolution, limit=24):
    params = {"address": addr, "platform": "solana", "interval": resolution, "limit": limit, "pm": "p"}
    try:
        r = requests.get(KLINE_URL, params=params, headers=HEADERS, timeout=15)
        if r.ok:
            raw = r.json().get("data", [])
            return [{"ts": int(i[5] / 1000) if i[5] > 10**10 else int(i[5]),
                     "o": float(i[0]), "h": float(i[1]), "l": float(i[2]),
                     "c": float(i[3]), "v": float(i[4])} for i in raw if isinstance(i, list) and len(i) >= 6]
    except:
        pass
    return []


def fetch_binance(addr):
    try:
        r = requests.get("{}?chainId=CT_501&contractAddress={}".format(BINANCE_DYNAMIC, addr), headers=HEADERS, timeout=12)
        if r.ok:
            d = r.json().get("data", {}) or {}
            return {
                "price": float(d.get("price", 0)),
                "mcap": float(d.get("marketCap", 0)),
                "vol_1h": float(d.get("volume1h", 0)),
                "vol_5m": float(d.get("volume5m", 0)),
                "holders": d.get("holders", 0),
                "change_1h": float(d.get("percentChange1h", 0)),
            }
    except:
        pass
    return None


def classify(snapshots):
    """Classify based on 3h of tracking data."""
    if len(snapshots) < 10:
        return "INSUFFICIENT_DATA", "Need at least 10 data points"

    # Get first and last
    first = snapshots[0]
    last = snapshots[-1]
    peak = max(snapshots, key=lambda s: s["price"])
    trough = min(snapshots, key=lambda s: s["price"])

    # Key metrics
    price_change = (last["price"] - first["price"]) / first["price"] * 100
    dd_from_peak = (trough["price"] - peak["price"]) / peak["price"] * 100
    peak_gain = (peak["price"] - first["price"]) / first["price"] * 100

    # Volume trajectory
    first_3_vol = sum(s["vol_5m"] for s in snapshots[:3]) / 3
    last_3_vol = sum(s["vol_5m"] for s in snapshots[-3:]) / 3
    vol_ratio = last_3_vol / first_3_vol if first_3_vol > 0 else 0

    # 1m K-line volume collapse (check last hour vs first hour)
    mid = len(snapshots) // 2
    early_vol = sum(s["vol_5m"] for s in snapshots[:mid]) / max(1, mid)
    late_vol = sum(s["vol_5m"] for s in snapshots[mid:]) / max(1, len(snapshots) - mid)
    vol_trajectory = late_vol / early_vol if early_vol > 0 else 0

    # V-reversal detection
    is_v_reversal = False
    if dd_from_peak < -10 and price_change > 0 and vol_ratio > V_REV_RECOVERY_VOL:
        is_v_reversal = True

    # DCB detection
    is_dcb = False
    if vol_trajectory < DCB_1M_VOL_COLLAPSE and price_change < 5:
        is_dcb = True

    # Real anomaly
    is_real = False
    if price_change > 10 and vol_ratio > REAL_ANOMALY_VOL_MIN:
        is_real = True

    # Final verdict
    if is_v_reversal:
        verdict = "V_REVERSAL"
        confidence = min(90, int(70 + (vol_ratio - 1.0) * 30))
        reason = "DD={:.0f}%, 现涨{:+.0f}%, 量恢复{:.1f}x".format(dd_from_peak, price_change, vol_ratio)
    elif is_dcb:
        verdict = "DEAD_CAT_BOUNCE"
        confidence = min(90, int(70 + (1 - vol_trajectory) * 40))
        reason = "量崩塌{:.1f}x, 涨仅{:+.0f}%".format(vol_trajectory, price_change)
    elif is_real:
        verdict = "REAL_ANOMALY"
        confidence = min(90, int(60 + (price_change / 50) * 30))
        reason = "持续涨{:+.0f}%, 量维持{:.1f}x".format(price_change, vol_ratio)
    else:
        verdict = "UNCLEAR"
        confidence = 30
        reason = "涨{:+.0f}%, 量{:.1f}x, DD={:.0f}%".format(price_change, vol_ratio, dd_from_peak)

    return verdict, reason, confidence, {
        "price_change": price_change,
        "dd_from_peak": dd_from_peak,
        "peak_gain": peak_gain,
        "vol_ratio": vol_ratio,
        "vol_trajectory": vol_trajectory,
        "peak_price": peak["price"],
        "trough_price": trough["price"],
        "start_price": first["price"],
        "end_price": last["price"],
    }


def main():
    parser = argparse.ArgumentParser(description="Live-track a pushed token for 3 hours")
    parser.add_argument("address", help="Token CA")
    parser.add_argument("--push-time", help="Push timestamp YYYY-MM-DD HH:MM (default: now)")
    parser.add_argument("--interval", type=int, default=5, help="Poll interval in minutes (default 5)")
    parser.add_argument("--duration", type=int, default=180, help="Total tracking duration in minutes (default 180)")
    parser.add_argument("--output", help="Save JSON results to file")
    args = parser.parse_args()

    addr = args.address.strip()
    total_minutes = args.duration
    interval = args.interval
    total_checks = total_minutes // interval

    push_time = datetime.now(TZ)
    if args.push_time:
        try:
            push_time = datetime.strptime(args.push_time, "%Y-%m-%d %H:%M").replace(tzinfo=TZ)
        except:
            print("Invalid push time format, using now")

    print("=" * 65)
    print("  Live Push Tracker")
    print("=" * 65)
    print("CA: {}".format(addr))
    print("Push time: {}".format(push_time.strftime("%Y-%m-%d %H:%M")))
    print("Tracking: {} minutes (check every {}min, {} checks)".format(total_minutes, interval, total_checks))
    print()

    # Get push-time K-line for baseline
    print("[Baseline] Fetching push-time data...")
    pre_candles = fetch_kline(addr, "5min", limit=36)
    if pre_candles:
        pre_prices = [c["c"] for c in pre_candles[-6:]]
        pre_vols = [c["v"] for c in pre_candles[-6:]]
        print("  Pre-push price range: {:.8f} ~ {:.8f}".format(min(pre_prices), max(pre_prices)))
        print("  Pre-push avg vol (5m): {}".format(fm(sum(pre_vols) / len(pre_vols))))

    binance = fetch_binance(addr)
    if binance:
        print("  Current: price={:.8f} mcap={} holders={}".format(
            binance["price"], fm(binance["mcap"]), binance["holders"]))

    print()
    print("Tracking started at {}...".format(datetime.now(TZ).strftime("%H:%M:%S")))
    print("-" * 65)

    snapshots = []
    start_time = time.time()
    end_time = start_time + total_minutes * 60

    check_num = 0
    while time.time() < end_time:
        check_num += 1
        elapsed = (time.time() - start_time) / 60
        remaining = (end_time - time.time()) / 60

        bd = fetch_binance(addr)
        k5m = fetch_kline(addr, "5min", limit=6)
        k1m = fetch_kline(addr, "1min", limit=12)

        snapshot = {
            "ts": datetime.now(TZ).strftime("%H:%M:%S"),
            "elapsed_min": elapsed,
        }

        if bd:
            snapshot["price"] = bd["price"]
            snapshot["mcap"] = bd["mcap"]
            snapshot["vol_1h"] = bd["vol_1h"]
            snapshot["vol_5m"] = bd["vol_5m"]
            snapshot["holders"] = bd["holders"]

        if k5m:
            snapshot["k5m_close"] = k5m[-1]["c"]
            k5m_vol = sum(c["v"] for c in k5m[-3:]) / 3
            snapshot["k5m_vol_3bar"] = k5m_vol

        if k1m:
            snapshot["k1m_close"] = k1m[-1]["c"]
            k1m_vol = sum(c["v"] for c in k1m[-3:]) / 3
            snapshot["k1m_vol_3bar"] = k1m_vol

        snapshots.append(snapshot)

        # Real-time display
        price_str = "{:.8f}".format(bd["price"]) if bd else "N/A"
        vol_str = "1m={} 5m={}".format(
            fm(k1m_vol) if k1m else "?",
            fm(k5m_vol) if k5m else "?")
        mcap_str = fm(bd["mcap"]) if bd else "?"

        print("  [{:>2d}/{}] {} | price={} | mcap={} | vol={} | {:.0f}m elapsed, {:.0f}m left".format(
            check_num, total_checks, snapshot["ts"], price_str, mcap_str, vol_str, elapsed, remaining))

        if remaining <= 0:
            break

        time.sleep(interval * 60)

    print("-" * 65)
    print("Tracking complete. {} snapshots collected.".format(len(snapshots)))
    print()

    # Classification
    if len(snapshots) >= 10:
        verdict, reason, confidence, metrics = classify(snapshots)

        print("=" * 65)
        print("  VERDICT: {}".format(verdict))
        print("=" * 65)
        print("Confidence: {}%".format(confidence))
        print("Reason: {}".format(reason))
        print()
        print("Price change: {:+.1f}%".format(metrics["price_change"]))
        print("Peak gain: {:+.1f}%".format(metrics["peak_gain"]))
        print("DD from peak: {:+.1f}%".format(metrics["dd_from_peak"]))
        print("Start: {:.8f} | Peak: {:.8f} | Trough: {:.8f} | End: {:.8f}".format(
            metrics["start_price"], metrics["peak_price"],
            metrics["trough_price"], metrics["end_price"]))
        print("Vol ratio (last/first): {:.1f}x".format(metrics["vol_ratio"]))
        print("Vol trajectory: {:.1f}x".format(metrics["vol_trajectory"]))

        # Verdict icon
        icons = {
            "V_REVERSAL": "🟢 V反 — 先跌后拉，真趋势",
            "REAL_ANOMALY": "🟢 真异动 — 持续拉升",
            "DEAD_CAT_BOUNCE": "🔴 死猫跳 — 拉完就崩",
            "UNCLEAR": "🟡 不明 — 继续观察",
        }
        if verdict in icons:
            print()
            print(icons[verdict])

        # Save output
        if args.output:
            output = {
                "address": addr,
                "verdict": verdict,
                "confidence": confidence,
                "reason": reason,
                "metrics": metrics,
                "snapshots": snapshots,
            }
            path = Path(args.output)
            path.write_text(json.dumps(output, indent=2, default=str), encoding="utf-8")
            print("\nSaved to: {}".format(path))
    else:
        print("Not enough data points for classification.")


if __name__ == "__main__":
    main()
