#!/usr/bin/env python3
"""Deep K-line + volume pattern analysis for success vs failure tokens."""
import sys, csv, time
from pathlib import Path
from collections import Counter

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

BINANCE_KLINE_URL = "https://dquery.sintral.io/u-kline/v1/k-line/candles"
HEADERS = {"Accept-Encoding": "identity", "User-Agent": "binance-web3/1.1 (Skill)"}


def fetch_kline(address, bars=90):
    params = {"address": address, "platform": "solana", "interval": "5min", "limit": bars, "pm": "p"}
    try:
        r = requests.get(BINANCE_KLINE_URL, params=params, headers=HEADERS, timeout=30)
        if r.ok:
            raw = (r.json().get("data") or [])
            return [{"ts": int(i[5] / 1000) if i[5] > 10**10 else int(i[5]),
                     "o": float(i[0]), "h": float(i[1]), "l": float(i[2]),
                     "c": float(i[3]), "v": float(i[4])} for i in raw if isinstance(i, list) and len(i) >= 6]
    except Exception:
        pass
    return []


def compute_patterns(candles, entry_idx):
    """Compute K-line pattern features from candles, with entry_idx as the signal bar."""
    if not candles or entry_idx >= len(candles):
        return {}

    n = len(candles)
    # Pre-entry (before signal): indices 0 to entry_idx-1
    pre = candles[:entry_idx] if entry_idx > 0 else []
    # Post-entry (after signal): entry_idx to end
    post = candles[entry_idx:]

    features = {}

    if pre and len(pre) >= 5:
        # Pre-signal: price movement
        pre_prices = [c["c"] for c in pre]
        pre_highs = [c["h"] for c in pre]
        pre_lows = [c["l"] for c in pre]
        pre_vols = [c["v"] for c in pre]
        features["pre_range_pct"] = (max(pre_highs) / min(pre_lows) - 1) * 100 if min(pre_lows) > 0 else 0
        features["pre_return_pct"] = (pre_prices[-1] / pre_prices[0] - 1) * 100 if pre_prices[0] > 0 else 0
        features["pre_avg_vol"] = sum(pre_vols) / len(pre_vols)
        features["pre_max_vol"] = max(pre_vols)

        # Pre-signal: consecutive direction
        up_bars = sum(1 for i in range(1, len(pre)) if pre_prices[i] > pre_prices[i - 1])
        down_bars = sum(1 for i in range(1, len(pre)) if pre_prices[i] < pre_prices[i - 1])
        features["pre_up_ratio"] = up_bars / max(1, up_bars + down_bars)

        # Recent trend (last 5 bars before signal)
        recent5 = pre[-5:]
        features["pre_recent_return"] = (recent5[-1]["c"] / recent5[0]["c"] - 1) * 100 if recent5[0]["c"] > 0 else 0
        recent_vols = [c["v"] for c in recent5]
        features["pre_recent_avg_vol"] = sum(recent_vols) / len(recent_vols)

    if post and len(post) >= 3:
        post_prices = [c["c"] for c in post]
        post_highs = [c["h"] for c in post]
        post_lows = [c["l"] for c in post]
        post_vols = [c["v"] for c in post]

        # Post-signal: first 3 bars
        first3_vols = post_vols[:3] if len(post_vols) >= 3 else post_vols
        features["post_first3_avg_vol"] = sum(first3_vols) / len(first3_vols)
        features["post_first_bar_return"] = (post_prices[1] / post_prices[0] - 1) * 100 if len(post_prices) > 1 and post_prices[0] > 0 else 0

        # Volume explosion: max volume in post vs pre average
        if "pre_avg_vol" in features and features["pre_avg_vol"] > 0:
            features["vol_explosion_ratio"] = max(post_vols) / features["pre_avg_vol"]

        # Post peak and trough
        peak_idx, peak_c = max(enumerate(post), key=lambda x: x[1]["h"])
        trough_idx, trough_c = min(enumerate(post), key=lambda x: x[1]["l"])
        features["post_peak_pct"] = (peak_c["h"] / post[0]["c"] - 1) * 100 if post[0]["c"] > 0 else 0
        features["post_trough_pct"] = (trough_c["l"] / post[0]["c"] - 1) * 100 if post[0]["c"] > 0 else 0
        features["post_peak_bar"] = peak_idx
        features["post_trough_bar"] = trough_idx

        # First bar volume vs average pre volume
        if "pre_avg_vol" in features and features["pre_avg_vol"] > 0:
            features["first_bar_vol_ratio"] = post_vols[0] / features["pre_avg_vol"]

        # Sustained volume: is volume increasing or decreasing?
        half = len(post_vols) // 2
        first_half_vol = sum(post_vols[:half]) / max(1, half)
        second_half_vol = sum(post_vols[half:]) / max(1, len(post_vols) - half)
        features["vol_trend"] = "increasing" if second_half_vol > first_half_vol * 1.2 else ("decreasing" if second_half_vol < first_half_vol * 0.8 else "flat")

    # Signal bar characteristics
    sig_bar = candles[entry_idx]
    features["sig_body_pct"] = abs(sig_bar["c"] - sig_bar["o"]) / sig_bar["o"] * 100 if sig_bar["o"] > 0 else 0
    features["sig_wick_top"] = (sig_bar["h"] - max(sig_bar["c"], sig_bar["o"])) / sig_bar["o"] * 100 if sig_bar["o"] > 0 else 0
    features["sig_wick_bottom"] = (min(sig_bar["c"], sig_bar["o"]) - sig_bar["l"]) / sig_bar["o"] * 100 if sig_bar["o"] > 0 else 0

    return features


def main():
    # Load performance data
    perf_all = {}
    for fname in ["bottom_push_perf_20260515.csv", "bottom_push_perf_20260516.csv"]:
        p = ROOT / "gmgn_outputs" / fname
        if p.exists():
            with p.open("r", encoding="utf-8-sig") as f:
                for r in csv.DictReader(f):
                    if r["address"] not in perf_all:
                        perf_all[r["address"]] = r

    all_items = []
    for addr, p in perf_all.items():
        gain = float(p.get("max_gain_pct", 0) or 0)
        all_items.append(dict(
            symbol=p.get("symbol", "?"), address=addr,
            gain=gain, succ=gain >= 10,
            sig_pct=float(p.get("price_change_pct", 0) or 0),
            peak=float(p.get("time_to_peak_min", 0) or 0),
            vol=float(p.get("volume_usd", 0) or 0),
            mcap=float(p.get("current_mcap", 0) or 0),
            ath_r=float(p.get("ath_mcap", 0) or 0) / max(1, float(p.get("current_mcap", 0) or 0)),
            sig_type=p.get("signal_type", ""),
            candles=int(p.get("candles", 0) or 0),
            event_ts=int(float(p.get("event_ts", 0) or 0)),
        ))

    success = [t for t in all_items if t["succ"]]
    failed = [t for t in all_items if not t["succ"]]

    # Sample: top 40 success and all failed tokens (by gain)
    success_sample = sorted(success, key=lambda x: -x["gain"])[:40]
    failed_sample = sorted(failed, key=lambda x: x["gain"])[:40]

    med = lambda arr: sorted(arr)[len(arr) // 2] if arr else 0
    avg = lambda arr: sum(arr) / len(arr) if arr else 0

    print(f"Analyzing K-line patterns for {len(success_sample)} success + {len(failed_sample)} failed tokens...")
    time.sleep(0.5)

    success_features = []
    for t in success_sample:
        candles = fetch_kline(t["address"], bars=90)
        if len(candles) < 20:
            continue
        # Find the candle closest to event_ts
        event_ts = t["event_ts"]
        entry_idx = 0
        for i, c in enumerate(candles):
            if c["ts"] >= event_ts - 300:  # within 1 bar of signal
                entry_idx = i
                break
        if entry_idx < 5:
            entry_idx = len(candles) // 2  # fallback

        patterns = compute_patterns(candles, entry_idx)
        if patterns:
            patterns["symbol"] = t["symbol"]
            patterns["gain"] = t["gain"]
            patterns["mcap"] = t["mcap"]
            success_features.append(patterns)

    failed_features = []
    for t in failed_sample:
        candles = fetch_kline(t["address"], bars=90)
        if len(candles) < 20:
            continue
        event_ts = t["event_ts"]
        entry_idx = 0
        for i, c in enumerate(candles):
            if c["ts"] >= event_ts - 300:
                entry_idx = i
                break
        if entry_idx < 5:
            entry_idx = len(candles) // 2

        patterns = compute_patterns(candles, entry_idx)
        if patterns:
            patterns["symbol"] = t["symbol"]
            patterns["gain"] = t["gain"]
            patterns["mcap"] = t["mcap"]
            failed_features.append(patterns)

    print(f"\nGot patterns: {len(success_features)} success, {len(failed_features)} failed")

    # ===== COMPARE =====
    def compare_features(feats_s, feats_f):
        features_to_compare = [
            ("sig_body_pct", "信号K线实体%"),
            ("sig_wick_top", "信号K线上影线%"),
            ("sig_wick_bottom", "信号K线下影线%"),
            ("pre_range_pct", "信号前振幅%"),
            ("pre_return_pct", "信号前涨幅%"),
            ("pre_up_ratio", "信号前上涨K线占比"),
            ("pre_recent_return", "信号前5根涨幅%"),
            ("pre_avg_vol", "信号前均量"),
            ("pre_max_vol", "信号前最大量"),
            ("pre_recent_avg_vol", "信号前5根均量"),
            ("post_first3_avg_vol", "信号后3根均量"),
            ("post_first_bar_return", "信号后第1根涨跌%"),
            ("vol_explosion_ratio", "量能爆发比(后max/前均)"),
            ("first_bar_vol_ratio", "第一根量比(首根/前均)"),
            ("post_peak_pct", "后续最高涨幅%"),
            ("post_peak_bar", "峰顶在第几根"),
        ]

        print(f"\n{'特征':<22} {'成功':>10} {'失败':>10} {'差异':>8} {'方向':>6}")
        print("-" * 60)
        results = []
        for key, name in features_to_compare:
            svals = [f[key] for f in feats_s if key in f and f[key] > 0]
            fvals = [f[key] for f in feats_f if key in f and f[key] > 0]
            if not svals or not fvals:
                continue
            sm = med(svals)
            fm = med(fvals)
            if sm == 0 and fm == 0:
                continue
            ratio = sm / max(fm, 0.001)
            diff_text = f"{abs(ratio - 1) * 100:.0f}%"
            direction = "成功>失败" if ratio > 1.1 else ("失败>成功" if ratio < 0.9 else "持平")
            bar_len = min(40, int(abs(ratio - 1) * 200))
            bar = "=" * bar_len
            results.append((name, sm, fm, ratio, direction, bar))
            if "vol" in key.lower() or "量" in name:
                print(f'{name:<22} \${sm:>8,.0f} \${fm:>8,.0f} {diff_text:>8} {direction:>6} {bar}')
            elif "%" in name:
                print(f'{name:<22} {sm:>9.1f}% {fm:>9.1f}% {diff_text:>8} {direction:>6} {bar}')
            else:
                print(f'{name:<22} {sm:>10.2f} {fm:>10.2f} {diff_text:>8} {direction:>6} {bar}')

        return results

    results = compare_features(success_features, failed_features)

    # ===== VOLUME TREND =====
    print(f"\n\n=== 量能趋势 ===")
    vol_trend_s = Counter(f.get("vol_trend", "?") for f in success_features)
    vol_trend_f = Counter(f.get("vol_trend", "?") for f in failed_features)
    for trend in ["increasing", "flat", "decreasing"]:
        sc = vol_trend_s.get(trend, 0)
        fc = vol_trend_f.get(trend, 0)
        print(f"  {trend}: 成功{sc}({sc/len(success_features)*100:.0f}%) 失败{fc}({fc/len(failed_features)*100:.0f}%)")

    # ===== KEY INSIGHTS =====
    print(f"\n\n=== 关键发现 ===")
    # Find features where difference is largest
    insights = sorted(results, key=lambda x: abs(x[3] - 1), reverse=True)
    for name, sm, fm, ratio, direction, _ in insights[:8]:
        if direction != "持平":
            print(f"  {name}: 成功={sm:.1f} 失败={fm:.1f} -> {direction} (差异{abs(ratio-1)*100:.0f}%)")


if __name__ == "__main__":
    main()
