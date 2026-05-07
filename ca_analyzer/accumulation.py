"""
Quantitative accumulation/distribution detection using OBV divergence + VWAP analysis.

Usage:
    D:/software/anaconda/envs/py312/python.exe scripts/analyze_accumulation.py <CA>
    D:/software/anaconda/envs/py312/python.exe scripts/analyze_accumulation.py <CA> --resolution 5m --lookback 24h
"""
import argparse, json, shutil, subprocess, sys, time
from datetime import datetime, timezone

CHAIN = "sol"


def to_f(v, d=0.0):
    try:
        if v in (None, ""): return d
        return float(v)
    except: return d


def gmgn_prefix():
    exe = shutil.which("gmgn-cli") or shutil.which("gmgn-cli.cmd") or "gmgn-cli"
    if str(exe).lower().endswith(".ps1"):
        return ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", exe]
    return [exe]


def run_gmgn(args, timeout=30):
    cmd = [*gmgn_prefix(), *args, "--raw"]
    r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout)
    if r.returncode != 0: return None
    try: return json.loads(r.stdout)
    except: return None


def fetch_kline(address, resolution, lookback_sec):
    now = int(time.time()); start = now - lookback_sec
    data = run_gmgn(["market", "kline", "--chain", CHAIN, "--address", address,
                     "--resolution", resolution, "--from", str(start), "--to", str(now)])
    if not data: return []
    rows = data.get("list") or data.get("data", {}).get("list") or []
    candles = []
    for row in (rows if isinstance(rows, list) else []):
        if not isinstance(row, dict): continue
        raw_ts = int(to_f(row.get("time") or row.get("timestamp") or row.get("t")))
        ts = raw_ts // 1000 if raw_ts > 10_000_000_000 else raw_ts
        close = to_f(row.get("close") or row.get("c"))
        if ts <= 0 or close <= 0: continue
        candles.append({"ts": ts, "open": to_f(row.get("open") or row.get("o"), close),
                        "high": to_f(row.get("high") or row.get("h"), close),
                        "low": to_f(row.get("low") or row.get("l"), close),
                        "close": close, "volume": to_f(row.get("volume") or row.get("v"))})
    candles.sort(key=lambda c: c["ts"])
    return candles


def compute_ema(prices, period):
    ema = [0.0] * len(prices)
    if len(prices) < period: return ema
    ema[period - 1] = sum(prices[:period]) / period
    mult = 2 / (period + 1)
    for i in range(period, len(prices)):
        ema[i] = (prices[i] - ema[i - 1]) * mult + ema[i - 1]
    return ema


def compute_obv(candles):
    obv = [0.0]
    for i in range(1, len(candles)):
        if candles[i]["close"] > candles[i - 1]["close"]:
            obv.append(obv[-1] + candles[i]["volume"])
        elif candles[i]["close"] < candles[i - 1]["close"]:
            obv.append(obv[-1] - candles[i]["volume"])
        else:
            obv.append(obv[-1])
    return obv


def detect_divergence(candles, obv, window=12):
    divergences = []
    closes = [c["close"] for c in candles]
    for i in range(window, len(candles)):
        seg_p = closes[i - window:i]; seg_o = obv[i - window:i]
        n = len(seg_p); xm = (n - 1) / 2
        ym_p = sum(seg_p) / n; ym_o = sum(seg_o) / n
        denom_p = sum((j - xm) ** 2 for j in range(n))
        if denom_p <= 0: continue
        p_slope = sum((j - xm) * (seg_p[j] - ym_p) for j in range(n)) / denom_p
        o_slope = sum((j - xm) * (seg_o[j] - ym_o) for j in range(n)) / denom_p
        p_norm = p_slope / ym_p if ym_p > 0 else 0
        o_norm = o_slope / (abs(ym_o) + 1) if abs(ym_o) > 0 else 0

        if (p_norm <= 0.01) and o_norm > 0.01:
            divergences.append({"start": i - window, "end": i, "signal": "ACCUM (OBV up, price flat/down)", "strength": min(100, abs(o_norm) * 500)})
        elif p_norm > 0.01 and o_norm < -0.01:
            divergences.append({"start": i - window, "end": i, "signal": "DISTRIB (OBV down, price up)", "strength": min(100, abs(o_norm) * 500)})
    return divergences


def compute_score(candles):
    closes = [c["close"] for c in candles]; vols = [c["volume"] for c in candles]
    obv = compute_obv(candles); divs = detect_divergence(candles, obv)
    avg_v = sum(vols) / len(vols) if vols else 1

    recent_divs = [d for d in divs if d["end"] > len(candles) * 0.5]
    bull = sum(1 for d in recent_divs if "ACCUM" in d["signal"])
    bear = sum(1 for d in recent_divs if "DISTRIB" in d["signal"])

    up_v = [vols[i] for i in range(1, len(candles)) if closes[i] > closes[i - 1]]
    down_v = [vols[i] for i in range(1, len(candles)) if closes[i] < closes[i - 1]]
    avg_up = sum(up_v) / len(up_v) if up_v else 0
    avg_down = sum(down_v) / len(down_v) if down_v else 0
    asymmetry = (avg_up - avg_down) / avg_down if avg_down > 0 else 0

    q = len(candles) // 4
    early_v = sum(vols[:q]) / q if q > 0 else 0
    late_v = sum(vols[-q:]) / q if q > 0 else 0
    vol_trend = (late_v - early_v) / max(early_v, 1)

    obv_score = min(60, bull * 15) - min(60, bear * 15)
    vol_price_score = 30 if asymmetry > 0.3 else (-20 if asymmetry < -0.3 else 0)
    vol_trend_score = 15 if vol_trend > 0.3 else (-15 if vol_trend < -0.5 else 0)
    composite = obv_score + vol_price_score + vol_trend_score

    if composite >= 50: phase = "STRONG ACCUMULATION"
    elif composite >= 20: phase = "Weak accumulation"
    elif composite >= -20: phase = "No clear direction"
    elif composite >= -50: phase = "Weak distribution"
    else: phase = "STRONG DISTRIBUTION"

    return {"composite": composite, "phase": phase,
            "obv_bull": bull, "obv_bear": bear, "obv_score": obv_score,
            "asymmetry": round(asymmetry, 2), "vol_trend": round(vol_trend, 2),
            "divergences": divs[-10:]}


def print_report(candles, score, addr):
    closes = [c["close"] for c in candles]
    first, peak, low, cur = closes[0], max(c["high"] for c in candles), min(c["low"] for c in candles), closes[-1]
    print(f"\n{'='*70}")
    print(f"  Accumulation Analysis: {addr[:24]}...")
    print(f"  Candles: {len(candles)} | Price: {first:.10f} -> P{peak:.10f} -> L{low:.10f} -> Now{cur:.10f}")
    print(f"{'='*70}")
    print(f"  Score: {score['composite']:+d} -> {score['phase']}")
    print(f"  OBV: bull={score['obv_bull']} bear={score['obv_bear']} (score: {score['obv_score']:+d})")
    print(f"  Asymmetry: {score['asymmetry']:+.2f} | Vol trend: {score['vol_trend']:+.2f}")

    if score["divergences"]:
        print(f"\n  [Recent Divergences]")
        for d in score["divergences"]:
            icon = "+" if "ACCUM" in d["signal"] else "-"
            print(f"  #{d['start']:03d}-{d['end']:03d} {icon} {d['signal']} (strength: {d['strength']:.0f})")

    if score['composite'] >= 50: print(f"\n  >>> Strong accumulation: smart money buying at bottom <<<")
    elif score['composite'] >= 20: print(f"\n  >>> Possible bottom building, wait for breakout confirmation <<<")
    elif score['composite'] >= -20: print(f"\n  >>> No clear signal — avoid <<<")
    else: print(f"\n  >>> Distribution in progress — stay away <<<")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("ca"); p.add_argument("--resolution", default="5m"); p.add_argument("--lookback", default="24h")
    args = p.parse_args()
    lb = args.lookback.lower()
    sec = int(lb[:-1]) * 3600 if lb.endswith("h") else (int(lb[:-1]) * 86400 if lb.endswith("d") else 86400)
    candles = fetch_kline(args.ca, args.resolution, sec)
    if len(candles) < 12: print(f"Need 12+ candles, got {len(candles)}"); sys.exit(1)
    score = compute_score(candles)
    print_report(candles, score, args.ca)


if __name__ == "__main__":
    main()
