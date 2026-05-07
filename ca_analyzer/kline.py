"""
Quick K-line analysis for any Solana token.

Usage:
    D:/software/anaconda/envs/py312/python.exe scripts/analyze_kline.py <CA>
    D:/software/anaconda/envs/py312/python.exe scripts/analyze_kline.py <CA> --resolution 1h --lookback 7d
    D:/software/anaconda/envs/py312/python.exe scripts/analyze_kline.py <CA> --resolution 15m --lookback 24h
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
    now = int(time.time())
    start = now - lookback_sec
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
        candles.append({
            "ts": ts, "open": to_f(row.get("open") or row.get("o"), close),
            "high": to_f(row.get("high") or row.get("h"), close),
            "low": to_f(row.get("low") or row.get("l"), close),
            "close": close, "volume": to_f(row.get("volume") or row.get("v")),
        })
    candles.sort(key=lambda c: c["ts"])
    return candles


def analyze(candles, resolution):
    if len(candles) < 3:
        return {"error": f"only {len(candles)} candles"}

    closes = [c["close"] for c in candles]; highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]; vols = [c["volume"] for c in candles]
    first, peak, low, cur = closes[0], max(highs), min(lows), closes[-1]
    peak_i, low_i = highs.index(peak), lows.index(low)
    avg_vol = sum(vols) / len(vols) if vols else 1
    max_vol = max(vols) if vols else 0

    up_v = [vols[i] for i in range(1, len(candles)) if closes[i] > closes[i-1]]
    down_v = [vols[i] for i in range(1, len(candles)) if closes[i] < closes[i-1]]
    avg_up = sum(up_v) / len(up_v) if up_v else 0
    avg_down = sum(down_v) / len(down_v) if down_v else 0
    asymmetry = (avg_up - avg_down) / avg_down if avg_down > 0 else 0

    green = sum(1 for c in candles if c["close"] > c["open"])
    breakouts = [(i, (closes[i]/closes[i-1]-1)*100)
                 for i in range(1, len(closes)) if closes[i] > closes[i-1]*1.15]

    if low_i < peak_i and first > 0 and low > 0 and first / low >= 4:
        trend = f"N-type: first={first:.8f} -> low={low:.8f} -> peak={peak:.8f}"
    elif peak_i <= 3 and closes[-1] < peak * 0.7:
        trend = "K-line starts at peak, whole period is distribution"
    elif green/len(candles) > 0.6 and asymmetry > 0.3:
        trend = "Bullish: rising volume = strong"
    elif green/len(candles) < 0.4 and asymmetry < -0.3:
        trend = "Bearish: more selling volume"
    elif abs((cur-first)/first) < 0.1:
        trend = "Sideways chop"
    else:
        trend = "Mixed"

    return {
        "resolution": resolution, "candle_count": len(candles),
        "first_price": first, "peak_price": peak, "peak_idx": peak_i,
        "lowest_price": low, "lowest_idx": low_i, "current_price": cur,
        "total_change": round((cur-first)/first*100, 1),
        "peak_gain": round((peak-first)/first*100, 1),
        "drawdown_from_peak": round((peak-cur)/peak*100, 1),
        "avg_vol": round(avg_vol), "max_vol": round(max_vol),
        "asymmetry": round(asymmetry, 2),
        "asymmetry_desc": "Bull(up vol > down)" if asymmetry > 0.3 else ("Bear(down vol > up)" if asymmetry < -0.3 else "Balanced"),
        "green_ratio": round(green/len(candles), 2),
        "breakout_count": len(breakouts), "trend": trend,
    }


def print_report(candles, a, addr):
    print(f"\n{'='*70}")
    print(f"  K-line: {addr[:20]}... | {a['resolution']} | {a['candle_count']} candles")
    print(f"{'='*70}")
    if "error" in a: print(f"  ERROR: {a['error']}"); return
    print(f"  Price: {a['first_price']:.10f} -> P{a['peak_price']:.10f}(#{a['peak_idx']}) -> L{a['lowest_price']:.10f}(#{a['lowest_idx']}) -> Now{a['current_price']:.10f}")
    print(f"  Change: {a['total_change']:+.1f}% | To peak: {a['peak_gain']:+.1f}% | Drawdown: {a['drawdown_from_peak']:.1f}%")
    print(f"  Green: {a['green_ratio']:.0%} | Breakouts: {a['breakout_count']} | Vol: {a['asymmetry_desc']} ({a['asymmetry']:+.2f})")
    print(f"  Avg vol: {a['avg_vol']:,.0f} | Max: {a['max_vol']:,.0f} | Trend: {a['trend']}")

    step = max(1, len(candles) // 15)
    print(f"\n  [Trajectory (every {step} bars)]")
    for i in range(0, len(candles), step):
        c = candles[i]; t = datetime.fromtimestamp(c["ts"], tz=timezone.utc).strftime("%m-%d %H:%M")
        m = " PEAK" if i == a['peak_idx'] else (" LOW" if i == a['lowest_idx'] else "")
        print(f"  #{i:3d} {t} C:{c['close']:.8f} V:{c['volume']:,.0f}{m}")

    print(f"\n  [Last 10]")
    for c in candles[-10:]:
        i = candles.index(c); t = datetime.fromtimestamp(c["ts"], tz=timezone.utc).strftime("%m-%d %H:%M")
        print(f"  #{i:3d} {t} O:{c['open']:.8f} C:{c['close']:.8f} V:{c['volume']:,.0f}")


def main():
    p = argparse.ArgumentParser(description="Quick K-line analysis")
    p.add_argument("ca"); p.add_argument("--resolution", default="5m")
    p.add_argument("--lookback", default="24h")
    args = p.parse_args()
    lb = args.lookback.lower()
    sec = int(lb[:-1]) * 3600 if lb.endswith("h") else (int(lb[:-1]) * 86400 if lb.endswith("d") else 24*3600)
    candles = fetch_kline(args.ca, args.resolution, sec)
    if not candles: print("No data"); sys.exit(1)
    print_report(candles, analyze(candles, args.resolution), args.ca)


if __name__ == "__main__":
    main()
