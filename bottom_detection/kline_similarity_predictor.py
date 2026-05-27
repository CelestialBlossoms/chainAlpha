"""
K-line similarity-based outcome predictor for new abnormal CAs.

Given a new signal with its pre/post-signal kline features,
finds the top-K most similar historical signals and reports
the probability distribution of outcomes (peak gain, pattern, recovery).

Usage:
    python bottom_detection/kline_similarity_predictor.py <CA> [--signal-type new_revival]
    python bottom_detection/kline_similarity_predictor.py --test  # backtest all 495 signals
"""
import sys, json, math, time
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from db_client import db_op
import psycopg2
from config import DB_CONFIG

TOP_K = 30  # number of similar signals to match

# === Feature extraction ===

def extract_features(candles, sig_ts):
    """Extract normalized kline features around signal time."""
    if not candles or len(candles) < 20:
        return None

    # Find signal candle
    sig_idx = 0
    min_diff = float('inf')
    for i, c in enumerate(candles):
        d = abs(c["ts"] - sig_ts)
        if d < min_diff:
            min_diff = d
            sig_idx = i

    if sig_idx < 20:  # need at least 20 pre-signal candles
        return None

    sig_price = candles[sig_idx]["close"]
    pre = candles[:sig_idx + 1]
    post = candles[sig_idx:]

    feats = {}

    # 1. Pre-signal trend features (normalized)
    for window, label in [(5, "5c"), (20, "20c"), (48, "48c"), (96, "96c")]:
        if len(pre) >= window:
            w = pre[-window:]
            start_p = w[0]["close"]
            end_p = w[-1]["close"]
            feats[f"pre_change_{label}"] = (end_p - start_p) / start_p * 100 if start_p > 0 else 0
            highs = [c["high"] for c in w]
            lows = [c["low"] for c in w]
            feats[f"pre_volatility_{label}"] = (max(highs) - min(lows)) / min(lows) * 100 if min(lows) > 0 else 0
        else:
            feats[f"pre_change_{label}"] = 0
            feats[f"pre_volatility_{label}"] = 0

    # 2. Volume profile
    pre_vol_20 = sum(c["volume"] for c in pre[-20:]) / max(20, len(pre[-20:]))
    pre_vol_5 = sum(c["volume"] for c in pre[-5:]) / max(5, len(pre[-5:]))
    feats["vol_ratio_5_20"] = pre_vol_5 / pre_vol_20 if pre_vol_20 > 0 else 1.0

    # 3. Signal candle features
    sig_candle = candles[sig_idx]
    feats["sig_body_pct"] = (sig_candle["close"] - sig_candle["open"]) / sig_candle["open"] * 100 if sig_candle["open"] > 0 else 0
    feats["sig_wick_top_pct"] = (sig_candle["high"] - max(sig_candle["open"], sig_candle["close"])) / sig_candle["open"] * 100 if sig_candle["open"] > 0 else 0
    feats["sig_wick_bot_pct"] = (min(sig_candle["open"], sig_candle["close"]) - sig_candle["low"]) / sig_candle["open"] * 100 if sig_candle["open"] > 0 else 0

    # 4. Post-signal early direction (first 4 candles = 20min)
    if len(post) >= 4:
        p4 = post[:4]
        feats["post4_change"] = (p4[-1]["close"] - sig_price) / sig_price * 100
        feats["post4_high"] = (max(c["high"] for c in p4) - sig_price) / sig_price * 100
        feats["post4_low"] = (min(c["low"] for c in p4) - sig_price) / sig_price * 100
        feats["post4_volatility"] = (max(c["high"] for c in p4) - min(c["low"] for c in p4)) / sig_price * 100 if sig_price > 0 else 0
    else:
        feats["post4_change"] = 0; feats["post4_high"] = 0; feats["post4_low"] = 0; feats["post4_volatility"] = 0

    # 5. Signal metadata features (from DB record)
    feats["sig_price"] = sig_price
    feats["sig_idx"] = sig_idx

    return feats


def feature_vector(feats):
    """Return normalized numeric vector for similarity computation."""
    keys = [
        "pre_change_5c", "pre_change_20c", "pre_change_48c", "pre_change_96c",
        "pre_volatility_5c", "pre_volatility_20c", "pre_volatility_48c", "pre_volatility_96c",
        "vol_ratio_5_20",
        "sig_body_pct", "sig_wick_top_pct", "sig_wick_bot_pct",
        "post4_change", "post4_high", "post4_low", "post4_volatility",
    ]
    return [feats.get(k, 0) for k in keys]


def cosine_similarity(v1, v2):
    """Cosine similarity between two vectors."""
    dot = sum(a * b for a, b in zip(v1, v2))
    norm1 = math.sqrt(sum(a * a for a in v1))
    norm2 = math.sqrt(sum(b * b for b in v2))
    if norm1 == 0 or norm2 == 0:
        return 0
    return dot / (norm1 * norm2)


def euclidean_similarity(v1, v2):
    """1 / (1 + euclidean_distance), range (0, 1]."""
    dist = math.sqrt(sum((a - b) ** 2 for a, b in zip(v1, v2)))
    return 1 / (1 + dist)


def combined_similarity(v1, v2):
    """Average of cosine and euclidean similarities."""
    return (cosine_similarity(v1, v2) + euclidean_similarity(v1, v2)) / 2


# === Outcome labels ===

def compute_outcome(candles, sig_ts):
    """Compute outcome labels for a historical signal."""
    sig_idx = 0
    min_diff = float('inf')
    for i, c in enumerate(candles):
        d = abs(c["ts"] - sig_ts)
        if d < min_diff:
            min_diff = d
            sig_idx = i

    sig_price = candles[sig_idx]["close"]
    post = candles[sig_idx:]

    # Peak
    peak_high = sig_price
    peak_idx = 0
    for i, c in enumerate(post):
        if c["high"] > peak_high:
            peak_high = c["high"]
            peak_idx = i
    peak_gain = (peak_high - sig_price) / sig_price * 100
    peak_time_min = peak_idx * 5

    # Max DD
    lowest_price = sig_price
    for c in post:
        if c["low"] < lowest_price:
            lowest_price = c["low"]
    max_dd = (lowest_price - sig_price) / sig_price * 100

    # Recovery
    lowest_idx = next((i for i, c in enumerate(post) if c["low"] == lowest_price), 0)
    after_low = post[lowest_idx:]
    recovery_high = max(c["high"] for c in after_low) if after_low else lowest_price
    recovery_gain = (recovery_high - lowest_price) / lowest_price * 100 if lowest_price > 0 else 0

    # Pattern
    if peak_gain >= 50 and peak_time_min <= 30:
        pattern = "瞬爆急涨"
    elif peak_gain >= 20 and max_dd < -10:
        pattern = "冲高回落"
    elif peak_gain >= 20 and max_dd >= -10:
        pattern = "稳健上涨"
    elif max_dd < -40 and recovery_gain >= 30:
        pattern = "深跌反弹"
    elif max_dd < -20 and recovery_gain < 10:
        pattern = "持续阴跌"
    elif -5 <= (post[-1]["close"] - sig_price) / sig_price * 100 <= 5 and abs(peak_gain) < 15:
        pattern = "横盘震荡"
    elif peak_gain >= 10 and peak_gain < 20:
        pattern = "小幅上涨"
    elif max_dd < -30:
        pattern = "深度下跌"
    else:
        pattern = "其他"

    return {
        "peak_gain": peak_gain,
        "peak_time_min": peak_time_min,
        "max_dd": max_dd,
        "recovery_gain": recovery_gain,
        "pattern": pattern,
    }


# === Database loading ===

def load_database():
    """Load all 495 signals with klines, features, and outcomes."""
    cfg = DB_CONFIG.copy()
    if 'database' in cfg:
        cfg['dbname'] = cfg.pop('database')

    conn = psycopg2.connect(**cfg, connect_timeout=15)
    cur = conn.cursor()

    cur.execute("""
        SELECT address, symbol, signal_type, event_ts, current_mcap, liquidity,
               pool_mcap_ratio, age_sec, price_change_pct
        FROM bottom_top100_push_records
        WHERE pushed_at::date BETWEEN '2026-05-18' AND '2026-05-27'
          AND chain = 'sol'
          AND signal_type IN ('new_revival', 'abnormal', 'quiet_runup', 'watchlist_abnormal')
        ORDER BY event_ts
    """)
    signals = cur.fetchall()

    addresses = list(set(s[0] for s in signals))
    cur.execute("""
        SELECT address, ts, open, high, low, close, volume
        FROM bottom_kline_cache
        WHERE address = ANY(%s) AND chain = 'sol' AND resolution = '5m'
        ORDER BY address, ts
    """, (addresses,))

    all_klines = defaultdict(list)
    for row in cur:
        all_klines[row[0]].append({
            "ts": int(row[1]), "open": float(row[2]), "high": float(row[3]),
            "low": float(row[4]), "close": float(row[5]), "volume": float(row[6] or 0)
        })

    cur.close()
    conn.close()

    database = []
    for addr, sym, stype, ets, mcap, liq, ratio, age, chg_pct in signals:
        candles = all_klines.get(addr, [])
        if not candles or len(candles) < 30:
            continue

        feats = extract_features(candles, int(ets))
        if feats is None:
            continue

        outcome = compute_outcome(candles, int(ets))

        database.append({
            "addr": addr, "sym": sym, "stype": stype, "ets": ets,
            "mcap": float(mcap or 0), "liq": float(liq or 0),
            "ratio": float(ratio or 0), "age_h": float(age or 0) / 3600,
            "chg_pct": float(chg_pct or 0),
            "features": feats,
            "vector": feature_vector(feats),
            "outcome": outcome,
        })

    return database


def predict(query_feats, query_vector, database, top_k=TOP_K):
    """Find top-K similar signals and aggregate outcomes."""
    scored = []
    for entry in database:
        sim = combined_similarity(query_vector, entry["vector"])
        scored.append((sim, entry))

    scored.sort(key=lambda x: -x[0])
    top = scored[:top_k]

    # Aggregate outcomes
    outcomes = {
        "peak_gains": [],
        "peak_times": [],
        "max_dds": [],
        "recovery_gains": [],
        "patterns": defaultdict(int),
        "signal_types": defaultdict(int),
        "similarities": [],
        "matches": [],
        "mcap_range": defaultdict(int),
    }

    for sim, entry in top:
        o = entry["outcome"]
        outcomes["peak_gains"].append(o["peak_gain"])
        outcomes["peak_times"].append(o["peak_time_min"])
        outcomes["max_dds"].append(o["max_dd"])
        outcomes["recovery_gains"].append(o["recovery_gain"])
        outcomes["patterns"][o["pattern"]] += 1
        outcomes["signal_types"][entry["stype"]] += 1
        outcomes["similarities"].append(sim)

        m = entry["mcap"]
        if m < 50000: outcomes["mcap_range"]["<50K"] += 1
        elif m < 100000: outcomes["mcap_range"]["50K-100K"] += 1
        elif m < 300000: outcomes["mcap_range"]["100K-300K"] += 1
        else: outcomes["mcap_range"]["300K+"] += 1

        outcomes["matches"].append({
            "sym": entry["sym"], "stype": entry["stype"],
            "peak": o["peak_gain"], "pattern": o["pattern"],
            "similarity": sim,
        })

    return outcomes, top


def report(outcomes, top_k):
    """Print prediction report."""
    pk = sorted(outcomes["peak_gains"])
    n = len(pk)
    avg_sim = sum(outcomes["similarities"]) / len(outcomes["similarities"]) if outcomes["similarities"] else 0

    print(f"\n{'='*70}")
    print(f"  KLINE SIMILARITY PREDICTION (top {n} matches, avg_sim={avg_sim:.3f})")
    print(f"{'='*70}")

    print(f"\n  --- Peak Gain Prediction ---")
    print(f"  Min: {pk[0]:+.1f}%  P25: {pk[n//4]:+.1f}%  Median: {pk[n//2]:+.1f}%  P75: {pk[3*n//4]:+.1f}%  Max: {pk[-1]:+.1f}%")
    print(f"  Prob(>=20%): {sum(1 for p in pk if p >= 20)/n*100:.0f}%  Prob(>=50%): {sum(1 for p in pk if p >= 50)/n*100:.0f}%  Prob(>=100%): {sum(1 for p in pk if p >= 100)/n*100:.0f}%")

    dd = sorted(outcomes["max_dds"])
    print(f"\n  --- Max Drawdown Prediction ---")
    print(f"  Median DD: {dd[n//2]:+.1f}%  P75: {dd[3*n//4]:+.1f}%")

    rec = sorted(outcomes["recovery_gains"])
    print(f"\n  --- Recovery Prediction ---")
    print(f"  Median Recovery: {rec[n//2]:+.1f}%  Prob(recovery>=30%): {sum(1 for r in rec if r >= 30)/n*100:.0f}%")

    pt = sorted(outcomes["peak_times"])
    print(f"\n  --- Time to Peak Prediction ---")
    print(f"  Median: {pt[n//2]:.0f}min  P25: {pt[n//4]:.0f}min  P75: {pt[3*n//4]:.0f}min")

    print(f"\n  --- Pattern Probability ---")
    for pat, cnt in sorted(outcomes["patterns"].items(), key=lambda x: -x[1]):
        bar = "#" * int(cnt / n * 30)
        print(f"  {pat:<12} {cnt:>2}/{n} ({cnt/n*100:>5.1f}%)  {bar}")

    print(f"\n  --- Top {min(5, n)} Matches ---")
    for m in outcomes["matches"][:5]:
        print(f"  {m['sym']:<14} [{m['stype']:<14}] peak={m['peak']:>+7.1f}%  pattern={m['pattern']:<10}  sim={m['similarity']:.3f}")


# === Main ===

if __name__ == "__main__":
    print("Loading historical database...")
    database = load_database()
    print(f"Loaded {len(database)} signals with features.")

    # Backtest: leave-one-out cross-validation on a sample
    test_mode = "--test" in sys.argv
    if test_mode:
        print(f"\n{'='*70}")
        print("  BACKTEST: Leave-one-out prediction accuracy")
        print(f"{'='*70}")

        correct_pattern = 0
        correct_peak20 = 0
        total = 0
        peak_errors = []

        for i, entry in enumerate(database):
            # Remove self from DB
            train = database[:i] + database[i + 1:]
            pred, top = predict(entry["features"], entry["vector"], train, top_k=TOP_K)

            # Pattern accuracy
            predicted_pattern = max(pred["patterns"], key=pred["patterns"].get)
            if predicted_pattern == entry["outcome"]["pattern"]:
                correct_pattern += 1

            # Peak20 accuracy
            pk = sorted(pred["peak_gains"])
            med_peak = pk[len(pk) // 2]
            actual_peak20 = entry["outcome"]["peak_gain"] >= 20
            predicted_peak20 = med_peak >= 20
            if actual_peak20 == predicted_peak20:
                correct_peak20 += 1

            peak_errors.append(abs(med_peak - entry["outcome"]["peak_gain"]))
            total += 1

            if (i + 1) % 100 == 0:
                print(f"  Tested {i+1}/{len(database)}")

        print(f"\n  Total tested: {total}")
        print(f"  Pattern accuracy: {correct_pattern}/{total} ({correct_pattern/total*100:.1f}%)")
        print(f"  Peak20 accuracy: {correct_peak20}/{total} ({correct_peak20/total*100:.1f}%)")
        print(f"  Median peak error: {sorted(peak_errors)[total//2]:.1f}%")
        print(f"  Mean peak error: {sum(peak_errors)/total:.1f}%")

    # Single CA prediction
    ca_arg = next((a for a in sys.argv[1:] if not a.startswith("--")), None)
    if ca_arg:
        stype_arg = "new_revival"
        for i, a in enumerate(sys.argv):
            if a == "--signal-type" and i + 1 < len(sys.argv):
                stype_arg = sys.argv[i + 1]

        # Fetch klines for this CA
        cfg = DB_CONFIG.copy()
        if 'database' in cfg:
            cfg['dbname'] = cfg.pop('database')
        conn = psycopg2.connect(**cfg, connect_timeout=15)
        cur = conn.cursor()
        cur.execute("""
            SELECT ts, open, high, low, close, volume
            FROM bottom_kline_cache
            WHERE address = %s AND chain = 'sol' AND resolution = '5m'
            ORDER BY ts
        """, (ca_arg,))
        candles = [{"ts": int(r[0]), "open": float(r[1]), "high": float(r[2]),
                     "low": float(r[3]), "close": float(r[4]), "volume": float(r[5] or 0)}
                   for r in cur]
        cur.close(); conn.close()

        if len(candles) < 30:
            print(f"ERROR: Only {len(candles)} candles available for {ca_arg}")
            sys.exit(1)

        # Use last candle as "signal"
        sig_ts = candles[-1]["ts"]
        print(f"\nCA: {ca_arg} | Signal type: {stype_arg} | Signal ts: {sig_ts}")

        feats = extract_features(candles, sig_ts)
        if feats is None:
            print("ERROR: Cannot extract features")
            sys.exit(1)

        # Filter DB by signal type
        filtered_db = [e for e in database if e["stype"] == stype_arg]
        if not filtered_db:
            filtered_db = database

        vec = feature_vector(feats)
        outcomes, top = predict(feats, vec, filtered_db)
        report(outcomes, TOP_K)
        print(f"\n  (Filtered to signal_type={stype_arg}, DB={len(filtered_db)} signals)")

    if not test_mode and not ca_arg:
        print("\nUsage: python kline_similarity_predictor.py <CA> [--signal-type new_revival]")
        print("       python kline_similarity_predictor.py --test")
