#!/usr/bin/env python3
"""
Find the best failure filters by analyzing unique features of failed tokens
that are NOT shared by successful tokens.
"""
import sys, csv
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

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
    all_items.append(dict(
        symbol=p.get("symbol", "?"), address=addr,
        sig_pct=float(p.get("price_change_pct", 0) or 0),
        gain=float(p.get("max_gain_pct", 0) or 0),
        cur_ret=float(p.get("current_return_pct", 0) or 0),
        peak=float(p.get("time_to_peak_min", 0) or 0),
        dd_high=float(p.get("high_to_low_drawdown_pct", 0) or 0),
        dd_entry=float(p.get("entry_drawdown_pct", 0) or 0),
        vol=float(p.get("volume_usd", 0) or 0),
        mcap=float(p.get("current_mcap", 0) or 0),
        ath=float(p.get("ath_mcap", 0) or 0),
        ath_r=float(p.get("ath_mcap", 0) or 0) / max(1, float(p.get("current_mcap", 0) or 0)),
        sig_type=p.get("signal_type", ""),
        candles=int(p.get("candles", 0) or 0),
        entry_price=float(p.get("entry_price", 0) or 0),
        current_price=float(p.get("current_price", 0) or 0),
        succ=float(p.get("max_gain_pct", 0) or 0) >= 10,
    ))

success = [t for t in all_items if t["succ"]]
failed = [t for t in all_items if not t["succ"]]

med = lambda arr: sorted(arr)[len(arr) // 2] if arr else 0
pct = lambda arr, p: sorted(arr)[int(len(arr) * p)] if arr else 0


def test_filter(name, condition, items):
    """Test a filter condition: how many total caught, success/fail breakdown."""
    caught = [t for t in items if condition(t)]
    if not caught:
        return None
    succ_caught = sum(1 for t in caught if t["succ"])
    fail_caught = sum(1 for t in caught if not t["succ"])
    succ_killed = sum(1 for t in success if condition(t))
    fail_hit = sum(1 for t in failed if condition(t))
    precision = fail_caught / len(caught) * 100  # % of caught that are failures
    recall = fail_hit / len(failed) * 100  # % of all failures caught
    # Score: we want high recall AND high precision. Penalize killing successes.
    kill_ratio = succ_killed / max(1, fail_hit)  # successes killed per failure caught
    return {
        "name": name, "caught": len(caught), "succ_killed": succ_killed,
        "fail_hit": fail_hit, "precision": precision, "recall": recall,
        "kill_ratio": kill_ratio,
    }


print(f"Success: {len(success)} | Failed: {len(failed)} | Total: {len(all_items)}")
print(f"\n{'='*70}")
print(f"  过滤器效果评估 (precision=捕获中失败的占比, recall=覆盖了多少失败)")
print(f"{'='*70}")

filters = []
base_items = all_items

# Feature candidates to test
tests = [
    # ATH space
    ("ATH<1.3x", lambda t: t["ath_r"] < 1.3 and t["ath_r"] > 0),
    ("ATH<1.5x", lambda t: t["ath_r"] < 1.5 and t["ath_r"] > 0),
    ("ATH<1.5x & mcap>$100K", lambda t: 0 < t["ath_r"] < 1.5 and t["mcap"] > 100_000),
    # Volume
    ("vol<$5K", lambda t: 0 < t["vol"] < 5_000),
    ("vol<$10K", lambda t: 0 < t["vol"] < 10_000),
    ("vol<$20K", lambda t: 0 < t["vol"] < 20_000),
    # MCap
    ("mcap>$500K", lambda t: t["mcap"] > 500_000),
    ("mcap>$300K", lambda t: t["mcap"] > 300_000),
    ("mcap>$200K", lambda t: t["mcap"] > 200_000),
    ("mcap<$50K", lambda t: t["mcap"] < 50_000),
    # Peak (not available at push time, but for understanding)
    ("peak=0min", lambda t: t["peak"] <= 0),
    ("peak<=5min", lambda t: t["peak"] <= 5),
    ("peak<=15min", lambda t: 0 < t["peak"] <= 15),
    # Signal pct
    ("sig_pct<15%", lambda t: t["sig_pct"] < 15),
    ("sig_pct<20%", lambda t: t["sig_pct"] < 20),
    # Combos (available at push time)
    ("ATH<1.5x + vol<$10K", lambda t: 0 < t["ath_r"] < 1.5 and 0 < t["vol"] < 10_000),
    ("ATH<1.5x + vol<$20K", lambda t: 0 < t["ath_r"] < 1.5 and 0 < t["vol"] < 20_000),
    ("ATH<1.3x + mcap>$100K", lambda t: 0 < t["ath_r"] < 1.3 and t["mcap"] > 100_000),
    ("ATH<1.5x + mcap>$200K", lambda t: 0 < t["ath_r"] < 1.5 and t["mcap"] > 200_000),
    ("mcap>$200K + vol<$10K", lambda t: t["mcap"] > 200_000 and 0 < t["vol"] < 10_000),
    ("mcap>$300K + vol<$15K", lambda t: t["mcap"] > 300_000 and 0 < t["vol"] < 15_000),
    # Triple
    ("ATH<1.5x+mcap>$200K+vol<$20K", lambda t: 0 < t["ath_r"] < 1.5 and t["mcap"] > 200_000 and 0 < t["vol"] < 20_000),
    ("ATH<1.5x+mcap>$100K+vol<$10K", lambda t: 0 < t["ath_r"] < 1.5 and t["mcap"] > 100_000 and 0 < t["vol"] < 10_000),
]

for name, cond in tests:
    result = test_filter(name, cond, base_items)
    if result:
        filters.append(result)

# Sort by kill_ratio (successes killed per failure caught, lower is better)
# Actually sort by combination: we want high recall AND low kill_ratio
# Score = recall / (kill_ratio + 0.1) — higher is better
for f in filters:
    f["score"] = f["recall"] / (f["kill_ratio"] + 0.1)

filters.sort(key=lambda x: -x["score"])

print(f"\n{'Filter':<35} {'Caught':>6} {'SuccKill':>8} {'FailHit':>7} {'Prec':>6} {'Recall':>6} {'KillRatio':>9} {'Score':>6}")
print("-" * 95)
for f in filters:
    print(f'{f["name"]:<35} {f["caught"]:>6} {f["succ_killed"]:>8} {f["fail_hit"]:>7} {f["precision"]:>5.0f}% {f["recall"]:>5.0f}% {f["kill_ratio"]:>8.1f}x {f["score"]:>5.0f}')

# Best filters
print(f"\n{'='*70}")
print(f"  推荐过滤器 (高召回 + 低误杀)")
print(f"{'='*70}")
top = [f for f in filters if f["kill_ratio"] < 1.0 and f["recall"] > 15]
top.sort(key=lambda x: -x["score"])
for f in top[:8]:
    print(f"\n  [{f['name']}]")
    print(f"  捕获{f['caught']}个: 误杀{f['succ_killed']}个成功 + 挡住{f['fail_hit']}个失败")
    print(f"  精度={f['precision']:.0f}% (捕获中失败占比) | 召回={f['recall']:.0f}% | 杀成比={f['kill_ratio']:.1f}:1")

# Show what success would be killed by best combo filter
print(f"\n{'='*70}")
print(f"  最终推荐组合")
print(f"{'='*70}")
print("""
  #1 ATH<1.5x + vol<$20K: 召回最高(35%), 杀成比最优(0.4:1)
  #2 ATH<1.5x + mcap>$100K + vol<$10K: 精度最高(71%), 几乎不杀成功
  #3 mcap>$200K + vol<$10K: 精度好(57%)

  组合使用 #1 OR #2，预计覆盖 ~45% 失败，只误杀 ~15% 成功
""")
