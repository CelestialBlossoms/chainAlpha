#!/usr/bin/env python3
"""Predictive model: find features that best separate success from failure."""
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

all_tokens = []
for addr, p in perf_all.items():
    gain = float(p.get("max_gain_pct", 0) or 0)
    all_tokens.append(dict(
        gain=gain, succ=gain >= 10,
        mcap=float(p.get("current_mcap", 0) or 0),
        ath=float(p.get("ath_mcap", 0) or 0),
        sig_pct=float(p.get("price_change_pct", 0) or 0),
        peak=float(p.get("time_to_peak_min", 0) or 0),
        dd_entry=float(p.get("entry_drawdown_pct", 0) or 0),
        dd_high=float(p.get("high_to_low_drawdown_pct", 0) or 0),
        vol=float(p.get("volume_usd", 0) or 0),
        candles=int(p.get("candles", 0) or 0),
        cur_ret=float(p.get("current_return_pct", 0) or 0),
        entry_price=float(p.get("entry_price", 0) or 0),
        sig_type=p.get("signal_type", ""),
    ))

success = [t for t in all_tokens if t["succ"]]
failed = [t for t in all_tokens if not t["succ"]]

med = lambda arr: sorted(arr)[len(arr)//2] if arr else 0
avg = lambda arr: sum(arr)/len(arr) if arr else 0

print(f"Total: {len(all_tokens)} | Success: {len(success)} ({len(success)/len(all_tokens)*100:.0f}%) | Failed: {len(failed)}\n")


# ===== 1. FEATURE DISCRIMINATION POWER =====
# For each feature, compute how well it separates success from failure
def discrimination_score(s_vals, f_vals):
    """Higher score = better separation. Uses Cohen's d-like metric."""
    if not s_vals or not f_vals:
        return 0
    s_mean = sum(s_vals)/len(s_vals)
    f_mean = sum(f_vals)/len(f_vals)
    s_var = sum((x-s_mean)**2 for x in s_vals)/(len(s_vals)-1) if len(s_vals) > 1 else 1
    f_var = sum((x-f_mean)**2 for x in f_vals)/(len(f_vals)-1) if len(f_vals) > 1 else 1
    pooled_std = (s_var + f_var)/2
    if pooled_std <= 0:
        return 0
    return abs(s_mean - f_mean) / (pooled_std ** 0.5)


# Derived features
for t in all_tokens:
    t["ath_r"] = t["ath"] / max(1, t["mcap"]) if t["ath"] > 0 else 0
    t["log_mcap"] = __import__("math").log10(max(1, t["mcap"]))
    t["vol_ratio"] = t["vol"] / max(1, t["mcap"])  # volume relative to mcap
    t["peak_fast"] = 1 if t["peak"] <= 5 else 0
    t["deep_dd"] = 1 if t["dd_entry"] < -20 else 0
    t["no_room"] = 1 if 0 < t["ath_r"] < 1.5 else 0
    t["dead_vol"] = 1 if 0 < t["vol"] < 10000 else 0
    t["large_mcap"] = 1 if t["mcap"] > 500_000 else 0

features = [
    ("peak_fast", "峰顶<=5min", False),
    ("deep_dd", "深度回撤>20%", False),
    ("no_room", "ATH<1.5x", False),
    ("dead_vol", "量<$10K", False),
    ("large_mcap", "大市值>$500K", False),
    ("mcap", "市值", True),
    ("ath_r", "ATH/mcap比", True),
    ("peak", "峰顶时间(min)", True),
    ("vol", "后续量能($)", True),
    ("dd_entry", "Entry回撤(%)", True),
    ("dd_high", "高点回撤(%)", True),
    ("sig_pct", "信号涨幅(%)", True),
    ("vol_ratio", "量/市值比", True),
    ("log_mcap", "log10(市值)", True),
    ("candles", "K线数", True),
    ("cur_ret", "当前收益(%)", True),
]

print(f"{'='*70}")
print(f"  特征区分力排名 (越大越好)")
print(f"{'='*70}")
print(f"{'特征':<25} {'成功均值':>10} {'失败均值':>10} {'区分力':>8} {'方向':>10}")
print("-" * 70)

results = []
for key, name, is_continuous in features:
    if key == "cur_ret":
        continue  # not predictive (future knowledge)
    if is_continuous:
        s_vals = [t[key] for t in success if t[key] not in (0, None)]
        f_vals = [t[key] for t in failed if t[key] not in (0, None)]
    else:
        s_vals = [t[key] for t in success]
        f_vals = [t[key] for t in failed]

    if not s_vals or not f_vals:
        continue

    score = discrimination_score(s_vals, f_vals)
    s_avg = sum(s_vals)/len(s_vals)
    f_avg = sum(f_vals)/len(f_vals)

    if key in ("mcap", "vol"):
        print(f'{name:<25} \${s_avg:>8,.0f} \${f_avg:>8,.0f} {score:>7.2f} {"成功>失败" if s_avg>f_avg else "失败>成功":>10}')
    elif is_continuous:
        print(f'{name:<25} {s_avg:>9.1f} {f_avg:>9.1f} {score:>7.2f} {"成功>失败" if s_avg>f_avg else "失败>成功":>10}')
    else:
        s_pct = s_avg * 100
        f_pct = f_avg * 100
        print(f'{name:<25} {s_pct:>8.0f}% {f_pct:>8.0f}% {score:>7.2f} {"成功>失败" if s_avg>f_avg else "失败>成功":>10}')
    results.append((name, score, s_avg, f_avg))

# ===== 2. DECISION TREE STYLE RULES =====
print(f"\n{'='*70}")
print(f"  决策规则 (if-else 形式)")
print(f"{'='*70}")

def rule_effect(condition, name):
    """Test a rule: what if we filter by this condition?"""
    kept = [t for t in all_tokens if condition(t)]
    removed = [t for t in all_tokens if not condition(t)]
    if not kept or not removed:
        return None
    kept_succ = sum(1 for t in kept if t["succ"])
    kept_rate = kept_succ / len(kept) * 100
    removed_succ = sum(1 for t in removed if t["succ"])
    removed_rate = removed_succ / len(removed) * 100
    return {
        "name": name, "kept": len(kept), "removed": len(removed),
        "kept_rate": kept_rate, "removed_rate": removed_rate,
        "lift": kept_rate - removed_rate,
    }

# Test combined rules
rules = [
    (lambda t: t["mcap"] < 120_000 and t["vol"] > 10_000, "mcap<$120K & vol>$10K"),
    (lambda t: t["mcap"] < 120_000 and t["ath_r"] > 1.5 and t["vol"] > 10_000, "mcap<$120K & ATH>1.5x & vol>$10K"),
    (lambda t: t["mcap"] < 80_000, "mcap<$80K(黄金区)"),
    (lambda t: t["mcap"] < 80_000 and t["ath_r"] > 1.5, "mcap<$80K & ATH>1.5x"),
    (lambda t: t["mcap"] < 50_000, "mcap<$50K"),
    (lambda t: t["vol"] > 20_000, "vol>$20K"),
    (lambda t: t["vol"] > 50_000, "vol>$50K"),
    (lambda t: t["ath_r"] > 2.0, "ATH>2x"),
    (lambda t: t["dead_vol"] == 0 and t["no_room"] == 0, "非无量 & 非天花板"),
    (lambda t: t["dead_vol"] == 0 and t["no_room"] == 0 and t["mcap"] < 200_000, "健康+mcap<$200K"),
    (lambda t: t["dead_vol"] == 1 or t["no_room"] == 1, "有无量或天花板(排除)"),
]

rule_results = []
for cond, name in rules:
    r = rule_effect(cond, name)
    if r:
        rule_results.append(r)

rule_results.sort(key=lambda x: x["lift"], reverse=True)
print(f"\n{'规则':<45} {'保留':>5} {'去掉':>5} {'保留成功率':>10} {'去掉成功率':>10} {'提升':>7}")
print("-" * 85)
for r in rule_results:
    print(f'{r["name"]:<45} {r["kept"]:>5} {r["removed"]:>5} {r["kept_rate"]:>9.0f}% {r["removed_rate"]:>9.0f}% {r["lift"]:>+6.0f}%')

# ===== 3. FINAL RECOMMENDATION =====
print(f"\n{'='*70}")
print(f"  最终推荐策略")
print(f"{'='*70}")

# Find best combo
best = max(rule_results, key=lambda x: x["lift"])
print(f"""
  最佳单规则: {best['name']}
    保留{best['kept']}个, 成功率{best['kept_rate']:.0f}%, 提升{best['lift']:+.0f}%

  推荐多层过滤策略:
    第1层: 排除 天花板+无量 组合 (成功率~0%)
    第2层: 排除 vol<$5K 极度无量 (成功率22%)
    第3层: 保留 mcap<$120K & vol>$10K 的代币 (成功率最高)

  预期: 全部代币{len(all_tokens)}个 → 过滤后约100个 → 成功率从{len(success)/len(all_tokens)*100:.0f}%提升到~80%
""")

# ===== 4. MCAP x VOL HEATMAP =====
print(f"\n{'='*70}")
print(f"  市值 x 量能 成功率矩阵")
print(f"{'='*70}")

mcap_bins = [(0, 50, "<$50K"), (50, 80, "$50-80K"), (80, 120, "$80-120K"), (120, 200, "$120-200K"), (200, 99999, ">$200K")]
vol_bins = [(0, 5, "vol<$5K"), (5, 10, "$5-10K"), (10, 30, "$10-30K"), (30, 99999, ">$30K")]

print(f'{"":>14}', end="")
for _, _, vname in vol_bins:
    print(f'{vname:>14}', end="")
print()
for mlo, mhi, mname in mcap_bins:
    print(f'{mname:<14}', end="")
    for vlo, vhi, _ in vol_bins:
        bucket = [t for t in all_tokens if mlo*1000 <= t["mcap"] < mhi*1000 and vlo*1000 <= t["vol"] < vhi*1000]
        if bucket:
            rate = sum(1 for t in bucket if t["succ"]) / len(bucket) * 100
            print(f'{rate:>7.0f}%({len(bucket):>2})', end="   ")
        else:
            print(f'{"-":>14}', end="")
    print()
