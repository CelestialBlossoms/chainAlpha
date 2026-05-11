"""
Wallet cluster analysis — detect bundler groups, axiom bot clusters,
and distinguish natural trading from organized distribution.

Usage:
    D:/software/anaconda/envs/py312/python.exe scripts/analyze_wallet_clusters.py <CA>
    D:/software/anaconda/envs/py312/python.exe scripts/analyze_wallet_clusters.py <CA> --chain sol
"""
import argparse, json, shutil, subprocess, sys
from collections import Counter, defaultdict


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def gmgn_exe():
    exe = shutil.which("gmgn-cli") or shutil.which("gmgn-cli.cmd") or "gmgn-cli"
    return [exe]


def run_gmgn(args_list, timeout=60):
    cmd = gmgn_exe() + args_list + ["--raw"]
    r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8",
                       errors="replace", timeout=timeout)
    if r.returncode != 0:
        print(f"  [ERR] {' '.join(args_list[:4])}: {r.stderr[:200]}")
        return {}
    try:
        return json.loads(r.stdout)
    except json.JSONDecodeError:
        return {}


def to_f(v, default=0.0):
    try:
        if v in (None, ""): return default
        return float(v)
    except (ValueError, TypeError):
        return default


def to_i(v, default=0):
    try:
        if v in (None, ""): return default
        return int(float(v))
    except (ValueError, TypeError):
        return default


# ---------------------------------------------------------------------------
# Cluster Analysis
# ---------------------------------------------------------------------------
def analyze_cost_tiers(holders):
    """Analyze entry cost distribution across tiers."""
    costs = []
    for h in holders:
        if h.get("addr_type") == 2:
            continue
        avg_cost = to_f(h.get("avg_cost"))
        balance = to_f(h.get("balance"))
        buy_vol = to_f(h.get("buy_volume_cur"))
        tags = h.get("tags", [])
        if avg_cost > 0 and balance > 0:
            costs.append({"cost": avg_cost, "buy_vol": buy_vol, "tags": tags})

    if not costs:
        return {"error": "No cost data"}

    costs.sort(key=lambda x: x["cost"])
    mid = sum(c["cost"] for c in costs) / len(costs)
    deviation = sum(abs(c["cost"] - mid) / mid for c in costs) / len(costs) * 100

    # Define tiers
    tiers = [
        ("T1-snipe",    lambda c: c < 0.000010,  "$0-0.000010"),
        ("T2-early",    lambda c: 0.000010 <= c < 0.000020, "$0.000010-0.000020"),
        ("T3-mid",      lambda c: 0.000020 <= c < 0.000050, "$0.000020-0.000050"),
        ("T4-late",     lambda c: 0.000050 <= c < 0.000080, "$0.000050-0.000080"),
        ("T5-chasers",  lambda c: c >= 0.000080, "$0.000080+"),
    ]

    tier_results = []
    for name, fn, label in tiers:
        matched = [c for c in costs if fn(c["cost"])]
        if not matched:
            continue
        total_buy = sum(c["buy_vol"] for c in matched)
        all_tags = [t for c in matched for t in c["tags"]]
        tag_summary = Counter(all_tags).most_common(4)
        tier_results.append({
            "name": name,
            "label": label,
            "count": len(matched),
            "total_buy": total_buy,
            "cost_min": min(c["cost"] for c in matched),
            "cost_max": max(c["cost"] for c in matched),
            "top_tags": tag_summary,
        })

    return {
        "wallet_count": len(costs),
        "cost_min": costs[0]["cost"],
        "cost_max": costs[-1]["cost"],
        "cost_mean": mid,
        "deviation_pct": deviation,
        "tight_cost": deviation < 20,
        "tiers": tier_results,
    }


def analyze_position_distribution(holders):
    """Analyze position size distribution with exact-position matching."""
    positions = []
    addr_positions = []
    for h in holders:
        if h.get("addr_type") == 2:
            continue
        pct = to_f(h.get("amount_percentage")) * 100
        if pct > 0.01:
            positions.append(pct)
            addr_positions.append({
                "addr": h.get("address", "")[:10],
                "pct": pct,
                "tags": h.get("tags", []),
                "maker_tags": h.get("maker_token_tags", []),
            })

    ranges = [
        ("dust (<0.5%)",    lambda p: p <= 0.50),
        ("small (0.5-1%)",  lambda p: 0.50 < p <= 1.00),
        ("mid (1-1.5%)",    lambda p: 1.00 < p <= 1.50),
        ("large (1.5-2%)",  lambda p: 1.50 < p <= 2.00),
        ("whale (2-2.6%)",  lambda p: 2.00 < p <= 2.60),
        ("top (2.6%+)",     lambda p: p > 2.60),
    ]
    distribution = []
    for name, fn in ranges:
        count = sum(1 for p in positions if fn(p))
        distribution.append({"range": name, "count": count, "pct": count / len(positions) * 100 if positions else 0})

    # Check for equal-distribution pattern
    middle_band = sum(1 for p in positions if 0.50 <= p <= 1.00)
    equal_dist = middle_band / max(len(positions), 1) > 0.5

    # === NEW: Exact-position matching ===
    # Check if multiple wallets hold the EXACT same percentage (to 4 decimal places)
    exact_counter = Counter([round(p, 4) for p in positions])
    exact_clusters = [(pct, count) for pct, count in exact_counter.items() if count >= 3]
    exact_clusters.sort(key=lambda x: -x[1])
    exact_total = sum(count for _, count in exact_clusters)
    exact_match_signal = len(exact_clusters) >= 3  # 3+ clusters of 3+ identical positions

    # Narrow band check (0.01% precision, PEPTIDEPAY-style)
    narrow_counter = Counter([round(p, 2) for p in positions])
    narrow_clusters = [(pct, count) for pct, count in narrow_counter.items() if count >= 4]
    narrow_clusters.sort(key=lambda x: -x[1])
    narrow_max = narrow_clusters[0][1] if narrow_clusters else 0
    narrow_signal = narrow_max >= 8  # 8+ wallets at same 0.01% band

    return {
        "total": len(positions),
        "distribution": distribution,
        "equal_distribution_signal": equal_dist,
        "exact_clusters": exact_clusters[:10],
        "exact_total_wallets": exact_total,
        "exact_match_signal": exact_match_signal,
        "narrow_clusters": narrow_clusters[:10],
        "narrow_max": narrow_max,
        "narrow_signal": narrow_signal,
    }


def analyze_wallet_creation_clusters(holders):
    """Analyze wallet creation time clustering (bundle signal)."""
    creation_times = []
    for h in holders:
        if h.get("addr_type") == 2:
            continue
        ct = to_i(h.get("created_at"))
        if ct > 0:
            creation_times.append({
                "addr": h.get("address", "")[:10],
                "created_at": ct,
                "tags": h.get("tags", []),
            })

    if not creation_times:
        return {"error": "No creation time data", "total": 0}

    # Hour-level clustering
    hour_buckets = Counter()
    for ct in creation_times:
        hour_bucket = ct["created_at"] // 3600 * 3600
        hour_buckets[hour_bucket] += 1

    # Sort by count desc
    hour_clusters = sorted(hour_buckets.items(), key=lambda x: -x[1])
    max_hour_count = hour_clusters[0][1] if hour_clusters else 0

    # Day-level clustering
    day_buckets = Counter()
    for ct in creation_times:
        day_bucket = ct["created_at"] // 86400 * 86400
        day_buckets[day_bucket] += 1
    day_clusters = sorted(day_buckets.items(), key=lambda x: -x[1])
    max_day_count = day_clusters[0][1] if day_clusters else 0

    # Same-exact-second clusters (>5 wallets at same second)
    second_buckets = Counter()
    for ct in creation_times:
        second_buckets[ct["created_at"]] += 1
    same_second = [(ts, count) for ts, count in second_buckets.items() if count >= 3]
    same_second.sort(key=lambda x: -x[1])

    # Freshness: what % created in last 48h
    now = int(__import__("time").time())
    recent_48h = sum(1 for ct in creation_times if ct["created_at"] > now - 172800)
    recent_24h = sum(1 for ct in creation_times if ct["created_at"] > now - 86400)

    # Signals
    total = len(creation_times)
    same_hour_signal = max_hour_count > total * 0.3  # 30%+ in same hour
    same_day_signal = max_day_count > total * 0.5   # 50%+ in same day
    same_second_signal = len(same_second) > 0       # Any same-second clusters
    recent_signal = recent_48h > total * 0.6         # 60%+ recently created

    return {
        "total": total,
        "hour_clusters": [(ts, count) for ts, count in hour_clusters[:8]],
        "max_hour_count": max_hour_count,
        "max_hour_pct": max_hour_count / max(total, 1) * 100,
        "day_clusters": [(ts, count) for ts, count in day_clusters[:5]],
        "max_day_count": max_day_count,
        "max_day_pct": max_day_count / max(total, 1) * 100,
        "same_second_clusters": same_second[:5],
        "recent_48h": recent_48h,
        "recent_48h_pct": recent_48h / max(total, 1) * 100,
        "recent_24h": recent_24h,
        "same_hour_signal": same_hour_signal,
        "same_day_signal": same_day_signal,
        "same_second_signal": same_second_signal,
        "recent_signal": recent_signal,
    }


def analyze_trading_behavior(holders):
    """Analyze buy/sell uniformity."""
    total = 0
    single_buy = 0
    multi_buy = 0
    zero_sell = 0
    has_sold = 0
    buy_dist = Counter()

    for h in holders:
        if h.get("addr_type") == 2:
            continue
        total += 1
        bc = to_i(h.get("buy_tx_count_cur"))
        sc = to_i(h.get("sell_tx_count_cur"))
        sp = to_f(h.get("sell_amount_percentage"))

        buy_dist[bc] += 1
        if bc == 1:
            single_buy += 1
        if bc > 1:
            multi_buy += 1
        if sp == 0:
            zero_sell += 1
        else:
            has_sold += 1

    return {
        "total": total,
        "single_buy": single_buy,
        "single_buy_pct": single_buy / max(total, 1) * 100,
        "multi_buy": multi_buy,
        "multi_buy_pct": multi_buy / max(total, 1) * 100,
        "zero_sell": zero_sell,
        "zero_sell_pct": zero_sell / max(total, 1) * 100,
        "has_sold": has_sold,
        "has_sold_pct": has_sold / max(total, 1) * 100,
        "buy_tx_distribution": dict(sorted(buy_dist.items())),
        "single_buy_signal": single_buy / max(total, 1) > 0.7,
        "zero_sell_signal": zero_sell / max(total, 1) > 0.8,
    }


def analyze_tag_ecology(holders):
    """Analyze tag distribution and diversity."""
    tag_count = Counter()
    for h in holders:
        if h.get("addr_type") == 2:
            continue
        for t in h.get("tags", []):
            tag_count[t] += 1
        for t in h.get("maker_token_tags", []):
            tag_count[t] += 1

    dominant = tag_count.most_common(1)
    dominant_pct = dominant[0][1] / max(sum(tag_count.values()), 1) * 100 if dominant else 0

    return {
        "unique_tags": len(tag_count),
        "tag_breakdown": tag_count.most_common(15),
        "dominant_tag": dominant[0] if dominant else None,
        "dominant_pct": dominant_pct,
        "single_bot_dominance": dominant_pct > 70,
    }


def analyze_bot_clusters(holders):
    """Compare bot (axiom/bundler) vs non-bot wallets."""
    bot_tags = {"axiom", "bundler", "trojan", "photon", "bullx", "padre", "gmgn"}
    bots = []
    humans = []

    for h in holders:
        if h.get("addr_type") == 2:
            continue
        pct = to_f(h.get("amount_percentage")) * 100
        cost = to_f(h.get("avg_cost"))
        buy = to_f(h.get("buy_volume_cur"))
        tags = set(h.get("tags", []))
        if tags & bot_tags:
            bots.append({"pct": pct, "cost": cost, "buy": buy, "tags": tags})
        else:
            humans.append({"pct": pct, "cost": cost, "buy": buy, "tags": tags})

    def avg(lst, key):
        return sum(x[key] for x in lst) / len(lst) if lst else 0

    return {
        "bot_count": len(bots),
        "bot_avg_position": avg(bots, "pct"),
        "bot_avg_cost": avg(bots, "cost"),
        "bot_total_buy": sum(x["buy"] for x in bots),
        "human_count": len(humans),
        "human_avg_position": avg(humans, "pct"),
        "human_avg_cost": avg(humans, "cost"),
        "human_total_buy": sum(x["buy"] for x in humans),
        "bot_buy_ratio": sum(x["buy"] for x in bots) / max(sum(x["buy"] for x in bots) + sum(x["buy"] for x in humans), 1) * 100,
    }


def analyze_smart_money(holders):
    """Extract smart money wallet details."""
    sm_wallets = []
    for h in holders:
        if "smart_degen" in (h.get("tags") or []):
            sm_wallets.append({
                "address": h.get("address", ""),
                "hold_pct": to_f(h.get("amount_percentage")) * 100,
                "avg_cost": to_f(h.get("avg_cost")),
                "realized_pnl": to_f(h.get("realized_profit")),
                "unrealized_pnl": to_f(h.get("unrealized_profit")),
                "sold_pct": to_f(h.get("sell_amount_percentage")) * 100,
                "buy_vol": to_f(h.get("buy_volume_cur")),
                "sell_vol": to_f(h.get("sell_volume_cur")),
            })
    return sm_wallets


# ---------------------------------------------------------------------------
# Scoring & Verdict
# ---------------------------------------------------------------------------
def compute_bundle_score(cost_result, position_result, behavior_result, tag_result, bot_result, creation_result):
    """Compute bundle detection score 0-8."""
    score = 0
    signals = []

    if cost_result.get("tight_cost"):
        score += 1
        signals.append(f'TIGHT_COST ({cost_result["deviation_pct"]:.0f}% dev)')

    if position_result.get("equal_distribution_signal"):
        score += 1
        signals.append(f'EQUAL_POSITIONS ({position_result["narrow_max"]} wallets at same band)')

    if behavior_result.get("single_buy_signal"):
        score += 1
        signals.append(f'SINGLE_BUY ({behavior_result["single_buy_pct"]:.0f}%)')

    if behavior_result.get("zero_sell_signal"):
        score += 1
        signals.append(f'NO_SELL ({behavior_result["zero_sell_pct"]:.0f}%)')

    if tag_result.get("single_bot_dominance"):
        score += 1
        signals.append(f'SINGLE_BOT ({tag_result["dominant_tag"][0]} {tag_result["dominant_pct"]:.0f}%)')

    if bot_result.get("bot_buy_ratio", 0) > 70:
        score += 1
        signals.append(f'BOT_BUY_DOMINANCE ({bot_result["bot_buy_ratio"]:.0f}%)')

    # NEW: exact position matching
    if position_result.get("exact_match_signal"):
        score += 1
        signals.append(f'EXACT_POS_MATCH ({len(position_result["exact_clusters"])} clusters)')

    # NEW: wallet creation time clustering
    if creation_result.get("same_second_signal"):
        score += 1
        signals.append(f'SAME_SECOND_CREATION ({len(creation_result["same_second_clusters"])} clusters)')
    elif creation_result.get("same_hour_signal") and creation_result.get("recent_signal"):
        score += 1
        signals.append(f'SAME_HOUR+RECENT ({creation_result["max_hour_pct"]:.0f}% in 1h)')

    return score, signals


def verdict(score):
    if score >= 5:
        return "CONFIRMED BUNDLE", "Organized bot group distributing tokens across many wallets."
    elif score >= 3:
        return "SUSPICIOUS", "Some bundle-like patterns but not definitive."
    else:
        return "NATURAL", "No clear bundle pattern. Natural trading distribution."


# ---------------------------------------------------------------------------
# Printers
# ---------------------------------------------------------------------------
def print_cost_tiers(r):
    print(f"\n  {'='*60}")
    print(f"  CLUSTER 1: Entry Cost Tiers")
    print(f"  {'='*60}")
    if "error" in r:
        print(f"  {r['error']}")
        return
    print(f"  Wallets: {r['wallet_count']} | Range: ${r['cost_min']:.8f} ~ ${r['cost_max']:.8f}")
    print(f"  Mean: ${r['cost_mean']:.8f} | Deviation: {r['deviation_pct']:.1f}% {'[TIGHT]' if r['tight_cost'] else '[NATURAL]'}")
    print()
    for t in r["tiers"]:
        tag_str = " | ".join(f"{tag}({n})" for tag, n in t["top_tags"])
        print(f"  {t['name']:<15} {t['label']:<22} {t['count']:>3}w  Buy:${t['total_buy']:>8,.0f}  [{tag_str}]")


def print_position_distribution(r):
    print(f"\n  {'='*60}")
    print(f"  CLUSTER 2: Position Size Distribution")
    print(f"  {'='*60}")
    for d in r["distribution"]:
        bar = "#" * min(d["count"], 60)
        print(f"  {d['range']:<22} {d['count']:>4} ({d['pct']:>5.1f}%)  {bar}")
    if r["equal_distribution_signal"]:
        print(f"  [!] Equal-distribution pattern detected")

    # Exact position matching
    if r["exact_clusters"]:
        print(f"\n  --- Exact Position Clusters (same % to 4 decimals) ---")
        for pct, count in r["exact_clusters"][:8]:
            bar = "#" * count
            print(f"  {pct:.4f}%: {count:>3} wallets  {bar}")
        print(f"  Total wallets in exact clusters: {r['exact_total_wallets']} {'[SIGNAL]' if r['exact_match_signal'] else ''}")

    # Narrow band clusters
    if r["narrow_clusters"]:
        print(f"\n  --- Narrow Band Clusters (same 0.01% band) ---")
        for pct, count in r["narrow_clusters"][:6]:
            bar = "#" * count
            print(f"  {pct:.2f}%: {count:>3} wallets  {bar}")
        print(f"  Max in single band: {r['narrow_max']} {'[SIGNAL]' if r['narrow_signal'] else ''}")

    _print_position_distribution_analysis(r)


def _print_position_distribution_analysis(r):
    """Print descriptive interpretation of position size distribution."""
    lines = [f"\n  >>> Position Distribution Analysis <<<"]

    if r["exact_total_wallets"] >= 3:
        lines.append(f"  [!!] {r['exact_total_wallets']} wallets hold IDENTICAL position percentages — strong bundle signal: same entity split tokens evenly")
    elif r["exact_total_wallets"] >= 1:
        lines.append(f"  [!] Exact position matches found ({r['exact_total_wallets']} wallets) — possible coordinated split")

    if r["narrow_max"] >= 5:
        lines.append(f"  [!!] {r['narrow_max']} wallets clustered in same 0.01% band — suspicious uniformity, likely bot-split")
    elif r["narrow_max"] >= 3:
        lines.append(f"  [!] {r['narrow_max']} wallets in same narrow band — mild clustering, could be one entity using multiple wallets")

    dust_count = sum(d['count'] for d in r['distribution'] if 'dust' in d['range'])
    whale_count = sum(d['count'] for d in r['distribution'] if 'whale' in d['range'] or 'top' in d['range'])
    if dust_count > r.get('total', 1) * 0.7:
        lines.append(f"  Dust dominance: {dust_count}/{r.get('total', '?')} wallets are dust (<0.5%) — scattered retail, not bundled")
    if whale_count > 0:
        lines.append(f"  {whale_count} whale/top wallets — concentration risk among large holders")

    if not r["equal_distribution_signal"] and r["exact_total_wallets"] < 2 and r["narrow_max"] < 3:
        lines.append(f"  Distribution looks NATURAL — no exact matches, no tight bands")

    for line in lines:
        print(f"  {line}")


def print_creation_clusters(r):
    print(f"\n  {'='*60}")
    print(f"  CLUSTER 7: Wallet Creation Time Clustering")
    print(f"  {'='*60}")
    if "error" in r:
        print(f"  {r['error']}")
        return

    from datetime import datetime, timezone
    print(f"  Wallets with creation data: {r['total']}")
    print(f"  Recent 48h: {r['recent_48h']} ({r['recent_48h_pct']:.0f}%)  Recent 24h: {r['recent_24h']}")
    print()

    # Same-second clusters
    if r["same_second_clusters"]:
        print(f"  [!] SAME-SECOND CREATION CLUSTERS (3+ wallets):")
        for ts, count in r["same_second_clusters"]:
            dt = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            print(f"    {dt}: {count} wallets created at exact same second {'[STRONG BUNDLE]' if count >= 5 else ''}")
    else:
        print(f"  No same-second clusters (natural)")

    # Hour clusters
    print(f"\n  --- Hour-Level Clusters ---")
    print(f"  Max in single hour: {r['max_hour_count']} wallets ({r['max_hour_pct']:.0f}%) {'[SIGNAL]' if r['same_hour_signal'] else ''}")
    for ts, count in r["hour_clusters"][:5]:
        dt = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:00")
        bar = "#" * count
        print(f"  {dt}: {count:>3} wallets  {bar}")

    # Day clusters
    print(f"\n  --- Day-Level Clusters ---")
    print(f"  Max in single day: {r['max_day_count']} wallets ({r['max_day_pct']:.0f}%) {'[SIGNAL]' if r['same_day_signal'] else ''}")
    for ts, count in r["day_clusters"][:5]:
        dt = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
        bar = "#" * (count // 2) if count > 1 else "#"
        print(f"  {dt}: {count:>3} wallets  {bar}")

    if r["recent_signal"]:
        print(f"\n  [!] FRESH WALLETS: {r['recent_48h_pct']:.0f}% created within 48h = likely generated for this token")

    _print_creation_clusters_analysis(r)


def _print_creation_clusters_analysis(r):
    """Print descriptive interpretation of wallet creation time clusters."""
    lines = [f"\n  >>> Wallet Creation Time Analysis <<<"]

    # Same-second clusters
    if r["same_second_clusters"]:
        total_same_second = sum(count for _, count in r["same_second_clusters"])
        max_ss = max(count for _, count in r["same_second_clusters"])
        lines.append(f"  [!!] SAME-SECOND BUNDLE: {total_same_second} wallets created at identical timestamps across {len(r['same_second_clusters'])} moments")
        lines.append(f"       Max {max_ss} wallets at same second — physically impossible for humans, these are programmatically generated wallets")
        lines.append(f"       This is the STRONGEST bundle signal: one script created multiple wallets in the same second.")
    else:
        lines.append(f"  No same-second creation clusters — wallet creation timing looks natural")

    # Hour-level
    if r["max_hour_count"] >= 10:
        lines.append(f"  [!!] Hour-level cluster: {r['max_hour_count']} wallets in single hour — likely bot farm batch creation")
    elif r["max_hour_count"] >= 5:
        lines.append(f"  [!] Moderate hour cluster: {r['max_hour_count']} wallets in one hour — possible coordinated creation")
    else:
        lines.append(f"  Hour-level dispersion is NATURAL (max {r['max_hour_count']} in any hour)")

    # Day-level
    if r["max_day_count"] >= 15:
        lines.append(f"  [!!] Day-level cluster: {r['max_day_count']} wallets created on same day — bulk wallet generation")
    elif r["max_day_count"] >= 7:
        lines.append(f"  [!] Day cluster: {r['max_day_count']} wallets on one day — moderate concentration")

    # Recent wallet flood
    if r["recent_signal"]:
        lines.append(f"  [!!] FRESH WALLET FLOOD: {r['recent_48h_pct']:.0f}% wallets created within 48h of token launch")
        lines.append(f"       These wallets were likely generated specifically for this token — not organic existing users.")
    elif r["recent_48h"] > 5:
        lines.append(f"  {r['recent_48h']} wallets created in last 48h — some recent activity but not overwhelming")

    # Overall creation pattern
    signals_on = sum([bool(r.get("same_second_clusters")), r.get("same_hour_signal", False),
                      r.get("same_day_signal", False), r.get("recent_signal", False)])
    if signals_on >= 3:
        lines.append(f"  VERDICT: STRONG time-cluster bundle ({signals_on}/4 time signals active) — wallets farmed for this token")
    elif signals_on >= 1:
        lines.append(f"  VERDICT: Mild time anomalies ({signals_on}/4 signals) — some clustering but not definitive")
    else:
        lines.append(f"  VERDICT: Clean — no temporal bundle signals")

    for line in lines:
        print(f"  {line}")


def print_trading_behavior(r):
    print(f"\n  {'='*60}")
    print(f"  CLUSTER 3: Trading Behavior")
    print(f"  {'='*60}")
    print(f"  Total wallets: {r['total']}")
    print(f"  Single buy (1 tx):  {r['single_buy']:>3} ({r['single_buy_pct']:.0f}%) {'[SIGNAL]' if r['single_buy_signal'] else ''}")
    print(f"  Multi buy (2+ tx):  {r['multi_buy']:>3} ({r['multi_buy_pct']:.0f}%)")
    print(f"  Never sold (0%):    {r['zero_sell']:>3} ({r['zero_sell_pct']:.0f}%) {'[SIGNAL]' if r['zero_sell_signal'] else ''}")
    print(f"  Has sold (>0%):     {r['has_sold']:>3} ({r['has_sold_pct']:.0f}%)")
    print(f"  Buy tx distribution: {r['buy_tx_distribution']}")

    _print_trading_behavior_analysis(r)


def _print_trading_behavior_analysis(r):
    """Print descriptive interpretation of buy/sell behavior clusters."""
    lines = [f"\n  >>> Buy/Sell Behavior Analysis <<<"]

    total = r['total']
    single_pct = r['single_buy_pct']
    zero_sell_pct = r['zero_sell_pct']
    has_sold_pct = r['has_sold_pct']

    # Core bundle pattern: single buy + never sell
    if single_pct > 70 and zero_sell_pct > 80:
        lines.append(f"  [!!] CLASSIC BUNDLE PATTERN: {single_pct:.0f}% single-buy + {zero_sell_pct:.0f}% never-sold")
        lines.append(f"       This pattern indicates a single entity bought into N wallets and hasn't sold any —")
        lines.append(f"       typical of coordinated bundles waiting for exit liquidity before dumping together.")
    elif single_pct > 50 and zero_sell_pct > 60:
        lines.append(f"  [!] MODERATE BUNDLE SIGNAL: {single_pct:.0f}% single-buy + {zero_sell_pct:.0f}% never-sold — suspicious uniformity")

    # Healthy selling pattern
    if has_sold_pct > 40:
        lines.append(f"  Healthy selling: {has_sold_pct:.0f}% have sold some — natural profit-taking, not locked in")
    elif has_sold_pct < 15 and total > 20:
        lines.append(f"  [!] SELLING ANEMIA: only {has_sold_pct:.0f}% have sold any tokens — when they do sell, the flood could be severe")

    # Multi-buy interpretation
    multi_pct = r['multi_buy_pct']
    if multi_pct > 50:
        lines.append(f"  Multi-buy majority ({multi_pct:.0f}%): wallets buying in multiple tranches — looks like organic accumulation")
    elif multi_pct < 20:
        lines.append(f"  Near-universal single-buy ({100-multi_pct:.0f}%): each wallet bought exactly once — bot-like precision")

    # Distribution of buy counts
    dist = r['buy_tx_distribution']
    if isinstance(dist, dict):
        max_key = max(dist, key=dist.get) if dist else None
        max_val = dist.get(max_key, 0) if max_key is not None else 0
        if max_val > total * 0.5 and max_key is not None:
            lines.append(f"  Peak at {max_key} tx/wallet ({max_val}/{total} wallets) — clustered buy behavior")

    for line in lines:
        print(f"  {line}")


def print_tag_ecology(r):
    print(f"\n  {'='*60}")
    print(f"  CLUSTER 4: Tag Ecology")
    print(f"  {'='*60}")
    print(f"  Unique tag types: {r['unique_tags']} | Dominant: {r['dominant_tag'][0] if r['dominant_tag'] else 'N/A'} ({r['dominant_pct']:.0f}%)")
    if r["single_bot_dominance"]:
        print(f"  [!] Single bot dominance detected")
    print()
    for tag, count in r["tag_breakdown"]:
        bar = "#" * count
        print(f"  {tag:<25} {count:>3}  {bar}")


def print_bot_clusters(r):
    print(f"\n  {'='*60}")
    print(f"  CLUSTER 5: Bot vs Human")
    print(f"  {'='*60}")
    print(f"  {'':<18} {'Count':>6} {'Avg Pos':>8} {'Avg Cost':>14} {'Total Buy':>10}")
    print(f"  {'Bot wallets':<18} {r['bot_count']:>6} {r['bot_avg_position']:>7.2f}% ${r['bot_avg_cost']:>12.8f} ${r['bot_total_buy']:>9,.0f}")
    print(f"  {'Human wallets':<18} {r['human_count']:>6} {r['human_avg_position']:>7.2f}% ${r['human_avg_cost']:>12.8f} ${r['human_total_buy']:>9,.0f}")
    print(f"  Bot buy share: {r['bot_buy_ratio']:.0f}% {'[DOMINANT]' if r['bot_buy_ratio'] > 70 else ''}")


def print_smart_money(sm_list):
    print(f"\n  {'='*60}")
    print(f"  CLUSTER 6: Smart Money in Top 100")
    print(f"  {'='*60}")
    if not sm_list:
        print(f"  No smart money wallets in top 100 holders")
        return
    for sm in sm_list:
        addr = sm["address"][:10] + ".." + sm["address"][-4:]
        print(f"  {addr}  hold:{sm['hold_pct']:.2f}%  cost:${sm['avg_cost']:.8f}  "
              f"rPnL:${sm['realized_pnl']:.0f}  uPnL:${sm['unrealized_pnl']:.0f}  "
              f"sold:{sm['sold_pct']:.0f}%")


def print_verdict(score, signals, verdict_label, description):
    print(f"\n  {'='*60}")
    print(f"  BUNDLE DETECTION VERDICT")
    print(f"  {'='*60}")
    print(f"  Score: {score}/8")
    if signals:
        print(f"  Signals:")
        for s in signals:
            print(f"    [!] {s}")
    print(f"  Verdict: {verdict_label}")
    print(f"  {description}")

    # Comparison table (ASCII-only for cross-platform compatibility)
    print(f"""
  Reference comparison (8-dimension scoring):
  +-----------------+------+------+------+------+------+------+------+
  |                 | Cost | Pos  |Buy1tx|0Sell | Tags |Exact%|Creat |
  +-----------------+------+------+------+------+------+------+------+
  | WOWS (bundled)  | MED  | TIGHT| 60%  | 86%  | MIX  |  NO  |  NO  |
  | PEPTIDEPAY(bun) | TIGHT| TIGHT| 100% | 100% | BOT  | YES  | YES  |
  | VENIS (mixed)   | TIGHT| MED  | MIX  | MIX  | BOT  |  NO  |  NO  |
  | WILL (natural)  | LOOSE| WIDE | 31%  | 64%  | DIV  |  NO  |  NO  |
  +-----------------+------+------+------+------+------+------+------+
  TIGHT=uniform LOOSE=dispersed DIV=diverse
  Exact%=identical position%s  Creat=same-second wallet creation
""")

    _print_verdict_analysis(score, signals, verdict_label)


def _print_verdict_analysis(score, signals, verdict_label):
    """Print detailed interpretation of bundle detection dimensions and their combined meaning."""
    lines = [f"\n  >>> Bundle Dimension Interpretation <<<"]

    # Dimension explanation
    dims = {
        "TIGHT_COST": "Entry cost uniformity — if all wallets bought at near-identical price, suggests one planner executing one strategy",
        "BOT_BUY_DOMINANCE": "Bot dominance in buy volume (>70%) — bot-tagged wallets dominate buying, vs human/retail participation",
        "SINGLE_BUY": "Single-buy wallets — each wallet made exactly one purchase, characteristic of a split-and-hold bundle",
        "ZERO_SELL": "Zero-sell wallets — no selling activity, bundled wallets waiting for coordinated exit",
        "SINGLE_BOT_DOMINANCE": "Single bot tag dominates — one bot platform label appears on most wallets, suggesting same operator",
        "EXACT_MATCH": "Identical position percentages — wallets hold exactly the same %, token was split evenly by script",
        "SAME_SECOND": "Same-second wallet creation — wallets created at identical timestamps, physically impossible for humans",
    }

    matching_signals = []
    for sig_name in signals:
        for key, desc in dims.items():
            if key in sig_name.upper() or key.replace("_", " ") in sig_name.upper():
                matching_signals.append((key, desc))
                break

    if matching_signals:
        lines.append(f"  Active signal breakdown:")
        for name, desc in matching_signals[:8]:
            lines.append(f"    [{name}]: {desc}")
        lines.append("")

    # Overall interpretation
    if score >= 5:
        lines.append(f"  VERDICT: CONFIRMED BUNDLE (score {score}/8)")
        lines.append(f"  Multiple dimensions are triggered simultaneously: cost uniformity + behavior uniformity +")
        lines.append(f"  wallet creation patterns all align. This is a coordinated multi-wallet operation.")
        lines.append(f"  Trading implication: expect synchronized dumps. Track these wallets for exit timing.")
    elif score >= 3:
        lines.append(f"  VERDICT: SUSPICIOUS (score {score}/8)")
        lines.append(f"  Several dimensions show anomalies but not all align. Could be partial bundle or")
        lines.append(f"  semi-coordinated group. Watch for sudden simultaneous selling across flagged wallets.")
    elif score >= 1:
        lines.append(f"  VERDICT: MOSTLY NATURAL (score {score}/8)")
        lines.append(f"  Minor signals detected but insufficient for bundle confirmation. Likely normal trading")
        lines.append(f"  with some bot activity (common on Pump.fun). Standard caution advised.")
    else:
        lines.append(f"  VERDICT: CLEAN (score 0/8)")
        lines.append(f"  No bundle signals detected across all 8 dimensions. Wallet distribution, cost basis,")
        lines.append(f"  trading behavior, creation timing, and tag ecology all appear organic and diverse.")

    for line in lines:
        print(f"  {line}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Wallet cluster & bundle detection analysis")
    parser.add_argument("address", help="Token contract address")
    parser.add_argument("--chain", default="sol", help="Chain: sol/bsc/base/eth")
    args = parser.parse_args()

    chain = args.chain
    addr = args.address

    print(f"\n{'#'*60}")
    print(f"#  WALLET CLUSTER ANALYSIS")
    print(f"#  Token: {addr[:12]}...")
    print(f"{'#'*60}")

    # Fetch data
    print("[1/2] Fetching top-100 holders...")
    holders_data = run_gmgn(["token", "holders", "--chain", chain, "--address", addr,
                             "--limit", "100", "--order-by", "amount_percentage", "--direction", "desc"])
    holders = holders_data.get("list", [])
    if not holders:
        print("  No holder data. Aborting.")
        sys.exit(1)
    print(f"  Got {len(holders)} holders")

    # Token info for context
    print("[2/2] Fetching token info...")
    info_data = run_gmgn(["token", "info", "--chain", chain, "--address", addr])
    info = info_data if info_data else {}
    dev = info.get("dev", {})
    wts = info.get("wallet_tags_stat", {})
    price = to_f(info.get("price"))
    supply = to_f(info.get("circulating_supply"))

    if info:
        print(f"  Token: {info.get('symbol','?')} | Price: ${price:.6f} | MCap: ${price*supply:,.0f}")
        print(f"  Creator: {dev.get('creator_token_status','?')} | "
              f"Bundlers: {wts.get('bundler_wallets','?')} | "
              f"Bundler vol: {to_f(info.get('stat',{}).get('top_bundler_trader_percentage',0))*100:.1f}%")

    # Run all cluster analyses
    cost_result = analyze_cost_tiers(holders)
    position_result = analyze_position_distribution(holders)
    behavior_result = analyze_trading_behavior(holders)
    tag_result = analyze_tag_ecology(holders)
    bot_result = analyze_bot_clusters(holders)
    sm_list = analyze_smart_money(holders)
    creation_result = analyze_wallet_creation_clusters(holders)

    # Print results
    print_cost_tiers(cost_result)
    print_position_distribution(position_result)
    print_trading_behavior(behavior_result)
    print_creation_clusters(creation_result)
    print_tag_ecology(tag_result)
    print_bot_clusters(bot_result)
    print_smart_money(sm_list)

    # Score & Verdict
    score, signals = compute_bundle_score(
        cost_result, position_result, behavior_result, tag_result, bot_result, creation_result
    )
    verdict_label, description = verdict(score)
    print_verdict(score, signals, verdict_label, description)


if __name__ == "__main__":
    main()
