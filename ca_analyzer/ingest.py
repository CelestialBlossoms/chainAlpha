"""
Ingest token CA analysis data into SQLite.

Fetches raw JSON from gmgn-cli endpoints and stores structured results.
Mirrors the analysis flow from run.py but persists everything to db.

Usage:
    D:/software/anaconda/envs/py312/python.exe ca_analyzer/ingest.py <CA>
    D:/software/anaconda/envs/py312/python.exe ca_analyzer/ingest.py <CA> --modules all
    D:/software/anaconda/envs/py312/python.exe ca_analyzer/ingest.py <CA> --db data/tokens.db
"""
import argparse
import json
import shutil
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB_DEFAULT = str(ROOT / "data" / "tokens.db")

# Ensure ca_analyzer is importable
sys.path.insert(0, str(ROOT / "ca_analyzer"))
from db import set_db_path, init_db, upsert_token, upsert_holders, upsert_pnl_summary
from db import upsert_kline_candles, upsert_kline_analysis, upsert_cluster_analysis
from db import log_ingest, update_ingest_log


# ---------------------------------------------------------------------------
# gmgn-cli helpers
# ---------------------------------------------------------------------------

def _gmgn_exe():
    exe = shutil.which("gmgn-cli") or shutil.which("gmgn-cli.cmd") or "gmgn-cli"
    return [exe]


def run_gmgn(args_list: list, timeout: int = 45) -> dict:
    cmd = _gmgn_exe() + args_list + ["--raw"]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8",
                           errors="replace", timeout=timeout)
    except subprocess.TimeoutExpired:
        print(f"  [TIMEOUT] gmgn-cli {' '.join(args_list[:4])}")
        return {}
    if r.returncode != 0:
        stderr = r.stderr.strip()
        if "429" in stderr or "RATE_LIMIT" in stderr:
            print(f"  [RATE LIMITED] {stderr[:200]}")
        elif stderr:
            print(f"  [ERROR] {stderr[:300]}")
        return {}
    try:
        return json.loads(r.stdout)
    except json.JSONDecodeError:
        print(f"  [JSON ERROR] {r.stdout[:200]}")
        return {}


def to_f(v, default=0.0):
    try:
        if v in (None, ""): return default
        return float(v)
    except (ValueError, TypeError):
        return default


# ---------------------------------------------------------------------------
# Module 1: Token Info + Security + Pool
# ---------------------------------------------------------------------------

def ingest_token_info(chain: str, address: str):
    print("[1/5] Fetching token info + security...")
    info = run_gmgn(["token", "info", "--chain", chain, "--address", address])
    if not info:
        print("  [FAIL] Token info fetch failed")
        return False
    time.sleep(0.5)
    security = run_gmgn(["token", "security", "--chain", chain, "--address", address])
    upsert_token(address, info, security or {})
    print(f"  [OK] Token: {info.get('symbol')} ({info.get('name')}) | MCap: ${to_f(info.get('price')) * to_f(info.get('circulating_supply')):,.0f}")
    return True


# ---------------------------------------------------------------------------
# Module 2: Holders + Traders + PnL
# ---------------------------------------------------------------------------

def ingest_holders_and_pnl(chain: str, address: str):
    print("[2/5] Fetching holders + traders + PnL...")

    # Top-100 holders by supply %
    holders_raw = run_gmgn([
        "token", "holders", "--chain", chain, "--address", address,
        "--limit", "100", "--order-by", "amount_percentage", "--direction", "desc"
    ])
    holders_list = holders_raw.get("list", []) if holders_raw else []
    print(f"  Holders fetched: {len(holders_list)}")
    if holders_list:
        upsert_holders(address, holders_list, "holder")

    time.sleep(1)

    # Top-100 traders by profit
    traders_raw = run_gmgn([
        "token", "traders", "--chain", chain, "--address", address,
        "--limit", "100", "--order-by", "profit", "--direction", "desc"
    ])
    traders_list = traders_raw.get("list", []) if traders_raw else []
    print(f"  Traders fetched: {len(traders_list)}")
    if traders_list:
        upsert_holders(address, traders_list, "trader")

    # ---- PnL Analysis ----
    def analyze_wallets(wallets, exclude_pool=True):
        r = {
            "total": 0, "wallets_with_balance": 0, "wallets_exited": 0,
            "pool_addresses": 0,
            "total_profit": 0.0, "total_realized_profit": 0.0,
            "total_unrealized_profit": 0.0,
            "total_cost": 0.0, "total_buy_vol": 0.0, "total_sell_vol": 0.0,
            "profitable_count": 0, "losing_count": 0, "breakeven_count": 0,
            "sum_profitable_profit": 0.0, "sum_losing_loss": 0.0,
            "realized_profitable_count": 0, "realized_losing_count": 0,
            "unrealized_profitable_count": 0, "unrealized_losing_count": 0,
        }
        for w in wallets:
            r["total"] += 1
            if exclude_pool and w.get("addr_type") == 2:
                r["pool_addresses"] += 1
                continue
            balance = to_f(w.get("balance"))
            sell_pct = to_f(w.get("sell_amount_percentage"))
            if sell_pct >= 1.0 or balance == 0:
                r["wallets_exited"] += 1
            else:
                r["wallets_with_balance"] += 1
            profit = to_f(w.get("profit"))
            realized = to_f(w.get("realized_profit"))
            unrealized = to_f(w.get("unrealized_profit"))
            total_cost = to_f(w.get("total_cost"))
            buy_vol = to_f(w.get("buy_volume_cur"))
            sell_vol = to_f(w.get("sell_volume_cur"))
            r["total_profit"] += profit
            r["total_realized_profit"] += realized
            r["total_unrealized_profit"] += unrealized
            r["total_cost"] += total_cost
            r["total_buy_vol"] += buy_vol
            r["total_sell_vol"] += sell_vol
            if profit > 0:
                r["profitable_count"] += 1
                r["sum_profitable_profit"] += profit
            elif profit < 0:
                r["losing_count"] += 1
                r["sum_losing_loss"] += profit
            else:
                r["breakeven_count"] += 1
            if realized > 0:
                r["realized_profitable_count"] += 1
            elif realized < 0:
                r["realized_losing_count"] += 1
            if unrealized > 0:
                r["unrealized_profitable_count"] += 1
            elif unrealized < 0:
                r["unrealized_losing_count"] += 1
        # Derived
        r["overall_roi_pct"] = (r["total_profit"] / r["total_cost"] * 100) if r["total_cost"] > 0 else 0
        if r["profitable_count"] + r["losing_count"] > 0:
            r["win_rate_pct"] = r["profitable_count"] / (r["profitable_count"] + r["losing_count"]) * 100
        else:
            r["win_rate_pct"] = 0
        r["profit_loss_ratio"] = abs(r["sum_profitable_profit"] / r["sum_losing_loss"]) if r["sum_losing_loss"] != 0 else 0
        return r

    def build_distribution(wallets):
        active = [w for w in wallets if w.get("addr_type") != 2]
        profits = [to_f(w.get("profit")) for w in active]
        if not profits:
            return []
        buckets_def = [
            (" < -$1000", lambda p: p < -1000),
            ("-$1000 ~ -$100", lambda p: -1000 <= p < -100),
            ("-$100 ~ -$10", lambda p: -100 <= p < -10),
            (" -$10 ~ $0", lambda p: -10 <= p < 0),
            ("   $0", lambda p: p == 0),
            ("  $0 ~ $10", lambda p: 0 < p <= 10),
            (" $10 ~ $100", lambda p: 10 < p <= 100),
            ("$100 ~ $1000", lambda p: 100 < p <= 1000),
            ("$1000 ~ $10000", lambda p: 1000 < p <= 10000),
            (" > $10000", lambda p: p > 10000),
        ]
        return [{"label": name, "count": sum(1 for p in profits if fn(p)),
                 "pct": round(sum(1 for p in profits if fn(p)) / len(profits) * 100, 1)}
                for name, fn in buckets_def]

    holders_r = analyze_wallets(holders_list)
    traders_r = analyze_wallets(traders_list)

    upsert_pnl_summary(address, "holders", holders_r, build_distribution(holders_list))
    upsert_pnl_summary(address, "traders", traders_r, build_distribution(traders_list))

    print(f"  [OK] Holders PnL: ${holders_r['total_profit']:,.0f} total, Win rate: {holders_r['win_rate_pct']:.1f}%")
    print(f"  [OK] Traders PnL: ${traders_r['total_profit']:,.0f} total, Win rate: {traders_r['win_rate_pct']:.1f}%")
    return True


# ---------------------------------------------------------------------------
# Module 3: K-line
# ---------------------------------------------------------------------------

def ingest_kline(chain: str, address: str, resolution: str = "5m",
                 lookback_hours: int = 24):
    print(f"[3/5] Fetching {resolution} K-line data ({lookback_hours}h lookback)...")
    now = int(time.time())
    start = now - lookback_hours * 3600
    data = run_gmgn([
        "market", "kline", "--chain", chain, "--address", address,
        "--resolution", resolution, "--from", str(start), "--to", str(now)
    ], timeout=30)

    if not data:
        print("  [SKIP] No K-line data")
        return False

    rows = data.get("list") or data.get("data", {}).get("list") or []
    candles = []
    for row in (rows if isinstance(rows, list) else []):
        if not isinstance(row, dict):
            continue
        raw_ts = int(to_f(row.get("time") or row.get("timestamp") or row.get("t")))
        ts = raw_ts // 1000 if raw_ts > 10_000_000_000 else raw_ts
        close = to_f(row.get("close") or row.get("c"))
        if ts <= 0 or close <= 0:
            continue
        candles.append({
            "ts": ts,
            "open": to_f(row.get("open") or row.get("o"), close),
            "high": to_f(row.get("high") or row.get("h"), close),
            "low": to_f(row.get("low") or row.get("l"), close),
            "close": close,
            "volume": to_f(row.get("volume") or row.get("v")),
        })
    candles.sort(key=lambda c: c["ts"])

    if len(candles) < 3:
        print(f"  [SKIP] Only {len(candles)} candles")
        return False

    upsert_kline_candles(address, resolution, candles)
    print(f"  [OK] {len(candles)} candles stored")

    # ---- K-line Analysis ----
    opens = [c["open"] for c in candles]
    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    volumes = [c["volume"] for c in candles]

    ath = max(highs)
    atl = min(lows)
    ath_idx = highs.index(ath)
    total_vol = sum(volumes)

    post_ath_lows = [lows[i] for i in range(ath_idx, len(lows))]
    max_dd = (ath - min(post_ath_lows)) / ath * 100 if ath > 0 else 0

    green = sum(1 for c in candles if c["close"] > c["open"])
    red = sum(1 for c in candles if c["close"] < c["open"])

    def sma(data, period):
        if len(data) >= period:
            return sum(data[-period:]) / period
        return sum(data) / len(data)

    sma20_val = sma(closes, 20)
    sma50_val = sma(closes, 50)

    half = len(volumes) // 2
    recent_avg = sum(volumes[-10:]) / 10 if len(volumes) >= 10 else sum(volumes) / len(volumes)
    early_avg = sum(volumes[:10]) / 10 if len(volumes) >= 10 else sum(volumes) / len(volumes)
    vol_ratio = recent_avg / early_avg if early_avg > 0 else 1

    recent_high = max(highs[-20:]) if len(highs) >= 20 else max(highs)
    recent_low = min(lows[-20:]) if len(lows) >= 20 else min(lows)

    # Trend
    last_close = closes[-1]
    if last_close < sma20_val and last_close < sma50_val:
        trend = "BEARISH (below both SMAs)"
    elif last_close > sma20_val and last_close < sma50_val:
        trend = "NEUTRAL (above SMA-20, below SMA-50)"
    elif last_close > sma50_val:
        trend = "BULLISH (above SMA-50)"
    else:
        trend = "MIXED"

    # Phases
    phases = []
    n = len(candles)
    if n >= 6:
        end_p1 = min(3, n // 4)
        if end_p1 > 0:
            phases.append({
                "name": "Launch Pump",
                "from": candles[0]["close"], "to": candles[end_p1 - 1]["close"],
            })
        dump_idx = end_p1
        min_close = closes[end_p1]
        for i in range(end_p1, ath_idx):
            if closes[i] < min_close:
                min_close = closes[i]
                dump_idx = i
        if dump_idx > end_p1:
            phases.append({
                "name": "First Dump",
                "from": closes[end_p1], "to": closes[dump_idx],
            })
        p3_start_idx = max(dump_idx + 1, end_p1 + 1)
        if ath_idx > p3_start_idx:
            phases.append({
                "name": "ATH Run",
                "from": closes[p3_start_idx], "to": ath,
            })
        if ath_idx < n - 3:
            post = [closes[i] for i in range(ath_idx, n)]
            crash_end = min(post)
            if crash_end < ath * 0.85:
                phases.append({
                    "name": "Post-ATH Crash",
                    "from": ath, "to": crash_end,
                })
        last_10 = closes[-12:] if n >= 12 else closes[-5:]
        phases.append({
            "name": "Consolidation (Current)",
            "from": min(last_10), "to": max(last_10),
        })

    k_result = {
        "candle_count": len(candles),
        "first_open": candles[0]["open"],
        "ath": ath, "ath_idx": ath_idx, "atl": atl, "last_close": last_close,
        "total_change_pct": (last_close - candles[0]["open"]) / candles[0]["open"] * 100 if candles[0]["open"] > 0 else 0,
        "ath_gain": ath / candles[0]["open"] if candles[0]["open"] > 0 else 0,
        "max_drawdown_pct": max_dd,
        "total_volume": total_vol,
        "avg_volume": total_vol / len(candles),
        "max_volume": max(volumes),
        "vol_ratio": vol_ratio,
        "green_candles": green, "red_candles": red,
        "green_ratio": green / len(candles),
        "sma20": sma20_val, "sma50": sma50_val,
        "resistance": recent_high, "support": recent_low,
        "trend": trend, "phases": phases,
    }
    upsert_kline_analysis(address, resolution, k_result)
    print(f"  [OK] K-line analysis: {trend} | ATH: ${ath:.8f} | Change: {k_result['total_change_pct']:.0f}%")
    return True


# ---------------------------------------------------------------------------
# Module 4: Wallet Cluster Analysis
# ---------------------------------------------------------------------------

def ingest_cluster(chain: str, address: str):
    print("[4/5] Fetching wallet cluster data...")
    holders_raw = run_gmgn([
        "token", "holders", "--chain", chain, "--address", address,
        "--limit", "100", "--order-by", "amount_percentage", "--direction", "desc"
    ])
    holders_list = holders_raw.get("list", []) if holders_raw else []
    if not holders_list:
        print("  [SKIP] No holder data for cluster analysis")
        return False

    active = [h for h in holders_list if h.get("addr_type") != 2]
    print(f"  Active holders: {len(active)}")

    # ---- Cost Tiers ----
    costs = []
    for h in active:
        avg_cost = to_f(h.get("avg_cost"))
        balance = to_f(h.get("balance"))
        buy_vol = to_f(h.get("buy_volume_cur"))
        tags = h.get("tags", [])
        if avg_cost > 0 and balance > 0:
            costs.append({"cost": avg_cost, "buy_vol": buy_vol, "tags": tags})

    cost_tier_mean = sum(c["cost"] for c in costs) / len(costs) if costs else 0
    cost_deviation = (sum(abs(c["cost"] - cost_tier_mean) for c in costs) / len(costs)) / cost_tier_mean * 100 if cost_tier_mean > 0 else 0

    # Tier breakdown
    tiers = defaultdict(lambda: {"count": 0, "buy_vol": 0, "tags": []})
    for c in costs:
        v = c["cost"]
        if v < 1e-7:
            key = "<$0.0000001"
        elif v < 2e-7:
            key = "$0.0000001-0.0000002"
        elif v < 5e-7:
            key = "$0.0000002-0.0000005"
        elif v < 1e-6:
            key = "$0.0000005-0.0000010"
        elif v < 2e-6:
            key = "$0.0000010-0.0000020"
        else:
            key = "$0.0000020+"
        tiers[key]["count"] += 1
        tiers[key]["buy_vol"] += c["buy_vol"]
        for t in c["tags"][:3]:
            tiers[key]["tags"].append(t)

    cost_tiers_list = [{"tier": k, "count": v["count"], "buy_vol": v["buy_vol"],
                        "tags": list(set(v["tags"]))}
                       for k, v in sorted(tiers.items())]

    # ---- Position Distribution ----
    pos_bins = {"dust (<0.5%)": 0, "small (0.5-1%)": 0, "mid (1-1.5%)": 0,
                "large (1.5-2%)": 0, "whale (2-2.6%)": 0, "top (2.6%+)": 0}
    for h in active:
        pct = to_f(h.get("amount_percentage")) * 100
        if pct < 0.5:
            pos_bins["dust (<0.5%)"] += 1
        elif pct < 1:
            pos_bins["small (0.5-1%)"] += 1
        elif pct < 1.5:
            pos_bins["mid (1-1.5%)"] += 1
        elif pct < 2:
            pos_bins["large (1.5-2%)"] += 1
        elif pct < 2.6:
            pos_bins["whale (2-2.6%)"] += 1
        else:
            pos_bins["top (2.6%+)"] += 1

    # Narrow band detection
    pct_bands = defaultdict(int)
    for h in active:
        band = round(to_f(h.get("amount_percentage")), 4)
        pct_bands[band] += 1
    narrow_clusters = sum(1 for c in pct_bands.values() if c >= 4)
    max_in_band = max(pct_bands.values()) if pct_bands else 0

    # ---- Trading Behavior ----
    single_buy = 0
    multi_buy = 0
    never_sold = 0
    has_sold = 0
    for h in active:
        tx_count = h.get("buy_count", 0) or h.get("buy", 0) or h.get("buy_tx_count_cur", 0) or 0
        if tx_count <= 1:
            single_buy += 1
        else:
            multi_buy += 1
        sell_amount_pct = to_f(h.get("sell_amount_percentage"))
        if sell_amount_pct == 0:
            never_sold += 1
        else:
            has_sold += 1

    total_behavior = single_buy + multi_buy
    single_buy_pct = single_buy / total_behavior * 100 if total_behavior > 0 else 0
    multi_buy_pct = multi_buy / total_behavior * 100 if total_behavior > 0 else 0
    never_sold_pct = never_sold / (never_sold + has_sold) * 100 if (never_sold + has_sold) > 0 else 0

    # ---- Bot vs Human ----
    bot_tags = {"axiom", "trojan", "padre", "fomo", "gmgn", "bullx"}
    bot_wallets = []
    human_wallets = []
    for h in active:
        tags = set(h.get("tags", [])) | set(h.get("maker_token_tags", []))
        if tags & bot_tags:
            bot_wallets.append(h)
        else:
            human_wallets.append(h)

    def avg_pos(wallets):
        return sum(to_f(w.get("amount_percentage")) for w in wallets) / len(wallets) * 100 if wallets else 0
    def avg_cost(wallets):
        costs = [to_f(w.get("avg_cost")) for w in wallets if to_f(w.get("avg_cost")) > 0]
        return sum(costs) / len(costs) if costs else 0
    def total_buy(wallets):
        return sum(to_f(w.get("buy_volume_cur")) for w in wallets)

    bot_avg_pos = avg_pos(bot_wallets)
    bot_avg_cost = avg_cost(bot_wallets)
    bot_total_buy = total_buy(bot_wallets)
    human_avg_pos = avg_pos(human_wallets)
    human_avg_cost = avg_cost(human_wallets)
    human_total_buy = total_buy(human_wallets)
    bot_buy_share = bot_total_buy / (bot_total_buy + human_total_buy) * 100 if (bot_total_buy + human_total_buy) > 0 else 0

    # ---- Tag Ecology ----
    tag_counter = defaultdict(int)
    for h in active:
        for t in (h.get("tags", []) or []):
            tag_counter[t] += 1
    tag_ecology = dict(sorted(tag_counter.items(), key=lambda x: x[1], reverse=True))

    # ---- Smart Money in Top 100 ----
    sm_count = sum(1 for h in active if "smart_degen" in (h.get("tags", []) or []))

    # ---- Creation Time Clustering ----
    creation_hours = defaultdict(int)
    creation_days = defaultdict(int)
    for h in active:
        created_at = h.get("created_at") or h.get("wallet_created_at") or 0
        if created_at and created_at > 0:
            from datetime import datetime, timezone
            dt = datetime.fromtimestamp(int(created_at), tz=timezone.utc)
            creation_hours[dt.strftime("%Y-%m-%d %H:00")] += 1
            creation_days[dt.strftime("%Y-%m-%d")] += 1

    creation_time_data = {
        "wallets_with_data": sum(1 for h in active if h.get("created_at") or h.get("wallet_created_at")),
        "recent_48h": sum(1 for h in active if (h.get("created_at") or h.get("wallet_created_at") or 0) > time.time() - 172800),
        "recent_24h": sum(1 for h in active if (h.get("created_at") or h.get("wallet_created_at") or 0) > time.time() - 86400),
        "max_in_hour": max(creation_hours.values()) if creation_hours else 0,
        "max_in_day": max(creation_days.values()) if creation_days else 0,
    }

    # ---- Bundle Verdict ----
    score = 0
    if cost_deviation < 25:
        score += 1  # tight cost basis
    if max_in_band >= 4:
        score += 1  # narrow position bands
    if single_buy_pct > 60:
        score += 1  # mostly single-buy (bot-like)
    if never_sold_pct > 80:
        score += 1  # nobody selling
    if len(set(tag_counter.keys())) < 5:
        score += 1  # low tag diversity
    if bot_buy_share > 70:
        score += 1  # bot-dominated
    if creation_time_data["max_in_hour"] >= 5:
        score += 1  # creation time clustering
    if sm_count == 0:
        score += 1  # no smart money

    if score <= 1:
        verdict = "NATURAL"
    elif score <= 3:
        verdict = "MIXED"
    elif score <= 5:
        verdict = "SUSPICIOUS"
    else:
        verdict = "BUNDLED"

    cluster_data = {
        "cost_tier_count": len(costs),
        "cost_tier_mean": cost_tier_mean,
        "cost_tier_deviation_pct": round(cost_deviation, 1),
        "cost_tiers": cost_tiers_list,
        "position_dist": pos_bins,
        "position_narrow_band_clusters": narrow_clusters,
        "position_max_in_band": max_in_band,
        "behavior_single_buy_count": single_buy,
        "behavior_multi_buy_count": multi_buy,
        "behavior_never_sold_count": never_sold,
        "behavior_has_sold_count": has_sold,
        "behavior_single_buy_pct": round(single_buy_pct, 1),
        "behavior_multi_buy_pct": round(multi_buy_pct, 1),
        "behavior_never_sold_pct": round(never_sold_pct, 1),
        "bot_wallet_count": len(bot_wallets),
        "bot_avg_position": round(bot_avg_pos, 2),
        "bot_avg_cost": bot_avg_cost,
        "bot_total_buy": bot_total_buy,
        "human_wallet_count": len(human_wallets),
        "human_avg_position": round(human_avg_pos, 2),
        "human_avg_cost": human_avg_cost,
        "human_total_buy": human_total_buy,
        "bot_buy_share_pct": round(bot_buy_share, 1),
        "tag_ecology": tag_ecology,
        "smart_money_in_top100": sm_count,
        "creation_time_clusters": creation_time_data,
        "bundle_score": score,
        "bundle_verdict": verdict,
    }
    upsert_cluster_analysis(address, cluster_data)
    print(f"  [OK] Cluster: Score {score}/8 | Verdict: {verdict} | Bots: {len(bot_wallets)}/{len(active)}")
    return True


# ---------------------------------------------------------------------------
# Module 5: Pool Analysis
# ---------------------------------------------------------------------------

def ingest_pool_analysis(chain: str, address: str):
    """Pool data is already captured in token info. This is a pass-through."""
    print("[5/5] Pool data already captured via token info")
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

MODULES = {
    "info":    ingest_token_info,
    "pnl":     ingest_holders_and_pnl,
    "kline":   ingest_kline,
    "cluster": ingest_cluster,
    "pool":    ingest_pool_analysis,
}


def main():
    parser = argparse.ArgumentParser(description="Ingest CA analysis data into SQLite")
    parser.add_argument("address", help="Token contract address")
    parser.add_argument("--chain", default="sol", help="Chain: sol/bsc/base/eth")
    parser.add_argument("--modules", default="all",
                        help="Comma-separated: info,pnl,kline,cluster,pool,all")
    parser.add_argument("--db", default=DB_DEFAULT, help="SQLite database path")
    args = parser.parse_args()

    chain = args.chain
    addr = args.address

    if args.modules == "all":
        selected = ["info", "pnl", "kline", "cluster"]
    else:
        selected = [m.strip() for m in args.modules.split(",") if m.strip() in MODULES]

    # Ensure data directory
    db_path = args.db
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    set_db_path(db_path)
    init_db()

    print(f"\n{'#'*60}")
    print(f"#  CA INGEST -> SQLite")
    print(f"#  CA: {addr}  |  Chain: {chain}")
    print(f"#  DB: {db_path}")
    print(f"#  Modules: {', '.join(selected)}")
    print(f"{'#'*60}")

    log_ingest(addr, chain, ",".join(selected), "running")

    results = {}
    try:
        for i, mod in enumerate(selected, 1):
            ok = MODULES[mod](chain, addr)
            results[mod] = ok
            if i < len(selected):
                time.sleep(0.5)

        all_ok = all(results.values())
        status = "success" if all_ok else "partial"
        update_ingest_log(addr, status)

        print(f"\n{'#'*60}")
        print(f"#  INGEST COMPLETE: {status}")
        print(f"#  Results: {', '.join(f'{m}={results[m]}' for m in selected)}")
        print(f"#  DB: {db_path}")
        print(f"{'#'*60}")

    except Exception as e:
        import traceback
        update_ingest_log(addr, "failed", str(e))
        print(f"\n  [FAIL] {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
