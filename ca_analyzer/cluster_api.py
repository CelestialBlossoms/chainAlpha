"""JSON-friendly CA cluster analysis for local dashboards and browser extensions."""

from __future__ import annotations

import json
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ca_analyzer.wallet_clusters import (
    analyze_bot_clusters,
    analyze_cost_tiers,
    analyze_position_distribution,
    analyze_tag_ecology,
    analyze_trading_behavior,
    analyze_wallet_creation_clusters,
    compute_bundle_score,
    run_gmgn,
    to_f,
    to_i,
    verdict,
)

ROOT_DIR = Path(__file__).resolve().parents[1]
PACKAGE_WALLET_MAP = ROOT_DIR / "gmgn_outputs" / "package_wallet_map.json"


def _ts_label(ts: int | float | None) -> str:
    if not ts:
        return ""
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _short(address: str) -> str:
    if not address:
        return ""
    return f"{address[:6]}...{address[-4:]}" if len(address) > 12 else address


def _load_wallet_labels() -> dict[str, dict[str, Any]]:
    if not PACKAGE_WALLET_MAP.exists():
        return {}
    try:
        data = json.loads(PACKAGE_WALLET_MAP.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _wallet_label(address: str, labels: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    item = labels.get(address)
    if not isinstance(item, dict):
        return None
    return {
        "name": item.get("name") or "",
        "groups": item.get("groups") if isinstance(item.get("groups"), list) else [],
    }


def _first_buy_ts(row: dict[str, Any]) -> int:
    for key in ("start_holding_at", "first_buy_at", "first_bought_at", "buy_at", "first_trade_at", "last_active_timestamp"):
        ts = to_i(row.get(key))
        if ts > 0:
            return ts
    return 0


def _holder_wallet(row: dict[str, Any], labels: dict[str, dict[str, Any]]) -> dict[str, Any]:
    address = str(row.get("address") or row.get("wallet_address") or "")
    tags = row.get("tags") if isinstance(row.get("tags"), list) else []
    maker_tags = row.get("maker_token_tags") if isinstance(row.get("maker_token_tags"), list) else []
    profit_change = to_f(row.get("profit_change"))
    realized_profit = to_f(row.get("realized_profit"))
    unrealized_profit = to_f(row.get("unrealized_profit"))
    first_buy_at = _first_buy_ts(row)
    created_at = to_i(row.get("created_at"))
    return {
        "address": address,
        "short": _short(address),
        "label": _wallet_label(address, labels),
        "tags": tags,
        "maker_tags": maker_tags,
        "hold_pct": to_f(row.get("amount_percentage")) * 100,
        "balance": to_f(row.get("balance")),
        "usd_value": to_f(row.get("usd_value")),
        "avg_cost": to_f(row.get("avg_cost")),
        "buy_volume": to_f(row.get("buy_volume_cur")),
        "sell_volume": to_f(row.get("sell_volume_cur")),
        "buy_tx_count": to_i(row.get("buy_tx_count_cur")),
        "sell_tx_count": to_i(row.get("sell_tx_count_cur")),
        "sold_pct": to_f(row.get("sell_amount_percentage")) * 100,
        "realized_profit": realized_profit,
        "unrealized_profit": unrealized_profit,
        "profit": to_f(row.get("profit")),
        "profit_pct": profit_change * 100,
        "created_at": created_at,
        "created_at_label": _ts_label(created_at),
        "first_buy_at": first_buy_at,
        "first_buy_at_label": _ts_label(first_buy_at),
    }


def _bucket_clusters(
    wallets: list[dict[str, Any]],
    ts_key: str,
    bucket_seconds: int,
    min_count: int,
    limit: int,
) -> list[dict[str, Any]]:
    buckets: dict[int, list[dict[str, Any]]] = {}
    for wallet in wallets:
        ts = to_i(wallet.get(ts_key))
        if ts <= 0:
            continue
        bucket = ts // bucket_seconds * bucket_seconds
        buckets.setdefault(bucket, []).append(wallet)

    clusters = []
    for bucket, items in buckets.items():
        if len(items) < min_count:
            continue
        clusters.append(
            {
                "bucket_ts": bucket,
                "bucket_label": _ts_label(bucket),
                "count": len(items),
                "wallets": sorted(items, key=lambda w: (-to_f(w.get("hold_pct")), w.get("address", "")))[:20],
            }
        )
    clusters.sort(key=lambda item: (-item["count"], item["bucket_ts"]))
    return clusters[:limit]


def _same_second_clusters(wallets: list[dict[str, Any]], ts_key: str, limit: int = 8) -> list[dict[str, Any]]:
    buckets: dict[int, list[dict[str, Any]]] = {}
    for wallet in wallets:
        ts = to_i(wallet.get(ts_key))
        if ts > 0:
            buckets.setdefault(ts, []).append(wallet)
    rows = [
        {
            "ts": ts,
            "label": _ts_label(ts),
            "count": len(items),
            "wallets": sorted(items, key=lambda w: (-to_f(w.get("hold_pct")), w.get("address", "")))[:20],
        }
        for ts, items in buckets.items()
        if len(items) >= 3
    ]
    rows.sort(key=lambda item: (-item["count"], item["ts"]))
    return rows[:limit]


def _creation_clusters(wallets: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "hour_clusters": _bucket_clusters(wallets, "created_at", 3600, 3, 10),
        "day_clusters": _bucket_clusters(wallets, "created_at", 86400, 5, 8),
        "same_second_clusters": _same_second_clusters(wallets, "created_at"),
    }


def _purchase_clusters(wallets: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "minute_clusters": _bucket_clusters(wallets, "first_buy_at", 60, 3, 10),
        "five_minute_clusters": _bucket_clusters(wallets, "first_buy_at", 300, 4, 10),
        "same_second_clusters": _same_second_clusters(wallets, "first_buy_at"),
    }


def _wallet_has_tag(wallet: dict[str, Any], tags: set[str]) -> bool:
    all_tags = {str(item).lower() for item in [*wallet.get("tags", []), *wallet.get("maker_tags", [])]}
    return bool(all_tags & tags)


def _wallet_in_label_group(wallet: dict[str, Any]) -> bool:
    label = wallet.get("label") or {}
    groups = [str(group).lower() for group in label.get("groups", [])]
    return any("bundle" in group or "bundler" in group for group in groups)


def _known_bundled_wallets(wallets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for wallet in wallets:
        tag_text = " ".join([*wallet.get("tags", []), *wallet.get("maker_tags", [])]).lower()
        label = wallet.get("label") or {}
        group_text = " ".join(str(group).lower() for group in label.get("groups", []))
        if "bundler" not in tag_text and "bundle" not in group_text:
            continue
        rows.append(wallet)
    rows.sort(key=lambda w: (-to_f(w.get("hold_pct")), -to_f(w.get("buy_volume"))))
    return rows[:30]


def _wallet_summary(name: str, wallets: list[dict[str, Any]], total_wallets: int) -> dict[str, Any]:
    count = len(wallets)
    profit_values = [to_f(wallet.get("profit_pct")) for wallet in wallets if wallet.get("profit_pct") not in (None, "")]
    return {
        "name": name,
        "count": count,
        "wallet_pct": count / max(total_wallets, 1) * 100,
        "hold_pct": sum(to_f(wallet.get("hold_pct")) for wallet in wallets),
        "buy_volume": sum(to_f(wallet.get("buy_volume")) for wallet in wallets),
        "realized_profit": sum(to_f(wallet.get("realized_profit")) for wallet in wallets),
        "unrealized_profit": sum(to_f(wallet.get("unrealized_profit")) for wallet in wallets),
        "avg_profit_pct": sum(profit_values) / len(profit_values) if profit_values else 0,
        "profitable_pct": sum(1 for value in profit_values if value > 0) / len(profit_values) * 100 if profit_values else 0,
        "wallets": sorted(wallets, key=lambda w: (-to_f(w.get("hold_pct")), -to_f(w.get("unrealized_profit"))))[:30],
    }


def _cluster_addresses(clusters: list[dict[str, Any]]) -> set[str]:
    return {
        str(wallet.get("address") or "")
        for cluster in clusters
        for wallet in cluster.get("wallets", [])
        if wallet.get("address")
    }


def _bundle_category_summary(wallets: list[dict[str, Any]], creation_clusters: dict[str, Any], purchase_clusters: dict[str, Any]) -> list[dict[str, Any]]:
    creation_addrs = _cluster_addresses(creation_clusters.get("same_second_clusters", []))
    purchase_addrs = _cluster_addresses(purchase_clusters.get("same_second_clusters", []))
    purchase_5m_addrs = _cluster_addresses(purchase_clusters.get("five_minute_clusters", []))

    categories = [
        ("GMGN\u6346\u7ed1\u6807\u7b7e", [w for w in wallets if _wallet_has_tag(w, {"bundler"})]),
        ("\u672c\u5730\u6807\u8bb0\u94b1\u5305", [w for w in wallets if _wallet_in_label_group(w)]),
        ("\u673a\u5668\u4eba\u5de5\u5177\u94b1\u5305", [w for w in wallets if _wallet_has_tag(w, {"axiom", "trojan", "photon", "bullx", "padre", "gmgn", "pepeboost"})]),
        ("\u72d9\u51fb\u94b1\u5305", [w for w in wallets if _wallet_has_tag(w, {"sniper"})]),
        ("\u65b0\u9c9c\u94b1\u5305", [w for w in wallets if _wallet_has_tag(w, {"fresh_wallet"})]),
        ("\u540c\u79d2\u521b\u5efa\u7c07", [w for w in wallets if w.get("address") in creation_addrs]),
        ("\u540c\u79d2\u8d2d\u4e70\u7c07", [w for w in wallets if w.get("address") in purchase_addrs]),
        ("5\u5206\u949f\u8d2d\u4e70\u7c07", [w for w in wallets if w.get("address") in purchase_5m_addrs]),
        ("\u672a\u5356\u51fa\u94b1\u5305", [w for w in wallets if to_f(w.get("sold_pct")) <= 0]),
    ]
    return [_wallet_summary(name, rows, len(wallets)) for name, rows in categories if rows]


def _chip_distribution(holders: list[dict[str, Any]]) -> dict[str, Any]:
    wallets = [h for h in holders if h.get("addr_type") != 2]
    holding = [h for h in wallets if to_f(h.get("balance")) > 0]

    def profit_sum(rows: list[dict[str, Any]], key: str) -> float:
        return sum(to_f(row.get(key)) for row in rows if row.get("addr_type") != 2)

    return {
        "top3_pct": sum(to_f(h.get("amount_percentage")) for h in holders[:3]) * 100,
        "top5_pct": sum(to_f(h.get("amount_percentage")) for h in holders[:5]) * 100,
        "top10_pct": sum(to_f(h.get("amount_percentage")) for h in holders[:10]) * 100,
        "top20_pct": sum(to_f(h.get("amount_percentage")) for h in holders[:20]) * 100,
        "top30_pct": sum(to_f(h.get("amount_percentage")) for h in holders[:30]) * 100,
        "top50_pct": sum(to_f(h.get("amount_percentage")) for h in holders[:50]) * 100,
        "top30_realized_profit": profit_sum(holders[:30], "realized_profit"),
        "top30_unrealized_profit": profit_sum(holders[:30], "unrealized_profit"),
        "top30_profit": profit_sum(holders[:30], "realized_profit") + profit_sum(holders[:30], "unrealized_profit"),
        "top50_realized_profit": profit_sum(holders[:50], "realized_profit"),
        "top50_unrealized_profit": profit_sum(holders[:50], "unrealized_profit"),
        "top50_profit": profit_sum(holders[:50], "realized_profit") + profit_sum(holders[:50], "unrealized_profit"),
        "holder_rows": len(holders),
        "wallet_rows": len(wallets),
        "holding_wallets": len(holding),
        "exited_wallets": len(wallets) - len(holding),
    }


def _risk_factors(score: int, signals: list[str], creation_result: dict[str, Any], behavior_result: dict[str, Any]) -> list[str]:
    factors = []
    if score >= 5:
        factors.append("\u591a\u7ef4\u5ea6\u6346\u7ed1\u7279\u5f81\u540c\u65f6\u51fa\u73b0")
    elif score >= 3:
        factors.append("\u5b58\u5728\u90e8\u5206\u6346\u7ed1\u76f8\u4f3c\u7ed3\u6784")
    if signals:
        factors.extend(signals[:5])
    if creation_result.get("same_second_signal"):
        factors.append("\u5b58\u5728\u540c\u79d2\u521b\u5efa\u94b1\u5305\u7c07")
    if behavior_result.get("single_buy_signal"):
        factors.append("\u5355\u6b21\u4e70\u5165\u94b1\u5305\u5360\u6bd4\u504f\u9ad8")
    if behavior_result.get("zero_sell_signal"):
        factors.append("\u672a\u5356\u51fa\u94b1\u5305\u5360\u6bd4\u504f\u9ad8")
    return factors


def analyze_ca_clusters(address: str, chain: str = "sol", limit: int = 100) -> dict[str, Any]:
    """Fetch GMGN holder data and return display-ready cluster analysis."""
    limit = max(20, min(int(limit or 100), 200))
    holders_data = run_gmgn(
        [
            "token",
            "holders",
            "--chain",
            chain,
            "--address",
            address,
            "--limit",
            str(limit),
            "--order-by",
            "amount_percentage",
            "--direction",
            "desc",
        ],
        timeout=75,
    )
    if not isinstance(holders_data, dict):
        return {"ok": False, "address": address, "chain": chain, "error_type": "gmgn_failed", "error": "GMGN holder request failed"}

    holders = holders_data.get("list")
    holders = holders if isinstance(holders, list) else []
    if not holders:
        return {
            "ok": False,
            "address": address,
            "chain": chain,
            "error_type": "empty_holders",
            "error": "GMGN returned an empty holder list for this CA",
        }

    info = run_gmgn(["token", "info", "--chain", chain, "--address", address], timeout=60) or {}
    labels = _load_wallet_labels()
    wallets = [_holder_wallet(row, labels) for row in holders if row.get("addr_type") != 2]
    wallets = [wallet for wallet in wallets if wallet["address"]]

    cost_result = analyze_cost_tiers(holders)
    position_result = analyze_position_distribution(holders)
    behavior_result = analyze_trading_behavior(holders)
    tag_result = analyze_tag_ecology(holders)
    bot_result = analyze_bot_clusters(holders)
    creation_result = analyze_wallet_creation_clusters(holders)
    score, signals = compute_bundle_score(cost_result, position_result, behavior_result, tag_result, bot_result, creation_result)
    verdict_label, verdict_desc = verdict(score)

    price = to_f(info.get("price"))
    supply = to_f(info.get("circulating_supply"))
    stat = info.get("stat") if isinstance(info.get("stat"), dict) else {}
    token = {
        "symbol": info.get("symbol") or "",
        "name": info.get("name") or "",
        "price": price,
        "market_cap": price * supply if price and supply else to_f(info.get("market_cap")),
        "liquidity": to_f(info.get("liquidity")),
        "holder_count": to_i(info.get("holder_count")),
        "top_bundler_trader_pct": to_f(stat.get("top_bundler_trader_percentage")) * 100,
        "top_bot_degen_pct": to_f(stat.get("top_bot_degen_percentage")) * 100,
    }

    buy_counter = Counter(wallet.get("buy_tx_count", 0) for wallet in wallets)
    creation_clusters = _creation_clusters(wallets)
    purchase_clusters = _purchase_clusters(wallets)
    return {
        "ok": True,
        "address": address,
        "chain": chain,
        "generated_at": int(time.time()),
        "generated_at_label": _ts_label(int(time.time())),
        "token": token,
        "bundle": {"score": score, "label": verdict_label, "description": verdict_desc, "signals": signals},
        "chip_distribution": _chip_distribution(holders),
        "holder_trader_structure": {
            "behavior": behavior_result,
            "bot": bot_result,
            "tag_ecology": tag_result,
            "buy_tx_distribution": dict(sorted(buy_counter.items())),
        },
        "bundle_category_summary": _bundle_category_summary(wallets, creation_clusters, purchase_clusters),
        "known_bundled_wallets": _known_bundled_wallets(wallets),
        "bundle_time_clusters": creation_clusters,
        "purchase_time_clusters": purchase_clusters,
        "position_clusters": {
            "exact_clusters": position_result.get("exact_clusters", []),
            "narrow_clusters": position_result.get("narrow_clusters", []),
            "distribution": position_result.get("distribution", []),
        },
        "cost_tiers": cost_result,
        "risk_factors": _risk_factors(score, signals, creation_result, behavior_result),
    }
