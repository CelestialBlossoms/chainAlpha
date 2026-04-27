#!/usr/bin/env python3
"""
Monitor 1h trending tokens by storing each processed Top100 holder snapshot as JSON.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from psycopg2.extras import Json

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from config import TG_BOT_TOKEN, TG_CHAT_ID
from db_client import db_op


CHAIN = "sol"
TREND_INTERVAL = os.getenv("BOTTOM_TREND_INTERVAL", "1h")
TREND_LIMIT = int(os.getenv("BOTTOM_TREND_LIMIT", "100"))
DEFAULT_INTERVAL_SEC = int(os.getenv("BOTTOM_SCAN_INTERVAL", "3600"))
TOP_HOLDER_LIMIT = int(os.getenv("BOTTOM_TOP_HOLDER_LIMIT", "100"))
RECENT_COMPARE_LIMIT = int(os.getenv("BOTTOM_RECENT_COMPARE_LIMIT", "10"))
MIN_SNAPSHOT_INTERVAL_SEC = int(os.getenv("BOTTOM_MIN_SNAPSHOT_INTERVAL_SEC", "3000"))
MIN_MCAP_USD = float(os.getenv("BOTTOM_MIN_MCAP_USD", "40000"))
MIN_TOKEN_AGE_SEC = int(os.getenv("BOTTOM_MIN_TOKEN_AGE_SEC", str(5 * 3600)))
MIN_FEE_SOL = float(os.getenv("BOTTOM_MIN_FEE_SOL", "10"))

MIN_ACCUMULATED_PCT_DELTA = float(os.getenv("BOTTOM_MIN_ACCUM_PCT_DELTA", "0.015"))
MIN_DISTRIBUTED_PCT_DELTA = float(os.getenv("BOTTOM_MIN_DISTRIB_PCT_DELTA", "0.015"))
MIN_ROTATION_PCT = float(os.getenv("BOTTOM_MIN_ROTATION_PCT", "0.02"))
MIN_NETFLOW_USD = float(os.getenv("BOTTOM_MIN_NETFLOW_USD", "5000"))
MIN_SIGNAL_SCORE = int(os.getenv("BOTTOM_MIN_SIGNAL_SCORE", "60"))

SOL_CA_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,50}$")


def now_ts() -> int:
    return int(time.time())


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def to_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def valid_sol_ca(address: str) -> bool:
    return bool(SOL_CA_RE.match(address or ""))


def gmgn_command_prefix() -> list[str]:
    executable = shutil.which("gmgn-cli") or shutil.which("gmgn-cli.cmd") or shutil.which("gmgn-cli.ps1")
    if not executable:
        return ["gmgn-cli"]
    if executable.lower().endswith(".ps1"):
        return ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", executable]
    return [executable]


def run_gmgn(args: list[str], timeout: int = 75) -> dict[str, Any] | list[Any] | None:
    cmd = [*gmgn_command_prefix(), *args, "--raw"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", timeout=timeout)
    except Exception as exc:
        print(f"gmgn exception: {' '.join(cmd)} -> {exc}")
        return None
    if result.returncode != 0:
        err = (result.stderr or result.stdout or "").strip()
        print(f"gmgn failed rc={result.returncode}: {' '.join(cmd)}")
        if err:
            print(err[:500])
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        print(f"gmgn json decode failed: {exc}")
        return None


def token_address(row: dict[str, Any]) -> str:
    return str(row.get("address") or row.get("token_address") or row.get("ca") or "").strip()


def holder_key(holder: dict[str, Any]) -> str:
    return str(holder.get("address") or holder.get("wallet_address") or "").strip()


def is_pool_holder(holder: dict[str, Any]) -> bool:
    return to_int(holder.get("addr_type")) == 2 or "pool" in str(holder.get("exchange") or "").lower()


def calc_mcap(row: dict[str, Any]) -> float:
    for key in ("market_cap", "usd_market_cap", "mcap", "fdv", "fully_diluted_valuation"):
        value = to_float(row.get(key))
        if value > 0:
            return value
    price = to_float(row.get("price"))
    supply = to_float(row.get("circulating_supply") or row.get("total_supply") or row.get("supply"))
    return price * supply if price > 0 and supply > 0 else 0.0


def first_value(row: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def token_created_ts(row: dict[str, Any]) -> int:
    value = first_value(
        row,
        (
            "created_at",
            "creation_timestamp",
            "created_timestamp",
            "create_timestamp",
            "open_timestamp",
            "launch_timestamp",
            "pool_creation_timestamp",
            "pair_created_at",
        ),
    )
    ts = to_int(value)
    return ts // 1000 if ts > 10_000_000_000 else ts


def token_age_sec(row: dict[str, Any]) -> int:
    created_ts = token_created_ts(row)
    return now_ts() - created_ts if created_ts > 0 else 0


def fee_sol(row: dict[str, Any]) -> float:
    value = first_value(
        row,
        (
            "fee_sol",
            "fees_sol",
            "total_fee_sol",
            "fee",
            "fees",
            "total_fee",
            "tx_fee_sol",
        ),
    )
    fee = to_float(value)
    return fee / 1_000_000_000 if fee > 1_000_000 else fee


def token_filter_reason(row: dict[str, Any]) -> str | None:
    mcap = calc_mcap(row)
    if mcap < MIN_MCAP_USD:
        return f"市值${mcap:,.0f}<{MIN_MCAP_USD:,.0f}"
    age = token_age_sec(row)
    if age and age < MIN_TOKEN_AGE_SEC:
        return f"创建{age / 3600:.1f}h<{MIN_TOKEN_AGE_SEC / 3600:.1f}h"
    fee = fee_sol(row)
    if fee < MIN_FEE_SOL:
        return f"手续费{fee:.2f}SOL<{MIN_FEE_SOL:.2f}SOL"
    return None


def fetch_trending_tokens() -> list[dict[str, Any]]:
    data = run_gmgn(
        ["market", "trending", "--chain", CHAIN, "--interval", TREND_INTERVAL, "--limit", str(TREND_LIMIT)]
    )
    if not isinstance(data, dict):
        return []
    rows = data.get("data", {}).get("rank") or data.get("rank") or data.get("list") or []
    tokens = []
    seen = set()
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        address = token_address(row)
        if not valid_sol_ca(address) or address in seen:
            continue
        seen.add(address)
        tokens.append(row)
    return tokens


def fetch_top100_holders(address: str) -> list[dict[str, Any]]:
    data = run_gmgn(
        [
            "token",
            "holders",
            "--chain",
            CHAIN,
            "--address",
            address,
            "--limit",
            str(TOP_HOLDER_LIMIT),
            "--order-by",
            "amount_percentage",
            "--direction",
            "desc",
        ],
        timeout=90,
    )
    if not isinstance(data, dict):
        return []
    holders = data.get("list") or data.get("data", {}).get("list") or []
    return holders if isinstance(holders, list) else []


def normalize_holder(holder: dict[str, Any], rank_no: int) -> dict[str, Any] | None:
    wallet = holder_key(holder)
    if not wallet or is_pool_holder(holder):
        return None
    return {
        "rank": rank_no,
        "wallet": wallet,
        "hold_pct": to_float(holder.get("amount_percentage")),
        "usd_value": to_float(holder.get("usd_value")),
        "buy_volume": to_float(holder.get("buy_volume_cur")),
        "sell_volume": to_float(holder.get("sell_volume_cur")),
        "netflow": to_float(holder.get("netflow_usd")),
        "avg_cost": to_float(holder.get("avg_cost")),
        "profit": to_float(holder.get("profit")),
        "buy_count": to_int(holder.get("buy_tx_count_cur")),
        "sell_count": to_int(holder.get("sell_tx_count_cur")),
        "start_holding_at": to_int(holder.get("start_holding_at")),
        "last_active_at": to_int(holder.get("last_active_timestamp")),
        "tags": holder.get("maker_token_tags") or holder.get("tags") or [],
    }


def build_snapshot_json(token: dict[str, Any], raw_holders: list[dict[str, Any]]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    holders = []
    for rank_no, holder in enumerate(raw_holders, start=1):
        normalized = normalize_holder(holder, rank_no)
        if normalized:
            holders.append(normalized)

    summary = {
        "holder_count": len(raw_holders),
        "non_pool_count": len(holders),
        "top10_pct": sum(h["hold_pct"] for h in holders[:10]),
        "top20_pct": sum(h["hold_pct"] for h in holders[:20]),
        "top50_pct": sum(h["hold_pct"] for h in holders[:50]),
        "top100_pct": sum(h["hold_pct"] for h in holders[:100]),
        "buy_volume": sum(h["buy_volume"] for h in holders),
        "sell_volume": sum(h["sell_volume"] for h in holders),
        "netflow": sum(h["netflow"] for h in holders),
        "mcap": calc_mcap(token),
        "price": to_float(token.get("price")),
        "liquidity": to_float(token.get("liquidity") or token.get("pool_liquidity")),
        "created_ts": token_created_ts(token),
        "age_sec": token_age_sec(token),
        "fee_sol": fee_sol(token),
    }
    return summary, holders


def recent_snapshots(address: str, limit: int = RECENT_COMPARE_LIMIT) -> list[dict[str, Any]]:
    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, summary, holders, analysis
            FROM bottom_top100_snapshots
            WHERE chain=%s AND address=%s
            ORDER BY snapshot_ts DESC
            LIMIT %s
            """,
            (CHAIN, address, limit),
        )
        return [
            {"id": row[0], "summary": row[1] or {}, "holders": row[2] or [], "analysis": row[3] or {}}
            for row in cur.fetchall()
        ]

    return db_op(_op)


def latest_snapshot_ts(address: str) -> int | None:
    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            SELECT snapshot_ts
            FROM bottom_top100_snapshots
            WHERE chain=%s AND address=%s
            ORDER BY snapshot_ts DESC
            LIMIT 1
            """,
            (CHAIN, address),
        )
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else None

    return db_op(_op)


def recent_snapshot_skip_reason(address: str) -> str | None:
    latest_ts = latest_snapshot_ts(address)
    if not latest_ts:
        return None
    age = now_ts() - latest_ts
    if age < MIN_SNAPSHOT_INTERVAL_SEC:
        return f"最近快照{age / 60:.1f}m<{MIN_SNAPSHOT_INTERVAL_SEC / 60:.1f}m"
    return None


def compare_holder_sets(current_holders: list[dict[str, Any]], previous_holders: list[dict[str, Any]]) -> dict[str, Any]:
    current = {h["wallet"]: h for h in current_holders}
    previous = {h["wallet"]: h for h in previous_holders}

    accumulated_delta = 0.0
    distributed_delta = 0.0
    new_holder_pct = 0.0
    exited_holder_pct = 0.0
    netflow_delta = 0.0
    tagged_delta = 0.0
    top_buyers = []
    top_sellers = []

    for wallet, cur in current.items():
        old = previous.get(wallet)
        old_pct = to_float(old.get("hold_pct")) if old else 0.0
        delta = cur["hold_pct"] - old_pct
        buy_delta = cur["buy_volume"] - (to_float(old.get("buy_volume")) if old else 0.0)
        sell_delta = cur["sell_volume"] - (to_float(old.get("sell_volume")) if old else 0.0)
        net_delta = buy_delta - sell_delta
        netflow_delta += net_delta
        if delta > 0:
            accumulated_delta += delta
            top_buyers.append({"wallet": wallet, "pct_delta": delta, "netflow": net_delta, "tags": cur.get("tags", [])})
        elif delta < 0:
            distributed_delta += abs(delta)
            top_sellers.append({"wallet": wallet, "pct_delta": delta, "netflow": net_delta, "tags": cur.get("tags", [])})
        if not old:
            new_holder_pct += cur["hold_pct"]
        if delta > 0 and any(str(tag) in {"smart_degen", "renowned", "bundler", "rat_trader", "fresh_wallet"} for tag in cur.get("tags", [])):
            tagged_delta += delta

    for wallet, old in previous.items():
        if wallet not in current:
            exited_holder_pct += old["hold_pct"]

    rotation_score = accumulated_delta / max(distributed_delta, 0.000001)
    return {
        "accumulation_pct_delta": accumulated_delta,
        "distribution_pct_delta": distributed_delta,
        "rotation_score": rotation_score,
        "new_holder_pct": new_holder_pct,
        "exited_holder_pct": exited_holder_pct,
        "turnover_pct": new_holder_pct + exited_holder_pct,
        "netflow_usd": netflow_delta,
        "tagged_delta": tagged_delta,
        "top_buyers": sorted(top_buyers, key=lambda item: item["pct_delta"], reverse=True)[:8],
        "top_sellers": sorted(top_sellers, key=lambda item: item["pct_delta"])[:8],
    }


def analyze_snapshot_change(current_holders: list[dict[str, Any]], recent_history: list[dict[str, Any]]) -> dict[str, Any]:
    if not current_holders or not recent_history:
        return {"score": 0, "signal_type": "baseline", "reasons": ["需要历史快照"], "history_count": len(recent_history)}

    previous_holders = recent_history[0].get("holders") or []
    earliest_holders = recent_history[-1].get("holders") or []
    if not previous_holders:
        return {"score": 0, "signal_type": "baseline", "reasons": ["上一轮快照为空"], "history_count": len(recent_history)}

    last_change = compare_holder_sets(current_holders, previous_holders)
    window_change = compare_holder_sets(current_holders, earliest_holders) if earliest_holders else last_change
    historical_analyses = [snap.get("analysis") or {} for snap in recent_history]
    accumulation_hits = sum(1 for item in historical_analyses if item.get("signal_type") in {"accumulation", "rotation"})
    distribution_hits = sum(1 for item in historical_analyses if item.get("signal_type") == "distribution")

    accumulated_delta = last_change["accumulation_pct_delta"]
    distributed_delta = last_change["distribution_pct_delta"]
    turnover_pct = last_change["turnover_pct"]
    tagged_delta = last_change["tagged_delta"]
    netflow_delta = last_change["netflow_usd"]
    score = 0
    reasons = []
    signal_type = "watch"

    if accumulated_delta >= MIN_ACCUMULATED_PCT_DELTA:
        score += 30
        reasons.append(f"本轮Top100增持{accumulated_delta:.2%}")
    if netflow_delta >= MIN_NETFLOW_USD:
        score += 25
        reasons.append(f"本轮净买入${netflow_delta:,.0f}")
    if tagged_delta >= 0.005:
        score += 15
        reasons.append(f"标签钱包增持{tagged_delta:.2%}")
    if window_change["accumulation_pct_delta"] >= MIN_ACCUMULATED_PCT_DELTA * 2:
        score += 20
        reasons.append(f"近{len(recent_history)}次累计增持{window_change['accumulation_pct_delta']:.2%}")
    if window_change["netflow_usd"] >= MIN_NETFLOW_USD * 2:
        score += 15
        reasons.append(f"近{len(recent_history)}次净买入${window_change['netflow_usd']:,.0f}")
    if accumulation_hits >= 2:
        score += 10
        reasons.append(f"历史连续吸筹/换筹{accumulation_hits}次")
    if turnover_pct >= MIN_ROTATION_PCT and accumulated_delta >= distributed_delta * 0.8:
        score += 20
        signal_type = "rotation"
        reasons.append(f"换筹{turnover_pct:.2%}")
    if distributed_delta >= MIN_DISTRIBUTED_PCT_DELTA and distributed_delta > accumulated_delta * 1.3:
        signal_type = "distribution"
        score = max(score - 30, 0)
        reasons.append(f"派发{distributed_delta:.2%}")
    if distribution_hits >= 2:
        signal_type = "distribution"
        score = max(score - 20, 0)
        reasons.append(f"历史派发{distribution_hits}次")
    elif score >= MIN_SIGNAL_SCORE and signal_type != "rotation":
        signal_type = "accumulation"

    return {
        "score": min(score, 100),
        "signal_type": signal_type,
        "reasons": reasons,
        "history_count": len(recent_history),
        "accumulation_pct_delta": last_change["accumulation_pct_delta"],
        "distribution_pct_delta": last_change["distribution_pct_delta"],
        "rotation_score": last_change["rotation_score"],
        "new_holder_pct": last_change["new_holder_pct"],
        "exited_holder_pct": last_change["exited_holder_pct"],
        "netflow_usd": last_change["netflow_usd"],
        "window_accumulation_pct_delta": window_change["accumulation_pct_delta"],
        "window_distribution_pct_delta": window_change["distribution_pct_delta"],
        "window_netflow_usd": window_change["netflow_usd"],
        "accumulation_hits": accumulation_hits,
        "distribution_hits": distribution_hits,
        "top_buyers": last_change["top_buyers"],
        "top_sellers": last_change["top_sellers"],
    }


def save_snapshot(scan_id: str, token: dict[str, Any], summary: dict[str, Any], holders: list[dict[str, Any]], analysis: dict[str, Any]) -> int:
    address = token_address(token)

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO bottom_top100_snapshots (
                scan_id, chain, trend_interval, address, symbol, snapshot_ts,
                signal_type, signal_score, summary, holders, analysis, raw_token
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
            """,
            (
                scan_id,
                CHAIN,
                TREND_INTERVAL,
                address,
                token.get("symbol"),
                now_ts(),
                analysis.get("signal_type"),
                analysis.get("score", 0),
                Json(summary),
                Json(holders),
                Json(analysis),
                Json(token),
            ),
        )
        return int(cur.fetchone()[0])

    return db_op(_op)


def send_tg(text: str) -> None:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": text, "disable_web_page_preview": True},
            timeout=15,
        )
        if not resp.ok:
            print(f"tg failed: {resp.status_code} {resp.text[:200]}")
    except Exception as exc:
        print(f"tg exception: {exc}")


def signal_text(token: dict[str, Any], analysis: dict[str, Any]) -> str:
    address = token_address(token)
    return (
        f"Top100筹码异动 | ${token.get('symbol') or 'UNKNOWN'}\n"
        f"类型: {analysis['signal_type']} | 分数: {analysis['score']}\n"
        f"CA: {address}\n"
        f"市值: ${calc_mcap(token):,.0f} | 价格: {to_float(token.get('price')):.12f}\n"
        f"增持: {analysis['accumulation_pct_delta']:.2%} | 减持: {analysis['distribution_pct_delta']:.2%}\n"
        f"新进: {analysis['new_holder_pct']:.2%} | 退出: {analysis['exited_holder_pct']:.2%}\n"
        f"换筹比: {analysis['rotation_score']:.2f} | 净买入: ${analysis['netflow_usd']:,.0f}\n"
        f"近{analysis.get('history_count', 0)}次: 增持{analysis.get('window_accumulation_pct_delta', 0):.2%} | "
        f"减持{analysis.get('window_distribution_pct_delta', 0):.2%} | 净买入${analysis.get('window_netflow_usd', 0):,.0f}\n"
        f"理由: {', '.join(analysis['reasons']) or '无'}\n"
        f"https://gmgn.ai/sol/token/{address}"
    )


def should_notify(analysis: dict[str, Any]) -> bool:
    return analysis.get("score", 0) >= MIN_SIGNAL_SCORE or analysis.get("signal_type") == "distribution"


def handle_token(scan_id: str, token: dict[str, Any], notify: bool) -> bool:
    address = token_address(token)
    raw_holders = fetch_top100_holders(address)
    if not raw_holders:
        print(f"{address[:8]} no holders")
        return False
    summary, holders = build_snapshot_json(token, raw_holders)
    history = recent_snapshots(address)
    analysis = analyze_snapshot_change(holders, history)
    snapshot_id = save_snapshot(scan_id, token, summary, holders, analysis)
    print(
        f"{address[:8]} snapshot={snapshot_id} history={len(history)} "
        f"type={analysis.get('signal_type')} score={analysis.get('score')} "
        f"acc={analysis.get('accumulation_pct_delta', 0):.2%} "
        f"dist={analysis.get('distribution_pct_delta', 0):.2%} "
        f"win_acc={analysis.get('window_accumulation_pct_delta', 0):.2%}"
    )
    if notify and should_notify(analysis):
        send_tg(signal_text(token, analysis))
    return True


def scan_once(args: argparse.Namespace) -> None:
    scan_id = str(uuid.uuid4())
    tokens = fetch_trending_tokens()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] scan_id={scan_id} trending={len(tokens)}")
    processed = 0
    skipped = 0
    for token in tokens[: args.max_tokens]:
        try:
            skip_reason = token_filter_reason(token)
            if skip_reason:
                skipped += 1
                print(f"{token_address(token)[:8]} skip {skip_reason}")
                continue
            skip_reason = recent_snapshot_skip_reason(token_address(token))
            if skip_reason:
                skipped += 1
                print(f"{token_address(token)[:8]} skip {skip_reason}")
                continue
            if handle_token(scan_id, token, args.notify):
                processed += 1
        except Exception as exc:
            print(f"{token_address(token)[:8]} failed: {exc}")
        time.sleep(args.token_delay)
    print(f"scan_id={scan_id} processed={processed}/{len(tokens)} skipped={skipped}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Monitor 1h trending tokens with one JSON snapshot table.")
    parser.add_argument("--once", action="store_true", help="Run once and exit.")
    parser.add_argument("--watch", action="store_true", help="Run forever.")
    parser.add_argument("--notify", action="store_true", help="Send Telegram messages for new signals.")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL_SEC, help="Watch interval seconds.")
    parser.add_argument("--max-tokens", type=int, default=TREND_LIMIT)
    parser.add_argument("--token-delay", type=float, default=0.5, help="Delay between holder calls.")
    parser.add_argument("--min-snapshot-minutes", type=float, default=MIN_SNAPSHOT_INTERVAL_SEC / 60, help="Skip same CA if latest snapshot is newer than this many minutes.")
    parser.add_argument("--min-mcap", type=float, default=MIN_MCAP_USD, help="Skip tokens below this market cap in USD.")
    parser.add_argument("--min-age-hours", type=float, default=MIN_TOKEN_AGE_SEC / 3600, help="Skip tokens younger than this many hours.")
    parser.add_argument("--min-fee-sol", type=float, default=MIN_FEE_SOL, help="Skip tokens below this SOL fee value.")
    return parser


def main() -> None:
    global MIN_MCAP_USD, MIN_TOKEN_AGE_SEC, MIN_FEE_SOL, MIN_SNAPSHOT_INTERVAL_SEC
    args = build_parser().parse_args()
    MIN_SNAPSHOT_INTERVAL_SEC = int(args.min_snapshot_minutes * 60)
    MIN_MCAP_USD = args.min_mcap
    MIN_TOKEN_AGE_SEC = int(args.min_age_hours * 3600)
    MIN_FEE_SOL = args.min_fee_sol
    while True:
        scan_once(args)
        if args.once or not args.watch:
            break
        print(f"sleep {args.interval}s")
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
