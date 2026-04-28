import argparse
import csv
import json
import shutil
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from db_client import db_op


DEFAULT_LIMIT = 20
DEFAULT_RESOLUTION = "5m"
DEFAULT_WIN_PROFIT_PCT = 30.0
DEFAULT_OUTPUT = "gmgn_outputs/alpha_push_winrate.csv"


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def to_ts(value: Any) -> int:
    if value in (None, ""):
        return 0
    if isinstance(value, datetime):
        return int(value.timestamp())
    try:
        ts = int(float(value))
        return ts // 1000 if ts > 10_000_000_000 else ts
    except (TypeError, ValueError):
        return 0


def gmgn_command_prefix() -> list[str]:
    executable = shutil.which("gmgn-cli") or shutil.which("gmgn-cli.cmd") or shutil.which("gmgn-cli.ps1")
    if not executable:
        return ["gmgn-cli"]
    if executable.lower().endswith(".ps1"):
        return ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", executable]
    return [executable]


def run_gmgn(args: list[str], timeout: int = 90) -> dict[str, Any] | list[Any] | None:
    cmd = [*gmgn_command_prefix(), *args, "--raw"]
    result = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", timeout=timeout)
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


def fetch_recent_alerts(limit: int) -> list[dict[str, Any]]:
    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                address,
                chain,
                symbol,
                mcap_at_alert,
                holder_count,
                first_seen_at,
                last_seen_at,
                alert_count,
                raw_stats
            FROM alpha_token_candidates
            ORDER BY last_seen_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
        return [
            {
                "address": row[0],
                "chain": row[1] or "sol",
                "symbol": row[2] or "UNKNOWN",
                "mcap_at_alert": to_float(row[3]),
                "holder_count": int(row[4] or 0),
                "first_seen_at": row[5],
                "last_seen_at": row[6],
                "alert_count": int(row[7] or 0),
                "raw_stats": row[8] if isinstance(row[8], dict) else {},
            }
            for row in rows
        ]

    return db_op(_op)


def extract_kline_rows(data: dict[str, Any] | list[Any] | None) -> list[dict[str, Any]]:
    if not data:
        return []
    if isinstance(data, list):
        rows = data
    else:
        rows = data.get("list") or data.get("data", {}).get("list") or data.get("data") or []
    candles = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        ts = to_ts(row.get("time") or row.get("timestamp") or row.get("t"))
        open_price = to_float(row.get("open") or row.get("o"))
        high = to_float(row.get("high") or row.get("h"))
        low = to_float(row.get("low") or row.get("l"))
        close = to_float(row.get("close") or row.get("c"))
        if ts and close > 0:
            candles.append(
                {
                    "ts": ts,
                    "open": open_price,
                    "high": high or close,
                    "low": low or close,
                    "close": close,
                    "volume": to_float(row.get("volume") or row.get("v")),
                }
            )
    candles.sort(key=lambda item: item["ts"])
    return candles


def fetch_kline(chain: str, address: str, resolution: str, from_ts: int, to_ts_value: int) -> list[dict[str, Any]]:
    data = run_gmgn(
        [
            "market",
            "kline",
            "--chain",
            chain,
            "--address",
            address,
            "--resolution",
            resolution,
            "--from",
            str(from_ts),
            "--to",
            str(to_ts_value),
        ]
    )
    return extract_kline_rows(data)


def analyze_alert(alert: dict[str, Any], resolution: str, now_ts: int) -> dict[str, Any]:
    alert_ts = to_ts(alert.get("last_seen_at"))
    raw_stats = alert.get("raw_stats") or {}
    chain = alert.get("chain") or "sol"
    address = alert["address"]
    candles = fetch_kline(chain, address, resolution, alert_ts, now_ts)
    entry_price = to_float(raw_stats.get("price"))
    if entry_price <= 0 and candles:
        entry_price = candles[0]["open"] or candles[0]["close"]
    if entry_price <= 0:
        entry_price = 0.0

    max_high = max((c["high"] for c in candles), default=0.0)
    min_low = min((c["low"] for c in candles), default=0.0)
    current_close = candles[-1]["close"] if candles else 0.0
    max_multiple = max_high / entry_price if entry_price > 0 and max_high > 0 else 0.0
    min_multiple = min_low / entry_price if entry_price > 0 and min_low > 0 else 0.0
    current_multiple = current_close / entry_price if entry_price > 0 and current_close > 0 else 0.0
    max_drawdown_pct = (min_low - entry_price) / entry_price * 100 if entry_price > 0 and min_low > 0 else 0.0
    current_pnl_pct = (current_close - entry_price) / entry_price * 100 if entry_price > 0 and current_close > 0 else 0.0
    return {
        "address": address,
        "chain": chain,
        "symbol": alert.get("symbol"),
        "alert_time": datetime.fromtimestamp(alert_ts).strftime("%Y-%m-%d %H:%M:%S") if alert_ts else "",
        "alert_hour": datetime.fromtimestamp(alert_ts).strftime("%Y-%m-%d %H:00") if alert_ts else "",
        "alert_count": alert.get("alert_count", 0),
        "entry_price": entry_price,
        "current_price": current_close,
        "max_price": max_high,
        "min_price": min_low,
        "current_multiple": current_multiple,
        "max_multiple": max_multiple,
        "min_multiple": min_multiple,
        "current_pnl_pct": current_pnl_pct,
        "max_drawdown_pct": max_drawdown_pct,
        "candles": len(candles),
        "mcap_at_alert": alert.get("mcap_at_alert"),
        "holder_count": alert.get("holder_count"),
    }


def fmt_money(value: float) -> str:
    if value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"${value / 1_000:.1f}K"
    return f"${value:.0f}"


def write_csv(path: str, rows: list[dict[str, Any]]) -> None:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "symbol",
        "address",
        "chain",
        "alert_time",
        "alert_hour",
        "alert_count",
        "entry_price",
        "current_price",
        "max_price",
        "min_price",
        "current_multiple",
        "max_multiple",
        "min_multiple",
        "current_pnl_pct",
        "max_drawdown_pct",
        "candles",
        "mcap_at_alert",
        "holder_count",
    ]
    with output.open("w", newline="", encoding="utf-8-sig") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fields})


def win_multiple_from_profit(win_profit_pct: float) -> float:
    return 1 + win_profit_pct / 100


def bucket_rows_by_hour(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    buckets = {}
    for row in rows:
        hour = row.get("alert_hour") or "unknown"
        buckets.setdefault(hour, []).append(row)
    return dict(sorted(buckets.items()))


def print_bucket_summary(rows: list[dict[str, Any]], win_multiple: float) -> None:
    buckets = bucket_rows_by_hour([row for row in rows if row["entry_price"] > 0 and row["candles"] > 0])
    if not buckets:
        return
    print("最近推送按1小时推送时间线:")
    for hour, items in buckets.items():
        winners = [row for row in items if row["max_multiple"] >= win_multiple]
        profitable_now = [row for row in items if row["current_multiple"] > 1]
        avg_max = sum(row["max_multiple"] for row in items) / len(items)
        worst_min = min(row["min_multiple"] for row in items)
        print(
            f"- {hour}: {len(items)}个 | 胜率{len(winners)}/{len(items)}="
            f"{len(winners) / len(items) * 100:.1f}% | 当前盈利{len(profitable_now)}/{len(items)} | "
            f"均最高{avg_max:.2f}x | 最低{worst_min:.2f}x"
        )
    print("")


def print_summary(rows: list[dict[str, Any]], win_profit_pct: float, output: str) -> None:
    win_multiple = win_multiple_from_profit(win_profit_pct)
    total = len(rows)
    valid = [row for row in rows if row["entry_price"] > 0 and row["candles"] > 0]
    winners = [row for row in valid if row["max_multiple"] >= win_multiple]
    profitable_now = [row for row in valid if row["current_multiple"] > 1]
    rugged = [row for row in valid if row["min_multiple"] <= 0.3]
    avg_max_multiple = sum(row["max_multiple"] for row in valid) / len(valid) if valid else 0.0
    avg_current_multiple = sum(row["current_multiple"] for row in valid) / len(valid) if valid else 0.0

    print(f"最近推送统计: {total} 个CA | 有效K线: {len(valid)} 个")
    print(f"最高盈利达到 {win_profit_pct:.0f}%({win_multiple:.2f}x) 胜率: {len(winners)}/{len(valid)} = {(len(winners) / len(valid) * 100 if valid else 0):.1f}%")
    print(f"当前仍盈利比例: {len(profitable_now)}/{len(valid)} = {(len(profitable_now) / len(valid) * 100 if valid else 0):.1f}%")
    print(f"最低跌到 0.3x 以下: {len(rugged)}/{len(valid)} = {(len(rugged) / len(valid) * 100 if valid else 0):.1f}%")
    print(f"平均最高倍数: {avg_max_multiple:.2f}x | 平均当前倍数: {avg_current_multiple:.2f}x")
    print(f"CSV: {output}")
    print("")
    print_bucket_summary(rows, win_multiple)
    for row in sorted(valid, key=lambda item: item["max_multiple"], reverse=True):
        print(
            f"${row['symbol']} {row['address'][:8]} | 推送={row['alert_time']} | "
            f"市值={fmt_money(row['mcap_at_alert'] or 0)} | 入场={row['entry_price']:.12g} | "
            f"最高={row['max_multiple']:.2f}x | 最低={row['min_multiple']:.2f}x | "
            f"当前={row['current_multiple']:.2f}x({row['current_pnl_pct']:+.1f}%) | "
            f"最大回撤={row['max_drawdown_pct']:+.1f}%"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze deep_alpha_pro alert win rate from recent pushed CAs.")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Analyze recent N pushed CAs, default 20.")
    parser.add_argument("--resolution", default=DEFAULT_RESOLUTION, choices=("1m", "5m", "15m", "1h"), help="Kline resolution.")
    parser.add_argument("--win-profit-pct", type=float, default=DEFAULT_WIN_PROFIT_PCT, help="Winner threshold by max profit percent, default 30 means max price >= 1.3x entry.")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="CSV output path.")
    args = parser.parse_args()

    alerts = fetch_recent_alerts(args.limit)
    now_ts = int(time.time())
    rows = []
    for index, alert in enumerate(alerts, start=1):
        label = f"${alert['symbol']}({alert['address'][:8]})"
        print(f"[{index}/{len(alerts)}] analyze {label}")
        try:
            rows.append(analyze_alert(alert, args.resolution, now_ts))
        except Exception as exc:
            print(f"{label} failed: {exc}")
    write_csv(args.output, rows)
    print_summary(rows, args.win_profit_pct, args.output)


if __name__ == "__main__":
    main()
