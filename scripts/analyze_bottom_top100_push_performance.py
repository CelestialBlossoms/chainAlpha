#!/usr/bin/env python3
"""
Analyze today's first bottom_top100_push_records movement per CA.

Usage:
    D:/software/anaconda/envs/py312/python.exe scripts/analyze_bottom_top100_push_performance.py
    D:/software/anaconda/envs/py312/python.exe scripts/analyze_bottom_top100_push_performance.py --resolution 5m --date 2026-05-15
"""

from __future__ import annotations

import argparse
import csv
import json
import requests
import shutil
import subprocess
import sys
import time
from datetime import date, datetime, time as dt_time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from db_client import db_op


DEFAULT_CHAIN = "sol"
DEFAULT_RESOLUTION = "5m"
DEFAULT_TZ = "Asia/Shanghai"
DEFAULT_OUTPUT = "gmgn_outputs/bottom_top100_push_today_performance.csv"


def configure_stdout() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return default


def to_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def normalize_ts(value: Any) -> int:
    if value in (None, ""):
        return 0
    if isinstance(value, datetime):
        return int(value.timestamp())
    ts = to_int(value)
    return ts // 1000 if ts > 10_000_000_000 else ts


def resolution_seconds(resolution: str) -> int:
    mapping = {
        "1m": 60,
        "5m": 5 * 60,
        "15m": 15 * 60,
        "30m": 30 * 60,
        "1h": 60 * 60,
        "4h": 4 * 60 * 60,
        "1d": 24 * 60 * 60,
    }
    return mapping.get(resolution, 60)


def fmt_ts(ts: int, tz: ZoneInfo) -> str:
    if ts <= 0:
        return ""
    return datetime.fromtimestamp(ts, tz=tz).strftime("%Y-%m-%d %H:%M:%S")


def fmt_money(value: Any) -> str:
    amount = to_float(value)
    if amount >= 1_000_000:
        return f"${amount / 1_000_000:.2f}M"
    if amount >= 1_000:
        return f"${amount / 1_000:.1f}K"
    return f"${amount:.0f}"


def fmt_pct(value: Any, signed: bool = False) -> str:
    number = to_float(value)
    prefix = "+" if signed and number > 0 else ""
    return f"{prefix}{number:.1f}%"


def local_day_bounds(day: str | None, tz_name: str) -> tuple[int, int, str, ZoneInfo]:
    tz = ZoneInfo(tz_name)
    if day:
        target_day = date.fromisoformat(day)
    else:
        target_day = datetime.now(tz).date()
    start_dt = datetime.combine(target_day, dt_time.min, tzinfo=tz)
    end_dt = start_dt + timedelta(days=1)
    now_dt = datetime.now(tz)
    if target_day == now_dt.date():
        end_dt = min(end_dt, now_dt)
    return int(start_dt.timestamp()), int(end_dt.timestamp()), target_day.isoformat(), tz


def gmgn_command_prefix() -> list[str]:
    executable = shutil.which("gmgn-cli") or shutil.which("gmgn-cli.cmd") or shutil.which("gmgn-cli.ps1")
    if not executable:
        return ["gmgn-cli"]
    if executable.lower().endswith(".ps1"):
        return ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", executable]
    return [executable]


def run_gmgn(args: list[str], timeout: int = 90) -> dict[str, Any] | list[Any] | None:
    cmd = [*gmgn_command_prefix(), *args, "--raw"]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
        )
    except Exception as exc:
        print(f"GMGN 调用异常: {' '.join(cmd)} -> {exc}")
        return None
    if result.returncode != 0:
        err = (result.stderr or result.stdout or "").strip()
        print(f"GMGN 调用失败 rc={result.returncode}: {' '.join(cmd)}")
        if err:
            print(err[:500])
        return None
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        print(f"GMGN JSON 解析失败: {exc}")
        return None


def extract_kline_rows(data: dict[str, Any] | list[Any] | None) -> list[dict[str, Any]]:
    if not data:
        return []
    if isinstance(data, list):
        rows = data
    else:
        nested = data.get("data") if isinstance(data.get("data"), dict) else {}
        rows = data.get("list") or nested.get("list") or data.get("data") or []
    candles: list[dict[str, Any]] = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        ts = normalize_ts(row.get("time") or row.get("timestamp") or row.get("t"))
        close = to_float(row.get("close") or row.get("c"))
        if ts <= 0 or close <= 0:
            continue
        candles.append(
            {
                "ts": ts,
                "open": to_float(row.get("open") or row.get("o"), close),
                "high": to_float(row.get("high") or row.get("h"), close),
                "low": to_float(row.get("low") or row.get("l"), close),
                "close": close,
                "volume": to_float(row.get("volume") or row.get("v")),
                "amount": to_float(row.get("amount") or row.get("a")),
            }
        )
    candles.sort(key=lambda item: item["ts"])
    return candles


BINANCE_KLINE_URL = "https://dquery.sintral.io/u-kline/v1/k-line/candles"
BINANCE_HEADERS = {"Accept-Encoding": "identity", "User-Agent": "binance-web3/1.1 (Skill)"}

CHAIN_PLATFORM_MAP = {"sol": "solana", "bsc": "bsc", "base": "base", "eth": "ethereum"}


def _resolution_to_interval(resolution: str) -> str:
    mapping = {
        "1m": "1min", "5m": "5min", "15m": "15min", "30m": "30min",
        "1h": "1h", "4h": "4h", "1d": "1d",
    }
    return mapping.get(resolution, "5min")


def fetch_kline_once(
    chain: str,
    address: str,
    resolution: str,
    from_ts: int,
    to_ts_value: int,
) -> list[dict[str, Any]]:
    platform = CHAIN_PLATFORM_MAP.get(chain, "solana")
    interval = _resolution_to_interval(resolution)
    params = {
        "address": address,
        "platform": platform,
        "interval": interval,
        "from": from_ts * 1000,
        "to": to_ts_value * 1000,
        "pm": "p",
    }
    try:
        r = requests.get(BINANCE_KLINE_URL, params=params, headers=BINANCE_HEADERS, timeout=30)
        if r.status_code != 200:
            print(f"  Binance K-line http {r.status_code}: {address[:12]}..")
            return []
        payload = r.json()
        raw = payload.get("data") if isinstance(payload, dict) else None
        if not raw or not isinstance(raw, list):
            return []
        candles = []
        for item in raw:
            if not isinstance(item, list) or len(item) < 6:
                continue
            candles.append({
                "ts": int(item[5] / 1000) if item[5] > 10_000_000_000 else int(item[5]),
                "open": float(item[0]),
                "high": float(item[1]),
                "low": float(item[2]),
                "close": float(item[3]),
                "volume": float(item[4]),
                "amount": float(item[4]),
            })
        candles.sort(key=lambda c: c["ts"])
        return candles
    except Exception as exc:
        print(f"  Binance K-line fetch failed: {exc}")
        return []


def fetch_kline(
    chain: str,
    address: str,
    resolution: str,
    from_ts: int,
    to_ts_value: int,
    max_bars_per_request: int,
    request_sleep: float,
) -> list[dict[str, Any]]:
    # Binance K-line API: use limit parameter, single request covers the range
    platform = CHAIN_PLATFORM_MAP.get(chain, "solana")
    interval = _resolution_to_interval(resolution)
    step = resolution_seconds(resolution)
    max_bars = max_bars_per_request if max_bars_per_request > 0 else 500
    params = {
        "address": address,
        "platform": platform,
        "interval": interval,
        "limit": max_bars,
        "to": to_ts_value * 1000,
        "pm": "p",
    }
    try:
        r = requests.get(BINANCE_KLINE_URL, params=params, headers=BINANCE_HEADERS, timeout=30)
        if r.status_code != 200:
            return []
        payload = r.json()
        raw = payload.get("data") if isinstance(payload, dict) else None
        if not raw or not isinstance(raw, list):
            return []
        candles = []
        for item in raw:
            if not isinstance(item, list) or len(item) < 6:
                continue
            ts = int(item[5] / 1000) if item[5] > 10_000_000_000 else int(item[5])
            if ts < from_ts:
                continue
            candles.append({
                "ts": ts,
                "open": float(item[0]),
                "high": float(item[1]),
                "low": float(item[2]),
                "close": float(item[3]),
                "volume": float(item[4]),
                "amount": float(item[4]),
            })
        candles.sort(key=lambda c: c["ts"])
        return candles
    except Exception as exc:
        print(f"  Binance K-line fetch failed: {exc}")
        return []


def fetch_first_pushes(
    start_ts: int,
    end_ts: int,
    chain: str,
    limit: int,
    group_by_signal: bool,
) -> tuple[list[dict[str, Any]], int]:
    distinct_cols = "address, signal_type" if group_by_signal else "address"
    stats_join = (
        "f.address = s.address AND COALESCE(f.signal_type, '') = COALESCE(s.signal_type, '')"
        if group_by_signal
        else "f.address = s.address"
    )
    stats_group = "address, signal_type" if group_by_signal else "address"
    limit_sql = "LIMIT %s" if limit > 0 else ""
    params: list[Any] = [chain, start_ts, end_ts]
    if limit > 0:
        params.append(limit)

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*)
            FROM bottom_top100_push_records
            WHERE chain=%s
              AND event_ts >= %s
              AND event_ts < %s
              AND COALESCE(signal_type, '') <> ''
            """,
            (chain, start_ts, end_ts),
        )
        total_pushes = int(cur.fetchone()[0] or 0)
        cur.execute(
            f"""
            WITH todays AS (
                SELECT *
                FROM bottom_top100_push_records
                WHERE chain=%s
                  AND event_ts >= %s
                  AND event_ts < %s
                  AND COALESCE(signal_type, '') <> ''
            ),
            firsts AS (
                SELECT DISTINCT ON ({distinct_cols})
                    id,
                    pushed_at,
                    event_ts,
                    snapshot_id,
                    chain,
                    address,
                    symbol,
                    signal_type,
                    abnormal_rule,
                    trend_interval,
                    current_mcap,
                    first_signal_mcap,
                    first_signal_ts,
                    first_signal_change_pct,
                    price_change_pct,
                    max_abnormal_mcap,
                    ath_mcap,
                    liquidity,
                    pool_mcap_ratio,
                    age_sec,
                    extra
                FROM todays
                ORDER BY {distinct_cols}, event_ts ASC, id ASC
            ),
            stats AS (
                SELECT
                    {stats_group},
                    COUNT(*) AS push_count,
                    MAX(event_ts) AS last_event_ts,
                    ARRAY_AGG(DISTINCT signal_type ORDER BY signal_type) AS signal_types
                FROM todays
                GROUP BY {stats_group}
            )
            SELECT
                f.id,
                f.pushed_at,
                f.event_ts,
                f.snapshot_id,
                f.chain,
                f.address,
                f.symbol,
                f.signal_type,
                f.abnormal_rule,
                f.trend_interval,
                f.current_mcap,
                f.first_signal_mcap,
                f.first_signal_ts,
                f.first_signal_change_pct,
                f.price_change_pct,
                f.max_abnormal_mcap,
                f.ath_mcap,
                f.liquidity,
                f.pool_mcap_ratio,
                f.age_sec,
                f.extra,
                s.push_count,
                s.last_event_ts,
                s.signal_types
            FROM firsts f
            JOIN stats s ON {stats_join}
            ORDER BY f.event_ts ASC, f.id ASC
            {limit_sql}
            """,
            params,
        )
        rows = []
        for row in cur.fetchall():
            extra = row[20] if isinstance(row[20], dict) else {}
            rows.append(
                {
                    "id": row[0],
                    "pushed_at": row[1],
                    "event_ts": int(row[2] or 0),
                    "snapshot_id": int(row[3] or 0),
                    "chain": row[4] or chain,
                    "address": row[5],
                    "symbol": row[6] or extra.get("symbol") or "UNKNOWN",
                    "signal_type": row[7] or "",
                    "abnormal_rule": row[8] or "",
                    "trend_interval": row[9] or "",
                    "current_mcap": to_float(row[10]),
                    "first_signal_mcap": to_float(row[11]),
                    "first_signal_ts": int(row[12] or 0),
                    "first_signal_change_pct": to_float(row[13]),
                    "price_change_pct": to_float(row[14]),
                    "max_abnormal_mcap": to_float(row[15]),
                    "ath_mcap": to_float(row[16]),
                    "liquidity": to_float(row[17]),
                    "pool_mcap_ratio": to_float(row[18]),
                    "age_sec": int(row[19] or 0),
                    "extra": extra,
                    "push_count": int(row[21] or 0),
                    "last_event_ts": int(row[22] or 0),
                    "signal_types": [item for item in (row[23] or []) if item],
                }
            )
        return rows, total_pushes

    return db_op(_op)


def choose_entry_price(candle: dict[str, Any], entry_field: str) -> float:
    if entry_field == "close":
        return to_float(candle.get("close"))
    if entry_field == "high":
        return to_float(candle.get("high"))
    if entry_field == "low":
        return to_float(candle.get("low"))
    return to_float(candle.get("open")) or to_float(candle.get("close"))


def analyze_push(
    push: dict[str, Any],
    candles: list[dict[str, Any]],
    resolution: str,
    entry_field: str,
    tz: ZoneInfo,
) -> dict[str, Any]:
    event_ts = int(push.get("event_ts") or 0)
    step = resolution_seconds(resolution)

    # Find the candle that contains the signal event_ts (candle_ts <= event_ts < candle_ts + step).
    # Use its close price as entry — reflects the actual market price at signal time.
    signal_candle = None
    for candle in candles:
        candle_ts = int(candle.get("ts") or 0)
        if candle_ts <= event_ts < candle_ts + step:
            signal_candle = candle
            break
    if signal_candle is None:
        # Fallback: closest candle before event_ts
        before = [c for c in candles if int(c.get("ts") or 0) <= event_ts]
        if before:
            signal_candle = before[-1]

    # Post-signal candles for peak/drawdown tracking (include candle containing the signal)
    post_candles = [candle for candle in candles if int(candle["ts"]) + step > event_ts]
    if not post_candles:
        return {
            **push,
            "valid": False,
            "invalid_reason": "no_post_signal_kline",
            "candles": len(candles),
        }

    if signal_candle is None:
        signal_candle = post_candles[0]

    entry_price = to_float(signal_candle.get("close")) or to_float(signal_candle.get("open"))
    if entry_price <= 0:
        return {
            **push,
            "valid": False,
            "invalid_reason": "invalid_entry_price",
            "candles": len(post_candles),
        }

    entry_time = fmt_ts(int(signal_candle.get("ts") or 0), tz)
    entry_field_used = "close_at_signal"

    peak_index, peak = max(enumerate(post_candles), key=lambda item: to_float(item[1].get("high")))
    low_index, low = min(enumerate(post_candles), key=lambda item: to_float(item[1].get("low")))
    peak_price = to_float(peak.get("high"))
    lowest_price = to_float(low.get("low"))
    current_price = to_float(post_candles[-1].get("close"))
    post_peak_candles = post_candles[peak_index + 1 :]
    if post_peak_candles:
        post_peak_low_index, post_peak_low = min(
            enumerate(post_peak_candles, start=peak_index + 1),
            key=lambda item: to_float(item[1].get("low")),
        )
        post_peak_low_price = to_float(post_peak_low.get("low"))
        post_peak_low_ts = int(post_peak_low.get("ts") or 0)
    else:
        post_peak_low_index = peak_index
        post_peak_low_price = current_price
        post_peak_low_ts = int(post_candles[-1].get("ts") or 0)

    max_gain_pct = (peak_price / entry_price - 1) * 100 if peak_price > 0 else 0.0
    current_return_pct = (current_price / entry_price - 1) * 100 if current_price > 0 else 0.0
    lowest_return_pct = (lowest_price / entry_price - 1) * 100 if lowest_price > 0 else 0.0
    entry_drawdown_pct = min(0.0, lowest_return_pct)
    high_to_low_drawdown_pct = (
        (1 - post_peak_low_price / peak_price) * 100 if peak_price > 0 and post_peak_low_price > 0 else 0.0
    )
    high_to_current_drawdown_pct = (
        (1 - current_price / peak_price) * 100 if peak_price > 0 and current_price > 0 else 0.0
    )

    return {
        **push,
        "valid": True,
        "invalid_reason": "",
        "resolution": resolution,
        "entry_price_field": "close_at_signal",
        "entry_price": entry_price,
        "entry_time": entry_time,
        "peak_price": peak_price,
        "peak_time": fmt_ts(int(peak.get("ts") or 0), tz),
        "lowest_price": lowest_price,
        "lowest_time": fmt_ts(int(low.get("ts") or 0), tz),
        "post_peak_low_price": post_peak_low_price,
        "post_peak_low_time": fmt_ts(post_peak_low_ts, tz),
        "current_price": current_price,
        "current_time": fmt_ts(int(post_candles[-1].get("ts") or 0), tz),
        "max_gain_pct": max_gain_pct,
        "max_gain_multiple": peak_price / entry_price if entry_price > 0 and peak_price > 0 else 0.0,
        "current_return_pct": current_return_pct,
        "entry_drawdown_pct": entry_drawdown_pct,
        "lowest_return_pct": lowest_return_pct,
        "high_to_low_drawdown_pct": max(0.0, high_to_low_drawdown_pct),
        "high_to_current_drawdown_pct": max(0.0, high_to_current_drawdown_pct),
        "time_to_peak_min": max(0.0, (int(peak.get("ts") or 0) - event_ts) / 60),
        "time_to_low_min": max(0.0, (int(low.get("ts") or 0) - event_ts) / 60),
        "time_peak_to_low_min": max(0.0, (post_peak_low_ts - int(peak.get("ts") or 0)) / 60),
        "volume_usd": sum(to_float(candle.get("volume")) for candle in post_candles),
        "candles": len(post_candles),
        "peak_candle_index": peak_index,
        "lowest_candle_index": low_index,
        "post_peak_low_candle_index": post_peak_low_index,
    }


def bucket_label(value: float, buckets: list[tuple[float, str]]) -> str:
    for upper_bound, label in buckets:
        if value < upper_bound:
            return label
    return buckets[-1][1]


def print_bucket_stats(title: str, rows: list[dict[str, Any]], key: str, buckets: list[tuple[float, str]]) -> None:
    valid = [row for row in rows if row.get("valid")]
    total = len(valid)
    counts: dict[str, int] = {label: 0 for _, label in buckets}
    for row in valid:
        counts[bucket_label(to_float(row.get(key)), buckets)] += 1
    print(title)
    for _, label in buckets:
        count = counts[label]
        ratio = count / total * 100 if total else 0
        print(f"  {label:<12} {count:>4}/{total:<4} {ratio:>5.1f}%")


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * pct))))
    return ordered[index]


def print_summary(
    rows: list[dict[str, Any]],
    total_pushes: int,
    day: str,
    start_ts: int,
    end_ts: int,
    tz: ZoneInfo,
    output: str,
) -> None:
    valid = [row for row in rows if row.get("valid")]
    invalid = [row for row in rows if not row.get("valid")]
    gains = [to_float(row.get("max_gain_pct")) for row in valid]
    high_drawdowns = [to_float(row.get("high_to_low_drawdown_pct")) for row in valid]
    current_returns = [to_float(row.get("current_return_pct")) for row in valid]

    print("")
    print("bottom_top100_push_records 今日首次异动统计")
    print(f"日期窗口: {day} {fmt_ts(start_ts, tz)} -> {fmt_ts(end_ts, tz)}")
    print(f"推送记录: {total_pushes} | 统计样本: {len(rows)} | 有效K线: {len(valid)} | 无效/无K线: {len(invalid)}")
    if valid:
        print(
            "核心指标: "
            f"平均最高涨幅 {sum(gains) / len(gains):.1f}% | "
            f"中位最高涨幅 {percentile(gains, 0.5):.1f}% | "
            f"P75最高涨幅 {percentile(gains, 0.75):.1f}% | "
            f"平均高点后回撤 {sum(high_drawdowns) / len(high_drawdowns):.1f}% | "
            f"当前仍高于首异动 {sum(1 for value in current_returns if value > 0)}/{len(valid)}"
        )
    print(f"CSV: {output}")
    print("")

    print_bucket_stats(
        "首异动 -> 后续最高点涨幅分布",
        rows,
        "max_gain_pct",
        [
            (0, "<0%"),
            (50, "0-50%"),
            (100, "50-100%"),
            (200, "100-200%"),
            (500, "200-500%"),
            (float("inf"), ">=500%"),
        ],
    )
    print_bucket_stats(
        "高点后最大回撤分布",
        rows,
        "high_to_low_drawdown_pct",
        [
            (20, "0-20%"),
            (40, "20-40%"),
            (60, "40-60%"),
            (80, "60-80%"),
            (float("inf"), ">=80%"),
        ],
    )
    print_bucket_stats(
        "首异动后最低点相对 entry 的跌幅分布",
        rows,
        "entry_drawdown_pct",
        [
            (-80, "<=-80%"),
            (-50, "-80~-50%"),
            (-20, "-50~-20%"),
            (0, "-20~0%"),
            (float("inf"), "未破entry"),
        ],
    )

    if valid:
        print("")
        print("最高涨幅 Top 15")
        for row in sorted(valid, key=lambda item: to_float(item.get("max_gain_pct")), reverse=True)[:15]:
            print(
                f"  ${row['symbol']:<10} {row['address'][:8]} "
                f"{fmt_ts(row['event_ts'], tz)} {row['signal_type']:<16} "
                f"市值{fmt_money(row['current_mcap']):>8} "
                f"涨幅{fmt_pct(row['max_gain_pct'], signed=True):>8} "
                f"高点回撤{fmt_pct(row['high_to_low_drawdown_pct']):>7} "
                f"当前{fmt_pct(row['current_return_pct'], signed=True):>8} "
                f"K线{row['candles']}"
            )

        print("")
        print("高点后回撤 Top 15")
        for row in sorted(valid, key=lambda item: to_float(item.get("high_to_low_drawdown_pct")), reverse=True)[:15]:
            print(
                f"  ${row['symbol']:<10} {row['address'][:8]} "
                f"涨幅{fmt_pct(row['max_gain_pct'], signed=True):>8} "
                f"高点回撤{fmt_pct(row['high_to_low_drawdown_pct']):>7} "
                f"entry低点{fmt_pct(row['entry_drawdown_pct']):>8} "
                f"当前{fmt_pct(row['current_return_pct'], signed=True):>8} "
                f"peak={row['peak_time']} low={row['post_peak_low_time']}"
            )


def write_csv(path: str, rows: list[dict[str, Any]], tz: ZoneInfo) -> None:
    output = Path(path)
    if not output.is_absolute():
        output = ROOT_DIR / output
    output.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "valid",
        "invalid_reason",
        "id",
        "address",
        "symbol",
        "chain",
        "signal_type",
        "signal_types",
        "abnormal_rule",
        "trend_interval",
        "event_ts",
        "event_time",
        "push_count",
        "last_event_ts",
        "last_event_time",
        "snapshot_id",
        "current_mcap",
        "first_signal_mcap",
        "first_signal_ts",
        "first_signal_time",
        "first_signal_change_pct",
        "price_change_pct",
        "max_abnormal_mcap",
        "ath_mcap",
        "liquidity",
        "pool_mcap_ratio",
        "age_sec",
        "resolution",
        "entry_price_field",
        "entry_price",
        "entry_time",
        "peak_price",
        "peak_time",
        "lowest_price",
        "lowest_time",
        "post_peak_low_price",
        "post_peak_low_time",
        "current_price",
        "current_time",
        "max_gain_pct",
        "max_gain_multiple",
        "current_return_pct",
        "entry_drawdown_pct",
        "lowest_return_pct",
        "high_to_low_drawdown_pct",
        "high_to_current_drawdown_pct",
        "time_to_peak_min",
        "time_to_low_min",
        "time_peak_to_low_min",
        "volume_usd",
        "candles",
    ]
    with output.open("w", newline="", encoding="utf-8-sig") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            record = dict(row)
            record["event_time"] = fmt_ts(to_int(record.get("event_ts")), tz)
            record["last_event_time"] = fmt_ts(to_int(record.get("last_event_ts")), tz)
            record["first_signal_time"] = fmt_ts(to_int(record.get("first_signal_ts")), tz)
            record["signal_types"] = ",".join(record.get("signal_types") or [])
            writer.writerow({field: record.get(field, "") for field in fields})


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="统计 bottom_top100_push_records 今日首个异动 CA 后续最高涨幅和回撤。"
    )
    parser.add_argument("--date", help="统计日期 YYYY-MM-DD，默认当前时区今天。")
    parser.add_argument("--tz", default=DEFAULT_TZ, help=f"日期窗口时区，默认 {DEFAULT_TZ}。")
    parser.add_argument("--chain", default=DEFAULT_CHAIN, help=f"链，默认 {DEFAULT_CHAIN}。")
    parser.add_argument(
        "--resolution",
        default=DEFAULT_RESOLUTION,
        choices=("1m", "5m", "15m", "30m", "1h", "4h", "1d"),
        help=f"K线粒度，默认 {DEFAULT_RESOLUTION}。",
    )
    parser.add_argument(
        "--entry-field",
        default="open",
        choices=("open", "close", "high", "low"),
        help="首个异动K线使用哪个价格作为 entry，默认 open。",
    )
    parser.add_argument("--limit", type=int, default=0, help="最多分析 N 个样本，0 表示不限制。")
    parser.add_argument("--sleep", type=float, default=0.25, help="每次 GMGN K线请求后的等待秒数，默认 0.25。")
    parser.add_argument(
        "--max-bars-per-request",
        type=int,
        default=90,
        help="单次 K线请求覆盖的最大根数，默认 90，用于规避接口单次返回上限。",
    )
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="CSV 输出路径。")
    parser.add_argument(
        "--group-by-signal",
        action="store_true",
        help="按 CA + signal_type 取首次异动；默认一个 CA 只统计当天第一次异动。",
    )
    return parser.parse_args()


def main() -> None:
    configure_stdout()
    args = parse_args()
    start_ts, end_ts, day, tz = local_day_bounds(args.date, args.tz)
    pushes, total_pushes = fetch_first_pushes(start_ts, end_ts, args.chain, args.limit, args.group_by_signal)
    if not pushes:
        print(f"{day} 没有查询到 bottom_top100_push_records 异动推送记录。")
        return

    rows: list[dict[str, Any]] = []
    step = resolution_seconds(args.resolution)
    fetch_from_shift = step
    for index, push in enumerate(pushes, start=1):
        label = f"${push['symbol']}({push['address'][:8]})"
        print(f"[{index}/{len(pushes)}] 分析 {label} {fmt_ts(push['event_ts'], tz)} {push['signal_type']}")
        from_ts = max(start_ts, int(push["event_ts"]) - fetch_from_shift)
        try:
            candles = fetch_kline(
                args.chain,
                push["address"],
                args.resolution,
                from_ts,
                end_ts,
                args.max_bars_per_request,
                args.sleep,
            )
            rows.append(analyze_push(push, candles, args.resolution, args.entry_field, tz))
        except Exception as exc:
            rows.append(
                {
                    **push,
                    "valid": False,
                    "invalid_reason": f"exception:{exc}",
                    "candles": 0,
                }
            )
            print(f"  {label} 失败: {exc}")
        if args.sleep > 0 and index < len(pushes):
            time.sleep(args.sleep)

    output_path = args.output
    write_csv(output_path, rows, tz)
    print_summary(rows, total_pushes, day, start_ts, end_ts, tz, output_path)


if __name__ == "__main__":
    main()
