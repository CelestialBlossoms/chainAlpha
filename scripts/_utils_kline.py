"""Shared Binance Web3 K-line utilities — used by analysis scripts."""
import time
import requests
from typing import Optional

KLINE_URL = "https://dquery.sintral.io/u-kline/v1/k-line/candles"
HEADERS = {"Accept-Encoding": "identity", "User-Agent": "binance-web3/1.1 (Skill)"}


def parse(raw):
    """Parse Binance K-line 2D array into sorted dict list."""
    candles = []
    for item in (raw or []):
        if not isinstance(item, list) or len(item) < 6:
            continue
        ts = int(item[5] / 1000) if item[5] > 10 ** 10 else int(item[5])
        candles.append({
            "ts": ts, "open": float(item[0]), "high": float(item[1]),
            "low": float(item[2]), "close": float(item[3]), "volume": float(item[4]),
        })
    candles.sort(key=lambda c: c["ts"])
    return candles


def fetch(address: str, interval: str = "1min", limit: int = 12,
          from_ts: Optional[int] = None, to_ts: Optional[int] = None) -> list[dict]:
    """Fetch K-line from Binance Web3 API.

    Args:
        address: Token contract address
        interval: '1min' or '5min'
        limit: Max candles when not using time range
        from_ts / to_ts: UTC seconds timestamps for range query
    """
    params = {
        "address": address, "platform": "solana",
        "interval": interval, "pm": "p",
    }
    if from_ts is not None and to_ts is not None:
        params["from"] = from_ts * 1000
        params["to"] = to_ts * 1000
    else:
        params["limit"] = limit

    try:
        resp = requests.get(KLINE_URL, params=params, headers=HEADERS, timeout=25)
        if resp.status_code == 200:
            return parse(resp.json().get("data", []))
    except Exception:
        pass
    return []


def fetch_1m(address: str, limit: int = 12) -> list[dict]:
    return fetch(address, "1min", limit=limit)


def fetch_5m(address: str, limit: int = 36) -> list[dict]:
    return fetch(address, "5min", limit=limit)


def fetch_range(address: str, from_ts: int, to_ts: int, interval: str = "1min") -> list[dict]:
    return fetch(address, interval, from_ts=from_ts, to_ts=to_ts)


def completed_only(candles: list[dict], now_ts: Optional[int] = None) -> list[dict]:
    """Filter to only completed (closed) candles."""
    if not candles:
        return []
    now_ts = int(now_ts or time.time())
    closed = [c for c in candles if int(c.get("ts", 0)) <= now_ts - 60]
    return closed or list(candles[:-1])


def pct(price: float, base: float) -> float:
    """Return percentage change from base."""
    return (price / base - 1) * 100 if base > 0 and price > 0 else 0.0


def post_entry(candles: list[dict], entry_ts: int) -> list[dict]:
    """Return candles after entry timestamp."""
    return [c for c in candles if c["ts"] >= entry_ts]


def max_gain(candles: list[dict], entry_price: float) -> float:
    """Max gain percentage from entry price."""
    if not candles or entry_price <= 0:
        return 0.0
    return pct(max(c["high"] for c in candles), entry_price)


def max_drawdown(candles: list[dict], entry_price: float) -> float:
    """Max drawdown percentage from entry price."""
    if not candles or entry_price <= 0:
        return 0.0
    return pct(min(c["low"] for c in candles), entry_price)


def current_return(candles: list[dict], entry_price: float) -> float:
    """Current return from last candle close vs entry."""
    if not candles or entry_price <= 0:
        return 0.0
    return pct(candles[-1]["close"], entry_price)
