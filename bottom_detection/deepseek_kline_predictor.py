"""DeepSeek-backed K-line prediction for bottom abnormal CA pushes."""

from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import Any

import requests


ROOT_DIR = Path(__file__).resolve().parents[1]

DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
DEEPSEEK_MODEL = os.getenv("BOTTOM_DEEPSEEK_KLINE_MODEL", os.getenv("DEEPSEEK_MODEL", "deepseek-v4-pro"))
DEEPSEEK_TIMEOUT = int(os.getenv("BOTTOM_DEEPSEEK_KLINE_TIMEOUT", os.getenv("DEEPSEEK_TIMEOUT", "45")))
DEEPSEEK_KLINE_ENABLED = os.getenv("BOTTOM_DEEPSEEK_KLINE_PREDICTION_ENABLED", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
MAX_5M_CANDLES = int(os.getenv("BOTTOM_DEEPSEEK_KLINE_5M_CANDLES", "72"))
MAX_1M_CANDLES = int(os.getenv("BOTTOM_DEEPSEEK_KLINE_1M_CANDLES", "90"))
MAX_DOC_CHARS = int(os.getenv("BOTTOM_DEEPSEEK_KLINE_DOC_CHARS", "5000"))

SOURCE_DOCS = (
    "onchain_trading_guides/02-anomaly-detection-framework.md",
    "onchain_trading_guides/08-5m-fingerprint-encyclopedia.md",
    "onchain_trading_guides/09-bar-level-strategy.md",
)

REQUIRED_SCHEMA = {
    "summary": "",
    "bias": "unknown",
    "confidence": "low",
    "pattern_5m": {},
    "micro_1m": {},
    "forecast": {},
    "strategy_observations": [],
    "risk_factors": [],
    "watch_windows": [],
    "source_docs": list(SOURCE_DOCS),
}

FORBIDDEN_ADVICE_REPLACEMENTS = (
    ("建议买入", "买盘确认强"),
    ("可以买入", "买盘确认强"),
    ("可买入", "买盘确认强"),
    ("买入", "买盘"),
    ("加仓", "强度增加"),
    ("减仓", "强度下降"),
    ("止损", "失效观察线"),
    ("止盈", "峰值观察"),
    ("仓位", "暴露"),
    ("追高", "高位延续"),
    ("追", "延续观察"),
    ("入场", "观察点"),
)


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _round_price(value: Any) -> float:
    val = _to_float(value)
    if val == 0:
        return 0.0
    return round(val, 12)


def _round_amount(value: Any) -> float:
    return round(_to_float(value), 4)


def compact_candles(candles: list[dict[str, Any]] | None, limit: int) -> list[dict[str, Any]]:
    """Return compact OHLCV candles for prompt payloads."""
    rows = []
    for candle in (candles or [])[-max(1, limit) :]:
        ts = _to_int(candle.get("ts") or candle.get("t"))
        open_price = _round_price(candle.get("open") or candle.get("o"))
        high_price = _round_price(candle.get("high") or candle.get("h"))
        low_price = _round_price(candle.get("low") or candle.get("l"))
        close_price = _round_price(candle.get("close") or candle.get("c"))
        if ts <= 0 or open_price <= 0 or high_price <= 0 or low_price <= 0 or close_price <= 0:
            continue
        rows.append(
            {
                "t": ts,
                "o": open_price,
                "h": high_price,
                "l": low_price,
                "c": close_price,
                "v": _round_amount(candle.get("volume") or candle.get("v")),
                "a": _round_amount(candle.get("amount") or candle.get("a")),
            }
        )
    return rows


def _read_doc_excerpt(relative_path: str) -> str:
    path = ROOT_DIR / relative_path
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    if len(text) <= MAX_DOC_CHARS:
        return text
    return text[:MAX_DOC_CHARS].rstrip() + "\n\n[truncated]"


def load_strategy_reference() -> dict[str, str]:
    return {doc: _read_doc_excerpt(doc) for doc in SOURCE_DOCS}


def _safe_text(value: Any, limit: int = 180) -> str:
    text = str(value or "").strip()
    for old, new in FORBIDDEN_ADVICE_REPLACEMENTS:
        text = text.replace(old, new)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > limit:
        return text[: max(0, limit - 3)].rstrip() + "..."
    return text


def _safe_list(value: Any, limit: int = 5, text_limit: int = 140) -> list[str]:
    if not isinstance(value, list):
        return []
    result = []
    for item in value:
        text = _safe_text(item, text_limit)
        if text:
            result.append(text)
        if len(result) >= limit:
            break
    return result


def _extract_json_object(content: str) -> dict[str, Any] | None:
    text = (content or "").strip()
    if not text:
        return None
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```$", "", text)
    try:
        data = json.loads(text)
        return data if isinstance(data, dict) else None
    except json.JSONDecodeError:
        pass
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end <= start:
        return None
    try:
        data = json.loads(text[start : end + 1])
        return data if isinstance(data, dict) else None
    except json.JSONDecodeError:
        return None


def normalize_prediction(data: dict[str, Any], *, model: str, elapsed_ms: int) -> dict[str, Any]:
    pattern = data.get("pattern_5m") if isinstance(data.get("pattern_5m"), dict) else {}
    micro = data.get("micro_1m") if isinstance(data.get("micro_1m"), dict) else {}
    forecast = data.get("forecast") if isinstance(data.get("forecast"), dict) else {}
    confidence = str(data.get("confidence") or "low").lower()
    if confidence not in {"high", "medium", "low"}:
        confidence = "low"
    bias = str(data.get("bias") or "unknown").lower()
    if bias not in {"bullish", "neutral", "bearish", "volatile", "unknown"}:
        bias = "unknown"
    return {
        **REQUIRED_SCHEMA,
        "ready": True,
        "status": "ok",
        "model": model,
        "elapsed_ms": elapsed_ms,
        "generated_at": int(time.time()),
        "summary": _safe_text(data.get("summary"), 120),
        "bias": bias,
        "confidence": confidence,
        "pattern_5m": {
            "label": _safe_text(pattern.get("label"), 80),
            "basis": _safe_text(pattern.get("basis"), 160),
            "doc_wr20_pct": _to_float(pattern.get("doc_wr20_pct"), 0.0),
            "risk_level": _safe_text(pattern.get("risk_level"), 40),
        },
        "micro_1m": {
            "label": _safe_text(micro.get("label"), 80),
            "basis": _safe_text(micro.get("basis"), 160),
            "change_pct": round(_to_float(micro.get("change_pct")), 2),
            "volume_ratio": round(_to_float(micro.get("volume_ratio")), 2),
            "decision": _safe_text(micro.get("decision"), 80),
        },
        "forecast": {
            "next_5m": _safe_text(forecast.get("next_5m"), 120),
            "next_30m": _safe_text(forecast.get("next_30m"), 120),
            "next_4h": _safe_text(forecast.get("next_4h"), 120),
        },
        "strategy_observations": _safe_list(data.get("strategy_observations"), 5, 140),
        "risk_factors": _safe_list(data.get("risk_factors"), 5, 140),
        "watch_windows": _safe_list(data.get("watch_windows"), 4, 120),
        "source_docs": list(SOURCE_DOCS),
    }


def build_prompt_payload(
    *,
    address: str,
    signal: dict[str, Any],
    candles_5m: list[dict[str, Any]] | None,
    candles_1m: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    return {
        "task": "bottom_abnormal_ca_kline_prediction",
        "requirements": [
            "Use 02-anomaly-detection-framework, 08-5m-fingerprint-encyclopedia, and 09-bar-level-strategy.",
            "Analyze 5m structure and 1m micro confirmation from the supplied OHLCV data.",
            "Return observable K-line prediction only; do not output buy/sell/chase/stop-loss/take-profit/position sizing advice.",
            "Return strict JSON matching the requested schema.",
        ],
        "schema": REQUIRED_SCHEMA,
        "address": address,
        "signal": signal,
        "kline_5m": compact_candles(candles_5m, MAX_5M_CANDLES),
        "kline_1m": compact_candles(candles_1m, MAX_1M_CANDLES),
        "strategy_docs": load_strategy_reference(),
    }


def analyze_deepseek_kline_prediction(
    *,
    address: str,
    signal: dict[str, Any],
    candles_5m: list[dict[str, Any]] | None,
    candles_1m: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    if not DEEPSEEK_KLINE_ENABLED:
        return {"ready": False, "status": "disabled", "source_docs": list(SOURCE_DOCS)}
    if not DEEPSEEK_API_KEY:
        return {"ready": False, "status": "missing_api_key", "source_docs": list(SOURCE_DOCS)}

    prompt_payload = build_prompt_payload(
        address=address,
        signal=signal,
        candles_5m=candles_5m,
        candles_1m=candles_1m,
    )
    if not prompt_payload["kline_5m"] or not prompt_payload["kline_1m"]:
        return {
            "ready": False,
            "status": "missing_kline_data",
            "source_docs": list(SOURCE_DOCS),
            "kline_5m_count": len(prompt_payload["kline_5m"]),
            "kline_1m_count": len(prompt_payload["kline_1m"]),
        }

    system_prompt = (
        "You are a structured K-line prediction engine for Solana bottom-abnormal CA alerts. "
        "Use the supplied strategy documents and OHLCV data. Output JSON only. "
        "Do not give trading advice, order instructions, position sizing, stop-loss, or take-profit recommendations."
    )
    request_payload = {
        "model": DEEPSEEK_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(prompt_payload, ensure_ascii=False, separators=(",", ":"))},
        ],
        "temperature": 0.1,
        "max_tokens": 1200,
    }
    started = time.time()
    try:
        resp = requests.post(
            f"{DEEPSEEK_BASE_URL.rstrip('/')}/chat/completions",
            headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"},
            json=request_payload,
            timeout=DEEPSEEK_TIMEOUT,
        )
    except Exception as exc:
        return {"ready": False, "status": "exception", "error": str(exc), "source_docs": list(SOURCE_DOCS)}

    elapsed_ms = int((time.time() - started) * 1000)
    if not resp.ok:
        return {
            "ready": False,
            "status": f"http_{resp.status_code}",
            "error": resp.text[:240],
            "elapsed_ms": elapsed_ms,
            "source_docs": list(SOURCE_DOCS),
        }
    try:
        content = resp.json().get("choices", [{}])[0].get("message", {}).get("content") or ""
    except (ValueError, TypeError, KeyError, IndexError) as exc:
        return {"ready": False, "status": "bad_response", "error": str(exc), "elapsed_ms": elapsed_ms, "source_docs": list(SOURCE_DOCS)}

    data = _extract_json_object(content)
    if not data:
        return {
            "ready": False,
            "status": "json_parse_failed",
            "raw": content[:400],
            "elapsed_ms": elapsed_ms,
            "source_docs": list(SOURCE_DOCS),
        }
    return normalize_prediction(data, model=DEEPSEEK_MODEL, elapsed_ms=elapsed_ms)
