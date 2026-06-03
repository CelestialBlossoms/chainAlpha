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
DEEPSEEK_MODEL = os.getenv("BOTTOM_DEEPSEEK_KLINE_MODEL", "deepseek-chat")
DEEPSEEK_TIMEOUT = int(os.getenv("BOTTOM_DEEPSEEK_KLINE_TIMEOUT", os.getenv("DEEPSEEK_TIMEOUT", "360")))
DEEPSEEK_KLINE_ENABLED = os.getenv("BOTTOM_DEEPSEEK_KLINE_PREDICTION_ENABLED", "0").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
MAX_5M_CANDLES = int(os.getenv("BOTTOM_DEEPSEEK_KLINE_5M_CANDLES", "48"))
MAX_1M_CANDLES = int(os.getenv("BOTTOM_DEEPSEEK_KLINE_1M_CANDLES", "60"))
SOURCE_DOCS = (
    "onchain_trading_guides/00-交易策略速查.md",
    "onchain_trading_guides/01-底部异动检测框架.md",
    "onchain_trading_guides/03-CA分析方法论.md",
    "onchain_trading_guides/02-K线指纹百科全书.md",
)
ANALYSIS_SCOPE = (
    {"key": "push_record", "label": "推送记录", "checks": ["signal_type", "current_mcap", "ath_mcap", "age_sec", "pool_mcap_ratio"]},
    {"key": "chip_distribution", "label": "筹码分布", "checks": ["Top10/20/50/100集中度", "Top100吸筹/减持delta", "净流入"]},
    {"key": "price_structure_5m", "label": "5m价格结构", "checks": ["前4h前置结构", "pre-pump", "post回撤", "WR20/WR50历史分组"]},
    {"key": "micro_structure_1m", "label": "1m微结构", "checks": ["推送前后5min", "30min确认", "量比", "涨跌方向"]},
    {"key": "volume_liquidity", "label": "量能/流动性", "checks": ["breakout_volume", "breakout_volume_ratio", "pool_liquidity", "pool/mcap"]},
    {"key": "risk_factors", "label": "风险因子", "checks": ["追高", "放量下跌", "高集中度", "低流动性", "极端回撤"]},
)
METHODOLOGY_STEPS = (
    {
        "step": 1,
        "key": "push_record",
        "label": "查推送记录",
        "checks": ["signal_type", "current_mcap", "ath_mcap", "pool_mcap_ratio", "age_sec", "price_change_pct"],
        "data_source": "bottom_top100_push_records / signal payload",
    },
    {
        "step": 2,
        "key": "kline_baseline",
        "label": "拉5m+1m K线并计算baseline/峰/谷/回撤",
        "checks": ["pre_signal_baseline", "pre_high_low", "post_peak", "post_trough", "pullback", "current_state"],
        "data_source": "bottom_kline_cache / bottom_kline_cache_1m / Binance API",
    },
    {
        "step": 3,
        "key": "bar_level_analysis",
        "label": "逐bar分析",
        "checks": ["5m结构", "1m微观真相", "缩量企稳", "隐藏砸盘", "抢跑", "放量确认"],
        "data_source": "5m+1m OHLCV",
    },
    {
        "step": 4,
        "key": "historical_peer_baseline",
        "label": "查历史同类WR20基准",
        "checks": ["same_signal_type", "same_pullback_bucket", "same_mcap_bucket", "same_ath_ratio"],
        "data_source": "data/deepseek_discovery/signal_kline_records.jsonl",
    },
    {
        "step": 5,
        "key": "five_dimension_score",
        "label": "5维打分",
        "checks": ["信号类型", "回撤深度", "ATH空间", "1m确认", "5m形态"],
        "data_source": "综合",
    },
    {
        "step": 6,
        "key": "conclusion",
        "label": "输出结论",
        "checks": ["可入场/等确认/放弃", "具体条件", "可观察风险"],
        "data_source": "methodology_result",
    },
)

# Cached strategy docs with mtime-based invalidation
_doc_cache: dict[str, tuple[float, str]] = {}

# Cached system prompt — rebuild only when docs change on disk.
# DeepSeek caches the system prompt prefix across calls, so identical system
# prompts across independent conversations benefit from prompt caching.
_system_prompt_mtime: float = 0.0
_system_prompt_cache: str = ""

REQUIRED_SCHEMA = {
    "summary": "",
    "bias": "unknown",
    "confidence": "low",
    "pattern_5m": {},
    "micro_1m": {},
    "forecast": {},
    "purchase_value": {"label": "", "score_pct": 0, "basis": ""},
    "analysis_scope": list(ANALYSIS_SCOPE),
    "methodology_steps": list(METHODOLOGY_STEPS),
    "methodology_result": {},
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


# =========================================================================
#  Local bar-level fingerprint pre-computation (no API needed)
# =========================================================================

def compute_local_fingerprints(
    candles_5m: list[dict[str, Any]] | None,
    candles_1m: list[dict[str, Any]] | None,
    signal_ts: int = 0,
) -> dict[str, Any]:
    """
    Compute bar-level fingerprints locally from raw K-line data.
    These are cheap to compute and provide fallback analysis when DeepSeek is unavailable.
    Also injected into the prompt to reduce DeepSeek's workload.
    """
    fp: dict[str, Any] = {"ready": False, "signal_ts": _to_int(signal_ts)}

    if not candles_5m or len(candles_5m) < 12:
        return fp

    # Normalize candle keys (support both "ts"/"open" and "t"/"o")
    def _candle_dict(c: dict[str, Any]) -> dict[str, float]:
        return {
            "t": _to_int(c.get("ts") or c.get("t")),
            "o": _to_float(c.get("open") or c.get("o")),
            "h": _to_float(c.get("high") or c.get("h")),
            "l": _to_float(c.get("low") or c.get("l")),
            "c": _to_float(c.get("close") or c.get("c")),
            "v": _to_float(c.get("volume") or c.get("v")),
        }

    bars = [_candle_dict(c) for c in candles_5m]
    bars = [b for b in bars if b["o"] > 0 and b["h"] > 0]

    if len(bars) < 12:
        return fp

    n = len(bars)

    # ---- Capitulation bar detection (投降Bar) ----
    cap_bars: list[dict[str, Any]] = []
    for i in range(1, n):
        b = bars[i]
        body_pct = (b["c"] - b["o"]) / b["o"] * 100
        prev_v = bars[i - 1]["v"]
        vol_ratio = b["v"] / prev_v if prev_v > 0 else 1.0
        if body_pct < -8 and vol_ratio > 3:
            rel_pos = i - n  # negative = before "push" (last bar)
            cap_bars.append({
                "rel": rel_pos,
                "body_pct": round(body_pct, 1),
                "vol_ratio": round(vol_ratio, 1),
            })

    # ---- Hammer / Shooting star detection in last 12 bars ----
    last12 = bars[-12:]
    hammers = 0
    stars = 0
    for b in last12:
        body = abs(b["c"] - b["o"])
        upper_wick = b["h"] - max(b["c"], b["o"])
        lower_wick = min(b["c"], b["o"]) - b["l"]
        uw_ratio = upper_wick / max(body, 1e-12)
        lw_ratio = lower_wick / max(body, 1e-12)
        if lw_ratio > 3:
            hammers += 1
        if uw_ratio > 3:
            stars += 1

    # ---- Position in 4h range ----
    # Use last 48 bars (4h) or all available if fewer
    window = bars[-48:] if len(bars) >= 48 else bars
    rng_h = max(b["h"] for b in window)
    rng_l = min(b["l"] for b in window)
    last_close = window[-1]["c"]
    position = (last_close - rng_l) / (rng_h - rng_l) * 100 if rng_h > rng_l else 50
    baseline = last_close
    range_pct = (rng_h - rng_l) / rng_l * 100 if rng_l > 0 else 0.0
    pullback_from_high_pct = (baseline - rng_h) / rng_h * 100 if rng_h > 0 else 0.0

    # ---- Volume trend ----
    if len(window) >= 12:
        early_vol = sum(b["v"] for b in window[:6]) / 6
        late_vol = sum(b["v"] for b in window[-6:]) / 6
        vol_trend = late_vol / early_vol if early_vol > 0 else 1.0
    else:
        vol_trend = 1.0

    # ---- 30m segment trends (last 4 segments of 6 bars = 2h) ----
    segs = []
    for si in range(4):
        start = max(0, len(window) - (4 - si) * 6)
        end = max(0, len(window) - (3 - si) * 6)
        seg = window[start:end]
        if len(seg) >= 2 and seg[0]["o"] > 0:
            pct = (seg[-1]["c"] - seg[0]["o"]) / seg[0]["o"] * 100
            avg_v = sum(b["v"] for b in seg) / len(seg)
            n_bull = sum(1 for b in seg if b["c"] > b["o"])
            segs.append({"pct": round(pct, 1), "avg_vol": round(avg_v, 2), "bulls": n_bull})

    # ---- 1m micro-structure before the abnormal signal ----
    m1_post: dict[str, Any] = {}
    if candles_1m and len(candles_1m) >= 5:
        c1 = [_candle_dict(c) for c in candles_1m]
        if signal_ts > 0:
            c1 = [b for b in c1 if b["t"] <= signal_ts]
        c1 = [b for b in c1 if b["o"] > 0]
        if len(c1) >= 5:
            signal5 = c1[-5:]
            bl = signal5[0]["o"] if signal5 else 0
            if bl > 0:
                chg5 = (signal5[-1]["c"] - bl) / bl * 100
                signal5v = sum(b["v"] for b in signal5) / len(signal5)
                pre1 = c1[-35:-5] if len(c1) >= 35 else c1[: max(1, len(c1) - len(signal5))]
                pre1v = sum(b["v"] for b in pre1) / len(pre1) if pre1 else signal5v
                m1_post = {
                    "window": "pre_signal_last5",
                    "from_ts": signal5[0]["t"],
                    "to_ts": signal5[-1]["t"],
                    "chg_5min": round(chg5, 1),
                    "vol_ratio": round(signal5v / pre1v, 2) if pre1v > 0 else 0,
                    "direction": "up" if chg5 > 3 else ("down" if chg5 < -3 else "flat"),
                }
                if len(c1) >= 30:
                    last30 = c1[-30:]
                    base30 = last30[0]["o"]
                    m1_post["chg_30min"] = round((last30[-1]["c"] - base30) / base30 * 100, 1) if base30 > 0 else 0.0

    # ---- 1m bars before the final signal-confirmation window ----
    m1_pre: dict[str, Any] = {}
    if candles_1m and len(candles_1m) >= 15:
        c1 = [_candle_dict(c) for c in candles_1m]
        if signal_ts > 0:
            c1 = [b for b in c1 if b["t"] <= signal_ts]
        c1 = [b for b in c1 if b["o"] > 0]
        if len(c1) >= 15:
            pre_last5 = c1[-10:-5]
            dirs = [1 if b["c"] > b["o"] else -1 for b in pre_last5]
            m1_pre["window"] = "before_pre_signal_last5"
            m1_pre["last5_direction"] = "bullish" if sum(dirs) > 1 else ("bearish" if sum(dirs) < -1 else "neutral")
            m1_pre["last5_n_bull"] = sum(1 for d in dirs if d > 0)

    fp["ready"] = True
    fp["capitulation_bars"] = cap_bars
    fp["has_capitulation"] = len(cap_bars) > 0
    fp["latest_cap_rel"] = cap_bars[-1]["rel"] if cap_bars else None
    fp["hammers_last12"] = hammers
    fp["shooting_stars_last12"] = stars
    fp["position_pct"] = round(position, 1)
    fp["position_zone"] = "floor" if position < 20 else ("ceiling" if position > 80 else "mid")
    fp["baseline_close"] = _round_price(baseline)
    fp["window_high"] = _round_price(rng_h)
    fp["window_low"] = _round_price(rng_l)
    fp["range_pct"] = round(range_pct, 1)
    fp["pullback_from_high_pct"] = round(pullback_from_high_pct, 1)
    fp["vol_trend"] = round(vol_trend, 2)
    fp["vol_trend_label"] = "shrinking" if vol_trend < 0.5 else ("expanding" if vol_trend > 2 else "normal")
    fp["segments_30m"] = segs
    fp["m1_post"] = m1_post
    fp["m1_pre"] = m1_pre

    # ---- Quick verdict based on fingerprints ----
    verdict_parts = []
    if cap_bars:
        verdict_parts.append(f"capitulation_bar_at_rel={cap_bars[-1]['rel']}")
    if position < 20:
        verdict_parts.append("floor_price_zone")
    elif position > 80:
        verdict_parts.append("ceiling_price_zone")
    if m1_post.get("chg_5min", 0) > 3:
        verdict_parts.append("pre_signal_5min_pump")
    elif m1_post.get("chg_5min", 0) < -8:
        verdict_parts.append("pre_signal_5min_crash")
    fp["quick_verdict"] = ", ".join(verdict_parts) if verdict_parts else "no_clear_signal"

    return fp


def _read_doc_full(relative_path: str) -> str:
    path = ROOT_DIR / relative_path
    try:
        mtime = path.stat().st_mtime
        cached = _doc_cache.get(relative_path)
        if cached and cached[0] == mtime:
            return cached[1]
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return _doc_cache.get(relative_path, (0, ""))[1]  # return stale cache on error
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    _doc_cache[relative_path] = (mtime, text)
    return text


def load_strategy_reference() -> dict[str, str]:
    return {doc: _read_doc_full(doc) for doc in SOURCE_DOCS}


def build_cached_system_prompt() -> str:
    """
    Build system prompt containing role instructions + full strategy docs.
    Cached in memory — only rebuilds when docs change on disk.
    DeepSeek caches the system prompt prefix, so identical prompts across
    independent API calls benefit from prompt caching (lower latency & cost).
    """
    global _system_prompt_mtime, _system_prompt_cache

    # Check if any doc changed since last build
    latest_mtime = 0.0
    for doc_path in SOURCE_DOCS:
        try:
            mtime = (ROOT_DIR / doc_path).stat().st_mtime
        except OSError:
            mtime = 0.0
        latest_mtime = max(latest_mtime, mtime)

    if _system_prompt_cache and latest_mtime == _system_prompt_mtime:
        return _system_prompt_cache

    # Rebuild: load all docs fresh
    docs = load_strategy_reference()
    doc_texts = []
    for path, text in docs.items():
        if text:
            doc_texts.append(f"### {path}\n\n{text}")

    prompt = (
        "You are a structured purchase-value analysis engine for Solana bottom-abnormal CA alerts. "
        "Analyze 5m structure, 1m micro confirmation, and CA context from the supplied OHLCV and signal data. "
        "The local_fingerprints are pre-computed hints — validate them against raw K-line data, "
        "correct any errors, and incorporate them into your analysis. "
        "Use the CA methodology document as the decision workflow and the 5m fingerprint encyclopedia as the pattern reference. "
        "Every analysis must follow the 03-CA分析方法论 six-step workflow exactly: "
        "1) push record, 2) 5m+1m baseline/peak/trough/pullback, 3) bar-level 5m structure and 1m micro truth, "
        "4) historical peer WR20 baseline by signal×pullback×mcap×ATH, 5) five-dimension scoring, 6) conclusion label with concrete conditions. "
        "Before scoring, explicitly cover the supplied analysis_scope dimensions: push record, chip distribution, 5m price structure, 1m micro structure, volume/liquidity, and observable risk factors. "
        "Populate methodology_steps and methodology_result in the returned JSON. "
        "methodology_result must include step1_push_record, step2_kline_baseline, step3_bar_analysis, step4_historical_baseline, step5_five_dimension_score, and step6_conclusion. "
        "For step6_conclusion.label use one of: 可入场, 等确认, 放弃; include conditions and reasons, but do not include position sizing, stop-loss, or take-profit. "
        "Return observable purchase-value and K-line analysis only. "
        "Do not give trading advice, order instructions, position sizing, stop-loss, or take-profit recommendations. "
        "For purchase_value.label use one of: 高价值观察, 中等价值观察, 低价值/回避, 待观察. "
        "For purchase_value.score_pct return a 0-100 observation score, not a promise of return. "
        "Do not output reasoning text; return the final JSON object directly. "
        "Output JSON only.\n\n"
        "## Reference Strategy Documents\n\n"
        + "\n\n".join(doc_texts)
    )
    _system_prompt_mtime = latest_mtime
    _system_prompt_cache = prompt
    return prompt


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


def normalize_methodology_result(value: dict[str, Any] | None) -> dict[str, Any]:
    data = value if isinstance(value, dict) else {}

    def _step(key: str, limit: int = 220) -> str:
        step = data.get(key)
        if isinstance(step, dict):
            parts = [
                step.get("summary"),
                step.get("basis"),
                step.get("result"),
                step.get("label"),
            ]
            return _safe_text("；".join(str(item) for item in parts if item), limit)
        return _safe_text(step, limit)

    conclusion = data.get("step6_conclusion") if isinstance(data.get("step6_conclusion"), dict) else {}
    label = _safe_text(conclusion.get("label"), 20)
    if label not in {"可入场", "等确认", "放弃"}:
        label = "等确认"
    return {
        "step1_push_record": _step("step1_push_record"),
        "step2_kline_baseline": _step("step2_kline_baseline"),
        "step3_bar_analysis": _step("step3_bar_analysis"),
        "step4_historical_baseline": _step("step4_historical_baseline"),
        "step5_five_dimension_score": _step("step5_five_dimension_score"),
        "step6_conclusion": {
            "label": label,
            "conditions": _safe_list(conclusion.get("conditions"), 4, 120),
            "reasons": _safe_list(conclusion.get("reasons"), 4, 120),
        },
    }


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
    purchase = data.get("purchase_value") if isinstance(data.get("purchase_value"), dict) else {}
    methodology = data.get("methodology_result") if isinstance(data.get("methodology_result"), dict) else {}
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
        "purchase_value": {
            "label": _safe_text(purchase.get("label"), 40),
            "score_pct": max(0.0, min(100.0, round(_to_float(purchase.get("score_pct")), 1))),
            "basis": _safe_text(purchase.get("basis"), 180),
        },
        "analysis_scope": list(ANALYSIS_SCOPE),
        "methodology_steps": list(METHODOLOGY_STEPS),
        "methodology_result": normalize_methodology_result(methodology),
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
    local_fp: dict[str, Any] | None = None,
    signal_ts: int = 0,
) -> dict[str, Any]:
    signal_ts = _to_int(signal_ts) or _to_int((signal or {}).get("event_ts") or (signal or {}).get("signal_ts"))
    payload: dict[str, Any] = {
        "task": "bottom_abnormal_ca_kline_prediction",
        "schema": REQUIRED_SCHEMA,
        "analysis_scope": list(ANALYSIS_SCOPE),
        "methodology_steps": list(METHODOLOGY_STEPS),
        "address": address,
        "signal": signal,
        "signal_ts": signal_ts,
        "kline_5m": compact_candles(candles_5m, MAX_5M_CANDLES),
        "kline_1m": compact_candles(candles_1m, MAX_1M_CANDLES),
    }
    if local_fp and local_fp.get("ready"):
        payload["local_fingerprints"] = {
            "capitulation_bars": local_fp.get("capitulation_bars"),
            "has_capitulation": local_fp.get("has_capitulation"),
            "hammers_last12": local_fp.get("hammers_last12"),
            "shooting_stars_last12": local_fp.get("shooting_stars_last12"),
            "position_pct": local_fp.get("position_pct"),
            "position_zone": local_fp.get("position_zone"),
            "vol_trend": local_fp.get("vol_trend"),
            "vol_trend_label": local_fp.get("vol_trend_label"),
            "segments_30m": local_fp.get("segments_30m"),
            "m1_post": local_fp.get("m1_post"),
            "m1_pre": local_fp.get("m1_pre"),
            "quick_verdict": local_fp.get("quick_verdict"),
        }
    return payload


def analyze_deepseek_kline_prediction(
    *,
    address: str,
    signal: dict[str, Any],
    candles_5m: list[dict[str, Any]] | None,
    candles_1m: list[dict[str, Any]] | None,
    signal_ts: int = 0,
) -> dict[str, Any]:
    signal_ts = _to_int(signal_ts) or _to_int((signal or {}).get("event_ts") or (signal or {}).get("signal_ts"))
    # Always compute local fingerprints (cheap, no API)
    local_fp = compute_local_fingerprints(candles_5m, candles_1m, signal_ts=signal_ts)

    if not DEEPSEEK_KLINE_ENABLED:
        return _fallback_from_fingerprints(local_fp, status="disabled", signal=signal)
    if not DEEPSEEK_API_KEY:
        return _fallback_from_fingerprints(local_fp, status="missing_api_key", signal=signal)

    prompt_payload = build_prompt_payload(
        address=address,
        signal=signal,
        candles_5m=candles_5m,
        candles_1m=candles_1m,
        local_fp=local_fp,
        signal_ts=signal_ts,
    )
    if not prompt_payload["kline_5m"] or not prompt_payload["kline_1m"]:
        return _fallback_from_fingerprints(local_fp, status="missing_kline_data", signal=signal)

    # System prompt: role + full strategy docs (cached by DeepSeek across calls)
    system_prompt = build_cached_system_prompt()

    request_payload = {
        "model": DEEPSEEK_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(prompt_payload, ensure_ascii=False, separators=(",", ":"))},
        ],
        "temperature": 0.1,
        "max_tokens": 4096,
        "response_format": {"type": "json_object"},
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
        return _fallback_from_fingerprints(local_fp, status=f"exception:{exc}", signal=signal)

    elapsed_ms = int((time.time() - started) * 1000)
    if not resp.ok:
        return _fallback_from_fingerprints(local_fp, status=f"http_{resp.status_code}", signal=signal)

    try:
        message = resp.json().get("choices", [{}])[0].get("message", {}) or {}
        content = message.get("content") or ""
        reasoning_content = message.get("reasoning_content") or ""
    except (ValueError, TypeError, KeyError, IndexError) as exc:
        return _fallback_from_fingerprints(local_fp, status=f"bad_response:{exc}", signal=signal)

    if not content and reasoning_content:
        return _fallback_from_fingerprints(local_fp, status="reasoning_only_no_json", signal=signal)

    data = _extract_json_object(content)
    if not data:
        return _fallback_from_fingerprints(local_fp, status="json_parse_failed", signal=signal)

    result = normalize_prediction(data, model=DEEPSEEK_MODEL, elapsed_ms=elapsed_ms)
    # Attach local fingerprints for downstream consumers
    result["local_fingerprints"] = local_fp
    return result


def _fallback_methodology_result(local_fp: dict[str, Any], status: str, signal: dict[str, Any] | None = None) -> dict[str, Any]:
    signal = signal or {}
    m1p = local_fp.get("m1_post", {}) if isinstance(local_fp.get("m1_post"), dict) else {}
    signal_type = str(signal.get("signal_type") or "unknown")
    mcap = _to_float(signal.get("current_mcap"))
    ath = _to_float(signal.get("ath_mcap") or signal.get("pre_signal_ath_mcap"))
    ath_ratio = ath / mcap if mcap > 0 and ath > 0 else 0.0
    pos = _to_float(local_fp.get("position_pct"), 50.0)
    chg5 = _to_float(m1p.get("chg_5min"))
    has_cap = bool(local_fp.get("has_capitulation"))
    green_lights = 0
    red_lights = 0
    if signal_type == "new_revival":
        green_lights += 1
    elif signal_type == "abnormal":
        red_lights += 1
    if _to_float(local_fp.get("pullback_from_high_pct")) > -20:
        green_lights += 1
    elif _to_float(local_fp.get("pullback_from_high_pct")) <= -50:
        red_lights += 1
    if ath_ratio >= 3:
        green_lights += 1
    elif 0 < ath_ratio < 1.5:
        red_lights += 1
    if chg5 > 3:
        green_lights += 1
    elif chg5 < -3:
        red_lights += 1
    if has_cap and pos < 35:
        green_lights += 1
    elif pos > 80 and not has_cap:
        red_lights += 1
    if green_lights >= 4:
        label = "可入场"
    elif green_lights >= 3 and red_lights <= 1:
        label = "等确认"
    else:
        label = "放弃"
    return {
        "step1_push_record": (
            f"signal={signal_type}, mcap={mcap:.0f}, ath_ratio={ath_ratio:.2f}x, "
            f"pool_ratio={_to_float(signal.get('pool_mcap_ratio')):.2%}, age={_to_int(signal.get('age_sec'))}s"
        ),
        "step2_kline_baseline": (
            f"baseline={local_fp.get('baseline_close')}, high={local_fp.get('window_high')}, "
            f"low={local_fp.get('window_low')}, range={local_fp.get('range_pct')}%, "
            f"pullback={local_fp.get('pullback_from_high_pct')}%"
        ),
        "step3_bar_analysis": (
            f"5m pos={pos:.1f}%/{local_fp.get('position_zone')}, cap={has_cap}, "
            f"vol={local_fp.get('vol_trend_label')}; 1m 5min={chg5:+.1f}%, "
            f"direction={m1p.get('direction', '-')}"
        ),
        "step4_historical_baseline": (
            "local fallback uses guide bucket baseline; JSONL peer traversal requires DeepSeek/API path or offline entry_check"
        ),
        "step5_five_dimension_score": (
            f"green={green_lights}, red={red_lights}; dimensions=signal,pullback,ATH,1m_confirm,5m_pattern"
        ),
        "step6_conclusion": {
            "label": label,
            "conditions": [
                f"1m 5min change {chg5:+.1f}%",
                f"5m position {pos:.1f}% in window",
            ],
            "reasons": [
                f"DeepSeek unavailable ({status}); hard traversal used local K-line fingerprints",
                local_fp.get("quick_verdict", "no_clear_signal"),
            ],
        },
    }


def _fallback_from_fingerprints(local_fp: dict[str, Any], status: str, signal: dict[str, Any] | None = None) -> dict[str, Any]:
    """Build a minimal prediction from local fingerprints when DeepSeek is unavailable."""
    if not local_fp.get("ready"):
        return {
            "ready": False,
            "status": status,
            "analysis_scope": list(ANALYSIS_SCOPE),
            "methodology_steps": list(METHODOLOGY_STEPS),
            "methodology_result": normalize_methodology_result(_fallback_methodology_result(local_fp, status, signal)),
            "source_docs": list(SOURCE_DOCS),
        }

    pos = local_fp.get("position_pct", 50)
    has_cap = local_fp.get("has_capitulation", False)
    m1p = local_fp.get("m1_post", {})
    chg5 = m1p.get("chg_5min", 0)

    # Derive bias and confidence from local fingerprints
    if has_cap and pos < 30 and chg5 > -3:
        bias = "bullish"
        confidence = "medium"
        summary = f"投降Bar+地板价(pos={pos:.0f}%), 5min={chg5:+.1f}%"
    elif has_cap and pos < 30:
        bias = "bullish"
        confidence = "low"
        summary = f"投降Bar+地板价, 但5min未确认({chg5:+.1f}%)"
    elif pos > 70 and not has_cap:
        bias = "bearish"
        confidence = "medium"
        summary = f"天花板价(pos={pos:.0f}%), 无投降清洗"
    elif chg5 > 3:
        bias = "bullish"
        confidence = "medium"
        summary = f"异动前5min涨{chg5:+.1f}%, 量价确认"
    elif chg5 < -8:
        bias = "bearish"
        confidence = "medium"
        summary = f"异动前5min跌{chg5:+.1f}%, 微结构偏弱"
    else:
        bias = "neutral"
        confidence = "low"
        summary = f"pos={pos:.0f}%, 5min={chg5:+.1f}%, 方向不明"

    return {
        **REQUIRED_SCHEMA,
        "ready": True,
        "status": f"local_fallback({status})",
        "model": "local_bar_fingerprints",
        "elapsed_ms": 0,
        "generated_at": int(time.time()),
        "summary": summary[:120],
        "bias": bias,
        "confidence": confidence,
        "pattern_5m": {
            "label": f"pos={local_fp.get('position_zone','?')}, cap={has_cap}",
            "basis": f"cap_bars={len(local_fp.get('capitulation_bars',[]))}, hammers={local_fp.get('hammers_last12',0)}",
            "doc_wr20_pct": 66.7 if (has_cap and pos < 30) else (45.0 if not has_cap else 52.0),
            "risk_level": "low" if (has_cap and pos < 30) else ("high" if pos > 70 else "medium"),
        },
        "micro_1m": {
            "label": f"5min={chg5:+.1f}%, {m1p.get('direction','?')}",
            "basis": f"vol_ratio={m1p.get('vol_ratio',0)}",
            "change_pct": round(chg5, 2),
            "volume_ratio": round(m1p.get("vol_ratio", 0), 2),
            "decision": "confirm" if chg5 > 3 else ("reject" if chg5 < -8 else "wait"),
        },
        "forecast": {
            "next_5m": f"bias={bias}",
            "next_30m": "need 30min data" if "chg_30min" not in m1p else f"30min={m1p.get('chg_30min',0):+.1f}%",
            "next_4h": f"cap={has_cap}, pos={pos:.0f}%",
        },
        "purchase_value": {
            "label": "待观察",
            "score_pct": 0.0,
            "basis": f"DeepSeek unavailable ({status}); local fingerprints only",
        },
        "strategy_observations": [
            f"local_fallback: DeepSeek unavailable ({status})",
            local_fp.get("quick_verdict", ""),
        ],
        "risk_factors": (
            ["ceiling_price_zone"] if pos > 70 else []
        ) + (
            ["capitulation_without_recovery"] if has_cap and chg5 < -5 else []
        ),
        "watch_windows": ["5min", "30min"],
        "analysis_scope": list(ANALYSIS_SCOPE),
        "methodology_steps": list(METHODOLOGY_STEPS),
        "methodology_result": normalize_methodology_result(_fallback_methodology_result(local_fp, status, signal)),
        "source_docs": list(SOURCE_DOCS),
        "local_fingerprints": local_fp,
    }


# =========================================================================
#  Prompt cache warmup (runs once on import, non-blocking)
# =========================================================================

_warmup_done = False


def warmup_deepseek_cache() -> None:
    """
    Send a lightweight call to DeepSeek with the cached system prompt.
    This pre-warms DeepSeek's prompt cache so the first real prediction
    doesn't time out processing the 22K system prompt.
    Runs in a background thread, never blocks.
    """
    global _warmup_done
    if _warmup_done or not DEEPSEEK_API_KEY or not DEEPSEEK_KLINE_ENABLED:
        return
    _warmup_done = True

    import threading

    def _warmup():
        try:
            system_prompt = build_cached_system_prompt()
            payload = {
                "model": DEEPSEEK_MODEL,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": '{"task":"cache_warmup","reply":"cached"}'},
                ],
                "max_tokens": 20,
                "temperature": 0.0,
            }
            requests.post(
                f"{DEEPSEEK_BASE_URL.rstrip('/')}/chat/completions",
                headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"},
                json=payload,
                timeout=DEEPSEEK_TIMEOUT,
            )
        except Exception:
            pass  # warmup failure is non-fatal

    threading.Thread(target=_warmup, daemon=True).start()
