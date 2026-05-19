from __future__ import annotations

from typing import Any

from agents.base import BaseAgent
from agents.context import AgentContext
from agents.tools import safe_float


FIB_LEVELS = (0.0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0)


class KlineStructureAgent(BaseAgent):
    """Analyze K-line structure such as Fibonacci retracement zones."""

    name = "kline_structure"

    def observe(self, context: AgentContext) -> dict[str, Any]:
        bottom_decision = context.decision.get("bottom_signal") or {}
        summary = bottom_decision.get("summary") or {}
        analysis = bottom_decision.get("analysis") or {}
        return {
            "candles": context.candles,
            "raw_holders": context.raw_holders,
            "holders": context.holders,
            "summary": summary,
            "analysis": analysis,
        }

    def think(self, observation: dict[str, Any]) -> dict[str, Any]:
        candles = observation.get("candles") or []
        raw_holders = observation.get("raw_holders") or []
        holders = observation.get("holders") or []
        summary = observation.get("summary") or {}
        current_mcap = safe_float(summary.get("mcap"))
        fib = fibonacci_retracement_from_candles(candles, current_mcap)
        sideways = sideways_structure_from_candles(candles)
        volume_health = volume_health_from_candles(candles)
        holder_flow = holder_flow_structure(holders)
        bundle_unwind = bundle_unwind_structure(raw_holders, holders)
        structure_health = structure_health_score(fib, sideways, volume_health, holder_flow, bundle_unwind)
        return {
            "fibonacci": fib,
            "sideways": sideways,
            "volume_health": volume_health,
            "holder_flow": holder_flow,
            "bundle_unwind": bundle_unwind,
            "structure_health": structure_health,
        }

    def act(self, context: AgentContext, decision: dict[str, Any]) -> AgentContext:
        context.stats["kline_structure"] = decision
        context.decision[self.name] = decision
        return context


def fibonacci_retracement_from_candles(candles: list[dict[str, Any]], current_mcap: float) -> dict[str, Any]:
    valid = []
    for candle in candles or []:
        high = safe_float(candle.get("high"))
        low = safe_float(candle.get("low"))
        close = safe_float(candle.get("close"))
        if high > 0 and low > 0 and close > 0:
            valid.append(
                {
                    "ts": candle.get("ts"),
                    "high": high,
                    "low": low,
                    "close": close,
                }
            )

    if len(valid) < 2 or current_mcap <= 0:
        return {"ready": False, "reason": "not_enough_data"}

    current_close = valid[-1]["close"]
    high_index, high_candle = max(enumerate(valid), key=lambda item: item[1]["high"])

    start_index = None
    if high_index > 0:
        start_index, start_candle = min(
            ((index, valid[index]) for index in range(0, high_index + 1)),
            key=lambda item: item[1]["low"],
        )
        mode = "bottom_to_high"
    else:
        start_index, start_candle = min(enumerate(valid), key=lambda item: item[1]["low"])
        mode = "range_low_to_high"

    bottom_price = start_candle["low"]
    high_price = high_candle["high"]
    if bottom_price <= 0 or high_price <= bottom_price:
        return {"ready": False, "reason": "invalid_bottom_or_high"}

    bottom_mcap = current_mcap * (bottom_price / current_close) if current_close > 0 else 0.0
    high_mcap = current_mcap * (high_price / current_close) if current_close > 0 else 0.0
    if bottom_mcap <= 0 or high_mcap <= bottom_mcap:
        return {"ready": False, "reason": "invalid_mcap_projection"}

    position = (current_mcap - bottom_mcap) / (high_mcap - bottom_mcap)
    position = max(0.0, min(position, 1.0))
    retracement_from_high = 1.0 - position
    levels = {
        str(level): {
            "ratio": level,
            "mcap": bottom_mcap + (high_mcap - bottom_mcap) * level,
        }
        for level in FIB_LEVELS
    }
    nearest_level = min(FIB_LEVELS, key=lambda level: abs(position - level))

    return {
        "ready": True,
        "mode": mode,
        "bottom_price": bottom_price,
        "bottom_ts": start_candle.get("ts"),
        "bottom_index": start_index,
        "bottom_mcap": bottom_mcap,
        "high_price": high_price,
        "high_ts": high_candle.get("ts"),
        "high_index": high_index,
        "high_mcap": high_mcap,
        "current_price": current_close,
        "current_mcap": current_mcap,
        "position": position,
        "retracement_from_high": retracement_from_high,
        "nearest_level": nearest_level,
        "levels": levels,
    }


def valid_candles(candles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    valid = []
    for candle in candles or []:
        open_price = safe_float(candle.get("open"))
        high = safe_float(candle.get("high"))
        low = safe_float(candle.get("low"))
        close = safe_float(candle.get("close"))
        volume = safe_float(candle.get("volume"))
        if open_price > 0 and high > 0 and low > 0 and close > 0:
            valid.append(
                {
                    "ts": candle.get("ts"),
                    "open": open_price,
                    "high": high,
                    "low": low,
                    "close": close,
                    "volume": volume,
                }
            )
    return valid


def sideways_structure_from_candles(candles: list[dict[str, Any]], window: int = 18) -> dict[str, Any]:
    valid = valid_candles(candles)
    if len(valid) < 6:
        return {"ready": False, "reason": "not_enough_candles", "bars": len(valid)}

    recent = valid[-min(window, len(valid)) :]
    first_close = recent[0]["close"]
    last_close = recent[-1]["close"]
    recent_high = max(c["high"] for c in recent)
    recent_low = min(c["low"] for c in recent)
    range_pct = (recent_high - recent_low) / last_close if last_close > 0 else 0.0
    drift_pct = (last_close - first_close) / first_close if first_close > 0 else 0.0
    body_avg_pct = sum(abs(c["close"] - c["open"]) / c["open"] for c in recent) / len(recent)
    lower_close_count = sum(1 for i in range(1, len(recent)) if recent[i]["close"] < recent[i - 1]["close"])
    lower_close_ratio = lower_close_count / max(1, len(recent) - 1)
    near_low_position = (last_close - recent_low) / (recent_high - recent_low) if recent_high > recent_low else 0.5

    range_ok = range_pct <= 0.18
    body_ok = body_avg_pct <= 0.06
    drift_ok = abs(drift_pct) <= 0.10
    not_grinding_down = lower_close_ratio <= 0.62 or drift_pct >= -0.04
    stable = range_ok and body_ok and drift_ok and not_grinding_down

    if stable and near_low_position >= 0.35:
        verdict = "sideways_stable"
    elif stable:
        verdict = "sideways_near_low"
    elif drift_pct < -0.10:
        verdict = "drifting_down"
    elif range_pct > 0.28:
        verdict = "too_volatile"
    else:
        verdict = "not_compressed"

    return {
        "ready": True,
        "bars": len(recent),
        "range_pct": range_pct,
        "body_avg_pct": body_avg_pct,
        "drift_pct": drift_pct,
        "lower_close_ratio": lower_close_ratio,
        "range_high": recent_high,
        "range_low": recent_low,
        "near_low_position": near_low_position,
        "stable": stable,
        "verdict": verdict,
    }


def volume_health_from_candles(
    candles: list[dict[str, Any]],
    recent_window: int = 12,
    baseline_window: int = 36,
) -> dict[str, Any]:
    valid = valid_candles(candles)
    if len(valid) < 8:
        return {"ready": False, "reason": "not_enough_candles", "bars": len(valid)}

    recent = valid[-min(recent_window, len(valid)) :]
    baseline_start = max(0, len(valid) - len(recent) - baseline_window)
    baseline = valid[baseline_start : len(valid) - len(recent)] or valid[: -len(recent)] or valid
    recent_avg = sum(c["volume"] for c in recent) / len(recent)
    baseline_avg = sum(c["volume"] for c in baseline) / len(baseline) if baseline else recent_avg
    volume_ratio = recent_avg / baseline_avg if baseline_avg > 0 else 0.0
    max_recent = max(c["volume"] for c in recent)
    spike_ratio = max_recent / recent_avg if recent_avg > 0 else 0.0
    up_volume = 0.0
    down_volume = 0.0
    for i in range(1, len(recent)):
        if recent[i]["close"] > recent[i - 1]["close"]:
            up_volume += recent[i]["volume"]
        elif recent[i]["close"] < recent[i - 1]["close"]:
            down_volume += recent[i]["volume"]
    up_down_ratio = up_volume / down_volume if down_volume > 0 else (999.0 if up_volume > 0 else 0.0)

    dry = volume_ratio < 0.35
    normal = 0.35 <= volume_ratio <= 1.5 and spike_ratio <= 3.5
    controlled_expansion = 1.5 < volume_ratio <= 3.0 and spike_ratio <= 4.5 and up_down_ratio >= 0.8
    distribution_risk = volume_ratio > 1.5 and up_down_ratio < 0.75
    abnormal_spike = spike_ratio > 4.5

    if normal:
        verdict = "normal_volume"
    elif controlled_expansion:
        verdict = "controlled_expansion"
    elif dry:
        verdict = "dry_volume"
    elif distribution_risk:
        verdict = "sell_volume_pressure"
    elif abnormal_spike:
        verdict = "abnormal_spike"
    else:
        verdict = "mixed_volume"

    return {
        "ready": True,
        "recent_bars": len(recent),
        "baseline_bars": len(baseline),
        "recent_avg_volume": recent_avg,
        "baseline_avg_volume": baseline_avg,
        "volume_ratio": volume_ratio,
        "spike_ratio": spike_ratio,
        "up_volume": up_volume,
        "down_volume": down_volume,
        "up_down_volume_ratio": up_down_ratio,
        "normal": normal,
        "controlled_expansion": controlled_expansion,
        "distribution_risk": distribution_risk,
        "verdict": verdict,
    }


def holder_flow_structure(holders: list[dict[str, Any]]) -> dict[str, Any]:
    valid = [h for h in holders or [] if safe_float(h.get("hold_pct")) > 0]
    if not valid:
        return {"ready": False, "reason": "no_holders"}

    buy_volume = sum(safe_float(h.get("buy_volume")) for h in valid)
    sell_volume = sum(safe_float(h.get("sell_volume")) for h in valid)
    hold_pct = sum(safe_float(h.get("hold_pct")) for h in valid)
    net_buy = buy_volume - sell_volume
    buy_sell_ratio = buy_volume / sell_volume if sell_volume > 0 else (999.0 if buy_volume > 0 else 0.0)
    no_sell = [h for h in valid if safe_float(h.get("sell_volume")) <= 0 and int(h.get("sell_count") or 0) <= 0]
    net_buy_holders = [h for h in valid if safe_float(h.get("buy_volume")) > safe_float(h.get("sell_volume"))]
    net_sell_holders = [h for h in valid if safe_float(h.get("sell_volume")) > safe_float(h.get("buy_volume"))]
    active_holders = [h for h in valid if safe_float(h.get("buy_volume")) > 0 or safe_float(h.get("sell_volume")) > 0]

    return {
        "ready": True,
        "holder_count": len(valid),
        "active_holder_count": len(active_holders),
        "hold_pct": hold_pct,
        "buy_volume": buy_volume,
        "sell_volume": sell_volume,
        "net_buy_volume": net_buy,
        "buy_sell_ratio": buy_sell_ratio,
        "no_sell_count": len(no_sell),
        "no_sell_hold_pct": sum(safe_float(h.get("hold_pct")) for h in no_sell),
        "net_buy_count": len(net_buy_holders),
        "net_buy_hold_pct": sum(safe_float(h.get("hold_pct")) for h in net_buy_holders),
        "net_sell_count": len(net_sell_holders),
        "net_sell_hold_pct": sum(safe_float(h.get("hold_pct")) for h in net_sell_holders),
        "net_accumulation": net_buy > 0 and buy_sell_ratio >= 1.05,
    }


def bundle_unwind_structure(raw_holders: list[dict[str, Any]], holders: list[dict[str, Any]]) -> dict[str, Any]:
    raw_by_wallet = {}
    for raw in raw_holders or []:
        wallet = str(raw.get("address") or raw.get("wallet") or "")
        if wallet:
            raw_by_wallet[wallet] = raw

    bundled = []
    for holder in holders or []:
        tags = holder_tags(holder, raw_by_wallet.get(str(holder.get("wallet") or "")))
        if "bundler" in tags:
            bundled.append((holder, tags, raw_by_wallet.get(str(holder.get("wallet") or "")) or {}))

    if not bundled:
        return {"ready": True, "bundler_count": 0, "verdict": "no_current_bundler_holders"}

    buy_volume = sum(safe_float(h.get("buy_volume")) for h, _, _ in bundled)
    sell_volume = sum(safe_float(h.get("sell_volume")) for h, _, _ in bundled)
    hold_pct = sum(safe_float(h.get("hold_pct")) for h, _, _ in bundled)
    no_sell = [(h, raw) for h, _, raw in bundled if safe_float(h.get("sell_volume")) <= 0 and int(h.get("sell_count") or 0) <= 0]
    partial_sold = [(h, raw) for h, _, raw in bundled if safe_float(h.get("sell_volume")) > 0]
    heavy_sold = [
        (h, raw)
        for h, _, raw in bundled
        if safe_float(raw.get("sell_amount_percentage")) >= 0.5
        or (safe_float(h.get("sell_volume")) > 0 and safe_float(h.get("sell_volume")) >= safe_float(h.get("buy_volume")) * 0.5)
    ]
    net_buy = [(h, raw) for h, _, raw in bundled if safe_float(h.get("buy_volume")) > safe_float(h.get("sell_volume"))]
    sell_ratio = sell_volume / buy_volume if buy_volume > 0 else 0.0

    if sell_ratio >= 0.7 and hold_pct <= 0.08:
        verdict = "bundler_mostly_unwound"
    elif sell_ratio >= 0.4 and len(net_buy) >= len(heavy_sold):
        verdict = "bundler_partial_unwind_absorbed"
    elif sell_ratio >= 0.4:
        verdict = "bundler_partial_unwind"
    elif buy_volume > sell_volume:
        verdict = "bundler_still_accumulating"
    else:
        verdict = "bundler_mixed"

    return {
        "ready": True,
        "bundler_count": len(bundled),
        "bundler_hold_pct": hold_pct,
        "bundler_buy_volume": buy_volume,
        "bundler_sell_volume": sell_volume,
        "bundler_net_buy_volume": buy_volume - sell_volume,
        "bundler_sell_ratio": sell_ratio,
        "bundler_no_sell_count": len(no_sell),
        "bundler_no_sell_hold_pct": sum(safe_float(h.get("hold_pct")) for h, _ in no_sell),
        "bundler_partial_sold_count": len(partial_sold),
        "bundler_heavy_sold_count": len(heavy_sold),
        "bundler_net_buy_count": len(net_buy),
        "verdict": verdict,
    }


def holder_tags(holder: dict[str, Any], raw_holder: dict[str, Any] | None = None) -> set[str]:
    tags: set[str] = set()
    for source in (holder.get("tags"), (raw_holder or {}).get("maker_token_tags"), (raw_holder or {}).get("tags")):
        if isinstance(source, list):
            tags.update(str(tag) for tag in source if tag)
        elif source:
            tags.add(str(source))
    return tags


def structure_health_score(
    fib: dict[str, Any],
    sideways: dict[str, Any],
    volume_health: dict[str, Any],
    holder_flow: dict[str, Any],
    bundle_unwind: dict[str, Any],
) -> dict[str, Any]:
    score = 0
    reasons: list[str] = []

    if fib.get("ready"):
        retracement = safe_float(fib.get("retracement_from_high"))
        if 0.382 <= retracement <= 0.618:
            score += 25
            reasons.append("fib_healthy_retracement")
        elif 0.236 <= retracement < 0.382 or 0.618 < retracement <= 0.786:
            score += 15
            reasons.append("fib_acceptable_retracement")
        elif retracement > 0.786:
            reasons.append("fib_deep_retracement")
        else:
            score += 10
            reasons.append("fib_shallow_retracement")

    if sideways.get("ready"):
        if sideways.get("stable"):
            score += 25
            reasons.append(str(sideways.get("verdict")))
        elif sideways.get("verdict") == "sideways_near_low":
            score += 15
            reasons.append("sideways_near_low")
        else:
            reasons.append(str(sideways.get("verdict")))

    if volume_health.get("ready"):
        if volume_health.get("normal"):
            score += 20
            reasons.append("volume_normal")
        elif volume_health.get("controlled_expansion"):
            score += 16
            reasons.append("volume_controlled_expansion")
        elif volume_health.get("distribution_risk"):
            reasons.append("volume_distribution_risk")
        else:
            reasons.append(str(volume_health.get("verdict")))

    if holder_flow.get("ready"):
        if holder_flow.get("net_accumulation"):
            score += 15
            reasons.append("top_holders_net_accumulation")
        elif safe_float(holder_flow.get("buy_sell_ratio")) >= 0.85:
            score += 8
            reasons.append("top_holders_flow_balanced")
        else:
            reasons.append("top_holders_sell_pressure")

    if bundle_unwind.get("ready"):
        verdict = str(bundle_unwind.get("verdict") or "")
        if verdict == "bundler_partial_unwind_absorbed":
            score += 15
            reasons.append(verdict)
        elif verdict == "bundler_mostly_unwound":
            score += 10
            reasons.append(verdict)
        elif verdict == "bundler_still_accumulating":
            score += 8
            reasons.append(verdict)
        else:
            reasons.append(verdict)

    if score >= 80:
        verdict = "stable_absorption"
    elif score >= 65:
        verdict = "watch_stable"
    elif score >= 45:
        verdict = "mixed_structure"
    else:
        verdict = "unstable_or_distribution"

    return {
        "score": min(score, 100),
        "verdict": verdict,
        "reasons": reasons,
    }
