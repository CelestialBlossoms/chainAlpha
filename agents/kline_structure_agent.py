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
            "summary": summary,
            "analysis": analysis,
        }

    def think(self, observation: dict[str, Any]) -> dict[str, Any]:
        candles = observation.get("candles") or []
        summary = observation.get("summary") or {}
        current_mcap = safe_float(summary.get("mcap"))
        fib = fibonacci_retracement_from_candles(candles, current_mcap)
        return {
            "fibonacci": fib,
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
