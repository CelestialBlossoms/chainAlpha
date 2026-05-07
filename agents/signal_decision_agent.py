from __future__ import annotations

from typing import Any

from agents.base import BaseAgent
from agents.context import AgentContext
from agents.tools import safe_float
from bottom_detection import bottom_accumulation_monitor as bottom


class SignalDecisionAgent(BaseAgent):
    """Decide whether a token should be pushed, tracked, or removed."""

    name = "signal_decision"

    def observe(self, context: AgentContext) -> dict[str, Any]:
        stats = context.stats or {}
        bottom_decision = context.decision.get("bottom_signal") or {}
        analysis = bottom_decision.get("analysis") or {}
        signal_type = str(analysis.get("signal_type") or "")
        if "already_notified" in bottom_decision:
            already_notified = bool(bottom_decision.get("already_notified"))
        else:
            already_notified = bottom.previous_signal_exists(context.ca, signal_type) if signal_type else False
        if "has_previous_bottom_signal" in bottom_decision:
            has_previous_bottom_signal = bool(bottom_decision.get("has_previous_bottom_signal"))
        else:
            has_previous_bottom_signal = bottom.previous_bottom_signal_exists(context.ca)
        return {
            "ca": context.ca,
            "symbol": context.symbol,
            "mcap": safe_float(stats.get("mcap")),
            "pool_liquidity": safe_float(stats.get("pool_liquidity")),
            "price_change_pct": safe_float(stats.get("price_change_pct")),
            "pool_mcap_ratio": safe_float(stats.get("pool_mcap_ratio")),
            "chip_analysis": stats.get("chip_analysis") or {},
            "analysis": analysis,
            "should_notify": bool(bottom_decision.get("should_notify")),
            "already_notified": already_notified,
            "has_previous_bottom_signal": has_previous_bottom_signal,
            "signal_text": bottom_decision.get("signal_text") or "",
        }

    def think(self, observation: dict[str, Any]) -> dict[str, Any]:
        mcap = safe_float(observation.get("mcap"))
        pool_liquidity = safe_float(observation.get("pool_liquidity"))
        analysis = observation.get("analysis") or {}
        signal_type = str(analysis.get("signal_type") or "watch")
        should_notify = bool(observation.get("should_notify"))
        already_notified = bool(observation.get("already_notified"))
        has_previous_bottom_signal = bool(observation.get("has_previous_bottom_signal"))

        if 0 < mcap < 10_000:
            return {
                "action": "delete_frontend",
                "reason": "mcap_below_10k",
                "tg": False,
                "frontend": True,
                "delete": True,
            }

        if 0 < pool_liquidity < 10_000:
            return {
                "action": "delete_watchlist",
                "reason": "pool_liquidity_below_10k",
                "tg": False,
                "frontend": True,
                "delete": True,
            }

        if should_notify and not already_notified:
            return {
                "action": "push_tg_and_frontend",
                "reason": f"new_signal:{signal_type}",
                "tg": True,
                "frontend": True,
                "delete": False,
            }

        if should_notify and already_notified:
            return {
                "action": "frontend_update",
                "reason": f"already_notified:{signal_type}",
                "tg": False,
                "frontend": True,
                "delete": False,
            }

        if has_previous_bottom_signal:
            return {
                "action": "frontend_update",
                "reason": "previous_bottom_signal_tracking",
                "tg": False,
                "frontend": True,
                "delete": False,
            }

        return {
            "action": "observe",
            "reason": "no_signal",
            "tg": False,
            "frontend": False,
            "delete": False,
        }

    def act(self, context: AgentContext, decision: dict[str, Any]) -> AgentContext:
        context.decision["signal_decision"] = decision
        return context
