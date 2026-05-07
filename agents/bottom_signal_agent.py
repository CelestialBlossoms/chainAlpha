from __future__ import annotations

from typing import Any

from agents.base import BaseAgent
from agents.context import AgentContext
from bottom_detection import bottom_accumulation_monitor as bottom


class BottomSignalAgent(BaseAgent):
    """Run bottom-abnormal and EMA-ready data collection for one CA.

    This agent intentionally reuses the existing bottom monitor functions first.
    That keeps behavior aligned with production while giving us a smaller,
    runnable entry point for gradual refactoring.
    """

    name = "bottom_signal"

    def observe(self, context: AgentContext) -> dict[str, Any]:
        token: dict[str, Any] = {
            "address": context.ca,
            "source": context.source or "agent_manual",
            "_sources": [context.source or "agent_manual"],
        }

        info, security = bottom.fetch_token_metadata(context.ca)
        token = bottom.merge_token_metadata(token, info, security)
        pool_data = bottom.fetch_token_pool(context.ca)
        token = bottom.attach_token_pool(token, pool_data)
        kline_resolution = bottom.token_kline_resolution(token)
        candles = bottom.fetch_kline(context.ca, kline_resolution, token)
        raw_holders = bottom.fetch_top100_holders(context.ca)
        summary, holders = bottom.build_snapshot_json(token, raw_holders, candles, kline_resolution)
        history = bottom.recent_snapshots(context.ca)

        return {
            "token": token,
            "gmgn_info": info,
            "gmgn_security": security,
            "gmgn_pool": pool_data,
            "kline_resolution": kline_resolution,
            "candles": candles,
            "raw_holders": raw_holders,
            "holders": holders,
            "summary": summary,
            "history": history,
        }

    def think(self, observation: dict[str, Any]) -> dict[str, Any]:
        analysis = bottom.analyze_abnormal_snapshot(
            observation["holders"],
            observation["history"],
            observation["summary"],
        )
        token = observation["token"]
        return {
            "token": token,
            "summary": observation["summary"],
            "gmgn_info": observation["gmgn_info"],
            "gmgn_pool": observation["gmgn_pool"],
            "raw_holders": observation["raw_holders"],
            "holders": observation["holders"],
            "candles": observation["candles"],
            "history": observation["history"],
            "analysis": analysis,
            "signal_text": bottom.abnormal_signal_text(token, analysis),
            "should_notify": bottom.should_notify(analysis),
        }

    def act(self, context: AgentContext, decision: dict[str, Any]) -> AgentContext:
        analysis = decision["analysis"]
        context.token = decision.get("token", context.token)
        context.gmgn_info = decision.get("gmgn_info") or {}
        context.gmgn_pool = decision.get("gmgn_pool") or {}
        context.raw_holders = decision.get("raw_holders") or []
        context.holders = decision.get("holders") or []
        context.candles = decision.get("candles") or []
        context.history = decision.get("history") or []
        context.symbol = str((context.token or {}).get("symbol") or context.symbol or "")
        context.stats.update(
            {
                "source_agent": self.name,
                "signal_type": analysis.get("signal_type"),
                "abnormal_rule": analysis.get("abnormal_rule"),
                "mcap": analysis.get("current_mcap", 0),
                "ath_mcap": analysis.get("ath_mcap", 0),
                "price_change_pct": analysis.get("price_change_pct", 0),
                "pool_liquidity": analysis.get("pool_total_liquidity", 0),
                "pool_mcap_ratio": analysis.get("pool_mcap_ratio", 0),
                "history_count": analysis.get("history_count", 0),
            }
        )
        context.decision[self.name] = decision
        return context
