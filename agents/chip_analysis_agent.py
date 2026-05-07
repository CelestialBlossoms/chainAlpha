from __future__ import annotations

from typing import Any

from agents.base import BaseAgent
from agents.context import AgentContext
from agents.tools import safe_float
from tg_ca_chip_alert_bot import (
    aggregate_tag_stats,
    analyze_bottom_chip_sell,
    similar_hold_bundle_clusters,
    wallet_creation_clusters,
)


class ChipAnalysisAgent(BaseAgent):
    """Analyze holder concentration and tagged-wallet structure."""

    name = "chip_analysis"

    def observe(self, context: AgentContext) -> dict[str, Any]:
        return {
            "raw_holders": context.raw_holders,
            "holders": context.holders,
            "stats": context.stats,
            "gmgn_info": context.gmgn_info,
            "summary": context.decision.get("bottom_signal", {}).get("summary") or {},
        }

    def think(self, observation: dict[str, Any]) -> dict[str, Any]:
        raw_holders = observation.get("raw_holders") or []
        holders = observation.get("holders") or []
        summary = observation.get("summary") or {}
        kline_summary = summary.get("kline") or {}
        top10_hold = sum(safe_float(x.get("hold_pct", x.get("amount_percentage"))) for x in holders[:10])
        top20_hold = sum(safe_float(x.get("hold_pct", x.get("amount_percentage"))) for x in holders[:20])
        top100_hold = sum(safe_float(x.get("hold_pct", x.get("amount_percentage"))) for x in holders[:100])
        tag_stats = aggregate_tag_stats(raw_holders or holders)
        bundle_clusters = similar_hold_bundle_clusters(raw_holders or holders)
        creation_clusters = wallet_creation_clusters(raw_holders or holders)
        bottom_sell = analyze_bottom_chip_sell(raw_holders, holders, summary, kline_summary)
        return {
            "holder_count": len(holders),
            "top10_hold_pct": top10_hold,
            "top20_hold_pct": top20_hold,
            "top100_hold_pct": top100_hold,
            "buy_volume": sum(safe_float(x.get("buy_volume", x.get("buy_volume_cur"))) for x in holders),
            "sell_volume": sum(safe_float(x.get("sell_volume", x.get("sell_volume_cur"))) for x in holders),
            "netflow": sum(safe_float(x.get("netflow")) for x in holders),
            "tag_stats": tag_stats,
            "bundle_similarity_clusters": bundle_clusters,
            "wallet_creation_clusters": creation_clusters,
            "bottom_chip_sell": bottom_sell,
            "risk_flags": self._risk_flags(top10_hold, top100_hold),
        }

    def act(self, context: AgentContext, decision: dict[str, Any]) -> AgentContext:
        context.stats["chip_analysis"] = decision
        context.decision["chip_analysis"] = decision
        return context

    @staticmethod
    def _risk_flags(top10_hold: float, top100_hold: float) -> list[str]:
        flags = []
        if top10_hold >= 30:
            flags.append("top10_concentration_high")
        if top100_hold >= 70:
            flags.append("top100_concentration_high")
        return flags
