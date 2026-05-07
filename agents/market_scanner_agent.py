from __future__ import annotations

from typing import Any

from agents.base import BaseAgent
from agents.context import AgentContext


class MarketScannerAgent(BaseAgent):
    """Prepare a CA for deeper analysis.

    Current version is a skeleton. Existing scanners can call this first, then
    progressively move GMGN and database fetching logic here.
    """

    name = "market_scanner"

    def observe(self, context: AgentContext) -> dict[str, Any]:
        return {
            "ca": context.ca,
            "chain": context.chain,
            "source": context.source,
            "token": context.token,
        }

    def think(self, observation: dict[str, Any]) -> dict[str, Any]:
        ca = str(observation.get("ca") or "").strip()
        return {
            "ready": bool(ca),
            "reason": "ok" if ca else "missing_ca",
        }

    def act(self, context: AgentContext, decision: dict[str, Any]) -> AgentContext:
        context.decision["market_scanner"] = decision
        return context

