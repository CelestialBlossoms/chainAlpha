from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from agents.context import AgentContext


class BaseAgent(ABC):
    """Small observe/think/act contract for analysis agents."""

    name = "base"

    @abstractmethod
    def observe(self, context: AgentContext) -> dict[str, Any]:
        """Collect the data this agent needs."""

    @abstractmethod
    def think(self, observation: dict[str, Any]) -> dict[str, Any]:
        """Turn observations into a decision or analysis result."""

    @abstractmethod
    def act(self, context: AgentContext, decision: dict[str, Any]) -> AgentContext:
        """Apply the decision to the shared context."""

    def run(self, context: AgentContext) -> AgentContext:
        observation = self.observe(context)
        decision = self.think(observation)
        return self.act(context, decision)

