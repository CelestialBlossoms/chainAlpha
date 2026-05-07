from __future__ import annotations

from typing import Any


class AgentMemory:
    """Thin placeholder around project storage.

    Later this should wrap PostgreSQL and Redis helpers instead of duplicating
    database logic inside individual agents.
    """

    def get_token_history(self, ca: str) -> list[dict[str, Any]]:
        return []

    def save_decision(self, ca: str, decision: dict[str, Any]) -> None:
        return None

