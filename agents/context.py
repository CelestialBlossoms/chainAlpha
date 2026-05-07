from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class AgentContext:
    """Shared input/output object passed through agents."""

    ca: str
    chain: str = "sol"
    symbol: str = ""
    source: str = ""
    token: dict[str, Any] = field(default_factory=dict)
    gmgn_info: dict[str, Any] = field(default_factory=dict)
    gmgn_pool: dict[str, Any] = field(default_factory=dict)
    raw_holders: list[dict[str, Any]] = field(default_factory=list)
    holders: list[dict[str, Any]] = field(default_factory=list)
    candles: list[dict[str, Any]] = field(default_factory=list)
    history: list[dict[str, Any]] = field(default_factory=list)
    stats: dict[str, Any] = field(default_factory=dict)
    decision: dict[str, Any] = field(default_factory=dict)
