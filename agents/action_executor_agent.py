from __future__ import annotations

from typing import Any

from agents.base import BaseAgent
from agents.context import AgentContext
from bottom_detection import bottom_accumulation_monitor as bottom
from bottom_detection.bottom_watchlist_store import delete_watchlist_token
from tg_alert_stream import publish_tg_alert


class ActionExecutorAgent(BaseAgent):
    """Execute or dry-run the action chosen by SignalDecisionAgent."""

    name = "action_executor"

    def __init__(self, execute: bool = False) -> None:
        self.execute = execute

    def observe(self, context: AgentContext) -> dict[str, Any]:
        bottom_decision = context.decision.get("bottom_signal") or {}
        signal_decision = context.decision.get("signal_decision") or {}
        analysis = bottom_decision.get("analysis") or {}
        summary = bottom_decision.get("summary") or {}
        token = bottom_decision.get("token") or context.token or {}
        baseline = bottom.first_signal_baseline(context.ca, str(analysis.get("signal_type") or ""))
        extra = bottom.build_bottom_signal_extra(token, summary, analysis, baseline) if analysis else {}
        return {
            "ca": context.ca,
            "token": token,
            "summary": summary,
            "analysis": analysis,
            "signal_text": bottom_decision.get("signal_text") or "",
            "signal_decision": signal_decision,
            "extra": extra,
        }

    def think(self, observation: dict[str, Any]) -> dict[str, Any]:
        decision = observation.get("signal_decision") or {}
        action = decision.get("action") or "observe"
        plan: list[str] = []
        if action == "push_tg_and_frontend":
            plan = ["publish_frontend", "send_tg"]
        elif action == "frontend_update":
            plan = ["publish_frontend"]
        elif action == "delete_frontend":
            plan = ["delete_frontend"]
        elif action == "delete_watchlist":
            plan = ["delete_watchlist", "delete_frontend"]
        elif action == "observe":
            plan = []
        else:
            plan = ["unknown_action"]
        return {
            "execute": self.execute,
            "action": action,
            "reason": decision.get("reason"),
            "plan": plan,
        }

    def act(self, context: AgentContext, decision: dict[str, Any]) -> AgentContext:
        observation = self.observe(context)
        plan = decision.get("plan") or []
        results: list[dict[str, Any]] = []
        if not self.execute:
            context.decision[self.name] = {**decision, "results": [{"step": step, "status": "dry_run"} for step in plan]}
            return context

        for step in plan:
            try:
                if step == "send_tg":
                    bottom.send_tg(observation["signal_text"], extra=observation["extra"])
                    results.append({"step": step, "status": "ok"})
                elif step == "publish_frontend":
                    published = bottom.publish_frontend_signal_update(
                        observation["signal_text"],
                        observation["extra"],
                        status="frontend_update",
                    )
                    results.append({"step": step, "status": "ok" if published else "skipped"})
                    if not published and "send_tg" in plan:
                        break
                elif step == "delete_frontend":
                    publish_tg_alert(
                        f"delete {context.ca}",
                        "deep_alpha_removal",
                        status="delete",
                        ca=context.ca,
                        extra={"address": context.ca, "reason": decision.get("reason")},
                    )
                    results.append({"step": step, "status": "ok"})
                elif step == "delete_watchlist":
                    deleted = delete_watchlist_token(
                        context.ca,
                        str(decision.get("reason") or "agent_delete"),
                        current_mcap=observation["extra"].get("current_mcap"),
                        pool_liquidity=observation["extra"].get("pool_total_liquidity"),
                        pool_mcap_ratio=observation["extra"].get("pool_mcap_ratio"),
                        metadata={"trigger": self.name, "action": decision.get("action")},
                    )
                    results.append({"step": step, "status": "ok", "deleted": bool(deleted)})
                else:
                    results.append({"step": step, "status": "skipped_unknown"})
            except Exception as exc:
                results.append({"step": step, "status": "error", "error": str(exc)})
                break

        context.decision[self.name] = {**decision, "results": results}
        return context
