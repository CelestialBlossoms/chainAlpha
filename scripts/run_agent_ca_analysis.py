from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from agents.bottom_signal_agent import BottomSignalAgent
from agents.action_executor_agent import ActionExecutorAgent
from agents.chip_analysis_agent import ChipAnalysisAgent
from agents.context import AgentContext
from agents.kline_structure_agent import KlineStructureAgent
from agents.signal_decision_agent import SignalDecisionAgent


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Agent-based CA analysis.")
    parser.add_argument("ca", help="Solana token CA")
    parser.add_argument("--chain", default="sol")
    parser.add_argument("--source", default="manual_agent")
    parser.add_argument("--json", action="store_true", help="Print compact JSON instead of text.")
    parser.add_argument("--execute", action="store_true", help="Execute TG/frontend/delete actions. Default is dry-run.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    context = AgentContext(ca=args.ca, chain=args.chain, source=args.source)
    context = BottomSignalAgent().run(context)
    context = KlineStructureAgent().run(context)
    context = ChipAnalysisAgent().run(context)
    context = SignalDecisionAgent().run(context)
    context = ActionExecutorAgent(execute=args.execute).run(context)

    bottom_decision = context.decision.get("bottom_signal") or {}
    chip_decision = context.decision.get("chip_analysis") or {}
    kline_decision = context.decision.get("kline_structure") or {}
    signal_decision = context.decision.get("signal_decision") or {}
    action_execution = context.decision.get("action_executor") or {}
    analysis = bottom_decision.get("analysis") or {}
    if args.json:
        print(
            json.dumps(
                {
                    "ca": context.ca,
                    "stats": context.stats,
                    "analysis": analysis,
                    "kline_structure": kline_decision,
                    "chip_analysis": chip_decision,
                    "signal_decision": signal_decision,
                    "action_execution": action_execution,
                    "should_notify": bottom_decision.get("should_notify"),
                },
                ensure_ascii=False,
                default=str,
            )
        )
        return 0

    print(bottom_decision.get("signal_text") or "")
    print()
    print(
        "Agent decision: "
        f"signal_type={analysis.get('signal_type')} "
        f"rule={analysis.get('abnormal_rule')} "
        f"notify={bottom_decision.get('should_notify')} "
        f"action={signal_decision.get('action')} "
        f"reason={signal_decision.get('reason')} "
        f"execute={action_execution.get('execute')}"
    )
    chip = chip_decision or {}
    print(
        "Chip analysis: "
        f"holders={chip.get('holder_count')} "
        f"top10={safe_fmt(chip.get('top10_hold_pct'))} "
        f"top100={safe_fmt(chip.get('top100_hold_pct'))} "
        f"bundles={len(chip.get('bundle_similarity_clusters') or [])} "
        f"creation_clusters={len(chip.get('wallet_creation_clusters') or [])}"
    )
    fib = (kline_decision.get("fibonacci") or {})
    if fib.get("ready"):
        print(
            "Fibonacci: "
            f"position={float(fib.get('position') or 0):.3f} "
            f"retracement={float(fib.get('retracement_from_high') or 0):.1%} "
            f"nearest={fib.get('nearest_level')}"
        )
    print(f"Execution plan: {action_execution.get('plan') or []}")
    print(f"Execution results: {action_execution.get('results') or []}")
    return 0


def safe_fmt(value) -> str:
    try:
        return f"{float(value):.2%}"
    except (TypeError, ValueError):
        return "-"


if __name__ == "__main__":
    raise SystemExit(main())
