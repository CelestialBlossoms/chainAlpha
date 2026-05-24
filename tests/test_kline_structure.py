from __future__ import annotations

import json
import unittest
from pathlib import Path

from agents.kline_structure_agent import (
    bundle_unwind_structure,
    fibonacci_retracement_from_candles,
    holder_flow_structure,
    sideways_structure_from_candles,
    structure_health_score,
    volume_health_from_candles,
)


FIXTURES = Path(__file__).resolve().parent / "fixtures"


def load_fixture(name: str):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


class KlineStructureTests(unittest.TestCase):
    def test_stable_absorption_fixture_scores_as_healthy_structure(self) -> None:
        candles = load_fixture("kline_candles.json")
        holders = load_fixture("normalized_holders.json")

        fib = fibonacci_retracement_from_candles(candles, current_mcap=80_000)
        sideways = sideways_structure_from_candles(candles)
        volume = volume_health_from_candles(candles)
        holder_flow = holder_flow_structure(holders)
        bundle_unwind = bundle_unwind_structure([], holders)
        health = structure_health_score(fib, sideways, volume, holder_flow, bundle_unwind)

        self.assertTrue(fib["ready"])
        self.assertGreater(fib["high_mcap"], fib["current_mcap"])
        self.assertTrue(sideways["ready"])
        self.assertTrue(sideways["stable"])
        self.assertEqual(sideways["verdict"], "sideways_stable")
        self.assertTrue(volume["ready"])
        self.assertEqual(volume["verdict"], "normal_volume")
        self.assertTrue(holder_flow["net_accumulation"])
        self.assertEqual(bundle_unwind["verdict"], "bundler_still_accumulating")
        self.assertGreaterEqual(health["score"], 70)
        self.assertIn(health["verdict"], {"watch_stable", "stable_absorption"})


if __name__ == "__main__":
    unittest.main()
