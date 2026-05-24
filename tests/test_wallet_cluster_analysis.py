from __future__ import annotations

import json
import unittest
from pathlib import Path

from ca_analyzer.wallet_clusters import (
    analyze_bot_clusters,
    analyze_cost_tiers,
    analyze_position_distribution,
    analyze_tag_ecology,
    analyze_trading_behavior,
    analyze_wallet_creation_clusters,
    compute_bundle_score,
    verdict,
)


FIXTURES = Path(__file__).resolve().parent / "fixtures"


def load_fixture(name: str):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


class WalletClusterAnalysisTests(unittest.TestCase):
    def test_bundle_like_top100_fixture_keeps_cluster_signals(self) -> None:
        holders = load_fixture("gmgn_top100_holders.json")

        cost = analyze_cost_tiers(holders)
        position = analyze_position_distribution(holders)
        behavior = analyze_trading_behavior(holders)
        tags = analyze_tag_ecology(holders)
        bots = analyze_bot_clusters(holders)
        creation = analyze_wallet_creation_clusters(holders)
        score, signals = compute_bundle_score(cost, position, behavior, tags, bots, creation)
        label, description = verdict(score)

        self.assertTrue(cost["tight_cost"])
        self.assertTrue(position["exact_match_signal"])
        self.assertTrue(behavior["single_buy_signal"])
        self.assertTrue(behavior["zero_sell_signal"])
        self.assertTrue(tags["single_bot_dominance"])
        self.assertGreater(bots["bot_buy_ratio"], 70)
        self.assertTrue(creation["same_second_signal"])
        self.assertGreaterEqual(score, 6)
        self.assertEqual(label, "CONFIRMED BUNDLE")
        self.assertIn("Organized bot group", description)
        self.assertTrue(any("EXACT_POS_MATCH" in item for item in signals))


if __name__ == "__main__":
    unittest.main()
