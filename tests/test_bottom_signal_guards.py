from __future__ import annotations

import json
import unittest
from pathlib import Path

from agents.signal_decision_agent import SignalDecisionAgent
from bottom_detection import bottom_accumulation_monitor as bottom_monitor
from bottom_detection.bottom_accumulation_monitor import compute_risk_tags, should_notify, summarize_kline


FIXTURES = Path(__file__).resolve().parent / "fixtures"


def load_fixture(name: str):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


class BottomSignalGuardTests(unittest.TestCase):
    def test_summarize_kline_keeps_bottom_rebound_and_volume_context(self) -> None:
        candles = load_fixture("kline_candles.json")

        summary = summarize_kline(candles, "1m")

        self.assertEqual(summary["resolution"], "1m")
        self.assertEqual(summary["count"], len(candles))
        self.assertEqual(summary["signal_count"], 12)
        self.assertGreater(summary["bottom_to_current_pct"], 0)
        self.assertGreater(summary["volume_usd"], 0)
        self.assertIn("rebound_after_high", summary)

    def test_risk_tags_classify_known_failure_patterns_without_trade_advice(self) -> None:
        tags = compute_risk_tags(
            {
                "current_mcap": 80_000,
                "ath_mcap": 100_000,
                "price_change_pct": 60,
                "volume_usd": 5_000,
            }
        )

        self.assertIn("瞬爆", tags)
        self.assertIn("天花板", tags)
        self.assertIn("无量", tags)
        self.assertIn("黄金区间", tags)

    def test_signal_decision_push_update_and_delete_paths_are_stable(self) -> None:
        agent = SignalDecisionAgent()

        push = agent.think(
            {
                "mcap": 80_000,
                "pool_liquidity": 30_000,
                "should_notify": True,
                "already_notified": False,
                "has_previous_bottom_signal": False,
                "analysis": {"signal_type": "new_revival"},
            }
        )
        update = agent.think(
            {
                "mcap": 80_000,
                "pool_liquidity": 30_000,
                "should_notify": True,
                "already_notified": True,
                "has_previous_bottom_signal": False,
                "analysis": {"signal_type": "new_revival"},
            }
        )
        delete = agent.think(
            {
                "mcap": 9_000,
                "pool_liquidity": 30_000,
                "should_notify": False,
                "already_notified": False,
                "has_previous_bottom_signal": False,
                "analysis": {"signal_type": "watch"},
            }
        )

        self.assertTrue(should_notify({"signal_type": "new_revival"}))
        self.assertFalse(should_notify({"signal_type": "watch"}))
        self.assertEqual(push["action"], "push_tg_and_frontend")
        self.assertEqual(update["action"], "frontend_update")
        self.assertEqual(delete["action"], "delete_frontend")

    def test_watchlist_low_mcap_tokens_are_fast_scan_candidates(self) -> None:
        original_fetch = bottom_monitor.fetch_watchlist_records
        original_min = bottom_monitor.FAST_SCAN_MIN_MCAP
        original_max = bottom_monitor.FAST_SCAN_MAX_MCAP
        try:
            bottom_monitor.FAST_SCAN_MIN_MCAP = 40_000
            bottom_monitor.FAST_SCAN_MAX_MCAP = 300_000
            bottom_monitor.fetch_watchlist_records = lambda: [
                {
                    "ca": "2MBq3mrKSKf6NnG5x29rBK4B9f7CWR4N1EQJ18NsViRL",
                    "source": "manual",
                    "last_mcap": 60_000,
                    "current_mcap": 60_000,
                    "ath_mcap": 1_200_000,
                    "highest_mcap": 1_200_000,
                    "blacklisted": False,
                }
            ]

            token = bottom_monitor.fetch_watchlist_tokens()[0]

            self.assertEqual(bottom_monitor.calc_mcap(token), 60_000)
            self.assertEqual(token["history_highest_market_cap"], 1_200_000)
            self.assertTrue(bottom_monitor.is_fast_scan_watchlist_token(token))
            self.assertEqual(bottom_monitor.token_snapshot_interval_sec(token), 60)
        finally:
            bottom_monitor.fetch_watchlist_records = original_fetch
            bottom_monitor.FAST_SCAN_MIN_MCAP = original_min
            bottom_monitor.FAST_SCAN_MAX_MCAP = original_max


if __name__ == "__main__":
    unittest.main()
