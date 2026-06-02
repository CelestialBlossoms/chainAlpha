from __future__ import annotations

import unittest

from web_dashboard import app as dashboard_app


class AlphaLiveTrackMergeTests(unittest.TestCase):
    def test_merge_recomputes_pnl_after_smart_signal_overlay(self) -> None:
        original_load_smart = dashboard_app._load_smart_money_signal_items
        original_load_market = dashboard_app._load_market_signal_items
        original_deleted = dashboard_app._alpha_deleted_today_addresses
        original_health = dashboard_app._smart_signal_health_checked_item
        original_flag = dashboard_app.DEEP_ALPHA_SMART_ONLY_ENABLED
        try:
            dashboard_app._load_smart_money_signal_items = lambda _chain="sol": [
                {
                    "address": "AEKzRyNYPCnQ1oirnkBGfyu7ZdV5rZ14BpWwefdCAtch",
                    "smart_signal": True,
                    "smart_signal_trigger_mcap": 287_581.6866,
                    "current_mcap": 175_358.8417,
                    "peak_mcap": 342_346.0421,
                    "pnl_pct": -39.0,
                    "peak_pnl_pct": 19.0,
                }
            ]
            dashboard_app._load_market_signal_items = lambda _chain="sol": []
            dashboard_app._alpha_deleted_today_addresses = lambda: set()
            dashboard_app._smart_signal_health_checked_item = lambda item: item
            dashboard_app.DEEP_ALPHA_SMART_ONLY_ENABLED = True

            merged = dashboard_app._merge_live_track_smart_signals(
                [
                    {
                        "address": "AEKzRyNYPCnQ1oirnkBGfyu7ZdV5rZ14BpWwefdCAtch",
                        "source": "market_signal",
                        "entry_mcap": 166_452.6375,
                        "current_mcap": 175_358.8417,
                        "peak_mcap": 342_346.0421,
                        "pnl_pct": 5.35,
                        "peak_pnl_pct": 105.67,
                    }
                ]
            )
        finally:
            dashboard_app._load_smart_money_signal_items = original_load_smart
            dashboard_app._load_market_signal_items = original_load_market
            dashboard_app._alpha_deleted_today_addresses = original_deleted
            dashboard_app._smart_signal_health_checked_item = original_health
            dashboard_app.DEEP_ALPHA_SMART_ONLY_ENABLED = original_flag

        self.assertEqual(len(merged), 1)
        self.assertAlmostEqual(merged[0]["entry_mcap"], 166_452.6375)
        self.assertAlmostEqual(merged[0]["peak_mcap"], 342_346.0421)
        self.assertAlmostEqual(merged[0]["peak_pnl_pct"], 105.6717, places=3)
        self.assertAlmostEqual(merged[0]["pnl_pct"], 5.3506, places=3)


if __name__ == "__main__":
    unittest.main()
