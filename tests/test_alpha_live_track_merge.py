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

    def test_alpha_deleted_today_archive_round_trip(self) -> None:
        class FakeRedis:
            def __init__(self) -> None:
                self.values: dict[str, str] = {}
                self.sets: dict[str, set[str]] = {}

            def setex(self, key: str, _ttl: int, value: str) -> None:
                self.values[key] = value

            def sadd(self, key: str, value: str) -> None:
                self.sets.setdefault(key, set()).add(value)

            def expire(self, _key: str, _ttl: int) -> None:
                return None

            def smembers(self, key: str) -> set[str]:
                return self.sets.get(key, set())

            def get(self, key: str) -> str | None:
                return self.values.get(key)

            def srem(self, key: str, value: str) -> None:
                self.sets.get(key, set()).discard(value)

        fake = FakeRedis()
        now_ts = 1_800_000_000
        original_redis = dashboard_app.get_redis_client
        original_plugin = dashboard_app._alpha_deleted_today_plugin_events
        original_ttl = dashboard_app.ALPHA_DELETED_TODAY_TTL_SEC
        original_time = dashboard_app.time.time
        try:
            dashboard_app.get_redis_client = lambda: fake
            dashboard_app._alpha_deleted_today_plugin_events = lambda limit=500: []
            dashboard_app.ALPHA_DELETED_TODAY_TTL_SEC = 1800
            dashboard_app.time.time = lambda: now_ts

            dashboard_app._alpha_deleted_today_save(
                "CA111",
                {
                    "address": "CA111",
                    "symbol": "TEST",
                    "smart_signal": True,
                    "pushed_at": now_ts - 420,
                    "last_updated": now_ts,
                    "remove_reason": "30分钟内市值过低",
                },
            )
            items = dashboard_app._alpha_deleted_today_list()
        finally:
            dashboard_app.get_redis_client = original_redis
            dashboard_app._alpha_deleted_today_plugin_events = original_plugin
            dashboard_app.ALPHA_DELETED_TODAY_TTL_SEC = original_ttl
            dashboard_app.time.time = original_time

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["address"], "CA111")
        self.assertTrue(items[0]["removed_archive"])
        self.assertEqual(items[0]["removed_at"], now_ts)
        self.assertEqual(items[0]["removed_after_sec"], 420)

    def test_bottom_live_track_merges_alpha_deleted_archive(self) -> None:
        original_deleted = dashboard_app._alpha_deleted_today_list
        try:
            dashboard_app._alpha_deleted_today_list = lambda: [
                {
                    "address": "REMOVED",
                    "symbol": "DROP",
                    "status": "removed",
                    "source": "market_signal",
                    "removed_archive": True,
                    "pushed_at": 1000,
                    "removed_at": 1200,
                    "last_updated": 1200,
                }
            ]
            items = dashboard_app._bottom_live_track_with_alpha_deleted([])
        finally:
            dashboard_app._alpha_deleted_today_list = original_deleted

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["address"], "REMOVED")
        self.assertEqual(items[0]["status"], "removed")


if __name__ == "__main__":
    unittest.main()
