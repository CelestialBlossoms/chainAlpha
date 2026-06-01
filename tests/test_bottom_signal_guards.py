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

    def test_deepseek_kline_window_spec_depends_on_token_age_at_signal(self) -> None:
        old_spec = bottom_monitor.deepseek_kline_window_spec({"created_at": 1_000}, 1_000 + 5 * 3600)
        young_spec = bottom_monitor.deepseek_kline_window_spec({"created_at": 1_000}, 1_000 + 2 * 3600)

        self.assertEqual(old_spec["age_bucket"], "gte_4h")
        self.assertEqual(old_spec["5m_limit"], 48)
        self.assertEqual(old_spec["1m_limit"], 60)
        self.assertEqual(young_spec["age_bucket"], "lt_4h")
        self.assertEqual(young_spec["5m_limit"], 12)
        self.assertEqual(young_spec["1m_limit"], 30)

    def test_estimate_pre_signal_peak_mcap_uses_window_high_against_signal_close(self) -> None:
        peak = bottom_monitor.estimate_pre_signal_peak_mcap(
            50_000,
            [
                {"ts": 1, "high": 2.0, "close": 1.0},
                {"ts": 2, "high": 4.0, "close": 2.0},
            ],
        )

        self.assertEqual(peak["mcap"], 100_000)
        self.assertEqual(peak["price"], 4.0)

    def test_local_followup_t5_uses_post_signal_window(self) -> None:
        original_fetch = bottom_monitor.fetch_kline_range
        original_now = bottom_monitor.now_ts
        try:
            def fake_fetch(_address, resolution, start_ts, end_ts):
                if start_ts < 1_000:
                    return [
                        {"ts": 820 + i * 60, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 100}
                        for i in range(3)
                    ]
                return [
                    {"ts": 1_000 + i * 60, "open": 1 + i * 0.02, "high": 1.1 + i * 0.02, "low": 1, "close": 1.05 + i * 0.02, "volume": 200}
                    for i in range(5)
                ]

            bottom_monitor.fetch_kline_range = fake_fetch
            bottom_monitor.now_ts = lambda: 1_000 + 6 * 60

            analysis = bottom_monitor.analyze_local_followup_window(
                address="So11111111111111111111111111111111111111112",
                signal_type="new_revival",
                entry_price=1,
                entry_mcap=50_000,
                signal_ts=1_000,
                window_key="5m",
            )
        finally:
            bottom_monitor.fetch_kline_range = original_fetch
            bottom_monitor.now_ts = original_now

        self.assertEqual(analysis["window"], "T+5min")
        self.assertEqual(analysis["verdict"], "strong_confirm")
        self.assertGreater(analysis["change_pct"], 3)
        self.assertEqual(analysis["volume_ratio"], 2.0)

    def test_bottom_live_track_startup_backfills_due_local_followups(self) -> None:
        from web_dashboard import app as dashboard_app

        address = "So11111111111111111111111111111111111111112"
        saved: dict[str, dict] = {}
        broadcasts: list[list[dict]] = []
        original_list = dashboard_app._bottom_live_track_list_addresses
        original_load = dashboard_app._bottom_live_track_load
        original_save = dashboard_app._bottom_live_track_save
        original_broadcast = dashboard_app._bottom_live_track_broadcast
        original_time = dashboard_app.time.time
        original_analyze = bottom_monitor.analyze_local_followup_window
        try:
            dashboard_app._bottom_live_track_list_addresses = lambda: [address]
            dashboard_app._bottom_live_track_load = lambda _addr: {
                "address": address,
                "status": "tracking",
                "signal_type": "new_revival",
                "pushed_at": 1_000,
                "entry_price": 1.0,
                "entry_mcap": 50_000,
                "local_followups": {},
            }
            dashboard_app._bottom_live_track_save = lambda addr, data: saved.setdefault(addr, dict(data))
            dashboard_app._bottom_live_track_broadcast = lambda items: broadcasts.append(items)
            dashboard_app.time.time = lambda: 1_000 + 31 * 60
            bottom_monitor.analyze_local_followup_window = lambda **kwargs: {
                "ready": True,
                "window_key": kwargs["window_key"],
                "verdict": "turned_positive",
                "label": "转正确认",
                "change_pct": 12.5,
            }

            updated = dashboard_app._bottom_live_track_backfill_local_followups_once()
        finally:
            dashboard_app._bottom_live_track_list_addresses = original_list
            dashboard_app._bottom_live_track_load = original_load
            dashboard_app._bottom_live_track_save = original_save
            dashboard_app._bottom_live_track_broadcast = original_broadcast
            dashboard_app.time.time = original_time
            bottom_monitor.analyze_local_followup_window = original_analyze

        self.assertEqual(len(updated), 1)
        self.assertIn(address, saved)
        self.assertIn("5m", saved[address]["local_followups"])
        self.assertIn("30m", saved[address]["local_followups"])
        self.assertNotIn("4h", saved[address]["local_followups"])
        self.assertEqual(saved[address]["last_local_followup_key"], "30m")
        self.assertEqual(len(broadcasts), 1)

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
