from __future__ import annotations

import unittest

from deep_alpha.deep_alpha_pro import (
    aggregate_smart_sell_trades,
    merge_live_track_payload,
    maybe_send_signal_tg_alert,
    normalize_market_signal_item,
    normalize_smart_signal_item,
    post_push_peak_from_candles,
    smart_sell_stats_for_signal,
    smart_trade_rows_from_raw,
)


class DeepAlphaSmartSellTests(unittest.TestCase):
    def test_smart_sell_trades_are_grouped_by_token_address(self) -> None:
        raw = {
            "list": [
                {
                    "transaction_hash": "tx-sell-1",
                    "side": "sell",
                    "base_address": "Token111",
                    "maker": "WalletA",
                    "amount_usd": 82.5,
                    "timestamp": 200,
                    "maker_info": {"twitter_username": "alpha"},
                },
                {
                    "transaction_hash": "tx-sell-1",
                    "side": "sell",
                    "base_address": "Token111",
                    "maker": "WalletA",
                    "amount_usd": 82.5,
                    "timestamp": 200,
                },
                {
                    "transaction_hash": "tx-buy-1",
                    "side": "buy",
                    "base_address": "Token111",
                    "maker": "WalletB",
                    "amount_usd": 20,
                    "timestamp": 210,
                },
                {
                    "transaction_hash": "tx-sell-2",
                    "side": "sell",
                    "base_address": "Token222",
                    "maker": "WalletC",
                    "amount_usd": 12,
                    "timestamp": 220,
                },
            ]
        }

        grouped = aggregate_smart_sell_trades(smart_trade_rows_from_raw(raw))

        self.assertEqual(len(grouped["Token111"]), 1)
        self.assertEqual(grouped["Token111"][0]["address"], "WalletA")
        self.assertEqual(grouped["Token111"][0]["sell_amount"], 82.5)
        self.assertEqual(grouped["Token111"][0]["twitter_username"], "alpha")
        self.assertEqual(len(grouped["Token222"]), 1)

    def test_smart_sell_stats_only_count_sells_after_signal_trigger(self) -> None:
        rows = [
            {"address": "WalletOld", "sell_timestamp": 90, "sell_amount": 40},
            {"address": "WalletNew", "sell_timestamp": 120, "sell_amount": 60},
            {"address": "WalletLatest", "sell_timestamp": 140, "sell_amount": 30},
        ]

        stats = smart_sell_stats_for_signal(rows, trigger_at=100)

        self.assertEqual(stats["smart_sell_count"], 2)
        self.assertEqual(stats["smart_sell_total"], 90)
        self.assertEqual([row["address"] for row in stats["smart_sell_wallets"]], ["WalletLatest", "WalletNew"])

    def test_post_push_peak_ignores_pre_push_highs(self) -> None:
        candles = [
            {"ts": 0, "open": 0.09, "high": 1.0, "low": 0.08, "close": 0.09},
            {"ts": 60, "open": 0.10, "high": 0.15, "low": 0.09, "close": 0.12},
            {"ts": 120, "open": 0.12, "high": 0.20, "low": 0.11, "close": 0.18},
        ]

        stats = post_push_peak_from_candles(
            candles,
            pushed_at=100,
            entry_mcap=1_000,
            current_mcap=1_600,
            total_supply=10_000,
            current_ts=180,
        )

        self.assertEqual(stats["peak_source"], "binance_kline_1m")
        self.assertEqual(stats["peak_mcap"], 2_000)
        self.assertEqual(stats["peak_mcap_at"], 120)

    def test_signal_normalization_does_not_use_token_ath_as_peak(self) -> None:
        raw = {
            "token_address": "Token111",
            "trigger_at": 100,
            "trigger_mc": 100_000,
            "market_cap": 90_000,
            "ath": 1_000_000,
            "data": {"address": "Token111", "chain": "sol", "symbol": "T111", "total_supply": 1_000_000},
        }

        smart = normalize_smart_signal_item(raw, "sol", enrich_narrative=False)
        market = normalize_market_signal_item(raw, "sol", enrich_narrative=False)

        self.assertEqual(smart["peak_mcap"], 100_000)
        self.assertEqual(market["peak_mcap"], 100_000)

    def test_post_push_peak_does_not_use_dynamic_current_as_peak_when_kline_exists(self) -> None:
        candles = [
            {"ts": 0, "open": 0.10, "high": 0.12, "low": 0.09, "close": 0.11},
            {"ts": 300, "open": 0.11, "high": 0.15, "low": 0.10, "close": 0.12},
        ]

        stats = post_push_peak_from_candles(
            candles,
            pushed_at=100,
            entry_mcap=100_000,
            current_mcap=177_000,
            entry_price=0.10,
            current_ts=600,
            resolution="5m",
        )

        self.assertEqual(stats["peak_mcap"], 150_000)
        self.assertEqual(stats["peak_mcap_at"], 300)
        self.assertEqual(stats["peak_source"], "binance_kline_5m")

    def test_live_track_merge_preserves_entry_and_peak_then_recomputes_pnl(self) -> None:
        merged = merge_live_track_payload(
            {
                "entry_mcap": 110_000,
                "pushed_at": 1_000,
                "current_mcap": 150_000,
                "peak_mcap": 175_000,
                "peak_mcap_at": 1_300,
                "pnl_pct": 36.36,
                "peak_pnl_pct": 59.09,
            },
            {
                "entry_mcap": 120_000,
                "pushed_at": 2_000,
                "current_mcap": 89_900,
                "peak_mcap": 120_000,
                "peak_mcap_at": 2_000,
            },
        )

        self.assertEqual(merged["entry_mcap"], 110_000)
        self.assertEqual(merged["pushed_at"], 1_000)
        self.assertEqual(merged["current_mcap"], 89_900)
        self.assertEqual(merged["peak_mcap"], 175_000)
        self.assertEqual(merged["peak_mcap_at"], 1_300)
        self.assertAlmostEqual(merged["pnl_pct"], -18.2727, places=3)
        self.assertAlmostEqual(merged["peak_pnl_pct"], 59.0909, places=3)

    def test_fresh_signal_tg_alert_sends_once_and_writes_dedup(self) -> None:
        from deep_alpha import deep_alpha_pro as dap

        class FakeRedis:
            def __init__(self):
                self.values = {}

            def get(self, key):
                return self.values.get(key)

            def setex(self, key, _ttl, value):
                self.values[key] = value

        fake_redis = FakeRedis()
        sent = []
        original_get_redis = dap.get_redis_client
        original_send = dap.send_tg_alert
        original_enabled = dap.SIGNAL_TG_ENABLED
        original_max_age = dap.SIGNAL_TG_MAX_AGE_SEC
        try:
            dap.get_redis_client = lambda: fake_redis
            dap.send_tg_alert = lambda text, **kwargs: sent.append((text, kwargs)) or 123
            dap.SIGNAL_TG_ENABLED = True
            dap.SIGNAL_TG_MAX_AGE_SEC = 300
            item = {
                "address": "Token111",
                "symbol": "T111",
                "smart_signal_trigger_at": 1_000,
                "entry_mcap": 100_000,
                "current_mcap": 120_000,
                "peak_mcap": 150_000,
            }

            first = maybe_send_signal_tg_alert(item, "smart_money_signal", now_value=1_030)
            second = maybe_send_signal_tg_alert(item, "smart_money_signal", now_value=1_040)
        finally:
            dap.get_redis_client = original_get_redis
            dap.send_tg_alert = original_send
            dap.SIGNAL_TG_ENABLED = original_enabled
            dap.SIGNAL_TG_MAX_AGE_SEC = original_max_age

        self.assertEqual(first, 123)
        self.assertIsNone(second)
        self.assertEqual(len(sent), 1)
        self.assertIn("Deep Alpha 聪明钱信号", sent[0][0])

    def test_signal_tg_alert_dedups_by_source_and_address_when_trigger_changes(self) -> None:
        from deep_alpha import deep_alpha_pro as dap

        class FakeRedis:
            def __init__(self):
                self.values = {}

            def get(self, key):
                return self.values.get(key)

            def setex(self, key, _ttl, value):
                self.values[key] = value

        fake_redis = FakeRedis()
        sent = []
        original_get_redis = dap.get_redis_client
        original_send = dap.send_tg_alert
        original_enabled = dap.SIGNAL_TG_ENABLED
        original_max_age = dap.SIGNAL_TG_MAX_AGE_SEC
        try:
            dap.get_redis_client = lambda: fake_redis
            dap.send_tg_alert = lambda text, **kwargs: sent.append((text, kwargs)) or 456
            dap.SIGNAL_TG_ENABLED = True
            dap.SIGNAL_TG_MAX_AGE_SEC = 300

            first = maybe_send_signal_tg_alert(
                {
                    "address": "Token222",
                    "symbol": "T222",
                    "market_signal_trigger_at": 2_000,
                    "entry_mcap": 100_000,
                    "current_mcap": 120_000,
                    "peak_mcap": 150_000,
                },
                "market_signal",
                now_value=2_010,
            )
            second = maybe_send_signal_tg_alert(
                {
                    "address": "Token222",
                    "symbol": "T222",
                    "market_signal_trigger_at": 2_030,
                    "entry_mcap": 100_000,
                    "current_mcap": 130_000,
                    "peak_mcap": 160_000,
                },
                "market_signal",
                now_value=2_040,
            )
        finally:
            dap.get_redis_client = original_get_redis
            dap.send_tg_alert = original_send
            dap.SIGNAL_TG_ENABLED = original_enabled
            dap.SIGNAL_TG_MAX_AGE_SEC = original_max_age

        self.assertEqual(first, 456)
        self.assertIsNone(second)
        self.assertEqual(len(sent), 1)

    def test_old_signal_tg_alert_is_not_backfilled(self) -> None:
        from deep_alpha import deep_alpha_pro as dap

        sent = []
        original_send = dap.send_tg_alert
        original_enabled = dap.SIGNAL_TG_ENABLED
        original_max_age = dap.SIGNAL_TG_MAX_AGE_SEC
        try:
            dap.send_tg_alert = lambda text, **kwargs: sent.append((text, kwargs)) or 123
            dap.SIGNAL_TG_ENABLED = True
            dap.SIGNAL_TG_MAX_AGE_SEC = 300
            result = maybe_send_signal_tg_alert(
                {
                    "address": "Token111",
                    "symbol": "T111",
                    "market_signal_trigger_at": 1_000,
                    "entry_mcap": 100_000,
                    "current_mcap": 120_000,
                    "peak_mcap": 150_000,
                },
                "market_signal",
                now_value=1_301,
            )
        finally:
            dap.send_tg_alert = original_send
            dap.SIGNAL_TG_ENABLED = original_enabled
            dap.SIGNAL_TG_MAX_AGE_SEC = original_max_age

        self.assertIsNone(result)
        self.assertEqual(sent, [])


if __name__ == "__main__":
    unittest.main()
