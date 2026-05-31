from __future__ import annotations

import unittest

from deep_alpha.deep_alpha_pro import (
    aggregate_smart_sell_trades,
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

        self.assertEqual(stats["peak_source"], "binance_kline")
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


if __name__ == "__main__":
    unittest.main()
