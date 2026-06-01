from bottom_detection import deepseek_kline_predictor as predictor


def test_compact_candles_keeps_recent_valid_rows():
    candles = [
        {"ts": 1, "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 100},
        {"ts": 2, "open": 0, "high": 2, "low": 1, "close": 1.5, "volume": 100},
        {"ts": 3, "open": 1.5, "high": 1.8, "low": 1.2, "close": 1.4, "volume": 55.12345},
    ]

    rows = predictor.compact_candles(candles, 2)

    assert rows == [{"t": 3, "o": 1.5, "h": 1.8, "l": 1.2, "c": 1.4, "v": 55.1234, "a": 0.0}]


def test_local_fingerprint_uses_pre_signal_last_5m_not_launch_first_5m():
    candles_5m = [
        {"ts": i * 300, "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.0, "volume": 100}
        for i in range(12)
    ]
    candles_1m = []
    for i in range(60):
        if i < 5:
            open_price = 1.0 if i == 0 else 8.0
            close_price = 10.0
        elif i >= 55:
            open_price = 10.0 - (i - 55)
            close_price = open_price - 0.5
        else:
            open_price = close_price = 10.0
        candles_1m.append(
            {"ts": i * 60, "open": open_price, "high": max(open_price, close_price), "low": min(open_price, close_price), "close": close_price, "volume": 100}
        )

    fp = predictor.compute_local_fingerprints(candles_5m, candles_1m, signal_ts=59 * 60)

    assert fp["m1_post"]["window"] == "pre_signal_last5"
    assert fp["m1_post"]["chg_5min"] < 0
    assert "post5min_pump" not in fp["quick_verdict"]


def test_extract_json_object_from_fenced_content():
    data = predictor._extract_json_object(
        """```json
        {"summary":"ok","confidence":"high"}
        ```"""
    )

    assert data == {"summary": "ok", "confidence": "high"}


def test_normalize_prediction_strips_direct_trading_terms():
    normalized = predictor.normalize_prediction(
        {
            "summary": "建议买入，设置止损",
            "bias": "bullish",
            "confidence": "high",
            "pattern_5m": {"label": "底部", "basis": "可以买入"},
            "micro_1m": {"label": "放量", "decision": "不建议操作"},
            "forecast": {"next_5m": "追高风险"},
            "risk_factors": ["止盈过快"],
        },
        model="test",
        elapsed_ms=1,
    )

    assert normalized["ready"] is True
    assert "买入" not in normalized["summary"]
    assert "止损" not in normalized["summary"]
    assert "追高" not in normalized["forecast"]["next_5m"]
    assert normalized["confidence"] == "high"
