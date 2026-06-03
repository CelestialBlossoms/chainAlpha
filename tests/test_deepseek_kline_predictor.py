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
            {
                "ts": i * 60,
                "open": open_price,
                "high": max(open_price, close_price),
                "low": min(open_price, close_price),
                "close": close_price,
                "volume": 100,
            }
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
            "summary": "\u5efa\u8bae\u4e70\u5165\uff0c\u8bbe\u7f6e\u6b62\u635f",
            "bias": "bullish",
            "confidence": "high",
            "pattern_5m": {"label": "\u5e95\u90e8", "basis": "\u53ef\u4ee5\u4e70\u5165"},
            "micro_1m": {"label": "\u653e\u91cf", "decision": "\u4e0d\u5efa\u8bae\u64cd\u4f5c"},
            "forecast": {"next_5m": "\u8ffd\u9ad8\u98ce\u9669"},
            "risk_factors": ["\u6b62\u76c8\u8fc7\u5feb"],
        },
        model="test",
        elapsed_ms=1,
    )

    assert normalized["ready"] is True
    assert "\u4e70\u5165" not in normalized["summary"]
    assert "\u6b62\u635f" not in normalized["summary"]
    assert "\u8ffd\u9ad8" not in normalized["forecast"]["next_5m"]
    assert normalized["confidence"] == "high"


def test_prompt_payload_includes_six_step_methodology():
    payload = predictor.build_prompt_payload(
        address="CA",
        signal={"signal_type": "quiet_runup", "current_mcap": 100000},
        candles_5m=[],
        candles_1m=[],
        local_fp={},
        signal_ts=123,
    )

    assert [step["step"] for step in payload["methodology_steps"]] == [1, 2, 3, 4, 5, 6]
    assert payload["schema"]["methodology_result"] == {}


def test_local_fallback_uses_methodology_result():
    candles_5m = [
        {"ts": i * 300, "open": 1.0, "high": 1.08, "low": 0.95, "close": 1.0 + i * 0.002, "volume": 100 + i}
        for i in range(16)
    ]
    candles_1m = [
        {"ts": i * 60, "open": 1.0, "high": 1.02, "low": 0.98, "close": 1.0, "volume": 100}
        for i in range(20)
    ]
    local_fp = predictor.compute_local_fingerprints(candles_5m, candles_1m, signal_ts=19 * 60)

    fallback = predictor._fallback_from_fingerprints(
        local_fp,
        status="disabled",
        signal={"signal_type": "quiet_runup", "current_mcap": 120000, "ath_mcap": 500000},
    )

    assert fallback["methodology_steps"][0]["key"] == "push_record"
    assert set(fallback["methodology_result"]) >= {
        "step1_push_record",
        "step2_kline_baseline",
        "step3_bar_analysis",
        "step4_historical_baseline",
        "step5_five_dimension_score",
        "step6_conclusion",
    }
    conclusion = fallback["methodology_result"]["step6_conclusion"]
    assert conclusion["label"]
    assert isinstance(conclusion["conditions"], list)
    assert isinstance(conclusion["reasons"], list)
