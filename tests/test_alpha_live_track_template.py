from pathlib import Path


TEMPLATE = Path(__file__).resolve().parents[1] / "web_dashboard" / "templates" / "alpha_live_track.html"


def test_alpha_live_track_renders_token_name_and_peak_elapsed():
    html = TEMPLATE.read_text(encoding="utf-8")

    assert "function peakElapsedText(item)" in html
    assert "formatPeakElapsed(peakAt - pushedAt)" in html
    assert "const tokenName = String(item.name || \"\").trim();" in html
    assert "showTokenName" in html
    assert 'id="failed-track-rows"' in html
    assert "function isFailedItem(item)" in html
    assert "failedRowsEl.appendChild(row);" in html
    assert "htmlEscape(peakElapsedText(item))" in html
    assert 'id="outcome-stats"' in html
    assert "function computeOutcomeStats(list, now)" in html
    assert "const OUTCOME_WINDOW_SEC = 24 * 3600;" in html
    assert "const NORMAL_TARGET_GAIN_PCT = 50;" in html
    assert "const NORMAL_TARGET_MIN_PEAK_DELAY_SEC = 10 * 60;" in html
    assert "renderOutcomeStats(now);" in html
    assert "正常标的" in html
