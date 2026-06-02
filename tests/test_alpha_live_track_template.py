from pathlib import Path


TEMPLATE = Path(__file__).resolve().parents[1] / "web_dashboard" / "templates" / "alpha_live_track.html"


def test_alpha_live_track_renders_token_name_and_peak_elapsed():
    html = TEMPLATE.read_text(encoding="utf-8")

    assert "function peakElapsedText(item)" in html
    assert "formatPeakElapsed(peakAt - pushedAt)" in html
    assert "const tokenName = String(item.name || \"\").trim();" in html
    assert "showTokenName" in html
    assert "用时 ${htmlEscape(peakElapsedText(item))}" in html
