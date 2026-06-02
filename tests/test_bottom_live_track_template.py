from pathlib import Path


TEMPLATE = Path(__file__).resolve().parents[1] / "web_dashboard" / "templates" / "bottom_live_track.html"


def test_bottom_live_track_renders_peak_time_and_elapsed_in_table_and_drawer():
    html = TEMPLATE.read_text(encoding="utf-8")

    assert "function peakTimingText(item)" in html
    assert "peak_mcap_at" in html
    assert "formatDuration(peakAt - pushedAt)" in html
    assert "峰值 ${formatCap(peakMcap)} / ${htmlEscape(peakTimingText(item))}" in html
    assert "`${formatCap(item.peak_mcap)} / ${htmlEscape(peakTimingText(item))}`" in html
