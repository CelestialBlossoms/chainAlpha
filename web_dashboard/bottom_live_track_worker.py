#!/usr/bin/env python3
"""Standalone worker for bottom live-track market cap and kline refresh."""

from __future__ import annotations

from web_dashboard.app import (
    BOTTOM_LIVE_TRACK_KLINE_REFRESH_SEC,
    BOTTOM_LIVE_TRACK_KLINE_WINDOW_SEC,
    BOTTOM_LIVE_TRACK_MAX_WORKERS,
    BOTTOM_LIVE_TRACK_REFRESH_INTERVAL_SEC,
    BOTTOM_LIVE_TRACK_TTL_SEC,
    _bottom_live_track_bg_loop,
)


def main() -> None:
    print(
        "[BottomLiveTrackWorker] starting "
        f"refresh={BOTTOM_LIVE_TRACK_REFRESH_INTERVAL_SEC}s "
        f"ttl={BOTTOM_LIVE_TRACK_TTL_SEC}s "
        f"kline_refresh={BOTTOM_LIVE_TRACK_KLINE_REFRESH_SEC}s "
        f"kline_window={BOTTOM_LIVE_TRACK_KLINE_WINDOW_SEC}s "
        f"workers={BOTTOM_LIVE_TRACK_MAX_WORKERS}",
        flush=True,
    )
    _bottom_live_track_bg_loop()


if __name__ == "__main__":
    main()
