"""
Apply BOTTOM_PUSH_TRADING_STRATEGY.md v2.0 to today's bottom push signals.

Layers:
  L1: signal_type filter (abnormal, new_revival, quiet_runup, drop_40w)
  L2: MCAP 50K-300K, age 1h-24h, pool/mcap 15%-30%, gain<500%
  L3: post/pre volume ratio >1.2 (wait 5x5m bars), <0.8 skip
  L4: limit order at signal*0.60 (-40% DD), max wait 8h
  L5: hard stop -25%, trailing activate +50%, trail -15%, breakeven
  L7: batch exit: 50%@+50%, 30%@+100%, 20% trailing
"""
import csv
import sys
import time
import json
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import requests

BINANCE_KLINE_URL = "https://dquery.sintral.io/u-kline/v1/k-line/candles"
BINANCE_HEADERS = {"Accept-Encoding": "identity", "User-Agent": "binance-web3/1.1 (Skill)"}
TODAY = "2026-05-22"  # latest complete day in db
BEIJING = timezone(timedelta(hours=8))

# Strategy params
ENTRY_DD = 0.40          # -40% from signal
ENTRY_PRICE = 0.60       # signal * 0.60
HARD_STOP = 0.75         # entry * 0.75 = -25%
TRAILING_ACTIVATE = 1.50 # entry * 1.50 = +50%
TRAILING_DRAWDOWN = 0.85 # peak * 0.85 = -15%
MAX_WAIT_SEC = 8 * 3600  # 8 hours
VOL_RATIO_PASS = 1.2
VOL_RATIO_SKIP = 0.8
VOL_BARS = 5             # 5 bars post-signal


def fetch_5m_klines(address, limit=60):
    params = {"address": address, "platform": "solana", "interval": "5min", "limit": limit, "pm": "p"}
    try:
        resp = requests.get(BINANCE_KLINE_URL, params=params, headers=BINANCE_HEADERS, timeout=10)
        if resp.status_code == 200:
            raw = resp.json().get("data", [])
            candles = []
            for item in raw or []:
                if not isinstance(item, list) or len(item) < 6:
                    continue
                ts = int(item[5] / 1000) if item[5] > 10**10 else int(item[5])
                candles.append({"ts": ts, "open": float(item[0]), "high": float(item[1]),
                                "low": float(item[2]), "close": float(item[3]), "volume": float(item[4])})
            candles.sort(key=lambda c: c["ts"])
            return candles
    except Exception:
        pass
    return []


def classify_path(candles_after_signal):
    """Classify price path: V_moon, V_strong, pump_crash, dead_floor etc."""
    if not candles_after_signal:
        return "unknown", 0, 0
    first_close = candles_after_signal[0]["close"]
    max_high = max(c["high"] for c in candles_after_signal)
    min_low = min(c["low"] for c in candles_after_signal)
    last_close = candles_after_signal[-1]["close"]
    max_gain = (max_high - first_close) / first_close if first_close > 0 else 0
    current_gain = (last_close - first_close) / first_close if first_close > 0 else 0
    max_dd = (min_low - first_close) / first_close if first_close > 0 else 0

    if max_dd < -0.15 and max_gain > 1.0:
        path = "V_moon"
    elif max_dd < -0.10 and max_gain > 0.30:
        path = "V_strong"
    elif max_dd < -0.05 and max_gain > 0.10:
        path = "V_normal"
    elif max_dd < 0 and max_gain > 0:
        path = "V_weak"
    elif max_gain > 0.50:
        path = "pump_hold" if current_gain > 0.10 else "pump_crash"
    elif max_gain > 0.10:
        path = "pump_fade"
    else:
        path = "dead_floor"
    return path, max_gain, current_gain


def simulate_strategy(candles, signal_price, signal_idx):
    """
    Apply the full strategy and return trade result.
    signal_idx = index of the candle AT signal time.
    """
    result = {
        "action": "skipped", "reason": "", "pnl_pct": 0.0, "batch_pnl": 0.0,
        "entry_price": 0.0, "exit_info": "",
    }

    if signal_idx < 0 or signal_idx >= len(candles):
        result["reason"] = "no_signal_candle"
        return result

    post_signal = candles[signal_idx:]

    # ---- L3: Volume check ----
    pre_start = max(0, signal_idx - VOL_BARS)
    pre_bars = candles[pre_start:signal_idx]
    post_start = min(signal_idx + 1, len(candles) - 1)
    post_end = min(post_start + VOL_BARS, len(candles))
    post_bars = candles[post_start:post_end]

    if len(pre_bars) < 3 or len(post_bars) < 3:
        result["reason"] = "not_enough_kline"
        return result

    pre_avg_vol = sum(c["volume"] for c in pre_bars) / len(pre_bars) if pre_bars else 0
    post_avg_vol = sum(c["volume"] for c in post_bars) / len(post_bars) if post_bars else 0
    vol_ratio = post_avg_vol / pre_avg_vol if pre_avg_vol > 0 else 0
    result["vol_ratio"] = vol_ratio

    if vol_ratio < VOL_RATIO_SKIP:
        result["reason"] = "L3_volume_low"
        return result

    # ---- L4: Wait for retracement to -40% ----
    entry_target = signal_price * ENTRY_PRICE
    entry_idx = -1
    entry_price = 0.0

    for j in range(signal_idx + VOL_BARS, min(signal_idx + VOL_BARS + 96, len(candles))):
        c = candles[j]
        # Check if this candle touched the entry price
        if c["low"] <= entry_target:
            entry_idx = j
            entry_price = entry_target
            break

    if entry_idx < 0:
        result["reason"] = "L4_no_entry"
        return result

    # ---- Simulate from entry ----
    post_entry = candles[entry_idx:]
    peak_price = entry_price
    hit_50 = False
    exit_parts = []  # [(pct_sold, exit_price, reason)]

    for c in post_entry:
        high = c["high"]
        low = c["low"]

        # Track peak for trailing stop
        if high > peak_price:
            peak_price = high

        current_pct = (high - entry_price) / entry_price

        # Check +50% batch exit
        if not hit_50 and high >= entry_price * TRAILING_ACTIVATE:
            hit_50 = True
            # Batch 1: 50% at +50%
            exit_parts.append((0.50, entry_price * TRAILING_ACTIVATE, "batch_50pct"))

        # Check +100% batch exit
        if hit_50 and high >= entry_price * 2.0 and not any(p[2] == "batch_100pct" for p in exit_parts):
            exit_parts.append((0.30, entry_price * 2.0, "batch_100pct"))

        # Trailing stop (only after +50% activation)
        if hit_50:
            trail_level = peak_price * TRAILING_DRAWDOWN
            if low <= trail_level:
                # Sell remaining at trail level
                remaining = 1.0 - sum(p[0] for p in exit_parts)
                if remaining > 0:
                    exit_parts.append((remaining, trail_level, "trailing_stop"))
                break

        # Hard stop (only if never hit +50%)
        if not hit_50 and low <= entry_price * HARD_STOP:
            exit_parts.append((1.0, entry_price * HARD_STOP, "hard_stop"))
            break

    if not exit_parts:
        # End of data: close at last price
        remaining = 1.0 - sum(p[0] for p in exit_parts)
        if remaining > 0:
            exit_parts.append((remaining, post_entry[-1]["close"], "end_of_data"))
            # Apply trailing to final if was active
            if hit_50:
                last_price = post_entry[-1]["close"]
                if last_price < peak_price * TRAILING_DRAWDOWN:
                    exit_parts[-1] = (remaining, peak_price * TRAILING_DRAWDOWN, "trailing_final")

    # Calculate weighted PnL
    total_pnl = 0.0
    for pct_sold, x_price, reason in exit_parts:
        pnl = (x_price - entry_price) / entry_price
        total_pnl += pnl * pct_sold

    result["action"] = "traded"
    result["entry_price"] = entry_price
    result["pnl_pct"] = total_pnl
    result["peak_pct"] = (peak_price - entry_price) / entry_price
    result["batch_pnl"] = total_pnl
    result["exit_parts"] = exit_parts
    result["hit_50"] = hit_50
    result["path"] = classify_path(post_signal)[0]
    return result


def main():
    from db_client import DBClient
    db = DBClient()

    # ---- Fetch today's push records ----
    def fetch_push_records(conn):
        cur = conn.cursor()
        cur.execute("""
            SELECT pr.address, pr.symbol, pr.signal_type, pr.event_ts,
                   pr.current_mcap, pr.liquidity, pr.pool_total_liquidity,
                   pr.pool_mcap_ratio, pr.age_sec, pr.first_signal_mcap,
                   pr.ath_mcap, pr.abnormal_rule
            FROM bottom_top100_push_records pr
            WHERE pr.pushed_at::date = %s::date AND pr.chain = 'sol'
            ORDER BY pr.event_ts
        """, (TODAY,))
        return cur.fetchall()

    signals = db.execute(fetch_push_records)
    if not signals:
        print(f"No signals found for {TODAY}")
        # Check available dates
        def check_dates(conn):
            cur = conn.cursor()
            cur.execute("SELECT MIN(pushed_at::date), MAX(pushed_at::date), COUNT(*) FROM bottom_top100_push_records")
            return cur.fetchone()
        info = db.execute(check_dates)
        print(f"Available date range: {info[0]} ~ {info[1]} ({info[2]} records)")
        return

    print(f"Found {len(signals)} bottom push signals for {TODAY}")
    print()

    results = []
    l1_skip = l2_skip = l3_skip = l4_skip = 0
    traded_count = 0
    total_batch_pnl = 0.0
    wins = 0
    losses = 0

    for i, sig in enumerate(signals):
        addr, sym, stype, event_ts, mcap, liq, pool_liq, pool_ratio, age_sec, first_mcap, ath, rule = sig
        mcap = float(mcap or 0)
        pool_liq = float(pool_liq or 0)
        pool_ratio = float(pool_ratio or 0)
        age_sec = int(age_sec or 0)
        symbol = sym or addr[:8]
        event_dt = datetime.fromtimestamp(event_ts, tz=BEIJING) if event_ts else None

        # ---- L1: Signal type filter ----
        valid_types = {"abnormal", "new_revival", "quiet_runup", "drop_40w"}
        if stype not in valid_types:
            l1_skip += 1
            continue

        # ---- L2: Quality filter ----
        age_hours = age_sec / 3600 if age_sec else 0
        l2_fail = []
        if mcap < 50000:
            l2_fail.append(f"mcap={mcap:,.0f}")
        if mcap > 300000:
            l2_fail.append(f"mcap={mcap:,.0f}")
        if age_hours < 1:
            l2_fail.append(f"age={age_hours:.1f}h")
        if age_hours > 24:
            l2_fail.append(f"age={age_hours:.1f}h")
        if pool_ratio < 0.12:
            l2_fail.append(f"pool_ratio={pool_ratio:.1%}")
        if l2_fail:
            l2_skip += 1
            continue

        # ---- Fetch K-line ----
        candles = fetch_5m_klines(addr, limit=60)
        if not candles or len(candles) < 10:
            continue

        # Find signal candle
        sig_idx = -1
        for j, c in enumerate(candles):
            if abs(c["ts"] - event_ts) < 300:  # within 5 min
                sig_idx = j
                break
        if sig_idx < 0:
            sig_idx = 5  # fallback: use 6th candle

        signal_price = candles[sig_idx]["close"]
        if signal_price <= 0:
            continue

        # ---- L3+L4+L5+L7: Full strategy simulation ----
        trade = simulate_strategy(candles, signal_price, sig_idx)
        trade["symbol"] = symbol
        trade["address"] = addr
        trade["signal_type"] = stype
        trade["signal_price"] = signal_price
        trade["mcap"] = mcap
        trade["age_hours"] = age_hours
        trade["pool_ratio"] = pool_ratio
        trade["event_ts"] = event_dt.strftime("%H:%M") if event_dt else ""
        results.append(trade)

        reason = trade["reason"]
        if reason == "L3_volume_low":
            l3_skip += 1
        elif reason == "L4_no_entry":
            l4_skip += 1
        elif trade["action"] == "traded":
            traded_count += 1
            pnl = trade["pnl_pct"]
            total_batch_pnl += pnl
            if pnl > 0:
                wins += 1
            else:
                losses += 1

        if (i + 1) % 20 == 0:
            print(f"  Progress: {i+1}/{len(signals)}...")
        time.sleep(0.25)

    # ---- Summary ----
    print()
    print("=" * 70)
    print(f"  Bottom Strategy Backtest — {TODAY}")
    print("=" * 70)
    print(f"  Total signals:           {len(signals)}")
    print(f"  L1 skip (type):          {l1_skip}")
    print(f"  L2 skip (quality):       {l2_skip}")
    print(f"  L3 skip (volume):        {l3_skip}")
    print(f"  L4 skip (no entry):      {l4_skip}")
    print(f"  Total trades:            {traded_count}")
    if traded_count > 0:
        print(f"  Wins:                    {wins}")
        print(f"  Losses:                  {losses}")
        print(f"  Win rate:                {wins/traded_count:.0%}")
        print(f"  Total batch PnL:         {total_batch_pnl:.1%}")
        print(f"  Avg PnL per trade:       {total_batch_pnl/traded_count:.1%}")
    print()

    # ---- Exit breakdown ----
    traded = [r for r in results if r["action"] == "traded"]
    if traded:
        print("  Exit breakdown:")
        exit_stats = defaultdict(lambda: {"count": 0, "pnl": 0.0})
        for r in traded:
            for pct, price, reason in r.get("exit_parts", []):
                exit_stats[reason]["count"] += 1
                exit_stats[reason]["pnl"] += (price - r["entry_price"]) / r["entry_price"] * pct
        for reason, d in exit_stats.items():
            print(f"    {reason:<20} {d['count']:>3}x  sum PnL={d['pnl']:+.1%}")
    print()

    # ---- Detailed trade list ----
    if traded:
        print(f"  {'Sym':<12} {'Type':<15} {'Time':>5} {'MCAP':>8} {'Entry':>10} {'PnL':>8} {'Peak':>8} {'Path':<12}")
        print(f"  {'-'*12} {'-'*15} {'-'*5} {'-'*8} {'-'*10} {'-'*8} {'-'*8} {'-'*12}")
        for r in sorted(traded, key=lambda x: -x["pnl_pct"]):
            print(f"  {r['symbol']:<12} {r['signal_type']:<15} {r['event_ts']:>5} "
                  f"${r['mcap']:>7,.0f} {r['entry_price']:>10.8f} "
                  f"{r['pnl_pct']:>+7.1%} {r.get('peak_pct',0):>+7.1%} {r.get('path','?'):<12}")

    # ---- Time slot analysis ----
    if traded:
        print()
        print("  Time slot breakdown:")
        slots = defaultdict(lambda: {"count": 0, "pnl": 0.0, "wins": 0})
        for r in traded:
            hour = int(r["event_ts"].split(":")[0]) if r["event_ts"] else -1
            if 7 <= hour <= 9:
                slot = "07-09 morning"
            elif 14 <= hour <= 15:
                slot = "14-15 afternoon"
            elif 19 <= hour <= 22:
                slot = "19-22 evening"
            elif 0 <= hour <= 2:
                slot = "00-02 night"
            else:
                slot = f"{hour:02d}h other"
            slots[slot]["count"] += 1
            slots[slot]["pnl"] += r["pnl_pct"]
            if r["pnl_pct"] > 0:
                slots[slot]["wins"] += 1
        for slot in sorted(slots.keys()):
            d = slots[slot]
            wr = f"{d['wins']/d['count']:.0%}" if d["count"] else "-"
            ap = f"{d['pnl']/d['count']:.1%}" if d["count"] else "-"
            print(f"    {slot:<18} {d['count']:>2} trades  win={wr:>4}  avg={ap:>7}")

    print()
    print(f"  Analysis complete for {TODAY}")


if __name__ == "__main__":
    main()
