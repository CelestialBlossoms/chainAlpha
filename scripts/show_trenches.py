#!/usr/bin/env python3
"""Show near_completion tokens from gmgn-cli trenches - reads from saved file."""
import json, time, sys, io
from pathlib import Path
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Read from already-saved file
d = json.load(open(Path(__file__).resolve().parents[1] / "gmgn_outputs" / "trenches_raw.json", encoding="utf-8"))
now = int(time.time())
items = d.get('pump', [])

print("Near completion tokens: {}".format(len(items)))
print()
print("{:<16} {:>10} {:>8} {:>6} {:>5} {:>4} {:>4} {:>5} {:>7} {:>10} {}".format(
    "Symbol", "MCap", "Progress", "Age", "H", "SM", "KOL", "Rug", "Bundler", "Creator", "Twitter"))
print("-" * 105)

for r in items[:20]:
    sym = r.get('symbol', '?')[:14]
    mcap = r.get('usd_market_cap', 0)
    progress = r.get('progress', 0)
    created = r.get('created_timestamp', 0)
    age = (now - created) / 3600 if created > 0 else -1
    h = r.get('holder_count', 0)
    sm = r.get('smart_degen_count', 0)
    kol = r.get('renowned_count', 0)
    rug = r.get('rug_ratio', 0)
    bundler = r.get('bundler_trader_amount_rate', 0)
    creator_hold = r.get('creator_balance_rate', 0)
    tw = (r.get('twitter_handle') or '')[:15]

    fm = "${:.0f}K".format(mcap / 1e3) if mcap >= 1e3 else "${:.0f}".format(mcap)
    age_s = "{:.0f}h".format(age) if age > 0 else "new"
    flags = ""
    if progress >= 0.99:
        flags += "G"  # graduation
    if sm > 0:
        flags += " S" + str(sm)
    if kol > 0:
        flags += " K" + str(kol)
    if rug > 0.3:
        flags += " RUG"
    if bundler > 0.3:
        flags += " BOT"
    if creator_hold > 0.5:
        flags += " DEV"

    print("{:<16} {:>10} {:>7.0%} {:>6} {:>5} {:>4} {:>4} {:>5.0%} {:>6.0%} {:>9.0%} {:<15} {}".format(
        "$" + sym, fm, progress, age_s, h, sm, kol, rug, bundler, creator_hold, tw, flags))
