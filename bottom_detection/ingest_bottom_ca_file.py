#!/usr/bin/env python3
"""
Import one or many Solana token CAs and record one processed Top100 JSON snapshot per CA.
"""

from __future__ import annotations

import argparse
import re
import sys
import time
import uuid
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from bottom_accumulation_monitor import handle_token, valid_sol_ca


SOL_CA_RE = re.compile(r"[1-9A-HJ-NP-Za-km-z]{32,50}")


def extract_cas(text: str) -> list[str]:
    seen = set()
    addresses = []
    for match in SOL_CA_RE.findall(text):
        address = match.strip()
        if address in seen or not valid_sol_ca(address):
            continue
        seen.add(address)
        addresses.append(address)
    return addresses


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Import CA file into bottom_top100_snapshots.")
    parser.add_argument("file", nargs="?", help="Text file containing one or many CAs.")
    parser.add_argument("--ca", action="append", default=[], help="Direct CA input. Can be repeated or comma-separated.")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay between CAs to reduce rate-limit risk.")
    parser.add_argument("--limit", type=int, default=0, help="Max CAs to process. 0 means no limit.")
    parser.add_argument("--notify", action="store_true", help="Send Telegram notifications for generated signals.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    source_text = ""
    if args.file:
        source_text += Path(args.file).read_text(encoding="utf-8")
    if args.ca:
        source_text += "\n" + "\n".join(args.ca)

    addresses = extract_cas(source_text)
    if args.limit > 0:
        addresses = addresses[: args.limit]
    if not addresses:
        raise SystemExit("No valid Solana CA found in input.")

    scan_id = str(uuid.uuid4())
    print(f"scan_id={scan_id} found {len(addresses)} CA(s)")
    processed = 0
    for index, address in enumerate(addresses, start=1):
        try:
            token = {"address": address, "symbol": None}
            if handle_token(scan_id, token, args.notify):
                processed += 1
            print(f"[{index}/{len(addresses)}] {address} done")
        except Exception as exc:
            print(f"[{index}/{len(addresses)}] {address} failed: {exc}")
        time.sleep(args.delay)
    print(f"scan_id={scan_id} processed={processed}/{len(addresses)}")


if __name__ == "__main__":
    main()
