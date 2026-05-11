from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import binance_narrative
from db_client import db_op


def fetch_missing_watchlist_tokens(limit: int) -> list[dict[str, Any]]:
    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            ALTER TABLE bottom_watchlist_tokens
                ADD COLUMN IF NOT EXISTS symbol TEXT,
                ADD COLUMN IF NOT EXISTS narrative_desc TEXT,
                ADD COLUMN IF NOT EXISTS narrative_type TEXT,
                ADD COLUMN IF NOT EXISTS highest_mcap NUMERIC DEFAULT 0,
                ADD COLUMN IF NOT EXISTS ath_mcap NUMERIC DEFAULT 0,
                ADD COLUMN IF NOT EXISTS peak_mcap NUMERIC DEFAULT 0,
                ADD COLUMN IF NOT EXISTS current_mcap NUMERIC DEFAULT 0,
                ADD COLUMN IF NOT EXISTS last_mcap NUMERIC DEFAULT 0,
                ADD COLUMN IF NOT EXISTS blacklisted BOOLEAN DEFAULT false;
            """
        )
        cur.execute(
            """
            SELECT ca, symbol
            FROM bottom_watchlist_tokens
            WHERE ca IS NOT NULL
              AND COALESCE(blacklisted, false) = false
              AND NULLIF(BTRIM(COALESCE(narrative_desc, '')), '') IS NULL
            ORDER BY GREATEST(
                COALESCE(highest_mcap, 0),
                COALESCE(ath_mcap, 0),
                COALESCE(peak_mcap, 0),
                COALESCE(current_mcap, 0),
                COALESCE(last_mcap, 0)
            ) DESC
            LIMIT %s
            """,
            (limit,),
        )
        return [{"ca": row[0], "symbol": row[1] or ""} for row in cur.fetchall()]

    return db_op(_op)


def count_missing_watchlist_tokens() -> int:
    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*)
            FROM bottom_watchlist_tokens
            WHERE ca IS NOT NULL
              AND COALESCE(blacklisted, false) = false
              AND NULLIF(BTRIM(COALESCE(narrative_desc, '')), '') IS NULL
            """
        )
        return int(cur.fetchone()[0] or 0)

    return int(db_op(_op) or 0)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fill missing bottom_watchlist_tokens.narrative_desc via Binance Web3 narrative API."
    )
    parser.add_argument("--limit", type=int, default=100, help="Maximum rows to process.")
    parser.add_argument("--sleep", type=float, default=0.35, help="Delay between tokens in seconds.")
    parser.add_argument("--force", action="store_true", help="Bypass Redis narrative cache.")
    parser.add_argument("--translate", action="store_true", help="Allow DeepSeek translation for English narratives.")
    parser.add_argument("--dry-run", action="store_true", help="Print rows without calling Binance.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    binance_narrative.DEEPSEEK_TRANSLATE_ENABLED = bool(args.translate)
    limit = max(1, min(args.limit, 5000))
    tokens = fetch_missing_watchlist_tokens(limit)
    total_missing = count_missing_watchlist_tokens()
    print(f"missing narrative_desc: {total_missing}; processing: {len(tokens)}")

    filled = 0
    empty = 0
    failed = 0
    for index, token in enumerate(tokens, 1):
        ca = token["ca"]
        symbol = token.get("symbol") or None
        print(f"[{index}/{len(tokens)}] {symbol or '-'} {ca}")
        if args.dry_run:
            continue
        try:
            narrative = binance_narrative.get_binance_narrative(ca, symbol=symbol, force=args.force, save=True)
            desc = str((narrative or {}).get("narrative_desc") or "").strip()
            ntype = str((narrative or {}).get("narrative_type") or "").strip()
            if desc:
                filled += 1
                print(f"  filled: {ntype or '-'} | {desc[:120]}")
            else:
                empty += 1
                print("  no narrative returned")
        except Exception as exc:
            failed += 1
            print(f"  failed: {exc}")
        if args.sleep > 0 and index < len(tokens):
            time.sleep(args.sleep)

    print(f"done: filled={filled} empty={empty} failed={failed}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
