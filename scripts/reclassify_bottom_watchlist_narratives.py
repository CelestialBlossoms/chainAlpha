#!/usr/bin/env python3
"""Reclassify bottom_watchlist_tokens narratives with DeepSeek fallback."""

from __future__ import annotations

import argparse
import time
from datetime import datetime, timezone
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from binance_narrative import deepseek_analyze_narrative, keyword_classify_narrative_category
from db_client import db_op


def ensure_column() -> None:
    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            ALTER TABLE bottom_watchlist_tokens
                ADD COLUMN IF NOT EXISTS narrative_category TEXT;
            COMMENT ON COLUMN bottom_watchlist_tokens.narrative_category
                IS 'DeepSeek or keyword classified narrative category';
            """
        )

    db_op(_op)


def load_tokens(limit: int = 0, only_missing: bool = False) -> list[dict]:
    def _op(conn):
        cur = conn.cursor()
        where = [
            "ca IS NOT NULL",
            "(NULLIF(BTRIM(COALESCE(narrative_desc, '')), '') IS NOT NULL "
            "OR NULLIF(BTRIM(COALESCE(narrative_type, '')), '') IS NOT NULL)",
        ]
        if only_missing:
            where.append("NULLIF(BTRIM(COALESCE(narrative_category, '')), '') IS NULL")
        sql = f"""
            SELECT ca, symbol, narrative_desc, narrative_type, narrative_category
            FROM bottom_watchlist_tokens
            WHERE {' AND '.join(where)}
            ORDER BY updated_at DESC NULLS LAST, added_at DESC NULLS LAST, ca
        """
        params = []
        if limit > 0:
            sql += " LIMIT %s"
            params.append(limit)
        cur.execute(sql, params)
        return [
            {
                "ca": row[0],
                "symbol": row[1] or "",
                "narrative_desc": row[2] or "",
                "narrative_type": row[3] or "",
                "old_category": row[4] or "",
            }
            for row in cur.fetchall()
        ]

    return db_op(_op) or []


def save_category(token: dict, category: str, source: str, summary: str = "") -> None:
    now = datetime.now(timezone.utc).isoformat()

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE bottom_watchlist_tokens
            SET narrative_category = %s,
                updated_at = COALESCE(updated_at, now())
            WHERE ca = %s
            """,
            (category, token["ca"]),
        )
        cur.execute(
            """
            INSERT INTO token_narratives (
                ca, chain, source, symbol, narrative_desc, narrative_type, raw, updated_at
            ) VALUES (
                %s, 'sol', 'bottom_watchlist_reclassify', %s, %s, %s,
                jsonb_build_object(
                    'narrative_category', %s,
                    'classification_source', %s,
                    'classification_summary', %s,
                    'reclassified_at', %s
                ),
                now()
            )
            ON CONFLICT (ca) DO UPDATE SET
                symbol = COALESCE(EXCLUDED.symbol, token_narratives.symbol),
                narrative_desc = COALESCE(NULLIF(EXCLUDED.narrative_desc, ''), token_narratives.narrative_desc),
                narrative_type = COALESCE(NULLIF(EXCLUDED.narrative_type, ''), token_narratives.narrative_type),
                raw = COALESCE(token_narratives.raw, '{}'::jsonb) || EXCLUDED.raw,
                updated_at = now()
            """,
            (
                token["ca"],
                token.get("symbol") or None,
                token.get("narrative_desc") or "",
                token.get("narrative_type") or "",
                category,
                source,
                summary,
                now,
            ),
        )

    db_op(_op)


def classify_token(token: dict) -> tuple[str, str, str]:
    desc = token.get("narrative_desc") or ""
    narrative_type = token.get("narrative_type") or ""
    analysis = deepseek_analyze_narrative(
        desc,
        narrative_type,
        [],
        symbol=token.get("symbol") or "",
    )
    category = analysis.get("narrative_category") or ""
    summary = analysis.get("narrative_desc") or ""
    if category:
        return category, "deepseek", summary
    return keyword_classify_narrative_category(desc, narrative_type, []), "keyword", ""


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Reclassify bottom_watchlist_tokens.narrative_category from existing narrative_desc/type."
    )
    parser.add_argument("--limit", type=int, default=0, help="Maximum rows to process; 0 means all.")
    parser.add_argument("--only-missing", action="store_true", help="Only classify rows without narrative_category.")
    parser.add_argument("--sleep", type=float, default=0.2, help="Sleep seconds between rows.")
    parser.add_argument("--dry-run", action="store_true", help="Print classifications without writing DB.")
    args = parser.parse_args()

    ensure_column()
    tokens = load_tokens(limit=args.limit, only_missing=args.only_missing)
    print(f"tokens={len(tokens)} dry_run={args.dry_run} only_missing={args.only_missing}")
    updated = 0
    deepseek_count = 0
    keyword_count = 0
    for index, token in enumerate(tokens, 1):
        category, source, summary = classify_token(token)
        if source == "deepseek":
            deepseek_count += 1
        else:
            keyword_count += 1
        print(
            f"[{index}/{len(tokens)}] {token.get('symbol') or '-'} {token['ca'][:10]} "
            f"{token.get('old_category') or '-'} -> {category} ({source})"
        )
        if not args.dry_run:
            save_category(token, category, source, summary)
            updated += 1
        if args.sleep > 0 and index < len(tokens):
            time.sleep(args.sleep)
    print(f"updated={updated} deepseek={deepseek_count} keyword={keyword_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
