"""
Store helpers for alpha_abnormal_analysis table.
Mirrors bottom_watchlist_store.py but targets alpha pipeline.
"""
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from db_client import db_op


def upsert_alpha_abnormal_token(
    address: str,
    created_ts: int = 0,
    mcap: float = 0,
    symbol: str = "",
    source: str = "alpha_push",
    fee_sol: float = 0,
    launch_ts: int = 0,
    pool_liquidity: float = 0,
    pool_mcap_ratio: float = 0,
    note: str = "",
    ath_mcap: float = 0,
    narrative_desc: str = "",
    narrative_type: str = "",
    narrative_category: str = "",
) -> None:
    """Insert or update a token in alpha_abnormal_analysis."""

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO alpha_abnormal_analysis (
                ca, create_at, added_at, last_seen_at, updated_at, source, symbol,
                peak_mcap, last_mcap, highest_mcap, current_mcap,
                fee_sol, token_created_at, gmgn_created_at,
                token_launch_at, gmgn_open_at,
                last_pool_liquidity, last_pool_mcap_ratio,
                ath_mcap, note,
                narrative_desc, narrative_type, narrative_category
            ) VALUES (
                %s,
                CASE WHEN %s > 0 THEN to_timestamp(%s) ELSE NULL END,
                now(), now(), now(),
                %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s
            )
            ON CONFLICT (ca) DO UPDATE SET
                create_at = COALESCE(alpha_abnormal_analysis.create_at, EXCLUDED.create_at),
                last_seen_at = now(),
                updated_at = now(),
                source = COALESCE(alpha_abnormal_analysis.source, EXCLUDED.source),
                symbol = COALESCE(EXCLUDED.symbol, alpha_abnormal_analysis.symbol),
                peak_mcap = GREATEST(COALESCE(alpha_abnormal_analysis.peak_mcap, 0), EXCLUDED.peak_mcap),
                last_mcap = EXCLUDED.last_mcap,
                highest_mcap = GREATEST(COALESCE(alpha_abnormal_analysis.highest_mcap, 0), EXCLUDED.peak_mcap),
                ath_mcap = GREATEST(COALESCE(alpha_abnormal_analysis.ath_mcap, 0), EXCLUDED.ath_mcap),
                current_mcap = EXCLUDED.last_mcap,
                fee_sol = GREATEST(COALESCE(alpha_abnormal_analysis.fee_sol, 0), EXCLUDED.fee_sol),
                token_created_at = COALESCE(EXCLUDED.token_created_at, alpha_abnormal_analysis.token_created_at),
                gmgn_created_at = COALESCE(EXCLUDED.gmgn_created_at, alpha_abnormal_analysis.gmgn_created_at),
                token_launch_at = COALESCE(EXCLUDED.token_launch_at, alpha_abnormal_analysis.token_launch_at),
                gmgn_open_at = COALESCE(EXCLUDED.gmgn_open_at, alpha_abnormal_analysis.gmgn_open_at),
                last_pool_liquidity = COALESCE(EXCLUDED.last_pool_liquidity, alpha_abnormal_analysis.last_pool_liquidity),
                last_pool_mcap_ratio = COALESCE(EXCLUDED.last_pool_mcap_ratio, alpha_abnormal_analysis.last_pool_mcap_ratio),
                note = CASE
                    WHEN alpha_abnormal_analysis.note IS NULL THEN EXCLUDED.note
                    WHEN EXCLUDED.note <> '' THEN EXCLUDED.note
                    ELSE alpha_abnormal_analysis.note
                END,
                narrative_desc = COALESCE(EXCLUDED.narrative_desc, alpha_abnormal_analysis.narrative_desc),
                narrative_type = COALESCE(EXCLUDED.narrative_type, alpha_abnormal_analysis.narrative_type),
                narrative_category = COALESCE(EXCLUDED.narrative_category, alpha_abnormal_analysis.narrative_category)
            """,
            (
                address,
                created_ts, created_ts,
                source, symbol or "",
                mcap, mcap, mcap, mcap,
                fee_sol,
                created_ts if created_ts > 0 else None,
                created_ts if created_ts > 0 else None,
                launch_ts if launch_ts > 0 else None,
                launch_ts if launch_ts > 0 else None,
                pool_liquidity, pool_mcap_ratio,
                ath_mcap,
                note if note else f"alpha push source={source}",
                narrative_desc, narrative_type, narrative_category,
            ),
        )

    db_op(_op)


def batch_insert_alpha_abnormal(
    tokens: list[dict],
    source: str = "alpha_push",
) -> int:
    """Batch insert tokens into alpha_abnormal_analysis. Returns count inserted."""
    count = 0
    for t in tokens:
        try:
            upsert_alpha_abnormal_token(
                address=str(t.get("address", "")).strip(),
                created_ts=int(t.get("created_ts", 0) or 0),
                mcap=float(t.get("mcap", 0) or 0),
                symbol=str(t.get("symbol", "") or ""),
                source=str(t.get("source", source) or source),
                fee_sol=float(t.get("fee_sol", 0) or 0),
                launch_ts=int(t.get("launch_ts", 0) or 0),
                pool_liquidity=float(t.get("pool_liquidity", 0) or 0),
                pool_mcap_ratio=float(t.get("pool_mcap_ratio", 0) or 0),
                note=str(t.get("note", "") or ""),
                ath_mcap=float(t.get("ath_mcap", 0) or 0),
                narrative_desc=str(t.get("narrative_desc", "") or ""),
                narrative_type=str(t.get("narrative_type", "") or ""),
                narrative_category=str(t.get("narrative_category", "") or ""),
            )
            count += 1
        except Exception as exc:
            print(f"batch_insert_alpha_abnormal error for {t.get('address', '?')}: {exc}")
    return count


def delete_alpha_abnormal_token(
    address: str,
    reason: str = "unspecified",
) -> int:
    """Delete a token from alpha_abnormal_analysis. Returns row count (0 or 1)."""

    def _op(conn):
        cur = conn.cursor()
        cur.execute("DELETE FROM alpha_abnormal_analysis WHERE ca = %s", (address,))
        deleted = cur.rowcount
        if deleted:
            print(f"alpha_abnormal deleted: {address[:16]}... reason={reason}")
        return deleted

    return int(db_op(_op) or 0)


def list_alpha_abnormal_tokens(limit: int = 200) -> list[dict]:
    """List recent tokens from alpha_abnormal_analysis."""

    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            "SELECT ca, symbol, source, current_mcap, added_at, note "
            "FROM alpha_abnormal_analysis ORDER BY added_at DESC LIMIT %s",
            (limit,),
        )
        return [
            {"ca": r[0], "symbol": r[1], "source": r[2], "mcap": float(r[3] or 0),
             "added_at": str(r[4]), "note": r[5]}
            for r in cur.fetchall()
        ]

    return db_op(_op) or []
