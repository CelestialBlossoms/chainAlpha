from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from db_client import db_op
from bottom_detection.bottom_watchlist_store import ensure_watchlist_daily_mcap_columns


ADDRESS = "2tXpgu2DLTsPUf9zFmuZmA4xrYxXKBTpVq9wAM7hzs9y"
SYMBOL = "HANTA"
PRICE = Decimal("0.0018092399")
SUPPLY = Decimal("999939281")
CURRENT_MCAP = PRICE * SUPPLY
HIGHEST_MCAP = Decimal("2214314.289497926")
FEE_SOL = Decimal("116.09613590600975")
POOL_LIQUIDITY = Decimal("124688.39760055295")
POOL_RATIO = POOL_LIQUIDITY / CURRENT_MCAP
OPEN_TS = 1777843660

NARRATIVE_TYPE = "News-driven Fear Meme / Hantavirus"
NARRATIVE_DESC = (
    "$HANTA (Hantavirus) 是真实汉坦病毒爆发新闻驱动的恐惧 meme 币。"
    "叙事基于 2026 年 5 月 MV Hondius 号邮轮汉坦病毒疫情、多人死亡和 WHO 警报，"
    "把老鼠传播、高死亡率、呼吸衰竭等恐惧元素做成 Solana meme，"
    "核心靠真实新闻持续发酵和恐慌情绪传播。"
)
REMARK = (
    "一句话总结：搭着真实汉坦病毒邮轮疫情新闻的恐惧 meme，类似疫情期间的病毒/疾病主题币，"
    "靠实时热点、死亡病例和可能人传人的传播焦虑拉动关注；"
    "当前属于早期新闻热点驱动型 Solana meme。"
)


def main() -> None:
    ensure_watchlist_daily_mcap_columns()

    def op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO bottom_watchlist_tokens (
                ca, create_at, added_at, last_seen_at, source, symbol,
                peak_mcap, last_mcap, ath_mcap, highest_mcap, current_mcap,
                fee_sol, token_created_at, gmgn_created_at,
                last_pool_liquidity, last_pool_mcap_ratio,
                narrative_desc, narrative_type, remark, note
            ) VALUES (
                %s, to_timestamp(%s), now(), now(), %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, %s, %s
            )
            ON CONFLICT (ca) DO UPDATE SET
                create_at = COALESCE(bottom_watchlist_tokens.create_at, EXCLUDED.create_at),
                last_seen_at = now(),
                source = EXCLUDED.source,
                symbol = EXCLUDED.symbol,
                peak_mcap = GREATEST(COALESCE(bottom_watchlist_tokens.peak_mcap, 0), EXCLUDED.peak_mcap),
                last_mcap = EXCLUDED.last_mcap,
                ath_mcap = GREATEST(COALESCE(bottom_watchlist_tokens.ath_mcap, 0), EXCLUDED.ath_mcap),
                highest_mcap = GREATEST(COALESCE(bottom_watchlist_tokens.highest_mcap, 0), EXCLUDED.highest_mcap),
                current_mcap = EXCLUDED.current_mcap,
                fee_sol = GREATEST(COALESCE(bottom_watchlist_tokens.fee_sol, 0), EXCLUDED.fee_sol),
                token_created_at = COALESCE(NULLIF(EXCLUDED.token_created_at, 0), bottom_watchlist_tokens.token_created_at),
                gmgn_created_at = COALESCE(NULLIF(EXCLUDED.gmgn_created_at, 0), bottom_watchlist_tokens.gmgn_created_at),
                last_pool_liquidity = EXCLUDED.last_pool_liquidity,
                last_pool_mcap_ratio = EXCLUDED.last_pool_mcap_ratio,
                narrative_desc = EXCLUDED.narrative_desc,
                narrative_type = EXCLUDED.narrative_type,
                remark = COALESCE(NULLIF(EXCLUDED.remark, ''), bottom_watchlist_tokens.remark),
                note = EXCLUDED.note
            RETURNING ca, symbol, source, highest_mcap, current_mcap, gmgn_created_at,
                      fee_sol, last_pool_liquidity, last_pool_mcap_ratio,
                      narrative_type, narrative_desc, remark
            """,
            (
                ADDRESS,
                OPEN_TS,
                "manual_narrative_gmgn",
                SYMBOL,
                HIGHEST_MCAP,
                CURRENT_MCAP,
                HIGHEST_MCAP,
                HIGHEST_MCAP,
                CURRENT_MCAP,
                FEE_SOL,
                OPEN_TS,
                OPEN_TS,
                POOL_LIQUIDITY,
                POOL_RATIO,
                NARRATIVE_DESC,
                NARRATIVE_TYPE,
                REMARK,
                "manual narrative update from GMGN token info/pool for HANTA",
            ),
        )
        return cur.fetchone()

    print(db_op(op))


if __name__ == "__main__":
    main()
