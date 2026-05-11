import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from db_client import db_op


CA = "CB9dDufT3ZuQXqqSfa1c5kY935TEreyBw9XJXxHKpump"
SYMBOL = "USDUC"
CREATED_TS = 1747606449
PRICE = 0.026956172
SUPPLY = 999892394
CURRENT_MCAP = PRICE * SUPPLY
HIGHEST_MCAP = 0.07818611 * SUPPLY
POOL_LIQUIDITY = 1191924.43186382
POOL_RATIO = POOL_LIQUIDITY / CURRENT_MCAP if CURRENT_MCAP else 0
FEE_SOL = 1574.9511258984908

NARRATIVE_TYPE = "Stablecoin Satire / Rat-Trader Missed Case"
NARRATIVE_DESC = (
    "USDUC / unstable coin: 以 unstable coin / USDUC 为核心的稳定币反讽型 meme。"
    "当前 GMGN 数据显示已毕业到 pump_amm，流动性、手续费、持有人规模明显高于普通早期 meme。"
    "本条记录主要用于复盘老鼠仓提前布局、异动识别和未执行买入的案例。"
)
REMARK = (
    "复盘备注：2026-05-07 凌晨首次查询 USDUC 时，代币已处于上涨中；当时已观察到价格异动，"
    "并发现两个钱包提前大额买入："
    "8RjeyddZv1v8xcCTjucGJjSgDJWFtqJmiwXgZWJNXZwZ、"
    "3aMyt2Pick3YeZD8wohJa48T5BKcyxaUfbAe5o9g7aBM。"
    "当时判断这是老鼠仓提前布局，但没有立即买入；之后币价持续暴拉，"
    "从查询时位置到高点约 8X，高点接近 24M。"
    "一句话总结：发现了价格异动和两个老鼠仓钱包提前大额买入，但没有执行买入，错过后续主升浪。"
)
NOTE = "manual case review: rat-trader spotted but not bought, USDUC 2026-05-07"


def main() -> None:
    def _op(conn):
        cur = conn.cursor()
        cur.execute(
            """
            ALTER TABLE bottom_watchlist_tokens
                ADD COLUMN IF NOT EXISTS remark TEXT;
            INSERT INTO bottom_watchlist_tokens (
                ca, create_at, added_at, last_seen_at, source, symbol,
                peak_mcap, last_mcap, highest_mcap, current_mcap,
                token_created_at, gmgn_created_at, ath_mcap,
                fee_sol, last_pool_liquidity, last_pool_mcap_ratio,
                narrative_desc, narrative_type, remark, note, blacklisted
            ) VALUES (
                %s, to_timestamp(%s), now(), now(), 'manual_case_review', %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s, false
            )
            ON CONFLICT (ca) DO UPDATE SET
                create_at = COALESCE(bottom_watchlist_tokens.create_at, EXCLUDED.create_at),
                last_seen_at = now(),
                source = EXCLUDED.source,
                symbol = EXCLUDED.symbol,
                peak_mcap = GREATEST(COALESCE(bottom_watchlist_tokens.peak_mcap, 0), EXCLUDED.peak_mcap),
                last_mcap = EXCLUDED.last_mcap,
                highest_mcap = GREATEST(COALESCE(bottom_watchlist_tokens.highest_mcap, 0), EXCLUDED.highest_mcap),
                current_mcap = EXCLUDED.current_mcap,
                token_created_at = EXCLUDED.token_created_at,
                gmgn_created_at = EXCLUDED.gmgn_created_at,
                ath_mcap = GREATEST(COALESCE(bottom_watchlist_tokens.ath_mcap, 0), EXCLUDED.ath_mcap),
                fee_sol = GREATEST(COALESCE(bottom_watchlist_tokens.fee_sol, 0), EXCLUDED.fee_sol),
                last_pool_liquidity = EXCLUDED.last_pool_liquidity,
                last_pool_mcap_ratio = EXCLUDED.last_pool_mcap_ratio,
                narrative_desc = EXCLUDED.narrative_desc,
                narrative_type = EXCLUDED.narrative_type,
                remark = COALESCE(NULLIF(EXCLUDED.remark, ''), bottom_watchlist_tokens.remark),
                note = EXCLUDED.note,
                blacklisted = false
            RETURNING ca, symbol, source, highest_mcap, current_mcap, gmgn_created_at,
                      fee_sol, last_pool_liquidity, last_pool_mcap_ratio, narrative_type, remark
            """,
            (
                CA,
                CREATED_TS,
                SYMBOL,
                HIGHEST_MCAP,
                CURRENT_MCAP,
                HIGHEST_MCAP,
                CURRENT_MCAP,
                CREATED_TS,
                CREATED_TS,
                HIGHEST_MCAP,
                FEE_SOL,
                POOL_LIQUIDITY,
                POOL_RATIO,
                NARRATIVE_DESC,
                NARRATIVE_TYPE,
                REMARK,
                NOTE,
            ),
        )
        return cur.fetchone()

    row = db_op(_op)
    print(row)


if __name__ == "__main__":
    main()
