#!/usr/bin/env python3
"""Restore onchain_trading_guides from SQL backup file."""
import sys, json
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from db_client import db_op

def main():
    def run(conn):
        cur = conn.cursor()
        # Recreate correct schema
        cur.execute("DROP TABLE IF EXISTS onchain_trading_guides CASCADE")
        cur.execute("""
            CREATE TABLE onchain_trading_guides (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                note TEXT NOT NULL,
                category TEXT,
                chain TEXT,
                token_address TEXT,
                source_url TEXT,
                tags TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
                metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                is_archived BOOLEAN NOT NULL DEFAULT false,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """)

        # All 30 original records from SQL backup
        records = [
            (94, '底部异动策略分析：二段启动前放量信号', '二段启动前成交量通常极度萎缩；如果成交量长期萎靡后突然放量，且放量达到市值的 10% 左右，可能是二段启动前的重要信号，需要重点关注。', '交易策略', 'sol', None, None, ['底部异动','二段启动','成交量萎缩','放量','市值10%'], {"topic":"bottom_abnormal_volume_signal","source":"manual_note"}),
            (63, '战壕即将打满时的横盘发射观察', '在战壕即将打满的情况下，长时间横盘不动的代币，大概率会发射。', '形态观察', 'sol', None, None, ['战壕','横盘','即将打满','发射'], {"batch":"trading_review_notes_2026_05_14","source":"manual_bulk_summary"}),
            (64, '0.2 黄金支撑线规则', '万物皆可 0.2。5M 以上盘子如果实体跌破高点的 0.2 支撑，直接拉黑不看。所谓黄金支撑线可理解为该币阶段高点乘以 0.2。', '风险控制', 'sol', None, None, ['0.2','黄金支撑线','5M','拉黑','风控'], {"batch":"trading_review_notes_2026_05_14","source":"manual_bulk_summary"}),
            (65, '腰斩分批与跌破五倍风控', '深深浅浅的下跌中，可以腰斩买一波，再腰斩补一手；如果跌破五倍区间就割肉，避免无限补仓。', '风险控制', 'sol', None, None, ['腰斩','补仓','五倍','割肉','分批'], {"batch":"trading_review_notes_2026_05_14","source":"manual_bulk_summary"}),
            (66, '跟单有时效性', '跟单有很强时效性，吃到一波就要走，不到点不上。Bonk 台子少上，群里老大没说的不要再加仓，不要 FOMO。', '交易纪律', 'sol', None, None, ['跟单','时效性','FOMO','加仓','纪律'], {"batch":"trading_review_notes_2026_05_14","source":"manual_bulk_summary"}),
            (67, '好叙事要敢上仓位', '好的叙事、有共识的标的才有暴富可能。复盘中多次问题不是没看好，而是不敢上仓位，只上极小仓，原因是怕高、怕浇给、习惯买垃圾币。垃圾币往往只能归零，大仓位应留给有叙事和共识的标的。', '仓位管理', 'sol', None, None, ['叙事','共识','仓位','复盘','恐惧'], {"batch":"trading_review_notes_2026_05_14","source":"manual_bulk_summary"}),
            (68, '买分歧卖情绪', '交易节奏上买分歧、卖情绪，卖出要分段。不要因为自己有筹码就幻想，要从内盘到外盘、从 200K 到 500K、再到 1M/2M 一步步客观看。', '止盈退出', 'sol', None, None, ['买分歧','卖情绪','分段卖出','客观','节奏'], {"batch":"trading_review_notes_2026_05_14","source":"manual_bulk_summary"}),
            (69, '每日热点与看不懂叙事', '每日要分析热点叙事，例如文字江湖、贴吧叙事、Chinese、Plush 等。当看不懂的叙事过 100K 时也要关注，但每次只用小额试错，用一天利润赌，不要重仓硬冲。', '叙事热点', 'sol', None, None, ['每日热点','看不懂叙事','100K','试错','热点'], {"batch":"trading_review_notes_2026_05_14","source":"manual_bulk_summary"}),
            (70, '5M 金狗判断与 15M-50M 退出区间', '以 5M 作为金狗判断点。第一次冲上 5M 后回调下来分批上车，例如 3M 上 100、1.5M 上 300；随后在 15M-50M 区间结合标的强度、FOMO 程度、叙事等维度判断跑路位置。跑完等砸盘做二段，完成后拉黑蹲下一个。', '交易策略', 'sol', None, None, ['5M','金狗','15M','50M','二段'], {"batch":"trading_review_notes_2026_05_14","source":"manual_bulk_summary"}),
            (71, '浇给风险识别', '高风险浇给盘常见形态是分钟线上下插针、成交量很大，让人误以为狗庄很强；一路上涨几乎不回调，一旦回调就可能归零。', '风险控制', 'sol', None, None, ['浇给','插针','成交量','狗庄','归零'], {"batch":"trading_review_notes_2026_05_14","source":"manual_bulk_summary"}),
            (72, '周日周一少硬玩', 'Sol 链周日、周一通常好标少。没东西时不要硬玩，硬找标容易付出代价。严格遵守交易纪律意味着必须放弃不在舒适击球区里的机会。', '交易纪律', 'sol', None, None, ['周日','周一','休息','纪律','舒适区'], {"batch":"trading_review_notes_2026_05_14","source":"manual_bulk_summary"}),
            (73, '仿盘高捆绑隔夜观察规则', '又是仿盘又是高捆绑的标的，最差也要先观察一夜，隔夜不死才考虑；谨慎做法是直接不玩。仿盘、高捆绑、可爱头像组合风险很高。', '风险控制', 'sol', None, None, ['仿盘','高捆绑','隔夜','可爱头像','风险'], {"batch":"trading_review_notes_2026_05_14","source":"manual_bulk_summary"}),
            (74, 'Sol 可爱头像与国人诈骗盘风险', 'Sol 上可爱头像 meme 大部分风险较高，国人诈骗集团喜欢用可爱头像。应用类、AI、速通盘也容易成为国人作恶重灾区。', '规避清单', 'sol', None, None, ['可爱头像','国人盘','诈骗盘','AI','应用类'], {"batch":"trading_review_notes_2026_05_14","source":"manual_bulk_summary"}),
            (75, '舒适作业区黑名单', '长期应坚守舒适作业区：应用类不碰，速通 AI 不碰，3M 内国人 KOL 扎堆不碰，公司或名人发盘不碰，政治热点诈尸不碰，Sol 可爱头像不碰，中文 KOL 天天喊抄底的到顶金狗不抄。', '规避清单', 'sol', None, None, ['黑名单','应用类','AI','国人KOL','名人盘','政治热点'], {"batch":"trading_review_notes_2026_05_14","source":"manual_bulk_summary"}),
            (76, '优先老外社区独立 IP', '多玩老外主导的社区 meme，尤其是原创独立 IP、纯老外社区、底部有足够时间洗盘的标的。少玩仿盘、周边、应用类速通和 AI。历史金狗如 wojak、67、白鲸更符合这种审美。', '选标审美', 'sol', None, None, ['老外社区','独立IP','社区meme','洗盘','审美'], {"batch":"trading_review_notes_2026_05_14","source":"manual_bulk_summary"}),
            (77, '老金狗优先抄底', '想抄底时第一反应应该是抄老金狗，垃圾不看。老金狗、月度级别龙头在熊市通常存活更久，黄金支撑线附近性价比更高。', '交易策略', 'sol', None, None, ['老金狗','抄底','龙头','熊市','黄金支撑线'], {"batch":"trading_review_notes_2026_05_14","source":"manual_bulk_summary"}),
            (78, '三板斧策略一：日内新盘过 1M', '针对日内新盘，只关注过 1M 的盘子。过 1M 后回调没有浇给再上车；如果能破新高继续格局，到了新高附近破不了先跑。', '交易策略', 'sol', None, None, ['三板斧','日内新盘','1M','回调','新高'], {"batch":"trading_review_notes_2026_05_14","source":"manual_bulk_summary"}),
            (79, '三板斧策略二：过 1M 跌透不死', '过 1M 后跌透但没死的加入自选，通常会横在 100K 上方不破。底部能快速拉盘三倍以上的进入目标锁定，回调进场。熊市中这种 100K 拉到 10M 的模型较好用。', '交易策略', 'sol', None, None, ['三板斧','1M','跌透不死','100K','三倍'], {"batch":"trading_review_notes_2026_05_14","source":"manual_bulk_summary"}),
            (80, '三板斧策略三：月度龙头波段', '只做龙头波段，尤其月度级别金狗。熊市一个月度级别大金狗通常至少存活几个月，到了黄金支撑线附近上车胜率较高。上班时间少的人只做策略三也可以。', '交易策略', 'sol', None, None, ['三板斧','龙头','波段','月度金狗','熊市'], {"batch":"trading_review_notes_2026_05_14","source":"manual_bulk_summary"}),
            (81, '大金狗多维度重合模型', '高重视标的：原创独立 IP、纯老外社区、走势经典从 0 慢慢拉盘，经历洗盘和震荡后再拉上去，并且过 5M 后回调。直接速通且不洗盘的天花板通常不高，风险反而高。', '选标审美', 'sol', None, None, ['大金狗','独立IP','5M','洗盘','速通'], {"batch":"trading_review_notes_2026_05_14","source":"manual_bulk_summary"}),
            (82, '反弹次数与新高判断', '二次不冲高基本就是弱，第三次反弹还没新高就直接不玩。第一波反弹哪怕位置高点也没事，很多能反弹甚至反转新高；第二、第三波确定性明显下降。', '止盈退出', 'sol', None, None, ['反弹','新高','二次','第三次','弱势'], {"batch":"trading_review_notes_2026_05_14","source":"manual_bulk_summary"}),
            (83, '超跌反弹止盈三类', '止盈分三类：第一类超跌反弹，底部起来翻倍；第二类走双顶，前高附近卖；第三类强势突破新高，可在前高基础上再拿 2-3 倍，这类较少但命中可能 10X 以上。', '止盈退出', 'sol', None, None, ['止盈','超跌反弹','双顶','突破新高','10X'], {"batch":"trading_review_notes_2026_05_14","source":"manual_bulk_summary"}),
            (84, '砸不动的小头仓试探', '如果同一个位置被砸两次都没有跌破，可以默认短期砸不动，上一个头仓。热点标头仓被套后还能补，但仍要控制仓位。', '入场条件', 'sol', None, None, ['砸不动','头仓','支撑','热点','试探'], {"batch":"trading_review_notes_2026_05_14","source":"manual_bulk_summary"}),
            (85, '关键市值点位', '关键点位关注 1M、3M、5M，后续可类推 10M、30M、50M。交易时从低市值一步步验证强度，不要一眼望穿式幻想。', '关键点位', 'sol', None, None, ['1M','3M','5M','10M','关键点位'], {"batch":"trading_review_notes_2026_05_14","source":"manual_bulk_summary"}),
            (86, 'BSC 热点反弹时间窗', 'BSC 上午热点跌死后，晚上可能会有反弹。常见活跃时间窗：下午 3 点到 7 点、早上 10 点到 13 点、晚上 7 点到 10 点。BSC 更需要相信自己的审美，第一波灵活上，赚完就走。', '时间窗口', 'bsc', None, None, ['BSC','反弹','时间窗','热点','审美'], {"batch":"trading_review_notes_2026_05_14","source":"manual_bulk_summary"}),
            (87, 'Sol 下降趋势别抄底', 'Sol 的下降趋势不要轻易抄底。暴跌前后一段时间 Sol 行情往往明显变差，狗庄和敏感资金可能提前避险。', '风险控制', 'sol', None, None, ['下降趋势','抄底','暴跌','避险','Sol'], {"batch":"trading_review_notes_2026_05_14","source":"manual_bulk_summary"}),
            (88, '动物与抽象文化 IP 审美', '寓意正能量的小动物、抽象梗 meme 是老外较喜欢的类型，强庄也更容易诞生于这类币。社区类 meme、动物类、抽象文化 IP 胜率相对更高。', '选标审美', 'sol', None, None, ['动物','抽象梗','老外','强庄','社区meme'], {"batch":"trading_review_notes_2026_05_14","source":"manual_bulk_summary"}),
            (89, 'Volume/MC 与 LP 健康度', '二段启动前成交量通常极度萎缩，一旦放量超过市值 10% 可能是信号。流动性/市值比维持在 5%-10% 是较健康区间；池子太薄，大资金进不去，币走不远。', '指标观察', 'sol', None, None, ['Volume/MC','LP','流动性','二段','健康度'], {"batch":"trading_review_notes_2026_05_14","source":"manual_bulk_summary"}),
            (90, '懒人四策略', '懒人策略：1. 蓝筹标的 5M+ 持续跟进撸波段；2. 每日龙头二段并收藏；3. 龙头深度洗盘捡垃圾；4. 时间多时做日内 1.5 段。', '交易策略', 'sol', None, None, ['懒人策略','蓝筹','龙头','深度洗盘','日内'], {"batch":"trading_review_notes_2026_05_14","source":"manual_bulk_summary"}),
            (91, 'Sol 熊市打狗埋伏指南', '熊市中途优先埋伏过去半年传播度最广、IP 热度最高、天花板最高且跌幅够大的标的。通常满足这类条件的标的不多，可以分散埋伏。', '交易策略', 'sol', None, None, ['熊市','埋伏','IP热度','传播度','跌幅'], {"batch":"trading_review_notes_2026_05_14","source":"manual_bulk_summary"}),
            (92, '异动多数是洗盘开始', '大部分异动是狗庄洗盘的开始，不一定是拉盘。真正能涨起来的标的往往至少洗盘三次以上，K 线表现为涨上去、跌下来、再涨上去、再跌下来。百倍币通常经历多次洗盘。', '形态观察', 'sol', None, None, ['异动','洗盘','三次洗盘','百倍币','K线'], {"batch":"trading_review_notes_2026_05_14","source":"manual_bulk_summary"}),
            (93, '最佳二段 K 条件', '最佳二段 K 机会通常是一段 K 涨幅在 400K-700K 之间；一段 K 涨幅超过 1M 的较少上车，低于 400K 可能说明狗庄实力不够。还要结合 K 线形态、叙事爆发潜力和限价单。', '交易策略', 'sol', None, None, ['二段K','400K','700K','叙事','限价单'], {"batch":"trading_review_notes_2026_05_14","source":"manual_bulk_summary"}),
        ]

        inserted = 0
        for item in records:
            cur.execute("""
                INSERT INTO onchain_trading_guides (id, title, note, category, chain, token_address, source_url, tags, metadata)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (item[0], item[1], item[2], item[3], item[4], item[5], item[6], item[7], json.dumps(item[8], ensure_ascii=False)))
            inserted += 1

        cur.execute("SELECT setval('onchain_trading_guides_id_seq', 94)")
        print(f"Restored {inserted} original guides")

        # Now append our 3 new guides
        cur.execute("""
            INSERT INTO onchain_trading_guides (title, note, category, chain, tags, metadata)
            VALUES (%s,%s,%s,%s,%s,%s)
        """, (
            "回调反弹信号策略",
            "回调过程中散户筹码被逐步卖出，反弹中主力承接买入。V反型占成功31%，深V占13%。中位回调-15%、中位回拉+79%。5m量能>1.3x前量为Healthy V，<0.5x为死猫跳。最佳介入在推送后2-3h跌15-20%时。",
            "回调反弹",
            "sol",
            ['回调反弹','V反','洗盘','主力建仓','Deep V','交易策略'],
            json.dumps({"source": "data_analysis", "based_on": "2026-05-15~18 300+ samples"}, ensure_ascii=False),
        ))
        cur.execute("""
            INSERT INTO onchain_trading_guides (title, note, category, chain, tags, metadata)
            VALUES (%s,%s,%s,%s,%s,%s)
        """, (
            "蓝筹头部赢家钱包共振信号",
            "蓝筹头部钱包共振指大量蓝筹级赢家钱包同时对某代币集中买入。热点驱动型新币100%触发。共振次数越多、密度越高=实力资金入场。与底部异动双重确认=最高质量信号。",
            "钱包分析",
            "sol",
            ['共振信号','蓝筹钱包','聪明钱','金狗','热点驱动','钱包分析'],
            json.dumps({"source": "manual_knowledge"}, ensure_ascii=False),
        ))
        cur.execute("""
            INSERT INTO onchain_trading_guides (title, note, category, chain, tags, metadata)
            VALUES (%s,%s,%s,%s,%s,%s)
        """, (
            "斐波那契回调交易策略",
            "使用TradingView斐波那契工具，配置0.786/0.86/0.94参数。多头趋势0.5激进、0.618常规、0.786防守。空头趋势只在0.786-0.94下单。结合底部异动：0.618位(跌10-15%,成功率70%),0.86位(跌20-30%,成功率60%)。",
            "技术分析",
            "sol",
            ['斐波那契','Fibonacci','回调','入场点','技术分析','TradingView'],
            json.dumps({"source": "manual_knowledge"}, ensure_ascii=False),
        ))
        cur.execute("SELECT setval('onchain_trading_guides_id_seq', (SELECT MAX(id) FROM onchain_trading_guides))")
        print("+3 new guides appended")

    db_op(run)
    print("Done")

if __name__ == "__main__":
    main()
