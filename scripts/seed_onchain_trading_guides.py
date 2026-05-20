# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from db_client import db_op


BATCH = "trading_review_notes_2026_05_14"

NOTES = [
    {
        "title": "0.2 黄金支撑线规则",
        "note": "万物皆可 0.2。5M 以上盘子如果实体跌破高点的 0.2 支撑，直接拉黑不看。所谓黄金支撑线可理解为该币阶段高点乘以 0.2。",
        "category": "risk",
        "chain": "sol",
        "tags": ["0.2", "黄金支撑线", "5M", "拉黑", "风控"],
    },
    {
        "title": "腰斩分批与跌破五倍风控",
        "note": "深深浅浅的下跌中，可以腰斩买一波，再腰斩补一手；如果跌破五倍区间就割肉，避免无限补仓。",
        "category": "risk",
        "chain": "sol",
        "tags": ["腰斩", "补仓", "五倍", "割肉", "分批"],
    },
    {
        "title": "跟单有时效性",
        "note": "跟单有很强时效性，吃到一波就要走，不到点不上。Bonk 台子少上，群里老大没说的不要再加仓，不要 FOMO。",
        "category": "discipline",
        "chain": "sol",
        "tags": ["跟单", "时效性", "FOMO", "加仓", "纪律"],
    },
    {
        "title": "好叙事要敢上仓位",
        "note": "好的叙事、有共识的标的才有暴富可能。复盘中多次问题不是没看好，而是不敢上仓位，只上极小仓，原因是怕高、怕浇给、习惯买垃圾币。垃圾币往往只能归零，大仓位应留给有叙事和共识的标的。",
        "category": "positioning",
        "chain": "sol",
        "tags": ["叙事", "共识", "仓位", "复盘", "恐惧"],
    },
    {
        "title": "买分歧卖情绪",
        "note": "交易节奏上买分歧、卖情绪，卖出要分段。不要因为自己有筹码就幻想，要从内盘到外盘、从 200K 到 500K、再到 1M/2M 一步步客观看。",
        "category": "exit",
        "chain": "sol",
        "tags": ["买分歧", "卖情绪", "分段卖出", "客观", "节奏"],
    },
    {
        "title": "每日热点与看不懂叙事",
        "note": "每日要分析热点叙事，例如文字江湖、贴吧叙事、Chinese、Plush 等。当看不懂的叙事过 100K 时也要关注，但每次只用小额试错，用一天利润赌，不要重仓硬冲。",
        "category": "narrative",
        "chain": "sol",
        "tags": ["每日热点", "看不懂叙事", "100K", "试错", "热点"],
    },
    {
        "title": "5M 金狗判断与 15M-50M 退出区间",
        "note": "以 5M 作为金狗判断点。第一次冲上 5M 后回调下来分批上车，例如 3M 上 100、1.5M 上 300；随后在 15M-50M 区间结合标的强度、FOMO 程度、叙事等维度判断跑路位置。跑完等砸盘做二段，完成后拉黑蹲下一个。",
        "category": "strategy",
        "chain": "sol",
        "tags": ["5M", "金狗", "15M", "50M", "二段"],
    },
    {
        "title": "浇给风险识别",
        "note": "高风险浇给盘常见形态是分钟线上下插针、成交量很大，让人误以为狗庄很强；一路上涨几乎不回调，一旦回调就可能归零。",
        "category": "risk",
        "chain": "sol",
        "tags": ["浇给", "插针", "成交量", "狗庄", "归零"],
    },
    {
        "title": "周日周一少硬玩",
        "note": "Sol 链周日、周一通常好标少。没东西时不要硬玩，硬找标容易付出代价。严格遵守交易纪律意味着必须放弃不在舒适击球区里的机会。",
        "category": "discipline",
        "chain": "sol",
        "tags": ["周日", "周一", "休息", "纪律", "舒适区"],
    },
    {
        "title": "仿盘高捆绑隔夜观察规则",
        "note": "又是仿盘又是高捆绑的标的，最差也要先观察一夜，隔夜不死才考虑；谨慎做法是直接不玩。仿盘、高捆绑、可爱头像组合风险很高。",
        "category": "risk",
        "chain": "sol",
        "tags": ["仿盘", "高捆绑", "隔夜", "可爱头像", "风险"],
    },
    {
        "title": "Sol 可爱头像与国人诈骗盘风险",
        "note": "Sol 上可爱头像 meme 大部分风险较高，国人诈骗集团喜欢用可爱头像。应用类、AI、速通盘也容易成为国人作恶重灾区。",
        "category": "avoid",
        "chain": "sol",
        "tags": ["可爱头像", "国人盘", "诈骗盘", "AI", "应用类"],
    },
    {
        "title": "舒适作业区黑名单",
        "note": "长期应坚守舒适作业区：应用类不碰，速通 AI 不碰，3M 内国人 KOL 扎堆不碰，公司或名人发盘不碰，政治热点诈尸不碰，Sol 可爱头像不碰，中文 KOL 天天喊抄底的到顶金狗不抄。",
        "category": "avoid",
        "chain": "sol",
        "tags": ["黑名单", "应用类", "AI", "国人KOL", "名人盘", "政治热点"],
    },
    {
        "title": "优先老外社区独立 IP",
        "note": "多玩老外主导的社区 meme，尤其是原创独立 IP、纯老外社区、底部有足够时间洗盘的标的。少玩仿盘、周边、应用类速通和 AI。历史金狗如 wojak、67、白鲸更符合这种审美。",
        "category": "selection",
        "chain": "sol",
        "tags": ["老外社区", "独立IP", "社区meme", "洗盘", "审美"],
    },
    {
        "title": "老金狗优先抄底",
        "note": "想抄底时第一反应应该是抄老金狗，垃圾不看。老金狗、月度级别龙头在熊市通常存活更久，黄金支撑线附近性价比更高。",
        "category": "strategy",
        "chain": "sol",
        "tags": ["老金狗", "抄底", "龙头", "熊市", "黄金支撑线"],
    },
    {
        "title": "三板斧策略一：日内新盘过 1M",
        "note": "针对日内新盘，只关注过 1M 的盘子。过 1M 后回调没有浇给再上车；如果能破新高继续格局，到了新高附近破不了先跑。",
        "category": "strategy",
        "chain": "sol",
        "tags": ["三板斧", "日内新盘", "1M", "回调", "新高"],
    },
    {
        "title": "三板斧策略二：过 1M 跌透不死",
        "note": "过 1M 后跌透但没死的加入自选，通常会横在 100K 上方不破。底部能快速拉盘三倍以上的进入目标锁定，回调进场。熊市中这种 100K 拉到 10M 的模型较好用。",
        "category": "strategy",
        "chain": "sol",
        "tags": ["三板斧", "1M", "跌透不死", "100K", "三倍"],
    },
    {
        "title": "三板斧策略三：月度龙头波段",
        "note": "只做龙头波段，尤其月度级别金狗。熊市一个月度级别大金狗通常至少存活几个月，到了黄金支撑线附近上车胜率较高。上班时间少的人只做策略三也可以。",
        "category": "strategy",
        "chain": "sol",
        "tags": ["三板斧", "龙头", "波段", "月度金狗", "熊市"],
    },
    {
        "title": "大金狗多维度重合模型",
        "note": "高重视标的：原创独立 IP、纯老外社区、走势经典从 0 慢慢拉盘，经历洗盘和震荡后再拉上去，并且过 5M 后回调。直接速通且不洗盘的天花板通常不高，风险反而高。",
        "category": "selection",
        "chain": "sol",
        "tags": ["大金狗", "独立IP", "5M", "洗盘", "速通"],
    },
    {
        "title": "反弹次数与新高判断",
        "note": "二次不冲高基本就是弱，第三次反弹还没新高就直接不玩。第一波反弹哪怕位置高点也没事，很多能反弹甚至反转新高；第二、第三波确定性明显下降。",
        "category": "exit",
        "chain": "sol",
        "tags": ["反弹", "新高", "二次", "第三次", "弱势"],
    },
    {
        "title": "超跌反弹止盈三类",
        "note": "止盈分三类：第一类超跌反弹，底部起来翻倍；第二类走双顶，前高附近卖；第三类强势突破新高，可在前高基础上再拿 2-3 倍，这类较少但命中可能 10X 以上。",
        "category": "exit",
        "chain": "sol",
        "tags": ["止盈", "超跌反弹", "双顶", "突破新高", "10X"],
    },
    {
        "title": "砸不动的小头仓试探",
        "note": "如果同一个位置被砸两次都没有跌破，可以默认短期砸不动，上一个头仓。热点标头仓被套后还能补，但仍要控制仓位。",
        "category": "entry",
        "chain": "sol",
        "tags": ["砸不动", "头仓", "支撑", "热点", "试探"],
    },
    {
        "title": "关键市值点位",
        "note": "关键点位关注 1M、3M、5M，后续可类推 10M、30M、50M。交易时从低市值一步步验证强度，不要一眼望穿式幻想。",
        "category": "levels",
        "chain": "sol",
        "tags": ["1M", "3M", "5M", "10M", "关键点位"],
    },
    {
        "title": "BSC 热点反弹时间窗",
        "note": "BSC 上午热点跌死后，晚上可能会有反弹。常见活跃时间窗：下午 3 点到 7 点、早上 10 点到 13 点、晚上 7 点到 10 点。BSC 更需要相信自己的审美，第一波灵活上，赚完就走。",
        "category": "timing",
        "chain": "bsc",
        "tags": ["BSC", "反弹", "时间窗", "热点", "审美"],
    },
    {
        "title": "Sol 下降趋势别抄底",
        "note": "Sol 的下降趋势不要轻易抄底。暴跌前后一段时间 Sol 行情往往明显变差，狗庄和敏感资金可能提前避险。",
        "category": "risk",
        "chain": "sol",
        "tags": ["下降趋势", "抄底", "暴跌", "避险", "Sol"],
    },
    {
        "title": "动物与抽象文化 IP 审美",
        "note": "寓意正能量的小动物、抽象梗 meme 是老外较喜欢的类型，强庄也更容易诞生于这类币。社区类 meme、动物类、抽象文化 IP 胜率相对更高。",
        "category": "selection",
        "chain": "sol",
        "tags": ["动物", "抽象梗", "老外", "强庄", "社区meme"],
    },
    {
        "title": "Volume/MC 与 LP 健康度",
        "note": "二段启动前成交量通常极度萎缩，一旦放量超过市值 10% 可能是信号。流动性/市值比维持在 5%-10% 是较健康区间；池子太薄，大资金进不去，币走不远。",
        "category": "metrics",
        "chain": "sol",
        "tags": ["Volume/MC", "LP", "流动性", "二段", "健康度"],
    },
    {
        "title": "懒人四策略",
        "note": "懒人策略：1. 蓝筹标的 5M+ 持续跟进撸波段；2. 每日龙头二段并收藏；3. 龙头深度洗盘捡垃圾；4. 时间多时做日内 1.5 段。",
        "category": "strategy",
        "chain": "sol",
        "tags": ["懒人策略", "蓝筹", "龙头", "深度洗盘", "日内"],
    },
    {
        "title": "Sol 熊市打狗埋伏指南",
        "note": "熊市中途优先埋伏过去半年传播度最广、IP 热度最高、天花板最高且跌幅够大的标的。通常满足这类条件的标的不多，可以分散埋伏。",
        "category": "strategy",
        "chain": "sol",
        "tags": ["熊市", "埋伏", "IP热度", "传播度", "跌幅"],
    },
    {
        "title": "异动多数是洗盘开始",
        "note": "大部分异动是狗庄洗盘的开始，不一定是拉盘。真正能涨起来的标的往往至少洗盘三次以上，K 线表现为涨上去、跌下来、再涨上去、再跌下来。百倍币通常经历多次洗盘。",
        "category": "pattern",
        "chain": "sol",
        "tags": ["异动", "洗盘", "三次洗盘", "百倍币", "K线"],
    },
    {
        "title": "最佳二段 K 条件",
        "note": "最佳二段 K 机会通常是一段 K 涨幅在 400K-700K 之间；一段 K 涨幅超过 1M 的较少上车，低于 400K 可能说明狗庄实力不够。还要结合 K 线形态、叙事爆发潜力和限价单。",
        "category": "strategy",
        "chain": "sol",
        "tags": ["二段K", "400K", "700K", "叙事", "限价单"],
    },
]


def seed(conn):
    cur = conn.cursor()
    cur.execute(
        """
        DELETE FROM onchain_trading_guides
        WHERE metadata->>'source' IN ('manual_note', 'manual_bulk_summary')
           OR metadata->>'batch' = %s
        """,
        (BATCH,),
    )
    deleted = cur.rowcount
    inserted = []
    for item in NOTES:
        cur.execute(
            """
            INSERT INTO onchain_trading_guides (title, note, category, chain, tags, metadata)
            VALUES (%s, %s, %s, %s, %s, %s::jsonb)
            RETURNING id, title
            """,
            (
                item["title"],
                item["note"],
                item["category"],
                item["chain"],
                item["tags"],
                json.dumps({"source": "manual_bulk_summary", "batch": BATCH}, ensure_ascii=False),
            ),
        )
        inserted.append(cur.fetchone())
    return deleted, inserted


if __name__ == "__main__":
    deleted_count, inserted_rows = db_op(seed)
    print(f"deleted={deleted_count}")
    print(f"inserted={len(inserted_rows)}")
    for row_id, title in inserted_rows:
        print(f"{row_id}\t{title}")
