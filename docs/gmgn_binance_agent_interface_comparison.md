# GMGN 与 Binance Web3 Agent 接口互补开发说明

本文用于后续开发 `chainAlpha` 的代币分析 Agent。目标不是二选一，而是明确 GMGN 与 Binance Web3 各自擅长的部分，并设计一条稳定、低限流、可缓存的互补调用链。

## 总结

GMGN 更适合做链上深度分析：K 线、Top100 持仓、钱包标签、筹码结构、dev 数据、池子和安全判断。

Binance Web3 更适合做热榜发现和叙事补全：Trending、Top Search、Alpha、Topic Rush、Social Hype、AI narrative、交易量买卖拆分、Pro/KOL/新钱包/Bundler 聚合百分比。

推荐分工：

| 模块 | 主数据源 | 辅助数据源 | 说明 |
|---|---|---|---|
| 热门候选发现 | GMGN + Binance | 互相补漏 | GMGN 用 1m/5m/1h 热榜，Binance 用 Unified Rank / Meme Rush |
| 底部异动检测 | GMGN | Binance | GMGN K 线和 Top100 做实际判断，Binance 只补热度和叙事 |
| 筹码结构分析 | GMGN | Binance | GMGN Top100/holders/traders 是主来源 |
| 叙事识别 | Binance | GMGN | Binance Topic Rush / Social Hype 更强，GMGN link.description 可兜底 |
| 风险过滤 | GMGN | Binance | GMGN dev/security/pool 主导，Binance tokenTag/auditInfo 辅助 |
| 前端展示 | 本地结构化 JSON | Redis Stream | 不再从 TG 文本反解析 |

## 接口能力差异

### GMGN

主要通过 `gmgn-cli` 调用，当前项目中大量使用。

常用接口：

| 功能 | 命令 | 主要用途 |
|---|---|---|
| 热榜 | `gmgn-cli market trending` | 获取 1m/5m/1h/6h/24h 热门代币 |
| K 线 | `gmgn-cli market kline` | 获取 1m/5m/15m/1h/4h/1d OHLCV |
| 基础信息 | `gmgn-cli token info` | price、supply、holder、social、dev、stat、wallet_tags_stat |
| 安全信息 | `gmgn-cli token security` | 合约/权限/风险字段 |
| 池子信息 | `gmgn-cli token pool` | DEX、流动性、池子深度 |
| Top holders | `gmgn-cli token holders` | Top100 持仓、盈利、买卖、标签 |
| Top traders | `gmgn-cli token traders` | 聪明钱/KOL/狙击手等交易员分析 |

GMGN 强项：

- 能直接拿 Top100 钱包明细。
- 能按钱包标签过滤：`smart_degen`、`renowned`、`sniper`、`rat_trader`、`bundler`、`fresh_wallet`。
- 能拿钱包级别的买入、卖出、盈利、未实现盈利、持仓比例。
- 适合做筹码集中度、捆绑、同批簇、老鼠仓、新钱包、底部盈利钱包出货判断。
- K 线时间粒度适合底部启动和 EMA 判断。
- `token info` 里有 dev、pool、stat、wallet_tags_stat，适合风险过滤。

GMGN 弱项：

- 叙事字段不稳定，很多 token 的 `link.description` 为空或只是代币名。
- holders/traders 权重大，接口成本高，容易限流。
- `market trending` 每次最多通常 100 条，覆盖范围有限。
- 查询链路较重：一个 CA 深度分析通常需要 info + pool/security + holders + kline。

### Binance Web3

主要通过 HTTPS API 调用，已在 `binance_narrative.py` 中用于叙事补全。

常用接口：

| 功能 | API | 主要用途 |
|---|---|---|
| Unified Token Rank | `/token/pulse/unified/rank/list/ai` | Trending、Top Search、Alpha、Stock 榜单 |
| Token Search | `/market/token/search/ai` | 按 CA/name/symbol 搜索 token |
| Token Metadata | `/dex/market/token/meta/info/ai` | name、symbol、links、description、aiNarrativeFlag |
| Token Dynamic | `/market/token/dynamic/info/ai` | price、mcap、volume、holders、liquidity、买卖量 |
| Meme Rush | `/token/pulse/rank/list/ai` | 新币、临近迁移、已迁移 meme |
| Topic Rush | `/token/social-rush/rank/list/ai` | AI 生成市场热点、叙事、关联 token |
| Social Hype | `/social/hype/rank/leaderboard/ai` | 社交热度、情绪、AI 摘要 |
| Smart Money Inflow | `/wallet/token/inflow/rank/query/ai` | 聪明钱净流入 token 排名 |

Binance 强项：

- 热榜维度更丰富：Trending、Top Search、Alpha、Social Hype、Topic Rush。
- Unified Rank 支持过滤和排序：涨幅、成交量、市值、流动性、持有人、Top10、交易人数。
- 返回买卖量拆分：`volume24hBuy`、`volume24hSell`、`countBuy`、`countSell`。
- 返回聚合字段：`proHoldersPercent`、`newAddressHoldersPercent`、`bundlesHoldingPercent`、`holdersTop10Percent`。
- `tokenTag` 能快速识别：Smart Money Add/Remove、DEX Paid、Wash Trading、Volume Surging、Alpha、Community Recognized。
- Topic Rush / Social Hype 可以补 GMGN 缺失的叙事。

Binance 弱项：

- 不提供像 GMGN Top100 holders 那样的钱包级明细。
- 很多字段是平台聚合指标，只适合筛选和展示，不适合做最终筹码判断。
- Topic Rush 是按话题/关键词关联，可能有误匹配，需要用 CA 反查 tokenList 确认。
- `description` 有时只是 `Created on ...`，不能当叙事，需要过滤。

## 字段互补

### 候选发现

GMGN：

- `rank`
- `hot_level`
- `volume`
- `gas_fee`
- `smart_degen_count`
- `renowned_count`
- `change1m/change5m/change1h`
- `liquidity`
- `rug_ratio`
- `is_wash_trading`
- `bundler_rate`
- `rat_trader_amount_rate`

Binance：

- `percentChange1m/5m/1h/4h/24h`
- `volume1m/5m/1h/4h/24h`
- `volumeBuy/Sell`
- `countBuy/Sell`
- `uniqueTrader`
- `searchCount24h`
- `tokenTag`
- `auditInfo`
- `proHoldersPercent`
- `newAddressHoldersPercent`
- `bundlesHoldingPercent`

推荐逻辑：

1. GMGN 1m/5m/1h 热榜发现“链上真实交易异动”。
2. Binance Unified Rank 补“搜索热度、社交热度、交易人数、买卖量拆分”。
3. 两边都出现的 CA 提高优先级。
4. Binance 出现但 GMGN 未出现的 CA，可以进入轻量观察队列，不直接触发 TG。

### 深度筹码

GMGN 是主来源：

- Top100 持仓比例。
- Top10/Top100 盈利。
- holder 标签聚合。
- bottom profit wallets。
- smart/KOL/sniper/bundler/rat/fresh 钱包明细。
- 买入、卖出、净流、成本、盈利、卖出进度。

Binance 只能做辅助：

- `bundlesHoldingPercent`
- `newAddressHoldersPercent`
- `proHoldersPercent`
- `holdersTop10Percent`
- `tokenTag.Smart Money Add/Remove Holdings`

推荐逻辑：

- 最终风险判断以 GMGN Top100 和 holders/traders 为准。
- Binance 聚合字段用于预筛选和前端补充，不单独作为强结论。

### 叙事识别

GMGN：

- `token info.link.description`
- `link.twitter_username`
- `link.website`
- `link.telegram`
- `name/symbol`

Binance：

- Token Metadata `description`
- `aiNarrativeFlag`
- Token Search `tagsInfo`
- Unified Rank `tokenTag`
- Topic Rush `aiSummary`
- Topic Rush `name/type/topicLink`
- Social Hype `socialSummaryBriefTranslated/socialSummaryDetailTranslated`

推荐优先级：

1. Binance Topic Rush 命中 CA 的 `aiSummary`。
2. Binance Social Hype 命中 CA 的社交摘要。
3. Binance Token Metadata 的非泛化 `description`。
4. GMGN `link.description`。
5. name/symbol 只作为显示，不作为叙事兜底。

注意：

- `Created on https://...`、`Created on Pump.fun`、`Created on rapidlaunch` 这类描述必须过滤。
- Topic Rush 必须确认 `tokenList[].contractAddress == CA`，不能只靠关键词。

## 推荐 Agent 调用链

### 热榜扫描 Agent

输入：

- GMGN trending 1m/5m/1h。
- Binance Unified Rank 1m/5m/1h/24h。
- Binance Meme Rush new/finalizing/migrated。

输出：

- 候选 CA 列表。
- 每个 CA 的轻量元数据。
- 来源标记：`gmgn_trending`、`binance_trending`、`binance_topic`、`watchlist`。

推荐只写入观察队列，不直接 TG。

### 深度筹码 Agent

输入：

- 候选 CA。

调用：

- GMGN token info。
- GMGN token pool/security。
- GMGN token holders Top100。
- 必要时 GMGN token traders。
- GMGN K 线。

输出：

- `raw_stats` 结构化 JSON。
- 筹码、盈利、卖出进度、底部盈利钱包、风险判断。

### 叙事补全 Agent

输入：

- CA、symbol、name。

调用：

- Binance Token Search。
- Binance Token Metadata。
- Binance Token Dynamic。
- Binance Topic Rush。
- 必要时 Binance Social Hype。

输出：

- `narrative_desc`
- `narrative_type`
- `narrative_tags`
- `topic_link`
- `source`

缓存：

- Redis：`binance:narrative:sol:{ca}`，TTL 24h。
- PostgreSQL：`token_narratives`。
- 如果 CA 在 `bottom_watchlist_tokens`，同步 `narrative_desc/narrative_type`。

### 决策 Agent

输入：

- GMGN 深度筹码结果。
- Binance 热度/叙事补充。
- 数据库历史。

输出：

- `observe`
- `frontend_update`
- `push_tg_and_frontend`
- `delete_frontend`
- `delete_watchlist`

决策原则：

- TG 只发强信号。
- 前端可以持续更新已入库 CA。
- 叙事不作为单独买入依据，只提升解释性和优先级。

## 推荐数据模型

### token_narratives

```sql
CREATE TABLE IF NOT EXISTS token_narratives (
    ca TEXT PRIMARY KEY,
    chain TEXT DEFAULT 'sol',
    source TEXT NOT NULL DEFAULT 'binance_web3',
    symbol TEXT,
    name TEXT,
    narrative_desc TEXT,
    narrative_type TEXT,
    tags JSONB DEFAULT '[]'::jsonb,
    raw JSONB DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ DEFAULT now()
);
```

### Redis

```text
binance:narrative:sol:{ca}
```

建议值：

```json
{
  "address": "...",
  "symbol": "...",
  "name": "...",
  "narrative_desc": "...",
  "narrative_type": "...",
  "tags": ["Pumpfun", "DEX Paid"],
  "source": "binance_web3",
  "updated_ts": 1770000000
}
```

不要把完整 raw 塞进前端 Redis Stream。完整 raw 放 PostgreSQL `token_narratives.raw` 或 narrative 专用 Redis 缓存即可。

## 限流与缓存策略

GMGN：

- `trending/info/security/pool` 权重低，可以频繁。
- `holders/traders` 权重高，必须谨慎。
- 对同一 CA 的 holders/traders 建议至少 1-5 分钟缓存。
- 遇到 429 不要重试轰炸，按 reset 时间等待。

Binance：

- 文档没有明确 QPS，但接口返回大，尤其 Unified Rank/Topic Rush。
- narrative 查询建议 24h 缓存。
- 热榜建议每轮只拉必要 page，不要每个 CA 都 Topic Rush。
- Topic Rush 只对即将推送到前端/TG 的 CA 调用。

## 当前项目推荐落地方式

已实现方向：

- `binance_narrative.py`：负责 Binance 叙事获取、Redis 缓存、数据库落库。
- `deep_alpha/deep_alpha_pro.py`：推送前补 `narrative_desc/narrative_type`。
- `bottom_accumulation_monitor.py`：底部异动、EMA、每日 1M 前端消息带叙事。
- `web_dashboard`：优先显示结构化 `narrative_desc`。

下一步建议：

1. 把 Binance Unified Rank 接入候选发现，但先只做观察队列。
2. 给每个 CA 增加 `sources` 字段，记录来自 GMGN/Binance/DB/watchlist。
3. 建立统一的 `TokenSignal` dict/schema，避免前端再从文本解析。
4. 对 `holders/traders/Topic Rush` 做分层缓存，降低限流概率。
5. 前端详情页显示“数据来源”：GMGN 筹码、Binance 叙事、DB 历史。

## 结论

GMGN 是链上筹码和交易行为的主判定源。

Binance 是热度、搜索、社交叙事和聚合标签的补充源。

最佳结构是：

```text
发现候选：GMGN trending + Binance rank
深度分析：GMGN info/pool/security/holders/kline
叙事补全：Binance metadata/topic/social
历史判断：PostgreSQL + Redis
展示推送：结构化 JSON -> Redis Stream -> 前端/TG
```
