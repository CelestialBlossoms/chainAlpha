# 底部异动 Top100 表字段说明

本文说明 `bottom_top100_snapshots` 和 `bottom_top100_push_records` 两张表在业务里的作用、字段含义，以及它们之间的关联方式。

## 总体关系

`bottom_top100_snapshots` 是快照事实表。只要某个 CA 进入底部异动检测流程，系统读取 GMGN Top100 持仓、K线、市值、池子等数据后，就会写入一条快照。它保存的是“当时观察到的数据事实”，不代表一定推送。

`bottom_top100_push_records` 是推送事件表。只有某次检测真的推送到插件或前端时，才写入一条记录。它保存的是“这次为什么被推送、推送时市值和池子是多少”。同一个 CA 可以多次推送，所以 CA 不是唯一字段。

两张表通过 `bottom_top100_push_records.snapshot_id = bottom_top100_snapshots.id` 关联。查询某次推送对应的 Top100 持仓明细时，从推送表找到 `snapshot_id`，再去快照表读取 `holders`。

```sql
SELECT p.*, s.holders, s.summary, s.analysis
FROM bottom_top100_push_records p
JOIN bottom_top100_snapshots s ON s.id = p.snapshot_id
WHERE p.address = 'CA地址'
ORDER BY p.pushed_at DESC;
```

## bottom_top100_snapshots

业务定位：Top100 持仓快照表。它记录每一次进入检测流程时，GMGN 返回的 Top100 持仓结构和系统分析结果。

| 字段 | 业务含义 |
| --- | --- |
| `id` | 快照自增 ID。推送表的 `snapshot_id` 会指向这个字段。 |
| `scan_id` | 一轮扫描的批次 ID。同一次调度扫描里的多个 CA 会共享同一个扫描批次。 |
| `chain` | 链名称，目前主要是 `sol`。 |
| `trend_interval` | 代币来源的 GMGN trending 时间窗口，例如 `1m`、`5m`、`1h`。如果多个窗口去重合并，可能是多个窗口组合。 |
| `address` | 代币 CA。 |
| `symbol` | 快照时识别到的代币符号。 |
| `snapshot_ts` | 快照采集时间，Unix 秒。 |
| `signal_type` | 本次快照分析出的信号类型。`watch` 表示仅观察未推送；其他值如 `abnormal`、`new_revival`、`quiet_runup` 表示命中异动类型。 |
| `signal_score` | 本次信号评分，用于排序或分析强弱。 |
| `notified` | 历史兼容字段。当前是否推送应以 `bottom_top100_push_records` 为准。 |
| `summary` | 本次快照的摘要 JSON，包括市值、池子、Top10/20/50/100 持仓占比、买入额、卖出额、净流入等。 |
| `holders` | 本次快照归一化后的 GMGN Top100 持仓明细 JSON。这是查询当时 Top100 持仓结构的核心字段。 |
| `analysis` | 本次异动检测分析结果 JSON，包括 `signal_type`、涨幅、市值区间、池子比例、Top100 变化等。 |
| `raw_token` | 合并 trending、watchlist、metadata 后的原始代币数据 JSON，用于排查数据来源和原始字段。 |
| `created_at` | 数据库写入时间。 |

### 快照表的业务用途

- 对比当前 Top100 和上一次 Top100 的持仓变化。
- 判断是否是首次异动、是否已有相同类型信号。
- 计算首次异动市值和相对首次涨幅。
- 回查某一次推送时的 Top100 钱包结构。
- 清空这张表后，历史持仓、首次异动基线、重复推送判断都会从零开始。

## bottom_top100_push_records

业务定位：Top100 异动推送事件表。它只记录真实推送出去的异动事件，不存 Top100 holders 明细。

| 字段 | 业务含义 |
| --- | --- |
| `id` | 推送记录自增 ID。 |
| `pushed_at` | 数据库写入时间。 |
| `event_ts` | 推送发生时间，Unix 秒。 |
| `snapshot_id` | 关联 `bottom_top100_snapshots.id`，用于回查当时 GMGN Top100 持仓快照。 |
| `chain` | 链名称，目前主要是 `sol`。 |
| `source` | 推送来源模块，例如 `bottom_abnormal`。 |
| `status` | 推送状态，例如 `frontend_update`。 |
| `address` | 代币 CA。同一个 CA 可以多次推送多次记录，不能做唯一约束。 |
| `symbol` | 推送时识别到的代币符号。 |
| `signal_type` | 异动类型，例如 `abnormal`、`new_revival`、`drop_40w`、`drop_50w`、`quiet_breakout`、`quiet_runup`、`ema_golden_cross`。 |
| `abnormal_rule` | 命中的异动规则或档位。 |
| `trend_interval` | 该代币来自的 GMGN trending 时间窗口，例如 `1m`、`5m`、`1h`。 |
| `current_mcap` | 推送当时市值，美元。 |
| `first_signal_mcap` | 该异动类型在当前基线窗口内首次异动市值，美元。 |
| `first_signal_ts` | 该异动类型在当前基线窗口内首次异动时间，Unix 秒。 |
| `first_signal_change_pct` | 相对首次异动市值的涨幅百分比。 |
| `price_change_pct` | 本次异动检测使用的价格或市值涨幅百分比。 |
| `max_abnormal_mcap` | 当前异动规则允许或记录的最高异常市值档位，美元。 |
| `ath_mcap` | GMGN 或监控识别到的历史最高市值，美元。 |
| `liquidity` | 推送当时流动性，美元。 |
| `pool_total_liquidity` | 推送当时池子总流动性，美元。当前与 `liquidity` 保持兼容。 |
| `pool_mcap_ratio` | 池子流动性与市值比值。比如 `0.12` 表示池子约为市值的 12%。 |
| `age_sec` | 推送时代币年龄，秒。 |
| `text` | 推送给 TG 或插件前端的文本内容。 |
| `extra` | 推送时的完整结构化扩展数据 JSON。不包含 Top100 holders 明细，holders 通过 `snapshot_id` 去快照表查。 |

### 推送表的业务用途

- 统计实际推送过哪些 CA。
- 分析同一个 CA 多次异动时，市值、池子、涨幅如何变化。
- 查询每次推送的异动类型和触发规则。
- 通过 `snapshot_id` 回查这次推送对应的 Top100 持仓明细。
- 清空这张表后，只会清掉推送历史，不会清掉快照事实数据。

## 清空数据时的影响

如果只清空 `bottom_top100_snapshots`：

- 历史 Top100 持仓数据丢失。
- 首次异动市值会重新计算。
- 重复推送判断会失去历史参考。
- 旧的 `bottom_top100_push_records.snapshot_id` 会找不到对应快照。

如果只清空 `bottom_top100_push_records`：

- 推送历史丢失。
- Top100 快照仍然存在。
- 后续仍可基于快照做重复判断和持仓变化分析。

如果要完整重置异动历史，建议同时清空：

```sql
TRUNCATE TABLE bottom_top100_push_records RESTART IDENTITY;
TRUNCATE TABLE bottom_top100_snapshots RESTART IDENTITY;
```

如果要保留观察池，不要清空 `bottom_watchlist_tokens`。
