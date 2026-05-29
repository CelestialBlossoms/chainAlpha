# CA分析方法论

> 当用户输入一个CA地址后，如何结合历史数据、策略文档、实时K线进行完整分析。

---

## 一、数据来源

| 层级 | 来源 | 内容 | 获取方式 |
|------|------|------|---------|
| 推送记录 | `bottom_top100_push_records` | 信号类型/市值/ATH/币龄/池子 | DB查询 |
| 历史统计 | `signal_kline_records.jsonl` | 363个信号的outcome数据 | 本地文件 |
| 5m K线 | `bottom_kline_cache` 或 Binance API | 推送前后各4h的OHLCV | DB优先,miss时调API |
| 1m K线 | `bottom_kline_cache_1m` 或 Binance API | 推送前后微观结构 | DB优先,miss时调API |

### 实时K线API

```
URL: https://dquery.sintral.io/u-kline/v1/k-line/candles
参数: address=<CA>, platform=solana, interval=5min|1min, limit=48|60
格式: [open, high, low, close, volume, ts_ms, trade_count]
```

---

## 二、分析流程

### Step 1: 查推送记录 → 确定信号类型和市值档位

```sql
SELECT symbol, signal_type, event_ts, current_mcap, ath_mcap,
       price_change_pct, liquidity, pool_mcap_ratio, age_sec
FROM bottom_top100_push_records WHERE address = <CA>
```

拿到：信号类型(new_revival/abnormal)、市值、ATH、币龄、池子健康度。

**判断**: 如果 `price_change_pct > 200%` → 推送时已经暴涨过，信号可能滞后。

### Step 2: 拉取实时5m+1m K线 → 计算当前状态

```
5m K线: 48根, 覆盖推送前后各约2h
1m K线: 60根, 覆盖推送后约1h
```

计算核心指标:
- **baseline** = 推送前最后一根5m收盘价
- **peak** = 推送后最高价 → `peak_pct = (peak-baseline)/baseline*100`
- **trough** = 推送后最低价 → `trough_pct = (trough-baseline)/baseline*100`  
- **current** = 最新收盘价 → `cur_pct = (current-baseline)/baseline*100`
- **recovery** = 从最低点反弹幅度 → `(current-trough)/trough*100`

**判断回撤深度区间**:
| trough_pct | 区间 | 含义 |
|-----------|------|------|
| > -20% | 轻度 | 优质 |
| -20% ~ -50% | 中度 | 一半恢复一半死 |
| -50% ~ -80% | 重度 | 64%死亡 |
| < -80% | 极端 | WR50=0%, 不碰 |

### Step 3: 查历史同类 → 得到胜率基准

**方法**: 在363信号数据集中筛选同条件的信号，统计WR20。

筛选条件组合:
```
signal_type × mcap_bucket × trough_zone
```

参考数据 (来自 `10-drawdown-recovery-fingerprints.md`):

| 信号×市值×回撤 | n | WR20 | 建议 |
|---------------|-----|------|------|
| new_revival + 轻度回撤 | 38 | **92%** | 闭眼入 |
| new_revival + 中度回撤 | 59 | 58% | 等确认 |
| new_revival + 重度回撤 | 69 | 43% | 试错 |
| abnormal + 轻度回撤 | 87 | 54% | 可入 |
| abnormal + 中度回撤 | 70 | 41% | 谨慎 |
| abnormal + 重度回撤 | 28 | **36%** | 大概率放弃 |

**更精细的交集** (来自分析结果):

| 市值区间 | new_revival WR20 | abnormal WR20 |
|---------|-----------------|---------------|
| <$50K | 54% | 41% |
| $50-100K | 50%(高方差) | 53% |
| $100-300K | **70%**(最稳) | 48% |
| $300K+ | — | 40% |

### Step 4: 逐bar分析5m K线 → 找确认信号

**触底确认**: 找到trough bar的位置。

**量能分析**:
- `pre_trough_vol` = 触底前的平均成交量
- `post_trough_vol` = 触底后的平均成交量  
- `vol_ratio` = post/pre — **>1.5x说明有资金进场**

**Bar级信号扫描** (来自 `10-drawdown-recovery-fingerprints.md`):

| 信号 | 条件 | 含义 |
|------|------|------|
| **巨阳确认** | 触底后出现>20%阳线 + 量增>2x | 恢复概率高 |
| **量能爆发** | post-trough量比>3x | AOC式爆发 |
| **缩量触底** | trough bar成交量<前均30% | 卖压枯竭,好 |
| **放量触底** | trough bar成交量>前均3x | 恐慌未结束,坏 |
| **二次暴跌** | 反弹后出现更大的阴线 | 死亡信号 |
| **量崩塌** | post-trough量<pre-trough量50% | 无人接盘,坏 |

**绿红比**: 触底后 green_bars/red_bars — 恢复组通常绿>红。

### Step 5: 看1m微结构 → 辅助确认

```
5min涨跌: (第5根1m收盘-baseline)/baseline
30min涨跌: (第30根1m收盘-baseline)/baseline
量比: 后5min量/前10min量
```

| 1m信号 | 含义 |
|--------|------|
| 5min涨>3% | 推送后被抢筹(71%WR20) |
| 5min跌>8%+放量 | 恐慌,大概率死亡 |
| 30min转正 | 确认上涨(68%WR20) |
| 量比<0.5 | 缩量,观望 |
| 量比>2 | 异常放量,警惕 |

### Step 6: 前置指纹速查 → 看推送前特征

在5m K线的pre部分(推送前)检查 (来自 `08-5m-fingerprint-encyclopedia.md`):

```
投降Bar = 单根5m内 body<-8% + 量增>3x
pos = 当前价格在pre段4h范围的位置 (0%=地板, 100%=天花板)
```

| 前置特征 | 影响 |
|---------|------|
| 有投降Bar | WR20+10pp |
| pos<20%(地板价) | WR20+8pp |
| pos>80%(天花板价) | WR20-10pp |
| 无投降Bar | WR20基准降低 |

---

## 三、决策矩阵

结合以上所有信息，输出最终判断:

| 回撤 | 历史WR20 | 巨阳确认 | 量比 | 前置指纹 | 决策 |
|------|---------|---------|------|---------|------|
| 轻度 | 66-92% | 不需要 | — | — | **入场** |
| 轻度 | 66-92% | 无 | 量崩塌 | 天花板价 | 观望 |
| 中度 | 49-58% | ✅有 | >1.5x | — | **入场** |
| 中度 | 49-58% | ❌无 | <1.0x | — | 等确认 |
| 重度 | 36-43% | ✅有 | >2x | 有投降 | **试错入场,止损-15%** |
| 重度 | 36-43% | ❌无 | <1.0x | 无投降 | **放弃** |
| 极端 | 25% | 任意 | 任意 | 任意 | **不碰** |

---

## 四、分析工具

### 脚本

```bash
# 快速分析一个CA
python scripts/entry_check.py <CA地址>
```

输出: 推送记录 + 实时K线 + 回撤区间 + 历史同类对比 + 入场建议

### 数据文件

| 文件 | 用途 |
|------|------|
| `data/deepseek_discovery/signal_kline_records.jsonl` | 363个信号的历史outcome |
| `onchain_trading_guides/08-5m-fingerprint-encyclopedia.md` | 14种前置模式详解 |
| `onchain_trading_guides/09-bar-level-strategy.md` | 投降Bar/5min/30min策略 |
| `onchain_trading_guides/10-drawdown-recovery-fingerprints.md` | 回撤恢复K线指纹 |
| `onchain_trading_guides/11-ca-analysis-methodology.md` | 本文档 |

### 手工分析步骤速查

```bash
# 1. 查推送
SELECT * FROM bottom_top100_push_records WHERE address='<CA>';

# 2. 拉K线 (如果DB没有)
curl "https://dquery.sintral.io/u-kline/v1/k-line/candles?address=<CA>&platform=solana&interval=5min&limit=48"

# 3. 算回撤区间
trough_pct = (min(low) - baseline) / baseline * 100

# 4. 查历史同类
grep "<signal_type>" data/deepseek_discovery/signal_kline_records.jsonl | python -c "..."

# 5. 判断入场
if trough > -20% and signal_type == 'new_revival': → 入场
elif has_big_green_bar and vol_ratio > 1.5: → 入场
elif trough < -80%: → 放弃
else: → 等确认
```

---

*关联文档: 02 / 08 / 09 / 10*
*分析工具: scripts/entry_check.py*
