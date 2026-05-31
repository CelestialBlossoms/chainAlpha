# CA分析方法论

> 当用户输入一个CA地址后，如何结合历史数据、策略文档、实时K线进行完整分析。
>
> **⚠️ 核心原则: 所有分析必须同时基于5m和1m K线。5m看结构，1m看细节。只用5m会漏掉关键信号。**

---

## 零、为什么必须同时看5m和1m

| 维度 | 5m K线 | 1m K线 |
|------|--------|--------|
| 看什么 | 大结构: 横盘/暴跌/反弹 | 微结构: 推送前最后2分钟的真相 |
| 横盘判断 | 6根5m bar(30分钟)缩量窄幅 | 30根1m bar(30分钟)缩量窄幅 |
| 陷阱 | 一根+2%缩量5m bar看起来稳 | 里面的1m bar可能是-8%放量砸盘+6%反弹 |
| 确认 | 大级别横盘成立 | 最后5根1m是否缩量企稳 |

**案例: PAINT**
- 5m看: 推送时+2.1%缩量bar, 看起来横盘末端
- 1m看: 2分钟前刚-8.7%放量砸盘(V=$4.2K), 只反弹了2根1m bar就推送
- 结论: 1m揭示洗盘不充分, 5m的"横盘"是假象

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

### Step 2: 同时拉取5m和1m K线 → 三步分析

**必须同时拉取两种分辨率，缺一不可。**

```
5m K线: 48根, interval='5min', limit=48
1m K线: 60根, interval='1min', limit=60
```

**关键: baseline必须是推送时刻的5m收盘价，不是K线第一根bar!**

```
5m K线: 48根, 取推送前最后一根bar的close作为baseline
1m K线: 60根, 用于推送后微观确认
```

**Step 2a: 推送前涨幅 (Pre-Push Pump)**

```
pre_low  = 推送前K线的最低价
pre_high = 推送前K线的最高价
pre_pump_pct = (pre_high - pre_low) / pre_low × 100
pullback     = (baseline - pre_high) / pre_high × 100
```

含义:
- `pre_pump_pct` — 推送前已经涨了多少。>200%=信号滞后, 追高风险
- `pullback` — 推送时是否已从高点回落。负值=回调中(好), 正值或接近0=追在尖上(坏)

**Step 2b: 推送后变化 (Post-Push Change)**

```
post_peak   = 推送后最高价
post_trough = 推送后最低价
cur_pct     = (当前价 - baseline) / baseline × 100
trough_pct  = (post_trough - baseline) / baseline × 100  ← 这才是真正的回撤深度
recovery    = (当前价 - post_trough) / post_trough × 100
```

**Step 2c: 综合判断**

```
总涨幅 = (当前价 - pre_low) / pre_low × 100
       = 推送前已涨 pre_pump_pct% + 推送后变动 cur_pct%

回撤区间 = 按 trough_pct 判断 (轻度/中度/重度/极端)
```

**注意**: 回撤区间只看推送后的trough，不包括推送前的最低点。推送前的涨跌已经反映在 `pre_pump_pct` 里。

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

> **所有分析基于全局363个信号的实时K线数据，不是编造或个别案例。**

### Step 3b: ATH回撤深度 — 跌得越狠，反弹越猛

**核心规律（全局数据验证）**:

| ATH回撤深度 | n | WR20 | WR50 | AvgPeak |
|------------|-----|------|------|---------|
| 跌<30%(接近ATH) | 54 | 48% | 28% | +45% |
| 跌30-60% | 95 | 49% | 24% | +63% |
| 跌60-85% | 135 | 52% | 30% | +69% |
| **跌>85%(暴跌到底)** | **72** | **54%** | 26% | +39% |

> **趋势: ATH跌得越深，WR20越高。跌>85%的信号胜率(54%)显著高于接近ATH的信号(48%)。**

**最强组合: ATH暴跌 + 轻度post回撤**

| 组合 | n | WR20 | WR50 | AvgPeak |
|------|-----|------|------|---------|
| **ATH回撤>60% + 轻度post回撤** | **82** | **66%** | **43%** | **+104%** |
| ATH回撤<30% + 轻度post回撤 | 14 | 50% | — | — |

> **从ATH暴跌下来+推送后轻度回撤 = 历史上66%能到WR20, 平均峰值+104%。这是全数据集中胜率最高的组合之一。反之，接近ATH的币即使轻度回撤也只有50%。**

**为什么**: ATH暴跌意味着筹码充分换手、浮筹清洗彻底、上涨空间大。接近ATH意味着上方抛压重、空间小。

### Step 3c: 更精细的交集

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

### Step 5: 看1m微结构 → 决定性确认

**5m的横盘结论必须用1m验证。5m看起来稳 ≠ 真的稳。**

检查推送前最后10根1m bar:

```
最后5根1m: 是否缩量窄幅(量<$500, 波动<±3%)?
倒数6-10根: 是否有放量砸盘(量>$2K, 跌幅>5%)?
```

| 1m信号 | 含义 | 决策 |
|--------|------|------|
| 最后5根全缩量小bar(量<$500) | 充分洗盘 | ✅ 配合5m横盘可入场 |
| 倒数5-10根有放量砸盘(>$2K) | 洗盘不充分 | ❌ 即使5m好看也观望 |
| 推送前2根1m突然放量拉升 | 抢跑信号 | 🟡 可能是好事,等确认 |
| 5min跌>8%+放量 | 恐慌 | ❌ 放弃 |
| 30min转正 | 确认上涨 | ✅ 加仓 |

**横盘验证规则**: 5m的6根缩量窄幅bar(30分钟) + 1m最后5根也缩量窄幅 = 真正的横盘。任何一个不满足 = 假横盘。

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

结合回撤区间、历史WR20、ATH回撤深度、巨阳确认、量比，输出最终判断:

| Post回撤 | ATH回撤 | 历史WR20 | 巨阳确认 | 量比 | 决策 |
|---------|---------|---------|---------|------|------|
| 轻度 | >60%暴跌 | 66% | — | — | **入场**(最强组合) |
| 轻度 | <30%接近ATH | 50% | 需要 | >1.5x | 谨慎入场 |
| 轻度 | 30-60% | 50-60% | — | — | **可入场** |
| 中度 | >60%暴跌 | ~55% | ✅有 | >1.5x | **入场** |
| 中度 | <30%接近ATH | ~45% | ❌无 | <1.0x | 放弃(天花板) |
| 重度 | >60%暴跌 | ~50% | ✅有 | >2x | 试错入场 |
| 重度 | <30%接近ATH | ~35% | 任意 | 任意 | **放弃** |
| 极端 | 任意 | 25% | 任意 | 任意 | **不碰** |

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

# 2. 拉K线 (如果DB没有或者数据不全调用blance接口)
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
