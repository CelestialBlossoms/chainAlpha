# Chain Alpha Agents

这个目录用于存放后续 Agent 化的分析代码。

目标不是重写现有脚本，而是逐步把 `deep_alpha_pro.py`、`bottom_detection/bottom_accumulation_monitor.py`、`tg_ca_chip_alert_bot.py` 中的分析能力拆成可复用 Agent。

## 目录结构

```text
agents/
  README.md
  __init__.py
  base.py
  context.py
  memory.py
  tools.py
  market_scanner_agent.py
  chip_analysis_agent.py
  signal_decision_agent.py
  bottom_signal_agent.py
  kline_structure_agent.py
  action_executor_agent.py
```

## Agent 流程

```text
observe -> think -> act
```

- `observe`: 获取当前 CA、GMGN 数据、数据库历史、Redis 状态。
- `think`: 做筹码、K 线、叙事、风险、复推、底部异动判断。
- `act`: 写数据库、推前端、发 TG、删除归零 CA、生成复盘备注。

## 当前规划

- `MarketScannerAgent`: 负责发现和整理待分析 CA。
- `ChipAnalysisAgent`: 负责 Top100、标签钱包、盈利钱包、卖出进度等筹码分析。
- `SignalDecisionAgent`: 负责决定发 TG、只更新前端、加入/删除 watchlist。
- `BottomSignalAgent`: 复用当前底部监控函数，对单个 CA 跑底部异动分析。
- `KlineStructureAgent`: 分析 K 线结构，当前包含 Fibonacci 回撤位置。
- `ActionExecutorAgent`: 根据决策执行或 dry-run 发 TG、推前端、删除前端/数据库 watchlist。

后续新增分析功能优先放到这里，再由旧脚本调用 Agent，逐步解耦。

## 当前可运行入口

单个 CA 底部异动分析：

```powershell
D:\software\anaconda\envs\py312\python.exe scripts\run_agent_ca_analysis.py <CA>
```

输出 JSON：

```powershell
D:\software\anaconda\envs\py312\python.exe scripts\run_agent_ca_analysis.py <CA> --json
```

默认是 dry-run，只输出执行计划，不发 TG、不写 Redis、不删库。

真实执行：

```powershell
D:\software\anaconda\envs\py312\python.exe scripts\run_agent_ca_analysis.py <CA> --execute
```

当前链路：

```text
BottomSignalAgent -> KlineStructureAgent -> ChipAnalysisAgent -> SignalDecisionAgent -> ActionExecutorAgent
```

## 生产监控接入

`bottom_detection/bottom_accumulation_monitor.py` 的底部异动推送分支已经接入 Agent 决策/执行层。

开关：

```text
BOTTOM_USE_AGENT_DECISION=1
```

默认启用。需要回退旧逻辑时设置：

```text
BOTTOM_USE_AGENT_DECISION=0
```

当前生产接入范围：

- 已接入：底部异动 TG/前端执行决策。
- 暂未接入：EMA 金叉执行分支、扫描入口、快照保存。
