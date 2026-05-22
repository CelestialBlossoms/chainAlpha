# Chain Alpha Agents

这个目录存放底部异动生产链路使用的 Agent 化分析代码。

## 保留范围

- `BottomSignalAgent`: 复用 `bottom_detection.bottom_accumulation_monitor` 的单 CA 底部异动分析能力。
- `KlineStructureAgent`: 分析 K 线结构和关键回撤位置。
- `ChipAnalysisAgent`: 分析 Top100、标签钱包、盈利钱包和卖出进度。
- `SignalDecisionAgent`: 决定发 TG、更新前端、加入/删除 watchlist 等动作。
- `ActionExecutorAgent`: 执行 dry-run、TG、前端流、数据库 watchlist 操作。

## 生产接入

`bottom_detection/bottom_accumulation_monitor.py` 的底部异动推送分支已经接入 Agent 决策/执行层。

开关：

```text
BOTTOM_USE_AGENT_DECISION=1
```

需要回退旧逻辑时设置：

```text
BOTTOM_USE_AGENT_DECISION=0
```

当前生产链路：

```text
BottomSignalAgent -> KlineStructureAgent -> ChipAnalysisAgent -> SignalDecisionAgent -> ActionExecutorAgent
```
