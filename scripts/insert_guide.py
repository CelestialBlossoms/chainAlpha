#!/usr/bin/env python3
"""Insert trading guide into DB."""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from db_client import db_op

def main():
    content = open(ROOT / "onchain_trading_guides" / "03-pullback-bounce-strategy.md", "r", encoding="utf-8").read()

    def run(conn):
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS onchain_trading_guides (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                category TEXT DEFAULT '回调反弹',
                content TEXT,
                tags TEXT,
                version INTEGER DEFAULT 1,
                created_at TIMESTAMPTZ DEFAULT now(),
                updated_at TIMESTAMPTZ DEFAULT now()
            )
        """)
        cur.execute("""
            INSERT INTO onchain_trading_guides (title, category, content, tags)
            VALUES (%s, %s, %s, %s)
        """, (
            "回调反弹信号策略",
            "回调反弹",
            content,
            "回调反弹,V反,洗盘,主力建仓,Deep V,交易策略",
        ))

        # Guide 2: Blue-chip wallet resonance
        content2 = """## 蓝筹头部赢家钱包共振信号

蓝筹头部赢家钱包共振，是指在短时间内，大量曾在蓝筹级项目中获得头部收益的钱包，同时对某一代币进行集中买入。当代币触发该信号时，通常意味着该代币正在快速成为市场热点。真正的金狗，往往都会伴随多次、密集的共振信号出现。

---

## 最容易诞生共振信号的代币类型

### 1. 热点驱动型代币
由热点新闻、热门 IP、强叙事驱动的新币，在刚上线初期，99.99% 会触发蓝筹头部钱包共振信号。

### 2. 潜在大金狗代币
即将成长为大金狗的代币，往往会出现大量蓝筹头部钱包共振。共振次数越多、密度越高，说明真正的实力资金正在入场。

### 3. 关键判断
当一个代币在短时间内频繁触发共振信号时，一定要重点关注，大概率，它正在走向真正的大金狗行情。

---

## 与底部异动检测的关联

### 共振信号 + 底部异动 = 双重确认
- 底部异动检测：发现量价异常
- 钱包共振检测：发现聪明钱进场
- 两者同时命中 = 最高质量信号

### 数据关联
| 底部异动标签 | 共振信号关联 |
|------------|------------|
| 黄金区间 ($50-120K) | 蓝筹钱包共振高发区 |
| 天花板 (<1.5x) | 共振信号极少，不值得关注 |
| 死猫跳 (缩量>50%) | 无共振信号，纯资金拉盘 |
| Healthy V (放量>1.3x) | 往往伴随共振信号 |

### 实战策略
1. 底部异动推送 → 检查是否有蓝筹钱包共振
2. 有共振 + Healthy V → 高确定性信号
3. 无共振 + DCB → 拉高出货，远离

---

*生成日期: 2026-05-19*
"""
        cur.execute("""
            INSERT INTO onchain_trading_guides (title, category, content, tags)
            VALUES (%s, %s, %s, %s)
        """, (
            "蓝筹头部赢家钱包共振信号",
            "钱包分析",
            content2,
            "共振信号,蓝筹钱包,聪明钱,金狗,热点驱动,钱包分析",
        ))
        # Guide 3: Fibonacci strategy
        content3 = """## 斐波那契回调交易策略

斐波那契是 TradingView 自带分析工具。

---

## 参数配置

### 1. 配置斐波那契参数
使用以下三个关键位：**0.786、0.86、0.94**

（TradingView 默认只有 0.618、0.786 等，需要手动添加 0.86 和 0.94）

### 2. 使用方式
从**最低点拖动到最高点**。
- 如果是突然拉一根大阳线：将起涨前的低点作为低点，大阳线顶部作为高点
- 如果是 MEME 死了之后重新起飞：将起飞附近的低点作为低点，前一波高点作为高点

### 3. 下单位置
在底部围绕以下位置分批下单：
- **0.618**：多头强势回调位（激进）
- **0.786**：标准回调位
- **0.86**：深度回调位
- **0.94**：极端回调位（几乎回到原点，高风险高回报）

### 4. 多头趋势策略
多头趋势中，可以更激进：
- 激进点可以看 **0.5** 附近进场
- 0.618 是常规加仓位
- 0.786 是防守性加仓位

### 5. 空头趋势策略
空头趋势中，必须更保守：
- 尽量在 **0.786、0.86、0.94** 附近下单
- 不到位置不进场，宁可错过不可做错
- 0.94 附近是"极端恐惧"位，往往是反转点

---

## 与底部异动检测的关联

### 斐波那契 + 底部异动 = 精准入场

| 斐波那契位 | 底部异动特征 | 操作 |
|-----------|------------|------|
| 0.618 | 轻度回撤 10-15%，量能 UP | 激进加仓 |
| 0.786 | 中度回撤 15-25%，量能 FLAT | 标准加仓 |
| 0.86 | 深度回撤 25-35%，量能 UP | 重仓介入 |
| 0.94 | 极端回撤 >35%，量能 UP | 极限抄底 |

### 回测验证（基于 5/15-18 数据）

| 回调幅度 | 斐波那契位 | 最终涨幅中位 | 成功率 |
|---------|-----------|------------|-------|
| 10-15% | ~0.618 | +40% | 70% |
| 15-20% | ~0.786 | +49% | 65% |
| 20-30% | ~0.86 | +72% | 60% |
| 30-50% | ~0.94 | +38% | 50% |

### 实战流程
1. 底部异动推送 → 拉斐波那契（前低→前高）
2. 等价格回调到 0.786/0.86/0.94
3. 检查 5m 量能：放量（>1.3x）→ 确认入场；缩量（<0.5x）→ 放弃
4. 分批建仓：0.786 进 30%，0.86 进 40%，0.94 进 30%

---

*生成日期: 2026-05-19 | 参考: TradingView Fibonacci 工具*
"""
        cur.execute("""
            INSERT INTO onchain_trading_guides (title, category, content, tags)
            VALUES (%s, %s, %s, %s)
        """, (
            "斐波那契回调交易策略",
            "技术分析",
            content3,
            "斐波那契,Fibonacci,回调,入场点,技术分析,TradingView",
        ))
        print("3 guides inserted into onchain_trading_guides")

    db_op(run)

if __name__ == "__main__":
    main()
