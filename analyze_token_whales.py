"""
Analyze a token's whale/bundler addresses and trace the pump mechanics.
Usage: python analyze_token_whales.py <token_address> [--chain sol]
"""
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from collections import defaultdict

def run_cli(cmd: str) -> dict:
    """Run a gmgn-cli command and return parsed JSON."""
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=60)
    if result.returncode != 0:
        stderr = result.stderr.strip()
        # Handle rate limit
        if "429" in stderr or "RATE_LIMIT" in stderr:
            print(f"  [RATE LIMITED] {stderr[:200]}")
            return {}
        print(f"  [ERROR] {stderr[:300]}")
        return {}
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        print(f"  [JSON ERROR] {result.stdout[:200]}")
        return {}

def format_time(ts: int) -> str:
    if ts == 0:
        return "N/A"
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def format_usd(val) -> str:
    if val is None:
        return "$0"
    return f"${val:,.2f}"

def short_addr(addr: str) -> str:
    if not addr or len(addr) < 10:
        return addr
    return addr[:6] + "..." + addr[-4:]

def analyze_wallet(w, label="") -> dict:
    """Extract key metrics from a wallet entry."""
    tags = w.get("tags", [])
    maker_tags = w.get("maker_token_tags", [])
    all_tags = tags + maker_tags

    balance = w.get("balance", 0) or 0
    usd_value = w.get("usd_value", 0) or 0
    buy_vol = w.get("buy_volume_cur", 0) or 0
    sell_vol = w.get("sell_volume_cur", 0) or 0
    profit = w.get("profit", 0) or 0
    realized = w.get("realized_profit", 0) or 0
    unrealized = w.get("unrealized_profit", 0) or 0
    buy_count = w.get("buy_tx_count_cur", 0) or 0
    sell_count = w.get("sell_tx_count_cur", 0) or 0
    amount_pct = w.get("amount_percentage", 0) or 0
    avg_cost = w.get("avg_cost", 0) or 0
    avg_sold = w.get("avg_sold", 0) or 0
    transfer_in = w.get("transfer_in", False)
    sell_pct = w.get("sell_amount_percentage", 0) or 0
    net_flow = w.get("netflow_amount", 0) or 0
    native_balance = int(w.get("native_balance", 0) or 0) / 1e9  # SOL

    return {
        "address": w.get("address", ""),
        "label": label,
        "tags": all_tags,
        "balance": balance,
        "usd_value": usd_value,
        "amount_pct": amount_pct * 100,
        "buy_vol": buy_vol,
        "sell_vol": sell_vol,
        "buy_count": buy_count,
        "sell_count": sell_count,
        "profit": profit,
        "realized_profit": realized,
        "unrealized_profit": unrealized,
        "avg_cost": avg_cost,
        "avg_sold": avg_sold,
        "sell_pct": sell_pct * 100,
        "transfer_in": transfer_in,
        "net_flow": net_flow,
        "native_balance_sol": native_balance,
        "start_holding": format_time(w.get("start_holding_at", 0) or 0),
        "end_holding": format_time(w.get("end_holding_at", 0) or 0),
        "last_active": format_time(w.get("last_active_timestamp", 0) or 0),
    }


def main():
    if len(sys.argv) < 2:
        print("Usage: python analyze_token_whales.py <token_address> [--chain sol]")
        sys.exit(1)

    token = sys.argv[1]
    chain = "sol"
    for i, arg in enumerate(sys.argv):
        if arg == "--chain" and i + 1 < len(sys.argv):
            chain = sys.argv[i + 1]

    print(f"\n{'='*80}")
    print(f"  GoblinCoin (Goblin) 鲸鱼/庄家分析")
    print(f"  Token: {token}")
    print(f"  Chain: {chain}")
    print(f"{'='*80}")

    # Fetch token info
    print("\n[1/5] 获取代币基本信息...")
    info = run_cli(f'gmgn-cli token info --chain {chain} --address {token} --raw')
    if not info:
        print("无法获取代币信息，退出")
        sys.exit(1)

    price = info.get("price", 0)
    supply = info.get("circulating_supply", 0)
    mcap = float(price) * float(supply) if price and supply else 0
    liquidity = float(info.get("liquidity", 0))
    holders = info.get("holder_count", 0)
    cto = info.get("dev", {}).get("cto_flag", 0)
    creator = info.get("dev", {}).get("creator_address", "")
    creator_status = info.get("dev", {}).get("creator_token_status", "")
    ath_price = info.get("ath_price", 0)
    ath_mc = info.get("dev", {}).get("ath_token_info", {}).get("ath_mc", "0")
    wallet_stats = info.get("wallet_tags_stat", {})

    print(f"""
  ┌─────────────────────────────────────────────────────┐
  │ 代币概览                                             │
  ├─────────────────────────────────────────────────────┤
  │ 名称: Goblin (GoblinCoin)                           │
  │ 价格: ${float(price):.8f}                            │
  │ 市值: ${mcap:,.2f}                                  │
  │ ATH市值: ${float(ath_mc):,.2f}                      │
  │ ATH价格: ${float(ath_price):.8f}                    │
  │ 流动性: ${liquidity:,.2f}                            │
  │ 持有人数: {holders}                                  │
  │ CTO (社区接管): {'是' if cto == 1 else '否'}         │
  │ 创建者: {short_addr(creator)}                        │
  │ 创建者状态: {'已清仓' if creator_status == 'creator_close' else '仍持有'} │
  │ Smart Money: {wallet_stats.get('smart_wallets', 0)}   │
  │ KOL钱包: {wallet_stats.get('renowned_wallets', 0)}    │
  │ 狙击手钱包: {wallet_stats.get('sniper_wallets', 0)}   │
  │ 捆绑钱包: {wallet_stats.get('bundler_wallets', 0)}    │
  │ 老鼠仓: {wallet_stats.get('rat_trader_wallets', 0)}   │
  │ 新钱包: {wallet_stats.get('fresh_wallets', 0)}        │
  │ 创建者已发币数: {info.get('stat', {}).get('creator_created_count', 0)} │
  │ 捆绑交易占比: {float(info.get('stat', {}).get('top_bundler_trader_percentage', 0)) * 100:.1f}% │
  │ Bot Degen占比: {float(info.get('stat', {}).get('top_bot_degen_percentage', 0)) * 100:.1f}% │
  └─────────────────────────────────────────────────────┘""")

    # Security info
    print("\n[2/5] 获取安全信息...")
    time.sleep(1)
    sec = run_cli(f'gmgn-cli token security --chain {chain} --address {token} --raw')

    # Fetch holders
    print("[3/5] 获取大户持仓数据...")
    time.sleep(1)
    holders_raw = run_cli(
        f'gmgn-cli token holders --chain {chain} --address {token} --limit 50 --order-by amount_percentage --direction desc --raw'
    )

    # Fetch smart money holders
    print("[4/5] 获取Smart Money持仓...")
    time.sleep(1)
    sm_holders_raw = run_cli(
        f'gmgn-cli token holders --chain {chain} --address {token} --limit 30 --tag smart_degen --order-by unrealized_profit --direction desc --raw'
    )

    # Fetch top traders by buy volume
    print("[5/5] 获取最大买入交易者...")
    time.sleep(2)  # Rate limit precaution
    top_buyers_raw = run_cli(
        f'gmgn-cli token traders --chain {chain} --address {token} --limit 30 --order-by buy_volume_cur --direction desc --raw'
    )

    # =========================================================================
    # ANALYSIS SECTION
    # =========================================================================

    print(f"\n{'='*80}")
    print(f"  SECTION 1: 原始开发团队 (Dev Team) — 币价归零的元凶")
    print(f"{'='*80}")
    print(f"""
  这个代币开盘仅8秒就被开发者砸盘归零。以下是4个DEV关联钱包的链上行为:
  - 他们在开盘瞬间 (bundle snipe) 买入大量筹码
  - 几秒内全部卖出
  - 获利微薄 ($200-$650)，但导致了归零
  - 之后社区接管 (CTO) 将币重新拉起""")

    dev_wallets = [
        {
            "addr": "Hza17rrRc5AHmjGBQkesrqsh3cZEe7g5uFhUjz6LejpJ",
            "label": "CREATOR (创建者)",
            "tags": "creator, dev_team, bundler, sniper",
            "bought": "$256 → 96.4M tokens",
            "sold": "$914 → all 96.4M tokens",
            "profit": "$643",
            "timing": "开盘→砸盘 8秒内完成",
            "funded": "CuRb2RpKGkq...",
        },
        {
            "addr": "3QNKcR8g3dPFNNepFecAxjPLFdzC1cMJkSGuNptPU2UU",
            "label": "DEV TEAM #1",
            "tags": "dev_team, bundler, sniper",
            "bought": "$133 → 43.6M tokens",
            "sold": "$382 → all 43.6M tokens",
            "profit": "$238",
            "timing": "开盘→砸盘 16秒内完成",
            "funded": "Binance Hot Wallet",
        },
        {
            "addr": "74Gku52qWEn6ywACYHJTMnQoWBrTcxvfoQ5Jym6t1SZF",
            "label": "DEV TEAM #2",
            "tags": "dev_team, bundler, sniper",
            "bought": "$188 → 50.2M tokens",
            "sold": "$462 → all 50.2M tokens",
            "profit": "$261",
            "timing": "开盘→砸盘 16秒内完成",
            "funded": "HTX Hot Wallet",
        },
        {
            "addr": "CrVtHgzg1tyAP1uCkFmPJ29cmXFkGw3qx1Uy5QpKSmrh",
            "label": "DEV TEAM #3",
            "tags": "dev_team, bundler, sniper",
            "bought": "$149 → 44.5M tokens",
            "sold": "$404 → all 44.5M tokens",
            "profit": "$242",
            "timing": "开盘→砸盘 16秒内完成",
            "funded": "MEXC Hot Wallet",
        },
    ]
    for w in dev_wallets:
        print(f"  [{w['label']}] {w['addr']}")
        print(f"    标签: {w['tags']}")
        print(f"    买入: {w['bought']}")
        print(f"    卖出: {w['sold']}")
        print(f"    利润: {w['profit']}")
        print(f"    时间: {w['timing']}")
        print(f"    资金来源: {w['funded']}")
        print()

    # =========================================================================
    # SECTION 2: TOP HOLDERS (non-pool)
    # =========================================================================
    print(f"{'='*80}")
    print(f"  SECTION 2: 当前TOP持仓大户 (Top Holders)")
    print(f"{'='*80}")
    print(f"  {'地址':<46} {'持仓%':>6} {'市值':>10} {'买入成本':>10} {'已实现盈亏':>10} {'未实现盈亏':>10} {'标签'}")
    print(f"  {'-'*46} {'-'*6} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*20}")

    holder_list = holders_raw.get("list", []) if holders_raw else []
    shown = 0
    for h in holder_list:
        if h.get("addr_type") == 2:  # Skip pool address
            continue
        a = analyze_wallet(h)
        if a["amount_pct"] < 0.1:
            continue
        tags_str = ",".join(a["tags"][:3]) if a["tags"] else "-"
        print(f"  {a['address']:<46} {a['amount_pct']:>5.2f}% {a['usd_value']:>9,.0f} {a['buy_vol']:>9,.0f} {a['realized_profit']:>9,.0f} {a['unrealized_profit']:>9,.0f} {tags_str}")
        shown += 1
        if shown >= 15:
            break

    # =========================================================================
    # SECTION 3: BUNDLER/分发钱包（真正的庄家）
    # =========================================================================
    print(f"\n{'='*80}")
    print(f"  SECTION 3: 捆绑/分发钱包 — 将币从归零拉到3M的真正推手")
    print(f"{'='*80}")
    print(f"""
  这两个钱包是真正的"庄家"。它们通过200+笔小额买入吸筹，然后通过
  TRANSFER (非卖出) 把代币分发给大量钱包，制造去中心化假象。

  关键特征:
  - 大量买入 (209-222笔交易)
  - 从未在市场上卖出 (sell_count = 0)
  - 当前余额为0 (所有代币已通过transfer分发)
  - 被GMGN标记为 bundler
  - 资金来源均为Coinbase Hot Wallet""")

    distribution_wallets = [
        {
            "addr": "Em8J3gBWapfVBGVhVipwQnLrqCvnWBnLajw6XFsFECPF",
            "label": "主分发钱包 #1",
            "total_bought": "$41,784",
            "tokens_bought": "60,849,823 (7.9% supply)",
            "buy_txns": 209,
            "sell_txns": 0,
            "avg_buy_price": "$0.000687",
            "current_balance": 0,
            "sol_balance": "210.5 SOL",
            "fund_source": "Coinbase Hot Wallet 4",
            "note": "所有代币已通过transfer分发 → 制造持仓分散假象"
        },
        {
            "addr": "GmFrDZT2cdrqykgTikVdXbe8EtCgzUDM9VsDhQnwsUsG",
            "label": "主分发钱包 #2",
            "total_bought": "$39,501",
            "tokens_bought": "230,799,115 (30% supply!)",
            "buy_txns": 222,
            "sell_txns": 0,
            "avg_buy_price": "$0.000171",
            "current_balance": 0,
            "sol_balance": "49.5 SOL",
            "fund_source": "Unknown",
            "note": "所有代币已通过transfer分发 → 吸筹量最大"
        },
    ]
    for w in distribution_wallets:
        print(f"  [{w['label']}] {w['addr']}")
        for k, v in w.items():
            if k not in ("label", "addr"):
                print(f"    {k}: {v}")
        print()

    # =========================================================================
    # SECTION 4: SMART MONEY / KOL
    # =========================================================================
    print(f"{'='*80}")
    print(f"  SECTION 4: Smart Money & KOL 钱包分析")
    print(f"{'='*80}")

    sm_list = sm_holders_raw.get("list", []) if sm_holders_raw else []
    if sm_list:
        print(f"\n  Top Smart Money 持有者 (按未实现利润排序):")
        print(f"  {'地址':<46} {'持仓':>10} {'买入':>10} {'卖出':>10} {'已实现利润':>10} {'未实现利润':>10} {'标签'}")
        print(f"  {'-'*46} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*15}")
        for h in sm_list[:10]:
            a = analyze_wallet(h)
            tags_str = ",".join(a["tags"][:2]) if a["tags"] else "-"
            print(f"  {a['address']:<46} {a['balance']:>9,.0f} {a['buy_vol']:>9,.0f} {a['sell_vol']:>9,.0f} {a['realized_profit']:>9,.0f} {a['unrealized_profit']:>9,.0f} {tags_str}")

    # =========================================================================
    # SECTION 5: TOP BUYERS (WHO PUMPED IT)
    # =========================================================================
    print(f"\n{'='*80}")
    print(f"  SECTION 5: 最大买入量交易者 (谁在推高价格)")
    print(f"{'='*80}")
    print(f"  {'地址':<46} {'买入量':>12} {'买入笔数':>8} {'卖出量':>12} {'余额':>10} {'标签'}")
    print(f"  {'-'*46} {'-'*12} {'-'*8} {'-'*12} {'-'*10} {'-'*20}")

    buyer_list = top_buyers_raw.get("list", []) if top_buyers_raw else []
    for h in buyer_list[:15]:
        a = analyze_wallet(h)
        tags_str = ",".join(a["tags"][:3]) if a["tags"] else "-"
        print(f"  {a['address']:<46} ${a['buy_vol']:>10,.0f} {a['buy_count']:>7}  ${a['sell_vol']:>10,.0f} {a['balance']:>9,.0f} {tags_str}")

    # =========================================================================
    # SECTION 6: KOL/RENOWNED WALLETS
    # =========================================================================
    print(f"\n{'='*80}")
    print(f"  SECTION 6: KOL/知名钱包分析")
    print(f"{'='*80}")
    print(f"\n  等待rate limit恢复后补充KOL数据...")
    print(f"  已知: 14个KOL钱包持有此代币")
    print(f"  已知: 54个Smart Money钱包持有此代币")

    # =========================================================================
    # SUMMARY
    # =========================================================================
    print(f"\n{'='*80}")
    print(f"  总结: GoblinCoin 从归零到3M市值的庄家链上画像")
    print(f"{'='*80}")
    print(f"""
  ┌─────────────────────────────────────────────────────────────────────┐
  │ 第一阶段: 开发者砸盘归零 (2026-04-23)                                │
  │                                                                     │
  │ 创建者 Hza17rrRc5AHmjGBQkesrqsh3cZEe7g5uFhUjz6LejpJ 在pump.fun     │
  │ 创建了GoblinCoin。开盘瞬间 (bundle snipe)，创建者+3个DEV关联钱包      │
  │ 在秒级完成买入→卖出，合计获利约 $1,384，但币价归零。                  │
  │                                                                     │
  │ 创建者曾发过5个代币，此前最好的ATH市值为 $100K 级别。                 │
  ├─────────────────────────────────────────────────────────────────────┤
  │ 第二阶段: CTO社区接管 + 庄家建仓 (2026-04-23起)                      │
  │                                                                     │
  │ 社区接管 (CTO) 后，两个主力分发钱包入场:                              │
  │                                                                     │
  │ ★ Em8J3gBWapfVBGVhVipwQnLrqCvnWBnLajw6XFsFECPF                     │
  │   - 通过209笔小额买入累计 $41,784                                    │
  │   - 吸收 60.8M 代币 (7.9% supply)                                   │
  │   - 全部通过transfer分发给数百个小钱包                                │
  │   - 钱包余额 210.5 SOL，资金来自Coinbase                             │
  │                                                                     │
  │ ★ GmFrDZT2cdrqykgTikVdXbe8EtCgzUDM9VsDhQnwsUsG                     │
  │   - 通过222笔小额买入累计 $39,501                                    │
  │   - 吸收 230.8M 代币 (30% supply!)                                   │
  │   - 全部通过transfer分发，制造去中心化假象                            │
  │   - 钱包余额 49.5 SOL                                               │
  │                                                                     │
  │ 策略: 大资金通过bot分拆为大量小额买单 → 推高价格 → transfer到         │
  │       大量子钱包 → 制造"散户疯狂涌入"假象 → 吸引真实散户FOMO          │
  ├─────────────────────────────────────────────────────────────────────┤
  │ 第三阶段: Smart Money接力 (持续中)                                    │
  │                                                                     │
  │ 54个Smart Money钱包 + 14个KOL钱包进入:                               │
  │                                                                     │
  │ ★ DfMxre4cKmvogbLrPigxmibVTTQDuzjdXojWzjCXXhzj (pump_smart, photon)│
  │   - 通过transfer接收了大量代币（从分发钱包）                          │
  │   - 已卖出92%仓位，实现利润 $3,548                                    │
  │   - 仍持有 $9,587 未实现利润 $8,923                                  │
  │   - 总利润: $12,470 (3.58x)                                          │
  │                                                                     │
  │ ★ 7WXrVYgi2FHrDH57opvDWbiLacQTXYtvg8goALdwn9Um                     │
  │   - 已卖出96.8%仓位，实现利润 $3,699                                  │
  │   - 总利润: $7,683 (8.3x)                                            │
  │                                                                     │
  │ ★ RwT8dxbecoLaSxkPNwnK1zNpPYFoY1upDiFM5XMpGit (TOP2 holder)        │
  │   - 持有2.88% supply, 价值 $86,898                                   │
  │   - 仅卖出33%, 未实现利润 $85,567                                    │
  │   - 总利润: $85,357 (43x!) — 最大赢家                                │
  ├─────────────────────────────────────────────────────────────────────┤
  │ 风险信号                                                             │
  │                                                                     │
  │ ⚠️ 捆绑交易占比: 20% — 偏高                                          │
  │ ⚠️ Bot Degen占比: 41.15% — 很高，说明大量机器人交易                  │
  │ ⚠️ 两个分发钱包通过transfer分发代币 — 典型的做市操纵手法              │
  │ ⚠️ 分发钱包#2吸收了30% supply后全部转出                              │
  │ ⚠️ Smart Money 排名靠前的地址已大量出货 (sell 92-97%)                │
  │ ✅ Mint/Feeze已Renounce                                              │
  │ ✅ 流动性已销毁 (burn)                                                │
  │ ✅ 创建者已清仓                                                       │
  │ ✅ Top 10持仓仅18.4% — 相对分散                                       │
  └─────────────────────────────────────────────────────────────────────┘

  结论:
  这个代币的"庄家"是一个有组织的做市团队，通过两个核心分发钱包
  (Em8J3g...和GmFrDZT...) 使用bot分拆大单为200+笔小单进行吸筹，
  然后将代币通过transfer (非市场卖出) 分发到大量子钱包，制造持仓
  分散和社区狂热的假象。Smart Money和KOL随后接力拉升，但目前
  头部Smart Money已在大量出货 (卖出92-97%仓位)。

  关键监控地址 (庄家核心):
  - Em8J3gBWapfVBGVhVipwQnLrqCvnWBnLajw6XFsFECPF  (分发#1, 210 SOL余额)
  - GmFrDZT2cdrqykgTikVdXbe8EtCgzUDM9VsDhQnwsUsG  (分发#2, 49 SOL余额)
  - RwT8dxbecoLaSxkPNwnK1zNpPYFoY1upDiFM5XMpGit  (最大赢家, 43x利润)
  - DfMxre4cKmvogbLrPigxmibVTTQDuzjdXojWzjCXXhzj  (SM#1, 已出92%)
""")

    # Export whale watchlist
    print(f"\n{'='*80}")
    print(f"  鲸鱼监控地址列表 (导入监控bot)")
    print(f"{'='*80}")
    print("""
  WHALE_WATCHLIST = [
      "Em8J3gBWapfVBGVhVipwQnLrqCvnWBnLajw6XFsFECPF",  # 分发钱包#1
      "GmFrDZT2cdrqykgTikVdXbe8EtCgzUDM9VsDhQnwsUsG",  # 分发钱包#2
      "RwT8dxbecoLaSxkPNwnK1zNpPYFoY1upDiFM5XMpGit",  # TOP2 holder, 43x
      "DfMxre4cKmvogbLrPigxmibVTTQDuzjdXojWzjCXXhzj",  # SM#1, pump_smart
      "7WXrVYgi2FHrDH57opvDWbiLacQTXYtvg8goALdwn9Um",  # SM#2
      "9jyqFiLnruggwNn4EQwBNFXwpbLM9hrA4hV59ytyAVVz",  # KOL#1
      "Hza17rrRc5AHmjGBQkesrqsh3cZEe7g5uFhUjz6LejpJ",  # 创建者
      "3QNKcR8g3dPFNNepFecAxjPLFdzC1cMJkSGuNptPU2UU",  # DEV#1
      "74Gku52qWEn6ywACYHJTMnQoWBrTcxvfoQ5Jym6t1SZF",  # DEV#2
      "CrVtHgzg1tyAP1uCkFmPJ29cmXFkGw3qx1Uy5QpKSmrh",  # DEV#3
  ]
""")


if __name__ == "__main__":
    main()
