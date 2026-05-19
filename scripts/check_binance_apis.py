#!/usr/bin/env python3
"""List Binance Web3 APIs available for trending/discovery."""
import sys, io, requests, json
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

H = {"Accept-Encoding": "identity", "User-Agent": "binance-web3/1.1 (Skill)"}
fm = lambda v: "${:.1f}M".format(v/1e6) if v>=1e6 else ("${:.0f}K".format(v/1e3) if v>=1e3 else "${:.0f}".format(v))

# 1. Topic Rush
print("=== 1. Topic Rush (话题热度榜) ===")
print("URL: /bapi/defi/v2/.../social-rush/rank/list/ai")
r = requests.get(
    "https://web3.binance.com/bapi/defi/v2/public/wallet-direct/buw/wallet/market/token/social-rush/rank/list/ai",
    params={"chainId": "CT_501", "rankType": "10", "sort": "10", "asc": "false"}, headers=H, timeout=20)
topics = r.json().get("data", [])
print("Returns: 30 topics, each with tokenList")
print()
for t in topics[:10]:
    name = (t.get("name") or {}).get("topicNameCn", "?")
    inflow = float(t.get("topicNetInflow1h", 0) or 0)
    tokens = t.get("tokenList", [])
    top_sym = tokens[0].get("symbol", "?") if tokens else "?"
    print("  {:<35s} inflow={:+.1f} tokens={:>2} top=${}".format(name, inflow, len(tokens), top_sym))

# 2. Token Search by volume24h
print()
print("=== 2. Token Search (按24h量) ===")
print("URL: /bapi/defi/v5/.../token/search/ai")
r2 = requests.get(
    "https://web3.binance.com/bapi/defi/v5/public/wallet-direct/buw/wallet/market/token/search/ai",
    params={"keyword": "", "chainIds": "CT_501", "orderBy": "volume24h", "limit": 10}, headers=H, timeout=20)
rows = r2.json().get("data", [])
if isinstance(rows, list):
    for r3 in rows[:10]:
        sym = r3.get("symbol", "?")
        mcap = float(r3.get("marketCap", 0))
        vol24 = float(r3.get("volume24h", 0))
        change = r3.get("percentChange24h", "?")
        print("  ${:<14} mcap={:>8} vol24={:>8} change24h={}%".format(sym, fm(mcap), fm(vol24), change))
else:
    print("  No data - search requires keyword")

# 3. Token Dynamic (individual token)
print()
print("=== 3. Token Dynamic (实时动态) ===")
print("URL: /bapi/defi/v4/.../token/dynamic/info/ai")
print("Returns: price, mcap, volume5m/1h/24h, holders, smartMoneyHolders, kolHolders")

# 4. Token Metadata
print()
print("=== 4. Token Metadata (元数据) ===")
print("URL: /bapi/defi/v1/.../token/meta/info/ai")
print("Returns: name, symbol, description, aiNarrativeFlag, creatorAddress, links")

# 5. K-line
print()
print("=== 5. K-Line ===")
print("URL: dquery.sintral.io/u-kline/v1/k-line/candles")
print("Returns: OHLCV 2D array, intervals: 1min~1mon")

# Summary
print()
print("=" * 60)
print("对比 GMGN vs Binance:")
print("=" * 60)
print("GMGN trending:    量价驱动, 1m/5m/1h间隔, swap count排序")
print("Binance Topic Rush: 叙事驱动, 30个话题, 有净流入数据")
print("Binance Search:    按volume24h排序, 需要keyword(可以为空)")
print()
print("互补: GMGN发现量价异动 -> Binance验证叙事热度 -> 确认信号")
