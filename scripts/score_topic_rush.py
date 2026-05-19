#!/usr/bin/env python3
"""Score Topic Rush tokens against anomaly success criteria."""
import sys, io, requests, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

H = {"Accept-Encoding": "identity", "User-Agent": "binance-web3/1.1 (Skill)"}
fm = lambda v: "${:.0f}K".format(v/1e3) if v>=1e3 else "${:.0f}".format(v)

# Topic Rush
r = requests.get(
    "https://web3.binance.com/bapi/defi/v2/public/wallet-direct/buw/wallet/market/token/social-rush/rank/list/ai",
    params={"chainId": "CT_501", "rankType": "10", "sort": "10", "asc": "false"}, headers=H, timeout=20)
topics = r.json().get("data", [])

tokens_to_check = []
for t in topics:
    name = (t.get("name") or {}).get("topicNameCn", "?")
    inflow = float(t.get("topicNetInflow1h", 0) or 0)
    for tok in (t.get("tokenList") or []):
        tokens_to_check.append(dict(
            symbol=tok.get("symbol", "?"),
            address=tok.get("contractAddress", ""),
            topic=name, topic_inflow=inflow,
            net_inflow=float(tok.get("netInflow", 0) or 0),
        ))

print("Topic Rush: {} topics, {} tokens".format(len(topics), len(tokens_to_check)))
print()

scored = []
for i, tok in enumerate(tokens_to_check):
    addr = tok["address"]
    if not addr:
        continue
    try:
        r2 = requests.get(
            "https://web3.binance.com/bapi/defi/v4/public/wallet-direct/buw/wallet/market/token/dynamic/info/ai?chainId=CT_501&contractAddress=" + addr,
            headers=H, timeout=10)
        data = r2.json().get("data", {}) or {}

        mcap = float(data.get("marketCap", 0))
        vol1h = float(data.get("volume1h", 0))
        smart = data.get("smartMoneyHolders", 0) or 0
        kol = data.get("kolHolders", 0) or 0
        holders = data.get("holders", 0) or 0
        liq = float(data.get("liquidity", 0))
        bundle = data.get("bundlerHolders", 0) or 0

        score = 0
        reasons = []

        if 50000 <= mcap < 200000:
            score += 3; reasons.append("MCap黄金")
        elif mcap < 50000:
            score += 1; reasons.append("MCap偏小")
        elif mcap < 500000:
            score += 1

        if vol1h > 10000:
            score += 2; reasons.append("量足")
        elif vol1h > 5000:
            score += 1

        if smart >= 5:
            score += 2
        elif smart >= 2:
            score += 1
        reasons.append("SM{}".format(smart))

        if kol >= 3:
            score += 1
        if kol > 0:
            reasons.append("KOL{}".format(kol))

        if tok["topic_inflow"] > 1000:
            score += 2; reasons.append("热钱流入")
        elif tok["topic_inflow"] > 100:
            score += 1

        if bundle > 30:
            score -= 2; reasons.append("捆绑{}".format(bundle))

        liq_ratio = liq / max(1, mcap)
        if liq_ratio < 0.07:
            score -= 1; reasons.append("池薄")

        scored.append(dict(
            symbol=tok["symbol"], mcap=mcap, score=score,
            smart=smart, kol=kol, vol1h=vol1h, holders=holders,
            topic=tok["topic"], net_inflow=tok["net_inflow"],
            reasons=",".join(reasons),
        ))
    except:
        pass

    if (i + 1) % 15 == 0:
        time.sleep(0.3)

scored.sort(key=lambda x: -x["score"])

print("Scored {} tokens".format(len(scored)))
print()
print("{:>5} {:<16} {:>9} {:>8} {:>4} {:>4} {:>5} {:>30} {}".format(
    "Score", "Symbol", "MCap", "Vol1h", "SM", "KOL", "H", "Topic", "Reasons"))
print("-" * 110)

for s in scored[:25]:
    mark = "+" if s["score"] >= 7 else ("." if s["score"] >= 4 else "-")
    print("{} {:>2} ${:<15} {:>9} {:>8} {:>4} {:>4} {:>5} {:>30} {}".format(
        mark, s["score"], s["symbol"], fm(s["mcap"]), fm(s["vol1h"]),
        s["smart"], s["kol"], s["holders"], s["topic"][:30], s["reasons"]))

high = [s for s in scored if s["score"] >= 7]
mid = [s for s in scored if 4 <= s["score"] < 7]
low = [s for s in scored if s["score"] < 4]
print()
print("High(>=7): {} | Mid(4-6): {} | Low(<4): {}".format(len(high), len(mid), len(low)))

if high:
    print()
    print("=== TOP PICKS ===")
    for s in high:
        print("  ${:<14s} mcap={} SM={} KOL={} topic={}".format(
            s["symbol"], fm(s["mcap"]), s["smart"], s["kol"], s["topic"][:40]))
