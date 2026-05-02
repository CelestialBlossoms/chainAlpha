"""
Fetch all token transfers for a Solana wallet using public RPC.

Usage:
    D:/software/anaconda/envs/py312/python.exe scripts/scrape_wallet_txs.py <wallet>
    D:/software/anaconda/envs/py312/python.exe scripts/scrape_wallet_txs.py <wallet> --limit 200
"""
import argparse, json, sys, time
from pathlib import Path

import requests

RPC_URLS = ["https://api.mainnet-beta.solana.com"]
HEADERS = {"Content-Type": "application/json"}


def rpc_call(method, params, idx=0):
    if idx >= len(RPC_URLS): return None
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    try:
        resp = requests.post(RPC_URLS[idx], json=payload, headers=HEADERS, timeout=30)
        if resp.status_code == 429: time.sleep(2); return rpc_call(method, params, idx+1)
        data = resp.json()
        if "error" in data:
            return rpc_call(method, params, idx+1)
        return data.get("result")
    except: return None


def get_signatures(address, limit=100, before=None):
    params = [address, {"limit": limit}]
    if before: params[1]["before"] = before
    return rpc_call("getSignaturesForAddress", params)


def get_transaction(sig):
    return rpc_call("getTransaction", [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}])


def parse_token_transfers(tx):
    if not tx: return []
    meta = tx.get("meta", {})
    pre_b = meta.get("preTokenBalances", [])
    post_b = meta.get("postTokenBalances", [])
    pre_map = {}
    for b in pre_b:
        key = (b.get("owner", ""), b.get("mint", ""))
        pre_map[key] = float(b.get("uiTokenAmount", {}).get("amount", 0))
    transfers = []
    for b in post_b:
        key = (b.get("owner", ""), b.get("mint", ""))
        post_amt = float(b.get("uiTokenAmount", {}).get("amount", 0))
        pre_amt = pre_map.get(key, 0)
        diff = post_amt - pre_amt
        if abs(diff) > 0.000001:
            transfers.append({"owner": b.get("owner", ""), "mint": b.get("mint", ""),
                              "amount": round(diff, 6)})
    return transfers


def fetch_all(address, max_txs=200):
    print(f"Fetching signatures for {address}...")
    sigs = []; before = None
    while len(sigs) < max_txs:
        batch = get_signatures(address, limit=100, before=before)
        if not batch: break
        sigs.extend(batch)
        print(f"  {len(batch)} sigs (total: {len(sigs)})")
        if len(batch) < 100: break
        before = batch[-1]["signature"]
        time.sleep(0.5)
    sigs = sigs[:max_txs]

    results = {"token_transfers": [], "all_txs": []}
    for i, s in enumerate(sigs):
        sig = s["signature"]; block_time = s.get("blockTime", 0)
        if i % 20 == 0 and i > 0: print(f"  Processing {i}/{len(sigs)}..."); time.sleep(0.5)
        tx = get_transaction(sig)
        if not tx: continue
        tfs = parse_token_transfers(tx)
        for tf in tfs:
            results["token_transfers"].append({"signature": sig, "block_time": block_time, **tf})
        results["all_txs"].append({"signature": sig, "block_time": block_time})
    return results


def main():
    p = argparse.ArgumentParser()
    p.add_argument("address"); p.add_argument("--limit", type=int, default=200)
    args = p.parse_args()
    results = fetch_all(args.address, args.limit)
    print(f"\nTotal TXs: {len(results['all_txs'])} | Token transfers: {len(results['token_transfers'])}")

    from collections import Counter
    mint_ctr = Counter(tf["mint"] for tf in results["token_transfers"])
    print(f"\nTop token mints transferred:")
    for mint, cnt in mint_ctr.most_common(15): print(f"  {mint}: {cnt}")

    out = Path("solscan_output") / f"rpc_{args.address[:12]}.json"
    out.parent.mkdir(exist_ok=True)
    with open(out, "w", encoding="utf-8") as f: json.dump(results, f, ensure_ascii=False, indent=2, default=str)
    print(f"\nSaved: {out}")


if __name__ == "__main__":
    main()
