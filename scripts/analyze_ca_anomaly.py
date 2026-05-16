#!/usr/bin/env python3
"""
CA anomaly analysis agent: one-shot comprehensive diagnosis.
Usage:
    python scripts/analyze_ca_anomaly.py <CA>
    python scripts/analyze_ca_anomaly.py <CA> --chain sol
"""
import sys, json, time, argparse
from pathlib import Path
from datetime import datetime, timezone, timedelta

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from db_client import db_op

BINANCE_DYNAMIC_URL = "https://web3.binance.com/bapi/defi/v4/public/wallet-direct/buw/wallet/market/token/dynamic/info/ai"
BINANCE_KLINE_URL = "https://dquery.sintral.io/u-kline/v1/k-line/candles"
BINANCE_CHAIN_ID = "CT_501"
BINANCE_HEADERS = {"Accept-Encoding": "identity", "User-Agent": "binance-web3/1.1 (Skill)"}

TZ = timezone(timedelta(hours=8))
TAG_COLORS = {"瞬爆": "#f59e0b", "天花板": "#ef4444", "大市值": "#8b5cf6", "无量": "#64748b"}


def fm(v):
    v = float(v) if v else 0
    if v >= 1_000_000: return f"${v / 1_000_000:.2f}M"
    if v >= 1_000: return f"${v / 1_000:.1f}K"
    return f"${v:.0f}"


def fp(v, signed=False):
    v = float(v) if v else 0
    p = '+' if signed and v > 0 else ''
    return f"{p}{v:.1f}%"


def fetch_gmgn_token(address):
    """Get token info from GMGN API."""
    try:
        from config import GMGN_API_KEY
        params = {"chain": "sol", "address": address, "timestamp": int(time.time())}
        headers = {"X-APIKEY": GMGN_API_KEY}
        r = requests.get("https://openapi.gmgn.ai/v1/token/info", params=params, headers=headers, timeout=30)
        if r.ok:
            data = (r.json().get("data") or {})
            if data:
                price_raw = data.get("price", {})
                price = float(price_raw.get("price", 0)) if isinstance(price_raw, dict) else float(price_raw or 0)
                ath_price = float(data.get("ath_price") or 0)
                supply = float(data.get("circulating_supply") or 0)
                ath_mcap = ath_price * supply if ath_price and supply else 0
                dev = data.get("dev") or {}
                ath_info = dev.get("ath_token_info") or {}
                gmgn_ath_mcap = float(ath_info.get("ath_mc") or 0)
                return {
                    "symbol": data.get("symbol") or "?", "name": data.get("name") or "",
                    "price": price, "ath_price": ath_price,
                    "supply": supply, "holders": data.get("holder_count", 0),
                    "liquidity": float(data.get("liquidity") or 0),
                    "mcap": price * supply if price and supply else 0,
                    "ath_mcap": gmgn_ath_mcap or ath_mcap,
                    "launchpad": data.get("launchpad_platform") or "",
                    "created_ts": int(data.get("creation_timestamp") or 0),
                    "age_hours": (time.time() - int(data.get("creation_timestamp") or 0)) / 3600,
                }
    except Exception as e:
        print(f"  GMGN fetch warning: {e}")
    return None


def fetch_binance_data(address):
    """Get current price/mcap from Binance Web3."""
    url = f"{BINANCE_DYNAMIC_URL}?chainId={BINANCE_CHAIN_ID}&contractAddress={address}"
    try:
        r = requests.get(url, headers=BINANCE_HEADERS, timeout=12)
        if r.ok:
            d = (r.json().get("data") or {})
            if d:
                return {
                    "price": float(d.get("price") or 0),
                    "mcap": float(d.get("marketCap") or 0),
                    "liquidity": float(d.get("liquidity") or 0),
                    "holders": d.get("holders", 0),
                    "price_high_24h": float(d.get("priceHigh24h") or 0),
                    "price_low_24h": float(d.get("priceLow24h") or 0),
                    "volume_24h": float(d.get("volume24h") or 0),
                    "volume_1h": float(d.get("volume1h") or 0),
                    "volume_5m": float(d.get("volume5m") or 0),
                    "percent_change_1h": float(d.get("percentChange1h") or 0),
                    "percent_change_24h": float(d.get("percentChange24h") or 0),
                }
    except Exception:
        pass
    return None


def fetch_kline(address, bars=60, resolution="5min"):
    """Get recent K-line data."""
    params = {"address": address, "platform": "solana", "interval": resolution, "limit": bars, "pm": "p"}
    try:
        r = requests.get(BINANCE_KLINE_URL, params=params, headers=BINANCE_HEADERS, timeout=30)
        if r.ok:
            raw = (r.json().get("data") or [])
            return [{"ts": int(i[5] / 1000) if i[5] > 10**10 else int(i[5]),
                     "o": float(i[0]), "h": float(i[1]), "l": float(i[2]),
                     "c": float(i[3]), "v": float(i[4])} for i in raw if isinstance(i, list) and len(i) >= 6]
    except Exception:
        pass
    return []


def fetch_push_history(address):
    """Get push records and snapshots from DB."""
    def _run(conn):
        cur = conn.cursor()
        cur.execute("""
            SELECT id, signal_type, current_mcap, ath_mcap, price_change_pct,
                   event_ts, pushed_at,
                   extra->>'risk_tags' as risk_tags,
                   extra->>'narrative_desc' as nd,
                   extra->>'narrative_type' as nt,
                   pool_total_liquidity, pool_mcap_ratio
            FROM bottom_top100_push_records WHERE address = %s ORDER BY event_ts
        """, (address,))
        pushes = []
        for r in cur.fetchall():
            pushes.append({
                "id": r[0], "signal_type": r[1],
                "mcap": float(r[2] or 0), "ath_mcap": float(r[3] or 0),
                "price_change_pct": float(r[4] or 0),
                "event_ts": int(r[5] or 0),
                "risk_tags": json.loads(r[7]) if r[7] else [],
                "narrative_desc": (r[8] or "")[:120],
                "narrative_type": r[9] or "",
                "pool_liq": float(r[10] or 0), "pool_ratio": float(r[11] or 0),
            })

        cur.execute("""
            SELECT signal_type, signal_score, created_at
            FROM bottom_top100_snapshots WHERE address = %s
            ORDER BY created_at DESC LIMIT 30
        """, (address,))
        snapshots = [{"type": r[0], "score": r[1], "ts": r[2]} for r in cur.fetchall()]

        cur.execute("SELECT ca, current_mcap, ath_mcap, narrative_desc, narrative_type, source FROM bottom_watchlist_tokens WHERE ca = %s", (address,))
        wl = cur.fetchone()
        watchlist = None
        if wl:
            watchlist = {"mcap": float(wl[1] or 0), "ath": float(wl[2] or 0),
                         "narrative_desc": wl[3] or "", "narrative_type": wl[4] or "", "source": wl[5] or ""}

        return pushes, snapshots, watchlist
    return db_op(_run)


def diagnose(token, pushes, snapshots, watchlist, binance, candles):
    """Classify the token's anomaly signal."""
    tags = []
    reasons = []

    # Get latest push data
    latest_push = pushes[0] if pushes else None
    if latest_push:
        gain = latest_push.get("price_change_pct", 0)
        mcap = latest_push.get("mcap", 0)
        ath = latest_push.get("ath_mcap", 0)
        pool_liq = latest_push.get("pool_liq", 0)
        pool_ratio = latest_push.get("pool_ratio", 0)
    else:
        gain = mcap = ath = pool_liq = pool_ratio = 0

    # 1. Flash peak detection (瞬爆)
    if candles:
        # Find peak after first push event_ts
        if latest_push:
            event_ts = latest_push["event_ts"]
            post = [c for c in candles if c["ts"] >= event_ts - 300]
            if post:
                entry_c = post[0]["c"]
                peak_idx, peak_c = max(enumerate(post), key=lambda x: x[1]["h"])
                peak_ts = peak_c["ts"]
                time_to_peak = (peak_ts - event_ts) / 60
                peak_gain = (peak_c["h"] / entry_c - 1) * 100 if entry_c > 0 else 0

                if time_to_peak <= 5 or gain > 50:
                    tags.append("瞬爆")
                    reasons.append(f"峰顶仅{time_to_peak:.0f}分钟(瞬爆), 信号涨幅{gain:.0f}%")

    # 2. Ceiling (天花板)
    ath_ratio = ath / max(1, mcap) if ath and mcap else 0
    if 0 < ath_ratio < 1.5:
        tags.append("天花板")
        reasons.append(f"ATH/mcap={ath_ratio:.1f}x(天花板), 拉升空间不足")

    # 3. Large mcap (大市值)
    if mcap > 500_000:
        tags.append("大市值")
        reasons.append(f"市值{fm(mcap)}(大市值), 拉升成本高")

    # 4. Dead volume (无量)
    if binance:
        vol_1h = binance.get("volume_1h", 0)
        if vol_1h < 5000:
            tags.append("无量")
            reasons.append(f"1h量仅{fm(vol_1h)}(无量), 无人跟买")

    # 5. Signal count
    signal_count = len([s for s in snapshots if s["type"] != "watch"])
    watch_count = len([s for s in snapshots if s["type"] == "watch"])

    return {
        "tags": tags, "reasons": reasons,
        "ath_ratio": ath_ratio,
        "signal_count": signal_count, "watch_count": watch_count,
        "push_count": len(pushes),
        "in_watchlist": watchlist is not None,
    }


def print_report(address, token, pushes, snapshots, watchlist, binance, candles, diagnosis):
    """Print comprehensive analysis report."""
    print()
    print("=" * 70)
    print(f"  CA 异动分析报告")
    print("=" * 70)

    # Basic info
    sym = token["symbol"] if token else (pushes[0].get("symbol", "?") if pushes else "?")
    name = token["name"] if token else ""
    print(f"\n  代币: ${sym} {name}")
    print(f"  CA: {address}")
    print(f"  https://gmgn.ai/sol/token/{address}")

    # Current state
    print(f"\n  {'─' * 50}")
    print(f"  【当前状态】")
    if binance:
        print(f"  Binance实时价: {binance['price']:.8f}  |  市值: {fm(binance['mcap'])}")
        print(f"  24h高: {binance['price_high_24h']:.8f}  |  24h低: {binance['price_low_24h']:.8f}")
        print(f"  24h量: {fm(binance['volume_24h'])}  |  1h量: {fm(binance['volume_1h'])}")
        print(f"  1h涨跌: {fp(binance['percent_change_1h'], True)}  |  24h涨跌: {fp(binance['percent_change_24h'], True)}")
        print(f"  流动性: {fm(binance['liquidity'])}  |  持有人: {binance['holders']}")
    if token:
        print(f"  GMGN 市值: {fm(token['mcap'])}  |  ATH市值: {fm(token['ath_mcap'])}  |  持有人: {token['holders']}")
        print(f"  创建: {token['age_hours']:.1f}h前  |  Launchpad: {token['launchpad']}")

    # ATH analysis
    print(f"\n  {'─' * 50}")
    print(f"  【ATH分析】")
    if token:
        ath_ratio = token["ath_mcap"] / max(1, token["mcap"]) if token["mcap"] else 0
        print(f"  ATH市值: {fm(token['ath_mcap'])}  |  当前市值: {fm(token['mcap'])}")
        print(f"  ATH/当前: {ath_ratio:.1f}x  |  ATH价格: {token['ath_price']:.8f}")
    if watchlist:
        print(f"  Watchlist ATH: {fm(watchlist['ath'])}  |  Watchlist MCap: {fm(watchlist['mcap'])}")

    # Push history
    print(f"\n  {'─' * 50}")
    print(f"  【推送历史】共 {len(pushes)} 条")
    for i, p in enumerate(pushes):
        et = datetime.fromtimestamp(p["event_ts"], TZ).strftime("%m-%d %H:%M:%S")
        tags_str = " ".join(f"[{t}]" for t in p["risk_tags"]) if p["risk_tags"] else "无标签"
        print(f"  {i+1}. {et} | {p['signal_type']:<16s} | mcap={fm(p['mcap'])} ath={fm(p['ath_mcap'])} | "
              f"sig_pct={p['price_change_pct']:.1f}% | pool={fm(p['pool_liq'])}({p['pool_ratio']:.1%}) | {tags_str}")
        if p["narrative_desc"]:
            print(f"     叙事: {p['narrative_desc'][:100]}")

    # Recent snapshots
    signal_snaps = [s for s in snapshots if s["type"] != "watch"]
    print(f"\n  {'─' * 50}")
    print(f"  【扫描快照】共 {len(snapshots)} 条（最近30条），其中信号 {len(signal_snaps)} 条")
    if signal_snaps:
        latest_signals = signal_snaps[:10]
        for s in latest_signals:
            ts_str = s["ts"].astimezone(TZ).strftime("%m-%d %H:%M") if hasattr(s["ts"], 'astimezone') else str(s["ts"])
            print(f"  {ts_str} | {s['type']:<16s} | score={s['score']}")

    # K-line analysis
    print(f"\n  {'─' * 50}")
    print(f"  【K线分析】共 {len(candles)} 根")
    if candles:
        prices = [c["c"] for c in candles]
        highs = [c["h"] for c in candles]
        lows = [c["l"] for c in candles]
        vols = [c["v"] for c in candles]
        print(f"  当前价: {prices[-1]:.8f}  |  区间高: {max(highs):.8f}  |  区间低: {min(lows):.8f}")
        print(f"  区间涨幅: {fp((prices[-1]/prices[0]-1)*100, True)}  |  最高涨幅: {fp((max(highs)/prices[0]-1)*100, True)}")
        print(f"  均量: {fm(sum(vols)/len(vols))}  |  总量: {fm(sum(vols))}")
        # Volume trend
        first_half_vol = sum(vols[:len(vols)//2])
        second_half_vol = sum(vols[len(vols)//2:])
        vol_trend = "放量" if second_half_vol > first_half_vol * 1.3 else ("缩量" if second_half_vol < first_half_vol * 0.7 else "持平")
        print(f"  量能趋势: {vol_trend}  |  前半{fm(first_half_vol)} → 后半{fm(second_half_vol)}")
        # Recent 10 bars
        print(f"\n  最近10根K线:")
        print(f"  {'时间':<16} {'开盘':>12} {'最高':>12} {'最低':>12} {'收盘':>12} {'量':>10}")
        for c in candles[-10:]:
            ts = datetime.fromtimestamp(c["ts"], TZ).strftime("%m-%d %H:%M")
            print(f"  {ts:<16} {c['o']:>12.8f} {c['h']:>12.8f} {c['l']:>12.8f} {c['c']:>12.8f} {fm(c['v']):>10}")

    # Diagnosis
    print(f"\n  {'─' * 50}")
    print(f"  【综合诊断】")
    if diagnosis["tags"]:
        print(f"  风险标签: {' '.join(f'[{t}]' for t in diagnosis['tags'])}")
    else:
        print(f"  风险标签: 无 (健康)")
    for reason in diagnosis["reasons"]:
        print(f"    - {reason}")
    print(f"  ATH/mcap: {diagnosis['ath_ratio']:.1f}x")
    print(f"  推送次数: {diagnosis['push_count']}  |  信号快照: {diagnosis['signal_count']}  |  观察快照: {diagnosis['watch_count']}")
    print(f"  在观察池: {'是' if diagnosis['in_watchlist'] else '否'}")

    # Verdict
    tag_count = len(diagnosis["tags"])
    if tag_count == 0:
        verdict = "健康 [OK] - 无明显风险标签"
    elif tag_count == 1:
        verdict = f"轻度风险 [!] - {diagnosis['tags'][0]}"
    elif tag_count == 2:
        verdict = f"中度风险 [!!] - 成功率预计~47%"
    else:
        verdict = f"高风险 [XXX] - 成功率预计<20%"
    print(f"\n  >>> 判定: {verdict}")
    print(f"\n{'=' * 70}\n")


def main():
    parser = argparse.ArgumentParser(description="CA anomaly comprehensive analysis")
    parser.add_argument("address", help="Token contract address")
    parser.add_argument("--chain", default="sol")
    args = parser.parse_args()
    address = args.address.strip()

    print(f"正在分析 {address[:16]}...")

    # 1. GMGN token info
    print("[1/5] Fetching GMGN token info...")
    token = fetch_gmgn_token(address)

    # 2. Binance current data
    print("[2/5] Fetching Binance real-time data...")
    binance = fetch_binance_data(address)

    # 3. K-line
    print("[3/5] Fetching K-line data...")
    candles = fetch_kline(address, bars=60, resolution="5min")

    # 4. Push history
    print("[4/5] Querying push history & snapshots...")
    pushes, snapshots, watchlist = fetch_push_history(address)

    # 5. Diagnose
    print("[5/5] Running diagnosis...")
    diagnosis = diagnose(token, pushes, snapshots, watchlist, binance, candles)

    print_report(address, token, pushes, snapshots, watchlist, binance, candles, diagnosis)


if __name__ == "__main__":
    main()
