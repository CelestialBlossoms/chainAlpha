"""
DeepSeek-driven K-line pattern discovery pipeline.
Zero dependency on old strategy docs. DeepSeek analyzes raw 5m+1m data,
discovers patterns autonomously, names them, and writes the strategy doc.

Pipeline:
  Phase 1: Extract → data/signal_kline_records.jsonl
  Phase 2: Batch Discovery → DeepSeek finds patterns in batches of 30 signals
  Phase 3: Cross-Batch Synthesis → DeepSeek merges all patterns
  Phase 4: Document Generation → DeepSeek writes final strategy doc
"""
import sys, os, io, json, time, math
from pathlib import Path
from collections import defaultdict
from dataclasses import dataclass, field, asdict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import requests
from db_client import db_op
from bottom_detection.top100_push_record_store import ensure_top100_push_records_table

# ---- Config ----
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-v4-pro")
BATCH_SIZE = 30  # signals per DeepSeek call
DATA_DIR = ROOT / "data" / "deepseek_discovery"
OUTPUT_DIR = ROOT / "onchain_trading_guides"

# =========================================================================
#  PHASE 1: Extract all signal K-line records
# =========================================================================

def extract_all_signals():
    """Extract all new_revival + abnormal signals with their 5m+1m K-line data."""
    ensure_top100_push_records_table()

    # 1a. Get all signal records
    def _op(conn):
        cur = conn.cursor()
        cur.execute("""
            SELECT id, address, symbol, signal_type, event_ts,
                   current_mcap, ath_mcap, age_sec, price_change_pct,
                   liquidity, pool_mcap_ratio, extra
            FROM bottom_top100_push_records
            WHERE signal_type IN ('new_revival', 'abnormal')
              AND status NOT IN ('backfilled', 'db_only')
            ORDER BY event_ts
        """)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
    signals = db_op(_op) or []
    print(f"Phase 1: Found {len(signals)} signals ({sum(1 for s in signals if s['signal_type']=='new_revival')} new_revival, {sum(1 for s in signals if s['signal_type']=='abnormal')} abnormal)")

    # 1b. For each signal, fetch K-line data + compute outcome
    records = []
    for i, sig in enumerate(signals):
        addr = sig["address"]
        ets = sig["event_ts"]

        # Fetch 5m K-line (48 bars before + 48 after)
        def _k5(conn):
            cur = conn.cursor()
            cur.execute("""SELECT ts, open, high, low, close, volume
                FROM bottom_kline_cache WHERE address=%s AND resolution='5m'
                AND ts BETWEEN %s AND %s ORDER BY ts""",
                (addr, ets-14400, ets+14400))
            return [{"t":r[0],"o":float(r[1]),"h":float(r[2]),"l":float(r[3]),"c":float(r[4]),"v":float(r[5] or 0)} for r in cur]
        k5 = db_op(_k5) or []

        # Fetch 1m K-line (60 bars before + 120 after)
        def _k1(conn):
            cur = conn.cursor()
            cur.execute("""SELECT ts, open, high, low, close, volume
                FROM bottom_kline_cache_1m WHERE address=%s AND resolution='1m'
                AND ts BETWEEN %s AND %s ORDER BY ts""",
                (addr, ets-3600, ets+7200))
            return [{"t":r[0],"o":float(r[1]),"h":float(r[2]),"l":float(r[3]),"c":float(r[4]),"v":float(r[5] or 0)} for r in cur]
        k1 = db_op(_k1) or []

        if len(k5) < 24:  # need at least 2h of 5m data
            continue

        # Find push index
        push_5m = 0
        for j, b in enumerate(k5):
            if b["t"] > ets: push_5m = j; break

        push_1m = 0
        for j, b in enumerate(k1):
            if b["t"] > ets: push_1m = j; break

        # Compute outcome: peak, trough, WR20, WR50 from post-push 5m bars
        pre_bars = k5[:push_5m]
        post_bars = k5[push_5m:]
        baseline = pre_bars[-1]["c"] if pre_bars else (post_bars[0]["o"] if post_bars else 0)

        if baseline > 0 and len(post_bars) >= 4:
            peak = max(b["h"] for b in post_bars)
            trough = min(b["l"] for b in post_bars)
            peak_pct = (peak - baseline) / baseline * 100
            trough_pct = (trough - baseline) / baseline * 100
            final_pct = (post_bars[-1]["c"] - baseline) / baseline * 100

            # Hourly outcomes
            h1_peak = max(b["h"] for b in post_bars[:12]) if len(post_bars)>=12 else peak
            h1_pct = (h1_peak - baseline) / baseline * 100
            h4_peak = peak_pct
            wr20 = peak_pct >= 20
            wr50 = peak_pct >= 50
            wr100 = peak_pct >= 100
        else:
            peak_pct = trough_pct = final_pct = h1_pct = 0
            wr20 = wr50 = wr100 = False

        # Extract bar fingerprints (compact, for local similarity matching)
        fingerprints_5m = []
        for j, b in enumerate(k5):
            rel_pos = j - push_5m  # negative=before push, positive=after
            body = b["c"] - b["o"]
            body_pct = body / b["o"] * 100 if b["o"] > 0 else 0
            upper_wick = b["h"] - max(b["c"], b["o"])
            lower_wick = min(b["c"], b["o"]) - b["l"]
            total_range = b["h"] - b["l"]
            body_ratio = abs(body) / total_range if total_range > 0 else 0.5
            upper_ratio = upper_wick / max(abs(body), 1e-12)
            lower_ratio = lower_wick / max(abs(body), 1e-12)
            vol_vs_prev = b["v"] / k5[j-1]["v"] if j > 0 and k5[j-1]["v"] > 0 else 1.0

            fingerprints_5m.append({
                "rel": rel_pos,
                "body_pct": round(body_pct, 2),
                "dir": 1 if body > 0 else -1,
                "upper_ratio": round(min(upper_ratio, 10), 1),
                "lower_ratio": round(min(lower_ratio, 10), 1),
                "body_ratio": round(body_ratio, 2),
                "vol_x": round(min(vol_vs_prev, 20), 1),
                "v": round(b["v"], 2),
            })

        # Build record
        record = {
            "id": sig["id"],
            "symbol": sig["symbol"],
            "address": addr,
            "signal_type": sig["signal_type"],
            "event_ts": ets,
            "mcap": float(sig["current_mcap"] or 0),
            "ath_mcap": float(sig["ath_mcap"] or 0),
            "age_hours": float(sig["age_sec"] or 0) / 3600,
            "price_change_pct": float(sig["price_change_pct"] or 0),
            "liquidity": float(sig["liquidity"] or 0),
            "pool_mcap_ratio": float(sig["pool_mcap_ratio"] or 0),
            # K-line data
            "klines_5m": k5,
            "klines_1m": k1,
            "push_idx_5m": push_5m,
            "push_idx_1m": push_1m,
            "fingerprints_5m": fingerprints_5m,
            # Outcome
            "outcome": {
                "peak_pct": round(peak_pct, 1),
                "trough_pct": round(trough_pct, 1),
                "final_pct": round(final_pct, 1),
                "h1_peak_pct": round(h1_pct, 1),
                "wr20": wr20,
                "wr50": wr50,
                "wr100": wr100,
            }
        }
        records.append(record)

        if (i+1) % 50 == 0:
            print(f"  Extracted {i+1}/{len(signals)}...")

    # Save
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    out_path = DATA_DIR / "signal_kline_records.jsonl"
    with open(out_path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")

    print(f"Phase 1 done: {len(records)} valid records → {out_path}")
    print(f"  WR20 rate: {sum(1 for r in records if r['outcome']['wr20'])}/{len(records)} = {sum(1 for r in records if r['outcome']['wr20'])/max(len(records),1)*100:.0f}%")
    return records


# =========================================================================
#  PHASE 2: Batch Discovery via DeepSeek
# =========================================================================

DISCOVERY_SYSTEM_PROMPT = """You are an expert on-chain meme coin K-line pattern analyst. You will receive raw 5-minute and 1-minute candlestick data for a batch of trading signals. Each signal includes:

- 48 bars of 5m K-line BEFORE the push (4 hours), marking the pre-signal price structure
- 48 bars of 5m K-line AFTER the push (4 hours), showing the post-signal outcome
- 60 bars of 1m K-line BEFORE the push (1 hour), showing micro-structure before signal
- 120 bars of 1m K-line AFTER the push (2 hours), showing immediate reaction
- The ACTUAL outcome: peak%, trough%, final%, and whether WR20/WR50/WR100 was reached

Your task:
1. For each signal, identify the KEY bar-level patterns in the pre-push 5m data that characterize its structure
2. Group signals that share similar pre-push patterns
3. For each group, compute: count, WR20%, WR50%, avg peak%, avg trough%, the typical post-push journey
4. Name each group descriptively in Chinese (e.g., "恐慌投降后V反型", "缩量阴跌无抵抗型")
5. Identify "death patterns" — pre-push bar sequences that reliably predict failure (peak<20%)
6. Identify "golden patterns" — pre-push bar sequences that reliably predict success (peak>=50%)

Output a JSON object with this structure:
{
  "batch_summary": {
    "total_signals": N,
    "new_revival_count": N,
    "abnormal_count": N,
    "overall_wr20": X.X,
    "overall_wr50": X.X
  },
  "pattern_groups": [
    {
      "name": "恐慌投降后V反型",
      "description": "Detailed description of the pre-push bar sequence that defines this pattern...",
      "key_fingerprints": ["Bar[-X]: ...", "Bar[-Y]: ..."],
      "signal_count": N,
      "new_revival_count": N,
      "abnormal_count": N,
      "wr20": X.X,
      "wr50": X.X,
      "avg_peak_pct": X.X,
      "avg_trough_pct": X.X,
      "typical_post_journey": "Describe the typical post-push 5m bar sequence...",
      "entry_rule": "When to enter based on post-push confirmation...",
      "stop_loss_rule": "Where to place stop loss...",
      "confidence": "high|medium|low"
    }
  ],
  "death_patterns": [
    {
      "name": "...",
      "pre_push_characteristics": "...",
      "signal_count": N,
      "wr20": X.X
    }
  ],
  "golden_patterns": [
    {
      "name": "...",
      "entry_bar_sequence": "...",
      "signal_count": N,
      "wr50": X.X
    }
  ],
  "discovery_notes": "Any other interesting findings from this batch..."
}

IMPORTANT:
- Base ALL findings on the actual bar-by-bar data provided. Do not hallucinate patterns.
- If a pattern has fewer than 3 signals, mark confidence as "low".
- Use Chinese for all descriptions and names.
- Be specific about WHICH bars matter (e.g., "推送前第6-3根5m K线连续缩量小阳" not "推送前一段横盘")."""


def format_signal_for_deepseek(rec: dict) -> str:
    """Format a single signal's K-line data as compact text for DeepSeek analysis."""
    lines = []
    lines.append(f"--- Signal: {rec['symbol']} [{rec['signal_type']}] MCap=${rec['mcap']:,.0f} Age={rec['age_hours']:.0f}h ---")

    k5 = rec["klines_5m"]
    push_5m = rec["push_idx_5m"]

    # Pre-push 5m bars with annotations
    lines.append(f"\n5m K-line PRE-push ({push_5m} bars = {push_5m*5}min):")
    for j in range(max(0, push_5m-48), push_5m):
        b = k5[j]
        rel = j - push_5m
        body = b["c"] - b["o"]
        body_pct = body / b["o"] * 100 if b["o"] > 0 else 0
        uw = b["h"] - max(b["c"], b["o"])
        lw = min(b["c"], b["o"]) - b["l"]
        tr = b["h"] - b["l"]
        body_r = abs(body) / tr if tr > 0 else 0.5
        uw_r = uw / max(abs(body), 1e-12) if abs(body) > 1e-12 else (1 if uw > 0 else 0)
        lw_r = lw / max(abs(body), 1e-12) if abs(body) > 1e-12 else (1 if lw > 0 else 0)

        annotations = []
        if uw_r > 2: annotations.append("长上影")
        if lw_r > 2: annotations.append("长下影")
        if body_r < 0.3: annotations.append("十字星")
        if body_r > 0.8: annotations.append("光头光脚")
        if j > 0 and k5[j-1]["v"] > 0 and b["v"] / k5[j-1]["v"] > 3:
            annotations.append(f"量增{b['v']/k5[j-1]['v']:.0f}x")

        dir_sym = "阳" if body > 0 else "阴"
        ann_str = (" [" + ",".join(annotations) + "]") if annotations else ""
        lines.append(f"  [{rel:4d}] {dir_sym} {body_pct:+.1f}% V=${b['v']:.0f}{ann_str}")

    # Post-push 5m bars
    post_bars = k5[push_5m:push_5m+48]
    lines.append(f"\n5m K-line POST-push ({len(post_bars)} bars):")
    for j, b in enumerate(post_bars):
        body = b["c"] - b["o"]
        body_pct = body / b["o"] * 100 if b["o"] > 0 else 0
        dir_sym = "阳" if body > 0 else "阴"
        lines.append(f"  [+{j*5:4d}min] {dir_sym} {body_pct:+.1f}% V=${b['v']:.0f}")

    # Pre-push 1m bars (last 30 bars = 30min)
    k1 = rec["klines_1m"]
    push_1m = rec["push_idx_1m"]
    pre_1m = k1[max(0, push_1m-30):push_1m]
    if pre_1m:
        lines.append(f"\n1m K-line PRE-push (last {len(pre_1m)} bars):")
        for j, b in enumerate(pre_1m):
            rel = j - len(pre_1m)
            body = b["c"] - b["o"]
            body_pct = body / b["o"] * 100 if b["o"] > 0 else 0
            dir_sym = "阳" if body > 0 else "阴"
            lines.append(f"  [{rel:4d}min] {dir_sym} {body_pct:+.1f}% V=${b['v']:.0f}")

    # Post-push 1m bars (first 60 bars)
    post_1m = k1[push_1m:push_1m+60]
    if post_1m:
        lines.append(f"\n1m K-line POST-push (first {len(post_1m)} bars):")
        for j, b in enumerate(post_1m):
            body = b["c"] - b["o"]
            body_pct = body / b["o"] * 100 if b["o"] > 0 else 0
            dir_sym = "阳" if body > 0 else "阴"
            lines.append(f"  [+{j+1:4d}min] {dir_sym} {body_pct:+.1f}% V=${b['v']:.0f}")

    # Outcome
    o = rec["outcome"]
    lines.append(f"\nOUTCOME: Peak={o['peak_pct']:+.1f}% Trough={o['trough_pct']:+.1f}% Final={o['final_pct']:+.1f}% WR20={'YES' if o['wr20'] else 'NO'} WR50={'YES' if o['wr50'] else 'NO'} WR100={'YES' if o['wr100'] else 'NO'}")

    return "\n".join(lines)


def call_deepseek(system_prompt: str, user_content: str, timeout: int = 120) -> dict | None:
    """Call DeepSeek API, return parsed JSON response."""
    if not DEEPSEEK_API_KEY:
        print("  ERROR: DEEPSEEK_API_KEY not set")
        return None

    body = {
        "model": DEEPSEEK_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
        "stream": False,
        "temperature": 0.1,
        "max_tokens": 8192,
        "response_format": {"type": "json_object"},
    }

    for attempt in range(3):
        try:
            resp = requests.post(
                f"{DEEPSEEK_BASE_URL}/chat/completions",
                headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"},
                json=body, timeout=timeout,
            )
            if resp.ok:
                data = resp.json()
                content = data["choices"][0]["message"]["content"]
                content = content.strip()
                if content.startswith("```"):
                    lines = content.split("\n")
                    content = "\n".join(lines[1:-1] if lines[-1].strip()=="```" else lines[1:])
                return json.loads(content)
            else:
                print(f"  API error (attempt {attempt+1}): {resp.status_code} {resp.text[:200]}")
        except Exception as e:
            print(f"  Request failed (attempt {attempt+1}): {e}")
        if attempt < 2:
            time.sleep(2 * (attempt + 1))
    return None


def run_batch_discovery(records: list, batch_size: int = BATCH_SIZE) -> list[dict]:
    """Split records into batches, send each to DeepSeek for pattern discovery."""
    all_results = []
    batches = [records[i:i+batch_size] for i in range(0, len(records), batch_size)]
    print(f"\nPhase 2: {len(batches)} batches of up to {batch_size} signals each")

    for bi, batch in enumerate(batches):
        t0 = time.time()
        print(f"\n  Batch {bi+1}/{len(batches)}: {len(batch)} signals...", flush=True)

        # Format each signal
        user_parts = []
        for rec in batch:
            user_parts.append(format_signal_for_deepseek(rec))

        batch_prompt = f"""Analyze the following {len(batch)} meme coin trading signals. Each includes raw 5m+1m K-line data before and after the push event, plus the actual outcome.

For each signal, examine the bar-by-bar patterns in the PRE-push 5m data. Look at:
- Individual bar characteristics: body size, wick ratios, volume spikes
- Bar sequences: consecutive bullish/bearish bars, volume trends (increasing/decreasing)
- Key turning points: capitulation bars (big body + extreme volume), doji clusters (indecision), engulfing patterns
- The transition from pre-push to post-push: does the first post-push bar confirm or contradict the pattern?

{chr(10).join(user_parts)}

Output your pattern discovery as a JSON object following the structure specified in the system prompt."""

        print(f"    Sending to DeepSeek ({len(batch_prompt)} chars)...", flush=True)
        result = call_deepseek(DISCOVERY_SYSTEM_PROMPT, batch_prompt, timeout=300)
        elapsed = time.time() - t0
        if result:
            result["batch_index"] = bi
            result["batch_size"] = len(batch)
            all_results.append(result)
            n_pats = len(result.get('pattern_groups',[]))
            n_deaths = len(result.get('death_patterns',[]))
            n_golds = len(result.get('golden_patterns',[]))
            print(f"    Done in {elapsed:.0f}s | {n_pats} patterns, {n_deaths} deaths, {n_golds} goldens", flush=True)
        else:
            print(f"    FAILED after {elapsed:.0f}s - skipping batch", flush=True)

        # Save intermediate results
        inter_path = DATA_DIR / f"batch_{bi:03d}_result.json"
        with open(inter_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"    Saved to {inter_path.name}", flush=True)

        # Rate limiting
        if bi < len(batches) - 1:
            time.sleep(2)

    return all_results


# =========================================================================
#  PHASE 3: Cross-Batch Synthesis
# =========================================================================

SYNTHESIS_PROMPT = """You are synthesizing pattern discoveries from multiple independent batches of meme coin K-line analysis.

You will receive discovery results from {N} batches, each analyzing ~{batch_size} trading signals.
Each batch independently discovered patterns, death signals, and golden signals.

Your task:
1. MERGE similar patterns across batches — if two batches found essentially the same pattern but named it differently, unify them under one name
2. For each unified pattern, combine the statistics: total signal count, WR20%, WR50%, avg peak%
3. Rank patterns by: (a) frequency (how common), (b) reliability (WR20%), (c) profitability (avg peak%)
4. Identify the TOP 10 most important patterns that cover the majority of signals
5. Identify universal rules that hold across ALL patterns (e.g., "any pattern with a capitulation bar in the last 30min has WR20>70%")
6. Create a decision tree: if pre-push shows pattern X → check post-push bar Y → action Z

Output a JSON object:
{
  "total_signals_analyzed": N,
  "total_batches": N,
  "unified_patterns": [
    {
      "rank": 1,
      "name": "中文模式名",
      "aliases": ["其他批次的名字"],
      "description": "详细的bar级别特征描述",
      "pre_push_fingerprint": ["Bar[-X]: ...", "Bar[-Y]: ..."],
      "total_count": N,
      "new_revival_count": N,
      "abnormal_count": N,
      "wr20": X.XX,
      "wr50": X.XX,
      "wr100": X.XX,
      "avg_peak_pct": X.X,
      "avg_trough_pct": X.X,
      "median_peak_time_bars": N,
      "typical_post_5m_journey": "Bar-by-bar description of what typically happens after push...",
      "typical_post_1m_journey": "Bar-by-bar description of the 1m reaction...",
      "entry_strategy": {
        "when": "描述何时入场",
        "confirmation_bars": "需要哪些确认bar",
        "position_sizing": "建议仓位",
        "take_profit": [{"pct": 30, "close_ratio": 0.5}, ...],
        "stop_loss_pct": -XX
      },
      "risk_flags": ["什么情况下这个模式会失败"],
      "confidence": "high|medium|low"
    }
  ],
  "universal_rules": [
    "规则1: ...",
    "规则2: ..."
  ],
  "decision_tree": {
    "step1": "先看推送前最后30min是否有投降Bar(body<-8%+量增>5x)...",
    "step2": "...",
    "...": "..."
  },
  "death_patterns_universal": [
    {"characteristics": "...", "wr20": X.XX, "explanation": "为什么必死"}
  ],
  "golden_patterns_universal": [
    {"characteristics": "...", "wr50": X.XX, "explanation": "为什么是金矿"}
  ]
}

Use Chinese for all names, descriptions, and rules."""


def run_cross_batch_synthesis(all_results: list[dict]) -> dict:
    """Send all batch results to DeepSeek for cross-batch synthesis."""
    print(f"\nPhase 3: Cross-batch synthesis of {len(all_results)} batch results...")

    # Prepare input: summary of each batch's findings
    batch_summaries = []
    for br in all_results:
        summary = {
            "batch_index": br.get("batch_index"),
            "total_signals": br.get("batch_summary", {}).get("total_signals", 0),
            "overall_wr20": br.get("batch_summary", {}).get("overall_wr20", 0),
            "pattern_groups": [],
            "death_patterns": [],
            "golden_patterns": [],
        }
        for pg in br.get("pattern_groups", []):
            summary["pattern_groups"].append({
                "name": pg.get("name"),
                "count": pg.get("signal_count"),
                "wr20": pg.get("wr20"),
                "wr50": pg.get("wr50"),
                "avg_peak": pg.get("avg_peak_pct"),
                "key_fingerprints": pg.get("key_fingerprints", []),
            })
        for dp in br.get("death_patterns", []):
            summary["death_patterns"].append({
                "name": dp.get("name"),
                "count": dp.get("signal_count"),
                "wr20": dp.get("wr20"),
            })
        for gp in br.get("golden_patterns", []):
            summary["golden_patterns"].append({
                "name": gp.get("name"),
                "count": gp.get("signal_count"),
                "wr50": gp.get("wr50"),
            })
        batch_summaries.append(summary)

    user_prompt = f"""Synthesize pattern discoveries from {len(all_results)} independent analysis batches.

Batch summaries:
{json.dumps(batch_summaries, ensure_ascii=False, indent=2)}

Please merge similar patterns, rank them, and produce the unified pattern catalog."""

    result = call_deepseek(SYNTHESIS_PROMPT.format(N=len(all_results), batch_size=BATCH_SIZE),
                           user_prompt, timeout=180)

    if result:
        synth_path = DATA_DIR / "synthesis_result.json"
        with open(synth_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"  Synthesis complete → {synth_path}")

    return result


# =========================================================================
#  PHASE 4: Document Generation
# =========================================================================

DOC_GENERATION_PROMPT = """You are writing the definitive strategy document for on-chain meme coin trading based on K-line pattern analysis.

You have analyzed {total_signals} trading signals ({new_revival} new_revival + {abnormal} abnormal) with raw 5-minute and 1-minute candlestick data surrounding each push event. Your pattern discovery pipeline identified recurring bar-level patterns with statistically validated win rates.

Now write TWO documents in Chinese:

## Document 1: 08-5m-fingerprint-encyclopedia.md
The complete encyclopedia of 5m+1m bar-level patterns. Structure:
- 术语速查 (terminology reference)
- 前置模式总览 (overview of all pre-push patterns, ranked by frequency)
- 每种前置模式的详细分析: bar级别指纹特征、后4h分叉概率、典型1m微观结构、盈亏比
- 后4h走势独立胜率 (post-4h walk-off win rates)
- 1m+5m联合决策矩阵
- 实战操作流程

## Document 2: 09-bar-level-strategy.md
The trading strategy derived from bar-level patterns. Structure:
- 核心发现 (key statistical findings)
- 入场决策规则 (entry rules based on specific bar confirmations)
- 死亡模式识别 (how to identify death patterns from pre-push bars)
- 黄金模式识别 (how to identify golden patterns)
- 仓位管理和止盈止损
- 各市值段差异

Use the synthesis results below as the foundation. Be SPECIFIC — reference exact bar positions (e.g., "推送前第6根bar" not "推送前一段").
Include ALL statistical numbers from the data.

Synthesis results:
{synthesis_json}

Output a JSON object:
{{
  "doc_08": "Full markdown content of 08-5m-fingerprint-encyclopedia.md",
  "doc_09": "Full markdown content of 09-bar-level-strategy.md"
}}"""


def run_document_generation(synthesis: dict, records: list) -> dict:
    """Generate final strategy documents from synthesis results."""
    print(f"\nPhase 4: Generating final strategy documents...")

    n_new_revival = sum(1 for r in records if r["signal_type"] == "new_revival")
    n_abnormal = sum(1 for r in records if r["signal_type"] == "abnormal")

    user_prompt = DOC_GENERATION_PROMPT.format(
        total_signals=len(records),
        new_revival=n_new_revival,
        abnormal=n_abnormal,
        synthesis_json=json.dumps(synthesis, ensure_ascii=False, indent=2),
    )

    system_prompt = """You are an expert trading strategy document writer specializing in on-chain meme coin analysis. Write in Chinese. Be specific, data-driven, and actionable. Every claim must be backed by the statistics in the synthesis data. Use exact bar positions and percentages."""

    result = call_deepseek(system_prompt, user_prompt, timeout=300)

    if result:
        # Write the two documents
        for key, filename in [("doc_08", "08-5m-fingerprint-encyclopedia.md"),
                               ("doc_09", "09-bar-level-strategy.md")]:
            content = result.get(key, "")
            if content:
                out_path = OUTPUT_DIR / filename
                with open(out_path, "w", encoding="utf-8") as f:
                    f.write(content)
                print(f"  Written: {out_path} ({len(content)} chars)")

    return result


# =========================================================================
#  MAIN
# =========================================================================

def main():
    import argparse
    parser = argparse.ArgumentParser(description="DeepSeek K-line pattern discovery pipeline")
    parser.add_argument("--phase", choices=["1","2","3","4","all"], default="all",
                       help="Which phase to run")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE,
                       help=f"Signals per DeepSeek batch (default {BATCH_SIZE})")
    parser.add_argument("--limit", type=int, default=0,
                       help="Limit number of signals (for testing)")
    parser.add_argument("--skip-extraction", action="store_true",
                       help="Skip Phase 1, load existing records")
    args = parser.parse_args()

    # Phase 1: Extract
    if args.phase in ("1", "all") and not args.skip_extraction:
        records = extract_all_signals()
    else:
        rec_path = DATA_DIR / "signal_kline_records.jsonl"
        if rec_path.exists():
            records = []
            with open(rec_path, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        records.append(json.loads(line))
            print(f"Loaded {len(records)} existing records from {rec_path}")
        else:
            print("No existing records found, running Phase 1...")
            records = extract_all_signals()

    if args.limit > 0:
        records = records[:args.limit]
        print(f"Limited to {len(records)} signals")

    if not records:
        print("No records to analyze. Exiting.")
        return

    batch_size = args.batch_size

    # Phase 2: Batch Discovery
    if args.phase in ("2", "all"):
        all_results = run_batch_discovery(records, batch_size)
        results_path = DATA_DIR / "all_batch_results.json"
        with open(results_path, "w", encoding="utf-8") as f:
            json.dump(all_results, f, ensure_ascii=False, indent=2)
        print(f"\nPhase 2 done: {len(all_results)}/{math.ceil(len(records)/batch_size)} batches → {results_path}")
    else:
        results_path = DATA_DIR / "all_batch_results.json"
        if results_path.exists():
            with open(results_path, "r", encoding="utf-8") as f:
                all_results = json.load(f)
            print(f"Loaded {len(all_results)} existing batch results")
        else:
            print("No batch results found. Run Phase 2 first.")
            return

    # Phase 3: Synthesis
    if args.phase in ("3", "all"):
        synthesis = run_cross_batch_synthesis(all_results)
    else:
        synth_path = DATA_DIR / "synthesis_result.json"
        if synth_path.exists():
            with open(synth_path, "r", encoding="utf-8") as f:
                synthesis = json.load(f)
            print(f"Loaded existing synthesis")
        else:
            print("No synthesis found. Run Phase 3 first.")
            return

    # Phase 4: Document Generation
    if args.phase in ("4", "all"):
        docs = run_document_generation(synthesis, records)
        print(f"\nPhase 4 done: Documents written to {OUTPUT_DIR}/")
    else:
        print(f"\nPipeline complete. Run Phase 4 to generate final documents.")

    print(f"\nAll data in: {DATA_DIR}/")


if __name__ == "__main__":
    main()
