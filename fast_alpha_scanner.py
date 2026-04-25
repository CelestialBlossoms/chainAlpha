import json
import subprocess
import time
import requests
from datetime import datetime
from db_client import db_op
from config import TG_BOT_TOKEN, TG_CHAT_ID, CHAINS

# ---------------------------------------------------------------------------
# 快速检索配置
# ---------------------------------------------------------------------------
CHECK_INTERVAL = 30  # 每 30 秒执行一次
TREND_INTERVALS = ["1m", "5m"]  # 监控 1 分钟和 5 分钟榜单

def run_command(cmd):
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, encoding='utf-8')
        return result.stdout if result.returncode == 0 else None
    except: return None

def send_tg_alert(msg):
    if not TG_BOT_TOKEN or "你的" in TG_BOT_TOKEN: 
        print(f"DEBUG: {msg}")
        return
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": TG_CHAT_ID, "text": msg, "parse_mode": "Markdown"}, timeout=10)
    except: pass

# ---------------------------------------------------------------------------
# 深度分析逻辑 (购买价值评估)
# ---------------------------------------------------------------------------
def analyze_buyability(chain, address):
    """
    深度分析一个代币是否值得购买。
    返回: (score, reasons, details)
    """
    score = 0
    reasons = []
    
    # 1. 获取基础与聪明钱信息
    info_cmd = f"gmgn-cli token info --chain {chain} --address {address} --raw"
    info_raw = run_command(info_cmd)
    if not info_raw: return 0, ["无法获取详情"], {}
    info = json.loads(info_raw)
    
    # 2. 获取安全数据
    sec_cmd = f"gmgn-cli token security --chain {chain} --address {address} --raw"
    sec_raw = run_command(sec_cmd)
    sec = json.loads(sec_raw) if sec_raw else {}
    
    # --- 评分逻辑 ---
    
    # A. 安全底线 (如果没通过，直接 0 分)
    if chain == "sol" and not sec.get("renounced_mint"):
        return -1, ["危险：Mint 权限未放弃"], info
    if sec.get("is_honeypot") == "yes":
        return -1, ["危险：检测到蜜罐"], info
        
    # B. 加分项
    sm_wallets = info.get("wallet_tags_stat", {}).get("smart_wallets", 0)
    if sm_wallets >= 1:
        score += 20
        reasons.append(f"聪明钱进场: {sm_wallets}个")
    if sm_wallets >= 5:
        score += 30
        reasons.append("聪明钱集群进场 (强信号)")

    # C. 流动性检查
    liq = float(info.get("liquidity", 0))
    if liq > 10000:
        score += 20
        reasons.append("流动性已达 $10k+")
    
    # D. 持仓分布
    top10_rate = float(sec.get("top_10_holder_rate", 1))
    if top10_rate < 0.25:
        score += 20
        reasons.append("筹码极度分散 (好信号)")
    elif top10_rate > 0.5:
        score -= 20
        reasons.append("筹码过于集中 (风险)")

    return score, reasons, info

# ---------------------------------------------------------------------------
# 主扫描程序
# ---------------------------------------------------------------------------
def scan_fast_trends():
    for chain in CHAINS:
        for interval in TREND_INTERVALS:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 正在检索 {chain} {interval} 榜单...")
            cmd = f"gmgn-cli market trending --chain {chain} --interval {interval} --limit 30 --raw"
            output = run_command(cmd)
            if not output: continue
            
            try:
                data = json.loads(output)
                tokens = data.get("data", {}).get("rank", [])
                
                for t in tokens:
                    addr = t.get("address")
                    symbol = t.get("symbol")
                    mcap = float(t.get("market_cap") or t.get("usd_market_cap") or 0)
                    
                    # 只有处于极早期阶段 ($50K - $1M) 的才进行深度分析
                    if 50000 <= mcap <= 1000000:
                        # 检查数据库防止重复分析
                        is_processed = not db_op(lambda conn: conn.cursor().execute("SELECT 1 FROM alpha_signals WHERE address=%s", (addr,)).fetchone() is None)
                        if is_processed: continue
                        
                        print(f"发现新锐代币: {symbol}，正在执行 Buyability 分析...")
                        score, reasons, info = analyze_buyability(chain, addr)
                        
                        if score >= 40: # 达到 40 分才推送
                            status_icon = "🟢 值得关注" if score >= 70 else "🟡 投机观察"
                            msg = (
                                f"{status_icon} (得分: {score})\n\n"
                                f"代币: ${symbol} ({info.get('name')})\n"
                                f"市值: ${mcap/1000:.1f}K | 链: {chain.upper()}\n"
                                f"信号源: Trending {interval}\n\n"
                                f"购买理由:\n- " + "\n- ".join(reasons) + "\n\n"
                                f"地址: `{addr}`\n"
                                f"[在 GMGN 中打开](https://gmgn.ai/{chain}/token/{addr})"
                            )
                            send_tg_alert(msg)
                            
                            # 存入数据库
                            db_op(lambda conn: conn.cursor().execute(
                                "INSERT INTO alpha_signals (address, chain, symbol, mcap_at_alert, milestone) VALUES (%s, %s, %s, %s, %s)",
                                (addr, chain, symbol, mcap, f"FastTrend_{interval}")
                            ))
                            print(f"  [发送] {symbol} 分数: {score}")
                        else:
                            print(f"  [忽略] {symbol} 分数过低或存在风险: {score}")
                            
            except Exception as e:
                print(f"处理出错: {e}")
                continue
            time.sleep(1) # 频率限制保护

if __name__ == "__main__":
    print("快速阿尔法分析器启动...")
    while True:
        scan_fast_trends()
        time.sleep(CHECK_INTERVAL)
