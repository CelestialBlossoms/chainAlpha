import json
import subprocess
import time
import requests
from datetime import datetime
from db_client import db_op
from config import DB_CONFIG, TG_BOT_TOKEN, TG_CHAT_ID, CHAINS, MILESTONES, SCAN_INTERVAL

# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------
def run_command(cmd):
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, encoding='utf-8')
        return result.stdout if result.returncode == 0 else None
    except: return None

def send_tg_alert(msg):
    if not TG_BOT_TOKEN or "你的" in TG_BOT_TOKEN: return
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": TG_CHAT_ID, "text": msg, "parse_mode": "Markdown"}, timeout=10)
    except: pass

# ---------------------------------------------------------------------------
# 检查逻辑
# ---------------------------------------------------------------------------
def perform_security_check(chain, address):
    """安全检查：返回是否安全"""
    cmd = f"gmgn-cli token security --chain {chain} --address {address} --raw"
    output = run_command(cmd)
    if not output: return False, "无法获取安全数据"
    
    data = json.loads(output)
    # 核心安全逻辑：Mint 权限必须丢掉
    if chain == "sol" and not data.get("renounced_mint"):
        return False, "Mint 权限未放弃"
    if data.get("is_honeypot") == "yes":
        return False, "检测到蜜罐"
    if float(data.get("rug_ratio", 0)) > 0.5:
        return False, f"高归零风险 ({data.get('rug_ratio')})"
        
    return True, "安全"

def get_smart_money_count(chain, address):
    """获取聪明钱持仓数量"""
    cmd = f"gmgn-cli token info --chain {chain} --address {address} --raw"
    output = run_command(cmd)
    if not output: return 0
    data = json.loads(output)
    return data.get("wallet_tags_stat", {}).get("smart_wallets", 0)

# ---------------------------------------------------------------------------
# 主逻辑
# ---------------------------------------------------------------------------
def process_milestones(chain, tokens):
    for t in tokens:
        # 解析基础数据
        addr = t.get("address")
        symbol = t.get("symbol")
        # 兼容不同接口的市值字段
        mcap = float(t.get("market_cap") or t.get("usd_market_cap") or 0)
        
        for name, config in MILESTONES.items():
            target = config["target"]
            low, high = target * (1 - config["range"]), target * (1 + config["range"])
            
            if low <= mcap <= high:
                # 检查数据库，防止重复推送
                is_new = db_op(lambda conn: conn.cursor().execute("SELECT 1 FROM alpha_signals WHERE address=%s", (addr,)).fetchone() is None)
                
                if is_new:
                    print(f"检测到潜在信号: {symbol} 在 {name} 阶段...")
                    # 1. 安全检查
                    is_safe, reason = perform_security_check(chain, addr)
                    if not is_safe:
                        print(f"  [跳过] 安全未通过: {reason}")
                        continue
                    
                    # 2. 聪明钱检查
                    sm_count = get_smart_money_count(chain, addr)
                    
                    # 3. 构造报警
                    msg = (
                        f"🚀 *Alpha Milestone Alert: {name}*\n\n"
                        f"代币: ${symbol}\n"
                        f"链: {chain.upper()}\n"
                        f"当前市值: ${mcap/1_000_000:.2f}M\n"
                        f"Smart Money: {sm_count} 个钱包持仓\n"
                        f"地址: `{addr}`\n\n"
                        f"[点击跳转 GMGN](https://gmgn.ai/{chain}/token/{addr})"
                    )
                    
                    send_tg_alert(msg)
                    # 存入数据库
                    db_op(lambda conn: conn.cursor().execute(
                        "INSERT INTO alpha_signals (address, chain, symbol, mcap_at_alert, milestone) VALUES (%s, %s, %s, %s, %s)",
                        (addr, chain, symbol, mcap, name)
                    ))
                    print(f"  [成功] 已发送报警并记录: {symbol}")

def scan():
    for chain in CHAINS:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 扫描 {chain} Trending...")
        # 获取热门榜单
        cmd = f"gmgn-cli market trending --chain {chain} --limit 50 --raw"
        output = run_command(cmd)
        if not output: continue
        
        try:
            data = json.loads(output)
            tokens = data.get("data", {}).get("rank", [])
            process_milestones(chain, tokens)
        except: continue

if __name__ == "__main__":
    print("阿尔法推送机器人已启动...")
    while True:
        scan()
        time.sleep(SCAN_INTERVAL)
