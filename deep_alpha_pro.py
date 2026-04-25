import json
import subprocess
import time
import requests
from datetime import datetime
from collections import defaultdict
from db_client import db_op
from config import TG_BOT_TOKEN, TG_CHAT_ID, CHAINS

# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------
CHECK_INTERVAL = 45 
TREND_INTERVALS = ["1m", "5m"]

def run_command(cmd):
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, encoding='utf-8')
        return result.stdout if result.returncode == 0 else None
    except: return None

def send_tg_alert(msg):
    if not TG_BOT_TOKEN or "你的" in TG_BOT_TOKEN: 
        print(f"--- TG ALERT ---\n{msg}\n----------------")
        return
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": TG_CHAT_ID, "text": msg, "parse_mode": "Markdown"}, timeout=15)
    except: pass

# ---------------------------------------------------------------------------
# 关联性与控盘砸盘深度分析
# ---------------------------------------------------------------------------
def analyze_control_and_dump(holders_list):
    """
    通过前100钱包分析资金关联、控盘与砸盘
    """
    if not holders_list: return {}

    # 1. 聚类分析 (基于进场时间)
    time_clusters = defaultdict(list)
    total_supply_scanned = 0
    
    associated_supply = 0
    associated_count = 0
    sold_supply_from_clusters = 0
    
    for h in holders_list:
        addr = h.get("address")
        supply_pct = float(h.get("amount_percentage", 0)) * 100
        sell_pct = float(h.get("sell_amount_percentage", 0))
        buy_ts = h.get("start_holding_at", 0)
        tags = h.get("maker_token_tags", [])
        
        # 核心关联逻辑 A：官方标记的捆绑包或老鼠仓
        is_labeled_associated = "bundler" in tags or "rat_trader" in tags
        
        # 核心关联逻辑 B：时间聚类 (5秒内进场视为疑似关联)
        # 将时间戳规整到 5 秒区间
        time_key = buy_ts // 5 
        time_clusters[time_key].append(h)
        
        if is_labeled_associated:
            associated_supply += supply_pct
            associated_count += 1
            sold_supply_from_clusters += supply_pct * sell_pct

    # 找出最大的时间聚类 (疑似隐藏庄家)
    max_cluster_size = 0
    cluster_supply = 0
    for ts, hs in time_clusters.items():
        if len(hs) >= 3: # 超过或等于 3 个钱包在 5 秒内同步买入
            c_supply = sum(float(x.get("amount_percentage", 0)) * 100 for x in hs)
            if c_supply > cluster_supply:
                cluster_supply = c_supply
                max_cluster_size = len(hs)

    # 综合评估
    # 控盘率 = 已标记关联 + 时间聚类关联 (去重后的估算)
    control_ratio = max(associated_supply, cluster_supply)
    
    # 砸盘进度 = 关联钱包已卖出的比例
    # 粗略估算：如果关联钱包卖出比例 > 10% 则视为开始砸盘
    dump_progress = (sold_supply_from_clusters / associated_supply * 100) if associated_supply > 0 else 0

    return {
        "control_ratio": control_ratio,
        "cluster_size": max_cluster_size,
        "dump_progress": dump_progress,
        "verdict": "砸盘中" if dump_progress > 20 else ("高度控盘" if control_ratio > 40 else "筹码分散")
    }

def perform_deep_analysis(chain, address):
    # 1. 获取基本信息
    info_raw = run_command(f"gmgn-cli token info --chain {chain} --address {address} --raw")
    if not info_raw: return None
    info = json.loads(info_raw)
    
    # 2. 获取安全数据
    sec_raw = run_command(f"gmgn-cli token security --chain {chain} --address {address} --raw")
    sec = json.loads(sec_raw) if sec_raw else {}
    
    # 3. 获取前100持币者
    holders_raw = run_command(f"gmgn-cli token holders --chain {chain} --address {address} --limit 100 --raw")
    holders_data = json.loads(holders_raw) if holders_raw else {"list": []}
    holders_list = holders_data.get("list", [])
    
    # 执行筹码关联分析
    ctrl = analyze_control_and_dump(holders_list)
    
    # 组装数据
    stats = {
        "symbol": info.get("symbol"),
        "mcap": float(info.get("price", 0)) * float(info.get("circulating_supply", 0)),
        "sm_count": info.get("wallet_tags_stat", {}).get("smart_wallets", 0),
        "kol_count": info.get("wallet_tags_stat", {}).get("renowned_wallets", 0),
        "top10_rate": float(sec.get("top_10_holder_rate", 0)) * 100,
        "snipers": sec.get("sniper_count", 0),
        "rug_ratio": sec.get("rug_ratio", "0"),
        "control_ratio": ctrl.get("control_ratio", 0),
        "dump_progress": ctrl.get("dump_progress", 0),
        "cluster_size": ctrl.get("cluster_size", 0),
        "verdict": ctrl.get("verdict", "未知")
    }
    return stats

# ---------------------------------------------------------------------------
# 扫描主循环
# ---------------------------------------------------------------------------
def scan_pro():
    for chain in CHAINS:
        for interval in TREND_INTERVALS:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 扫描 {chain} {interval} 筹码信号...")
            output = run_command(f"gmgn-cli market trending --chain {chain} --interval {interval} --limit 15 --raw")
            if not output: continue
            
            try:
                data = json.loads(output)
                tokens = data.get("data", {}).get("rank", [])
                
                for t in tokens:
                    addr = t.get("address")
                    
                    def check_exists(conn):
                        cur = conn.cursor()
                        cur.execute("SELECT 1 FROM alpha_signals WHERE address=%s", (addr,))
                        return cur.fetchone() is not None
                    
                    if db_op(check_exists):
                        continue
                        
                    print(f"深度透视: {t.get('symbol')}...")
                    s = perform_deep_analysis(chain, addr)
                    if not s: continue
                    
                    # 警报逻辑：如果发现高度控盘或者是潜在砸盘，立即通知
                    if s["control_ratio"] > 30 or s["sm_count"] >= 3:
                        
                        alert_icon = "🛑" if "砸盘" in s["verdict"] else ("🟡" if s["control_ratio"] > 50 else "🟢")
                        
                        msg = (
                            f"{alert_icon} *筹码关联性报警* | ${s['symbol']}\n"
                            f"市值: ${s['mcap']/1000:.1f}K | 状态: {s['verdict']}\n\n"
                            f"🧬 *资金关联分析 (Top 100)*\n"
                            f"- 疑似关联总控盘: {s['control_ratio']:.1f}%\n"
                            f"- 最大同频率进场集群: {s['cluster_size']} 个钱包\n"
                            f"- 庄家出货进度: {s['dump_progress']:.1f}%\n\n"
                            f"📊 *基础结构*\n"
                            f"- Top 10 持仓: {s['top10_rate']:.1f}%\n"
                            f"- 狙击手数量: {s['snipers']}\n"
                            f"- 风险分数: {s['rug_ratio']}\n\n"
                            f"👥 *共识强度*\n"
                            f"- Smart Money: {s['sm_count']} | KOL: {s['kol_count']}\n\n"
                            f"地址: `{addr}`\n"
                            f"[在 GMGN 查看关联图谱](https://gmgn.ai/{chain}/token/{addr})"
                        )
                        send_tg_alert(msg)
                        
                        db_op(lambda conn: conn.cursor().execute(
                            "INSERT INTO alpha_signals (address, chain, symbol, mcap_at_alert, milestone) VALUES (%s, %s, %s, %s, %s)",
                            (addr, chain, s['symbol'], s['mcap'], f"DeepControl_{interval}")
                        ))
                    
            except Exception as e:
                print(f"Loop Error: {e}")
            time.sleep(2)

if __name__ == "__main__":
    print("深度关联分析机器人已启动...")
    while True:
        scan_pro()
        time.sleep(CHECK_INTERVAL)
