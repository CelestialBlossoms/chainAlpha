"""Entry check for a CA — uses push-time baseline, pre-push pump analysis."""
import sys,io,json,requests
sys.stdout=io.TextIOWrapper(sys.stdout.buffer,encoding='utf-8',errors='replace')
sys.path.insert(0,'.')
from db_client import db_op
from datetime import datetime,timezone

addr = sys.argv[1] if len(sys.argv)>1 else '54pHes6YaeL99S4eUVJxu6vhULZQsFcoDiCgHqt8pump'

# 1. Push record
def _op(conn):
    cur=conn.cursor()
    cur.execute("""SELECT symbol,signal_type,event_ts,current_mcap,ath_mcap,price_change_pct,
        liquidity,pool_mcap_ratio,age_sec FROM bottom_top100_push_records
        WHERE address=%s ORDER BY event_ts DESC LIMIT 1""",(addr,))
    cols=[d[0] for d in cur.description]
    rows=cur.fetchall()
    return dict(zip(cols,rows[0])) if rows else None
rec=db_op(_op)

if not rec:
    print('No push record for this CA')
    sys.exit(1)

ets=rec['event_ts']; rec_mcap=float(rec['current_mcap'])
ts_str=datetime.fromtimestamp(ets,tz=timezone.utc).strftime('%m-%d %H:%M UTC')
mins_ago=(datetime.now(timezone.utc).timestamp()-ets)/60
print(f'{rec["symbol"]} [{rec["signal_type"]}]  推送: {ts_str} ({mins_ago:.0f}min ago)')
print(f'推送MCap: ${rec_mcap:,.0f}  ATH: ${float(rec["ath_mcap"]):,.0f}  涨幅: {float(rec["price_change_pct"]):.1f}%')
print(f'Liq: ${float(rec["liquidity"]):,.0f}  PoolRatio: {float(rec["pool_mcap_ratio"]):.1%}  Age: {float(rec["age_sec"])/3600:.0f}h')

# 2. Fetch K-line
url='https://dquery.sintral.io/u-kline/v1/k-line/candles'
hdr={'Accept-Encoding':'identity','User-Agent':'binance-web3/1.1'}
def fetch(interval,limit):
    r=requests.get(url,params={'address':addr,'platform':'solana','interval':interval,'limit':limit},headers=hdr,timeout=15)
    bars=[]
    for b in r.json().get('data',[]):
        if len(b)<6: continue
        ts,o,h,l,c,v=int(b[5])//1000,float(b[0]),float(b[1]),float(b[2]),float(b[3]),float(b[4])
        if o>0: bars.append({'t':ts,'o':o,'h':h,'l':l,'c':c,'v':v})
    bars.sort(key=lambda x:x['t'])
    return bars

k5=fetch('5min',48); k1=fetch('1min',60)
pre5=[b for b in k5 if b['t']<=ets]; post5=[b for b in k5 if b['t']>ets]
pre1=[b for b in k1 if b['t']<=ets]; post1=[b for b in k1 if b['t']>ets]
print(f'Kline: 5m pre={len(pre5)} post={len(post5)}  1m pre={len(pre1)} post={len(post1)}')

# 3. Push-time baseline
bl = pre5[-1]['c'] if pre5 else (post5[0]['o'] if post5 else 0)
if bl <= 0: print('No valid baseline'); sys.exit(1)

# ---- Step A: Pre-push pump analysis ----
if pre5:
    pre_high=max(b['h'] for b in pre5); pre_low=min(b['l'] for b in pre5)
    pre_pump_pct=(pre_high-pre_low)/pre_low*100
    pullback=(bl-pre_high)/pre_high*100
    mcap_pre_low=rec_mcap*(pre_low/bl)
    mcap_pre_high=rec_mcap*(pre_high/bl)
    print(f'\n=== Step1: 推送前涨幅 (Pre-Push Pump) ===')
    print(f'Pre最低: MCap≈${mcap_pre_low:,.0f}   Pre最高: MCap≈${mcap_pre_high:,.0f}')
    print(f'推送前已涨: {pre_pump_pct:+.0f}%  (${mcap_pre_low:,.0f} → ${mcap_pre_high:,.0f})')
    status = '已从高点回落' if pullback<-5 else '正在高点附近'
    print(f'推送价 vs Pre高点: {pullback:+.1f}%  ({status})')
else:
    pre_pump_pct=0; pullback=0

# ---- Step B: Post-push change ----
pk=max(b['h'] for b in post5) if post5 else bl
tr=min(b['l'] for b in post5) if post5 else bl
cur=post5[-1]['c'] if post5 else bl
pk_pct=(pk-bl)/bl*100; tr_pct=(tr-bl)/bl*100; cur_pct=(cur-bl)/bl*100
post_tr_i=min(range(len(post5)),key=lambda i:post5[i]['l']) if post5 else 0
recovery=(cur-tr)/tr*100 if tr>0 else 0

print(f'\n=== Step2: 推送后变化 (Post-Push) ===')
print(f'推送MCap: ${rec_mcap:,.0f}')
print(f'Post峰值: {pk_pct:+.1f}% → MCap≈${rec_mcap*(1+pk_pct/100):,.0f}')
print(f'Post最低: {tr_pct:+.1f}% → MCap≈${rec_mcap*(1+tr_pct/100):,.0f}')
print(f'当前:     {cur_pct:+.1f}% → MCap≈${rec_mcap*(1+cur_pct/100):,.0f}')
print(f'底部反弹: {recovery:+.0f}%')

# ---- Step C: Combined view ----
print(f'\n=== Step3: 综合分析 ===')
total_pump=(cur-pre_low)/pre_low*100 if pre5 and pre_low>0 else 0
print(f'全程涨幅(Pre最低→当前): {total_pump:+.0f}%')
print(f'推送前已涨: {pre_pump_pct:+.0f}%  |  推送后变动: {cur_pct:+.1f}%')

# Drawdown zone from push
zone='轻度' if tr_pct>-20 else ('中度' if tr_pct>-50 else ('重度' if tr_pct>-80 else '极端'))
print(f'回撤(从推送价): {zone}({tr_pct:+.0f}%)')

# ---- Bar print ----
push_i=len(pre5)-1 if pre5 else -1
print(f'\n=== 5m K-line (push at bar {push_i}) ===')
for i,b in enumerate(k5):
    t=datetime.fromtimestamp(b['t'],tz=timezone.utc).strftime('%H:%M')
    body=b['c']-b['o']; bp=body/b['o']*100; d='+' if body>0 else '-'
    ref=(b['c']-bl)/bl*100
    m=' PUSH' if i==push_i else (' POST_TROUGH' if (post5 and i-push_i-1==post_tr_i) else (' POST_PEAK' if (post5 and i-push_i-1==max(range(len(post5)),key=lambda x:post5[x]['h'])) else ''))
    print(f'  [{i*5:3d}min] {t} {d}{abs(bp):.1f}% ref={ref:+.1f}% V=${b["v"]:,.0f}{m}')

# ---- 1m ----
if post1:
    bl1=post1[0]['o']
    print(f'\n=== 1m 最近10根 ===')
    for b in post1[:10]:
        t=datetime.fromtimestamp(b['t'],tz=timezone.utc).strftime('%H:%M:%S')
        body=b['c']-b['o']; bp=body/b['o']*100; d='+' if body>0 else '-'
        ref=(b['c']-bl1)/bl1*100
        print(f'  {t} {d}{abs(bp):.1f}% ref={ref:+.1f}% V=${b["v"]:,.0f}')
    if len(post1)>=5:
        chg5=(post1[4]['c']-bl)/bl*100
        chg30=(post1[29]['c']-bl)/bl*100 if len(post1)>=30 else 0
        post5v=sum(b['v'] for b in post1[:5])/5
        pre10v=sum(b['v'] for b in pre1[-10:])/10 if len(pre1)>=10 else 1
        vr=post5v/pre10v if pre10v>0 else 0
        print(f'5min: {chg5:+.1f}%  30min: {chg30:+.1f}%  VolRatio: {vr:.2f}x')

# ---- Historical ----
with open('data/deepseek_discovery/signal_kline_records.jsonl','r',encoding='utf-8',errors='replace') as f:
    hist=[json.loads(line) for line in f if line.strip()]

for zl,lo,hi in [('轻度',-20,999),('中度',-50,-20),('重度',-80,-50),('极端',-999,-80)]:
    if lo<tr_pct<=hi: zone=zl; break

g=[h for h in hist if lo<tr_pct<=hi and h['signal_type']==rec['signal_type']]
wr20=sum(1 for h in g if h['outcome']['wr20'])/max(len(g),1)*100 if g else 0
st=rec['signal_type']
print(f'\n历史同类({st}+{zone}回撤): n={len(g)}, WR20={wr20:.0f}%')

# ---- Verdict ----
print(f'\n=== 判断 ===')
signal=''
if pre_pump_pct>200: signal+=f'推送前已暴涨{pre_pump_pct:.0f}%,信号滞后! '
if pullback<-20: signal+=f'已从高点回落{abs(pullback):.0f}%,可能是回调买入机会 '
elif pullback>-5: signal+='推送在高点附近,追高风险 '
if rec['signal_type']=='new_revival' and tr_pct>-20: signal+='new_revival+轻度回撤→高质量 '
if rec_mcap<30000: signal+='市值过小(<30K),深度不足 '
elif rec_mcap>300000: signal+='大市值(>300K),弹性受限 '

if tr_pct>-20 and rec['signal_type']=='new_revival':
    print(f'轻度回撤+new_revival → 历史WR20={wr20:.0f}% → 可入场')
elif tr_pct>-20 and wr20>50:
    print(f'轻度回撤+WR20={wr20:.0f}% → 可入场')
elif tr_pct>-50 and post5 and cur_pct>0:
    print(f'中度回撤+已反弹至正 → 可轻仓试错')
elif tr_pct<-80:
    print(f'极端回撤 → WR20=25%, 不碰')
else:
    print(f'信号需确认 → {signal}')