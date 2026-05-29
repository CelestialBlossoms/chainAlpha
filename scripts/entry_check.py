"""Quick entry check for a CA against historical drawdown recovery patterns."""
import sys,io,json,requests
sys.stdout=io.TextIOWrapper(sys.stdout.buffer,encoding='utf-8',errors='replace')
sys.path.insert(0,'.')
from db_client import db_op
from datetime import datetime,timezone

addr = sys.argv[1] if len(sys.argv)>1 else 'CBi3Cm3XVbSeKuvi4qTZaZpLVZ84CN8TMLMgpJVPpump'

# 1. Push records
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
    print('No push record found for this CA')
    sys.exit(1)

ets=rec['event_ts']
ts_str=datetime.fromtimestamp(ets,tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
mins_ago=(datetime.now(timezone.utc).timestamp()-ets)/60
print(f'Symbol: {rec["symbol"]}  [{rec["signal_type"]}]')
print(f'Pushed: {ts_str}  ({mins_ago:.0f}min ago)')
print(f'MCap: ${float(rec["current_mcap"]):,.0f}  ATH: ${float(rec["ath_mcap"]):,.0f}')
print(f'Liq: ${float(rec["liquidity"]):,.0f}  PoolRatio: {float(rec["pool_mcap_ratio"]):.1%}')
print(f'PriceChg: {float(rec["price_change_pct"]):.1f}%  Age: {float(rec["age_sec"])/3600:.0f}h')

# 2. Fetch latest K-line from Binance API
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

bars5=fetch('5min',48); bars1=fetch('1min',60)
pre5=[b for b in bars5 if b['t']<=ets]; post5=[b for b in bars5 if b['t']>ets]
pre1=[b for b in bars1 if b['t']<=ets]; post1=[b for b in bars1 if b['t']>ets]
print(f'Kline: 5m pre={len(pre5)} post={len(post5)}  1m pre={len(pre1)} post={len(post1)}')

if not post5:
    print('No post-push K-line data yet')
    sys.exit(0)

bl=pre5[-1]['c'] if pre5 else post5[0]['o']
peak=max(b['h'] for b in post5)
trough=min(b['l'] for b in post5)
cur=post5[-1]['c']
peak_pct=(peak-bl)/bl*100; trough_pct=(trough-bl)/bl*100; cur_pct=(cur-bl)/bl*100
peak_i=max(range(len(post5)),key=lambda i:post5[i]['h'])
trough_i=min(range(len(post5)),key=lambda i:post5[i]['l'])
dd_from_peak=(cur-peak)/peak*100 if peak>0 else 0
recovery=(cur-trough)/trough*100 if trough>0 else 0

print(f'\n=== Post-Push 5m ({len(post5)}bars={len(post5)*5}min) ===')
print(f'Peak: {peak_pct:+.1f}% (t+{peak_i*5}min)  Trough: {trough_pct:+.1f}% (t+{trough_i*5}min)')
print(f'Current: {cur_pct:+.1f}%  Peak回撤: {dd_from_peak:.1f}%  底部反弹: {recovery:+.1f}%')

for i,b in enumerate(post5):
    t=datetime.fromtimestamp(b['t'],tz=timezone.utc).strftime('%H:%M')
    body=b['c']-b['o']; body_pct=body/b['o']*100 if b['o']>0 else 0
    ref=(b['c']-bl)/bl*100 if bl>0 else 0
    d='+' if body>0 else '-'
    uw=b['h']-max(b['c'],b['o']); lw=min(b['c'],b['o'])-b['l']
    notes=[]
    if lw>abs(body)*2: notes.append('L')
    if uw>abs(body)*2: notes.append('U')
    n=' ['+','.join(notes)+']' if notes else ''
    print(f'  [{i*5:3d}min] {t} {d}{abs(body_pct):.1f}% ref={ref:+.1f}% V=${b["v"]:,.0f}{n}')

# 1m
if post1:
    bl1=post1[0]['o']
    chg5=(post1[4]['c']-bl1)/bl1*100 if len(post1)>=5 else 0
    chg30=(post1[29]['c']-bl1)/bl1*100 if len(post1)>=30 else 0
    print(f'\n1m: 5min={chg5:+.1f}%  30min={chg30:+.1f}%')

# 3. Pre-push fingerprint
cap_bars=0
for i in range(1,len(pre5)):
    body_pct=(pre5[i]['c']-pre5[i]['o'])/pre5[i]['o']*100 if pre5[i]['o']>0 else 0
    prev_v=pre5[i-1]['v']; vol_ratio=pre5[i]['v']/prev_v if prev_v>0 else 1
    if body_pct<-8 and vol_ratio>3: cap_bars+=1

all_h=[b['h'] for b in pre5]; all_l=[b['l'] for b in pre5]
pos=(pre5[-1]['c']-min(all_l))/(max(all_h)-min(all_l))*100 if max(all_h)>min(all_l) else 50

# Volume trend
vol_trend=1.0
if len(post5)>=4:
    early_v=sum(b['v'] for b in post5[:2])/2
    late_v=sum(b['v'] for b in post5[-2:])/2
    vol_trend=late_v/early_v if early_v>0 else 1

print(f'\n=== 前置指纹 ===')
print(f'投降Bar: {cap_bars}个  pos: {pos:.0f}%  量趋势: {vol_trend:.2f}x')

# 4. Compare against historical
with open('data/deepseek_discovery/signal_kline_records.jsonl','r',encoding='utf-8') as f:
    hist=[json.loads(line) for line in f if line.strip()]

def get_zone(tp):
    if tp>-20: return '轻度',-20,999
    if tp>-50: return '中度',-50,-20
    if tp>-80: return '重度',-80,-50
    return '极端',-999,-80

zone,lo,hi=get_zone(trough_pct)
group=[h for h in hist if lo<h['outcome']['trough_pct']<=hi]
wr20_all=sum(1 for h in group if h['outcome']['wr20'])/max(len(group),1)*100
same_sig=[h for h in group if h['signal_type']==rec['signal_type']]
wr20_same=sum(1 for h in same_sig if h['outcome']['wr20'])/max(len(same_sig),1)*100

print(f'\n=== 历史同类 ===')
st = rec['signal_type']
print(f'{zone}回撤: n={len(group)}, WR20={wr20_all:.0f}%')
print(f'{zone}回撤+{st}: n={len(same_sig)}, WR20={wr20_same:.0f}%')

# 5. Verdict
print(f'\n=== 结论 ===')
if cap_bars>0 and pos<30 and recovery>20:
    print('有投降Bar+地板价+底部反弹 -> 可试错入场, 止损-15%')
elif trough_pct<-70 and recovery<20:
    print('回撤过深+反弹弱 -> 不建议入场')
elif pos>70 and cap_bars==0:
    print('天花板价+无投降清洗 -> 不建议入场')
elif zone=='极端':
    print('极端回撤(>80%) -> WR20仅25%, 不建议入场')
elif recovery>50 and cur_pct>-30:
    print('已大幅反弹, 错过最佳入场点。追入需严格止损-10%')
else:
    print('信号不明确 -> 等缩量横盘至少10min+锤子线/放量阳线后再决定')