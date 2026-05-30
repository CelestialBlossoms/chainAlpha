import sys,io,requests
sys.stdout=io.TextIOWrapper(sys.stdout.buffer,encoding='utf-8',errors='replace')
from datetime import datetime,timezone

addr='5nzAoCXsXnLDTd3amsDFqoksEEY4VhZWk6p2rkWCpump'
ets=1780073087; push_mcap=58447
url='https://dquery.sintral.io/u-kline/v1/k-line/candles'
hdr={'Accept-Encoding':'identity','User-Agent':'binance-web3/1.1'}
r=requests.get(url,params={'address':addr,'platform':'solana','interval':'5min','limit':48},headers=hdr,timeout=15)
k5=[]
for b in r.json().get('data',[]):
    if len(b)<6: continue
    o,h,l,c,v=float(b[0]),float(b[1]),float(b[2]),float(b[3]),float(b[4])
    if o>0: k5.append({'t':int(b[5])//1000,'o':o,'h':h,'l':l,'c':c,'v':v})
k5.sort(key=lambda x:x['t'])

pre5=[b for b in k5 if b['t']<=ets]; post5=[b for b in k5 if b['t']>ets]
bl=pre5[-1]['c']
pk=max(b['h'] for b in post5); tr=min(b['l'] for b in post5); cur=post5[-1]['c']
pk_pct=(pk-bl)/bl*100; tr_pct=(tr-bl)/bl*100; cur_pct=(cur-bl)/bl*100
push_i=len(pre5)-1

print(f'DATAHOUSE 推送后 {len(post5)}bar={len(post5)*5}min')
print(f'峰值: {pk_pct:+.1f}% (MCap ${push_mcap*(1+pk_pct/100):,.0f})')
print(f'最低: {tr_pct:+.1f}%  当前: {cur_pct:+.1f}% (MCap ${push_mcap*(1+cur_pct/100):,.0f})')
print()

for i,b in enumerate(k5):
    t=datetime.fromtimestamp(b['t'],tz=timezone.utc).strftime('%H:%M')
    body=b['c']-b['o']; bp=body/b['o']*100; d='+' if body>0 else '-'
    ref=(b['c']-bl)/bl*100
    m=' PUSH' if i==push_i else ''
    print(f'  [{i*5:3d}min] {t} {d}{abs(bp):.1f}% ref={ref:+.1f}% V=${b["v"]:,.0f}{m}')

r1=requests.get(url,params={'address':addr,'platform':'solana','interval':'1min','limit':60},headers=hdr,timeout=15)
k1=[]
for b in r1.json().get('data',[]):
    if len(b)<6: continue
    o,h,l,c,v=float(b[0]),float(b[1]),float(b[2]),float(b[3]),float(b[4])
    if o>0: k1.append({'t':int(b[5])//1000,'o':o,'h':h,'l':l,'c':c,'v':v})
k1.sort(key=lambda x:x['t'])
post1=[b for b in k1 if b['t']>ets]
print(f'\n1m post:')
for b in post1[:8]:
    t=datetime.fromtimestamp(b['t'],tz=timezone.utc).strftime('%H:%M:%S')
    body=b['c']-b['o']; bp=body/b['o']*100; d='+' if body>0 else '-'
    ref=(b['c']-bl)/bl*100
    print(f'  {t} {d}{abs(bp):.1f}% ref={ref:+.1f}% V=${b["v"]:,.0f}')

zone='轻度' if tr_pct>-20 else ('中度' if tr_pct>-50 else '重度')
print(f'\n回撤: {zone}({tr_pct:+.0f}%) | 轻度+ATH>5x -> 历史WR20=75%')
