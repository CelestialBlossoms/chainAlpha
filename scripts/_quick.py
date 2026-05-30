import sys,io,requests
sys.stdout=io.TextIOWrapper(sys.stdout.buffer,encoding='utf-8',errors='replace')
from datetime import datetime,timezone

addr='BYUPXbzLSccJp4LKHcsUNg6Uieq84d2L8FVF7z9Wpump'
ets=1780102557; push_mcap=50556
url='https://dquery.sintral.io/u-kline/v1/k-line/candles'
hdr={'Accept-Encoding':'identity','User-Agent':'binance-web3/1.1'}

k5=[]; k1=[]
for i,l in [('5min',24),('1min',60)]:
    r=requests.get(url,params={'address':addr,'platform':'solana','interval':i,'limit':l},headers=hdr,timeout=15)
    for b in r.json().get('data',[]):
        if len(b)<6: continue
        o,h,l,c,v=float(b[0]),float(b[1]),float(b[2]),float(b[3]),float(b[4])
        if o>0: (k5 if i=='5min' else k1).append({'t':int(b[5])//1000,'o':o,'h':h,'l':l,'c':c,'v':v})
k5.sort(key=lambda x:x['t']); k1.sort(key=lambda x:x['t'])

pre5=[b for b in k5 if b['t']<=ets]; post5=[b for b in k5 if b['t']>ets]
pre1=[b for b in k1 if b['t']<=ets]; post1=[b for b in k1 if b['t']>ets]

# Find push bar — closest bar before or at ets
push_bar=None
for b in k5:
    if b['t']<=ets: push_bar=b
if not push_bar and k5: push_bar=k5[0]
bl=push_bar['c'] if push_bar else 0
if bl<=0: print('No baseline'); exit()

pk=max(b['h'] for b in post5) if post5 else bl
tr=min(b['l'] for b in post5) if post5 else bl
cur=post5[-1]['c'] if post5 else bl
pk_pct=(pk-bl)/bl*100; tr_pct=(tr-bl)/bl*100; cur_pct=(cur-bl)/bl*100
tr_i=min(range(len(post5)),key=lambda i:post5[i]['l']) if post5 else 0
recovery=(cur-post5[tr_i]['l'])/post5[tr_i]['l']*100 if post5 and post5[tr_i]['l']>0 else 0

print(f'PAINT 推送后 {len(post5)}bar={len(post5)*5}min')
print(f'峰值:{pk_pct:+.1f}% (MCap ${push_mcap*(1+pk_pct/100):,.0f})')
print(f'最低:{tr_pct:+.1f}% (MCap ${push_mcap*(1+tr_pct/100):,.0f})')
print(f'当前:{cur_pct:+.1f}% (MCap ${push_mcap*(1+cur_pct/100):,.0f})')
print(f'底部反弹:{recovery:+.0f}%')
zone='轻度' if tr_pct>-20 else ('中度' if tr_pct>-50 else '重度')
print(f'回撤:{zone}({tr_pct:+.0f}%)')

push_i=len(pre5)-1
print(f'\n=== 5m ===')
for i,b in enumerate(k5):
    t=datetime.fromtimestamp(b['t'],tz=timezone.utc).strftime('%H:%M')
    body=b['c']-b['o']; bp=body/b['o']*100; d='+' if body>0 else '-'
    ref=(b['c']-bl)/bl*100
    m=' PUSH' if i==push_i else (' TR' if post5 and i-push_i-1==tr_i else '')
    print(f'  [{i*5:3d}min] {t} {d}{abs(bp):.1f}% ref={ref:+.1f}% V=${b["v"]:,.0f}{m}')

if post1:
    print(f'\n=== 1m post 最近15根 ===')
    for b in post1[:15]:
        t=datetime.fromtimestamp(b['t'],tz=timezone.utc).strftime('%H:%M:%S')
        body=b['c']-b['o']; bp=body/b['o']*100; d='+' if body>0 else '-'
        ref=(b['c']-bl)/bl*100
        print(f'  {t} {d}{abs(bp):.1f}% ref={ref:+.1f}% V=${b["v"]:,.0f}')
    if len(post1)>=5:
        chg5=(post1[4]['c']-bl)/bl*100
        p5v=sum(b['v'] for b in post1[:5])/5
        p10v=sum(b['v'] for b in post1[5:15])/10 if len(post1)>=15 else 1
        vr=f'{p5v/p10v:.1f}x' if p10v>0 else '?'
        print(f'5min:{chg5:+.1f}% VolRatio:{vr}')

if pre1:
    print(f'\n=== 1m pre 最后10根 ===')
    for b in pre1[-10:]:
        t=datetime.fromtimestamp(b['t'],tz=timezone.utc).strftime('%H:%M:%S')
        body=b['c']-b['o']; bp=body/b['o']*100; d='+' if body>0 else '-'
        ref=(b['c']-bl)/bl*100
        print(f'  {t} {d}{abs(bp):.1f}% ref={ref:+.1f}% V=${b["v"]:,.0f}')

print(f'\n时间:周日{datetime.fromtimestamp(ets,tz=timezone.utc).strftime("%H:%M")}UTC => 04-08h块 WR20=71%')
print(f'new_revival+{zone}回撤 => WR20≈{"59%" if zone=="轻度" else "?"}')
if zone=='轻度' and recovery>0:
    print('可入场(轻度回撤+周日窗口+已反弹)')
elif zone=='中度':
    print('中度回撤-需确认反弹持续性')
else:
    print('不建议')
