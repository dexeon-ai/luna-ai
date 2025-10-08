# plot_engine.py — Luna Technical Chart Engine v2
# - Primary: CoinGecko | Fallback: CoinPaprika
# - Resilient caching, no hard aborts
# - Updated 2025-10-08

import os, time, json, hashlib, requests, numpy as np, matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path

CACHE = Path("/tmp/luna_cache")
CACHE.mkdir(parents=True, exist_ok=True)
CG_API = "https://api.coingecko.com/api/v3/coins"
CACHE_TTL = 15*60

def _cache(fp): return fp.exists() and (time.time()-fp.stat().st_mtime)<CACHE_TTL

def _fetch_json(url):
    key = hashlib.sha1(url.encode()).hexdigest()
    fp = CACHE / f"{key}.json"
    if _cache(fp):
        return json.load(open(fp))
    for i in range(3):
        try:
            r = requests.get(url, timeout=12)
            if r.status_code == 429:
                time.sleep(3*(i+1)); continue
            r.raise_for_status()
            data = r.json()
            json.dump(data, open(fp,"w"))
            return data
        except Exception as e:
            print("[Retry]", e); time.sleep(2)
    if fp.exists(): return json.load(open(fp))
    return {}

def _resolve(symbol):
    m={"BTC":"bitcoin","ETH":"ethereum","SOL":"solana","BNB":"binancecoin"}
    return m.get(symbol.upper(),symbol.lower())

def _fetch_prices(cid,days="max"):
    url=f"{CG_API}/{cid}/market_chart?vs_currency=usd&days={days}"
    d=_fetch_json(url)
    px=d.get("prices",[])
    if not px:
        print("[Fallback] CoinPaprika")
        pid="btc-bitcoin" if cid=="bitcoin" else cid
        try:
            r=requests.get(f"https://api.coinpaprika.com/v1/tickers/{pid}/historical?start=2013-04-28&interval=1d")
            if r.ok:
                h=r.json(); px=[[time.time()*1000,x["close"]] for x in h if "close" in x]
        except Exception as e: print("[Paprika error]",e)
    if not px: return [],[]
    ts=[p[0]/1000 for p in px]; prices=[float(p[1]) for p in px]
    return ts,prices

def _fetch_info(cid):
    url=f"{CG_API}/{cid}?localization=false&tickers=false&market_data=true"
    return _fetch_json(url)

def build_tech_panel(symbol="BTC",out="/tmp/tech_panel.png"):
    cid=_resolve(symbol)
    _,s=_fetch_prices(cid,7); _,l=_fetch_prices(cid,"max"); info=_fetch_info(cid)
    if len(s)<10: s=list(np.linspace(1,2,20))
    if len(l)<50: l=s
    price=info.get("market_data",{}).get("current_price",{}).get("usd",s[-1])
    trend=_trendline(s); A,B=_pivots(s); fib=_fibs(A,B)
    sup,res=_zones(l)
    _render(s,l,trend,fib,sup,res,price,out)
    return {"chart_path":out,"metrics":{"price":price}}

def _trendline(y):
    y=np.array(y); x=np.arange(len(y))
    if len(y)<3: return y
    a,b=np.polyfit(x,y,1)
    return (a*x+b).tolist()

def _pivots(y):
    y=np.array(y)
    if len(y)<10:return (y[0],y[-1])
    mi,ma=int(np.argmin(y)),int(np.argmax(y))
    return (float(y[mi]),float(y[ma]))

def _fibs(A,B):
    r=[1.272,1.414,1.618,2.0]; lv=[]
    for rr in r: lv.append((f"{rr:.3f}x",B+(B-A)*(rr-1)))
    return lv

def _zones(y,bins=40):
    y=np.array(y)
    if len(y)<50:return [],[]
    h,e=np.histogram(y,bins=bins)
    c=(e[:-1]+e[1:])/2
    top=sorted(c[np.argsort(h)[-4:]])
    return top,top

def _render(s,l,t,fib,sup,res,price,out):
    plt.figure(figsize=(7.2,4.2),facecolor="#0b0f28")
    a1=plt.subplot(2,1,1,facecolor="#0f1433")
    x=np.arange(len(s)); a1.plot(x,s,color="#60d4ff",lw=2)
    a1.plot(x,t,"--",color="#9cff9c",lw=1.5)
    for n,v in fib:a1.axhline(v,color="#ffd166",ls=":",lw=1)
    a2=plt.subplot(2,1,2,facecolor="#0f1433")
    x2=np.arange(len(l)); a2.plot(x2,l,color="#b084ff",lw=1.4)
    for v in sup:a2.axhspan(v*0.99,v*1.01,color="#8be9fd",alpha=0.08)
    a2.axhline(price,color="white",ls="--",alpha=0.3)
    plt.tight_layout(); plt.savefig(out,dpi=180,bbox_inches="tight"); plt.close()
