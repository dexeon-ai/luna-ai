# luna_engine/strategies.py
import pandas as pd

def rsi_reversion(df: pd.DataFrame, buy_th=30, exit_th=55, lookback_hours=24*30):
    d = df.tail(lookback_hours).copy()
    if len(d) < 50: 
        return {"last_signal":"flat", "roi_pct":0.0, "trades":0}
    price = d["price"].astype(float)
    rsi   = d["rsi"].astype(float)
    pos = 0
    entry = 0.0
    pnl = 0.0
    trades = 0
    for i in range(1, len(d)):
        if pos == 0 and rsi.iloc[i] <= buy_th:
            pos = 1; entry = price.iloc[i]; trades += 1
        elif pos == 1 and rsi.iloc[i] >= exit_th:
            pnl += (price.iloc[i] / entry - 1.0)
            pos = 0
    if pos == 1:
        pnl += (price.iloc[-1] / entry - 1.0)
    last_signal = "long" if (pos == 1) else "flat"
    return {"last_signal": last_signal, "roi_pct": round(100*pnl, 2), "trades": trades}

def ma_cross(df: pd.DataFrame, fast=20, slow=50, lookback_hours=24*90):
    d = df.tail(lookback_hours).copy()
    if len(d) < slow+10: 
        return {"last_signal":"flat", "roi_pct":0.0, "trades":0}
    price = d["price"].astype(float)
    ema_f = d["ema20"] if fast==20 else price.ewm(span=fast, adjust=False).mean()
    ema_s = d["ema50"] if slow==50 else price.ewm(span=slow, adjust=False).mean()
    cross = (ema_f > ema_s).astype(int).diff().fillna(0)
    pos = 0; entry = 0.0; pnl = 0.0; trades = 0
    for i in range(1, len(d)):
        if pos == 0 and cross.iloc[i] == 1:
            pos = 1; entry = price.iloc[i]; trades += 1
        elif pos == 1 and cross.iloc[i] == -1:
            pnl += (price.iloc[i] / entry - 1.0); pos = 0
    if pos == 1:
        pnl += (price.iloc[-1] / entry - 1.0)
    last_signal = "long" if pos==1 else "flat"
    return {"last_signal": last_signal, "roi_pct": round(100*pnl, 2), "trades": trades}

def breakout_20d(df: pd.DataFrame, lookback_hours=24*60):
    d = df.tail(lookback_hours).copy()
    if len(d) < 24*21:
        return {"last_signal":"flat", "roi_pct":0.0, "trades":0}
    price = d["price"].astype(float)
    high20 = d["high_20d"]
    trig = (price > high20.shift(1)*1.005).astype(int).diff().fillna(0)
    pos = 0; entry=0.0; pnl=0.0; trades=0
    for i in range(1, len(d)):
        if pos==0 and trig.iloc[i]==1:
            pos=1; entry=price.iloc[i]; trades += 1
        elif pos==1 and (price.iloc[i] < d["ema20"].iloc[i]):
            pnl += (price.iloc[i]/entry - 1.0); pos=0
    if pos==1: pnl += (price.iloc[-1]/entry - 1.0)
    last_signal = "armed" if pos==0 else "triggered"
    return {"last_signal": last_signal, "roi_pct": round(100*pnl,2), "trades": trades}
