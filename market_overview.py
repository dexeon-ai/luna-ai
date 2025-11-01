# ============================================================
# market_overview.py — Retail Flow + Altseason cache builder
# Writes: luna_cache/data/global/market_state.json
# ============================================================

import json, pathlib
from datetime import datetime, timezone, timedelta
import numpy as np
import pandas as pd

ROOT = pathlib.Path(__file__).parent.resolve()
DATA_DIR = ROOT / "luna_cache" / "data"
COINS_DIR = DATA_DIR / "coins"
GLOBAL_DIR = DATA_DIR / "global"
SECTORS_MAP = DATA_DIR / "coin_sectors.json"  # optional

GLOBAL_DIR.mkdir(parents=True, exist_ok=True)

def load_sector_map():
    if SECTORS_MAP.exists():
        try:
            return json.loads(SECTORS_MAP.read_text(encoding="utf-8"))
        except:
            pass
    return {}  # fallback: unknown sector

def latest_row(df):
    if df.empty: return None
    r = df.iloc[-1]
    return {
        "ts": r["timestamp"],
        "price": float(r.get("price", np.nan)),
        "mcap": float(r.get("market_cap", np.nan)),
        "volume": float(r.get("volume_24h", r.get("volume", np.nan)))
    }

def pct_change(df, hours):
    if df.empty or "timestamp" not in df.columns or "price" not in df.columns:
        return np.nan
    target = datetime.now(timezone.utc) - timedelta(hours=hours)
    older = df[df["timestamp"] <= target]
    if older.empty: return np.nan
    base = older["price"].iloc[-1]
    if base in (0, np.nan, None): return np.nan
    latest = df["price"].iloc[-1]
    return (latest/base - 1)*100.0

def build_state():
    sector_map = load_sector_map()
    rows = []
    for p in COINS_DIR.glob("*.csv"):
        coin = p.stem
        try:
            df = pd.read_csv(p)
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
            df = df.dropna(subset=["timestamp"]).sort_values("timestamp")
            if df.empty: continue
            info = latest_row(df)
            if not info: continue
            pct24 = pct_change(df, 24)
            pct7d = pct_change(df, 24*7)
            vol = info["volume"] if info["volume"]==info["volume"] else 0.0
            mcap = info["mcap"] if info["mcap"]==info["mcap"] else np.nan
            sector = sector_map.get(coin, "Other")
            rows.append({"coin": coin, "mcap": mcap, "volume": vol, "pct24": pct24, "pct7d": pct7d, "sector": sector})
        except Exception:
            continue

    if not rows:
        return {"altseason_index":0, "notes":"No rows found"}

    df = pd.DataFrame(rows)
    # compute dominance from MCAP if available
    total_mcap = df["mcap"].replace(0,np.nan).dropna().sum()
    btc_mcap = df.loc[df["coin"]=="bitcoin","mcap"].dropna()
    btc_dom = float(btc_mcap.iloc[0]/total_mcap*100) if (len(btc_mcap)>0 and total_mcap>0) else None

    # altseason index: blend (% of coins beating BTC on 7d) with BTC dom downshift
    btc_7d = float(df.loc[df["coin"]=="bitcoin","pct7d"].fillna(0).iloc[0]) if "bitcoin" in df["coin"].values else 0.0
    outperform = df.loc[(df["coin"]!="bitcoin") & (df["pct7d"].notna()), "pct7d"] > btc_7d
    share = float(outperform.sum())/max(1,float((df["coin"]!="bitcoin").sum()))
    altseason = round(100*(0.7*share + 0.3*(1-(btc_dom/100.0)) if btc_dom else 0.7*share), 1)

    # Retail Attention Index (RAI) per coin (0–100): combine volume delta & 24h % move
    # Normalize by z-scores and min-max
    v = df["volume"].replace(0, np.nan)
    v_norm = (v - v.mean())/ (v.std() if v.std() else 1.0)
    p_norm = (df["pct24"].fillna(0) - df["pct24"].fillna(0).mean())/ (df["pct24"].fillna(0).std() or 1.0)
    rai_raw = 0.6*v_norm + 0.4*p_norm
    # scale 0..100
    rai_scaled = 50 + 15*rai_raw
    df["rai"] = rai_scaled.clip(0,100).fillna(0)

    # Sector aggregation by weight (sum mcap) and RAI delta (avg RAI)
    sector_groups = []
    for sector, g in df.groupby("sector"):
        weight = float(g["mcap"].replace(0,np.nan).dropna().sum() or 0.0)
        rai_delta = float(g["rai"].mean() or 0.0) - 50.0
        # color map: red (neg) to green (pos)
        c = "#ff6b6b" if rai_delta<-5 else "#f6c945" if rai_delta<5 else "#00e686"
        sector_groups.append({"name": sector, "weight": max(1.0, weight/1e9), "rai_delta": round(rai_delta,1), "rai_color": c})

    # Top movers by RAI
    top = df.sort_values("rai", ascending=False).head(25)
    top_movers = []
    for _, r in top.iterrows():
        top_movers.append({
            "coin": r["coin"],
            "sector": r["sector"],
            "rai": float(round(r["rai"],1)),
            "vol_delta": float(round(r["pct24"] if pd.notna(r["pct24"]) else 0.0,1)),
            "pct_24h": float(round(r["pct24"] if pd.notna(r["pct24"]) else 0.0,2))
        })

    state = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "altseason_index": float(altseason),
        "btc_dominance": float(round(btc_dom,2)) if btc_dom else None,
        "total_mcap": float(total_mcap) if total_mcap==total_mcap else None,
        "sectors": sector_groups,
        "top_movers": top_movers,
        "notes": "Retail metrics are volume/price proxies; integrate social signals later for richer RAI."
    }
    return state

if __name__ == "__main__":
    st = build_state()
    out = GLOBAL_DIR / "market_state.json"
    out.write_text(json.dumps(st, indent=2), encoding="utf-8")
    print("[Overview] ✅ market_state.json updated:", out)
