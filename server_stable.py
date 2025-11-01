"""
Luna AI — Control Panel (final build)
-------------------------------------
Reads data from your Parquet + metrics files and renders full dashboard.
"""

from __future__ import annotations
import json
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
from flask import Flask, jsonify, render_template, request, Response
import plotly.graph_objects as go
import plotly.io as pio

# ===== PATHS =====
FRAMES_DIR = Path(r"C:\Users\jmpat\Desktop\Luna AI\luna_cache\data\derived\frames")
DERIVED_DIR = FRAMES_DIR.parent
ROOT = Path(r"C:\Users\jmpat\Desktop\Luna AI")
TEMPLATES_DIR = ROOT / "templates"
STATIC_DIR = ROOT / "static"

app = Flask(__name__, template_folder=str(TEMPLATES_DIR), static_folder=str(STATIC_DIR))

# ===== COLORS =====
C = {
    "PRICE": "#e8ecff", "RSI": "#ff4d6d", "MACD": "#14d18f",
    "BANDS": "#a78bfa", "VOLUME": "#60a5fa", "MCAP": "#f59e0b",
    "LIQ": "#00e5a8", "FDV": "#f6c945", "SENT": "#f97316",
    "FG": "#e879f9", "WHALE": "#64d2ff",
}
METRIC_ORDER = ["1h","4h","8h","12h","24h","7d","30d","1y","all"]

# ===== HELPERS =====
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def fmt_updated(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("UTC %Y-%m-%d %H:%M")

# ===== LOADERS =====
def load_coin_data(symbol: str) -> pd.DataFrame:
    """Read Parquet file by symbol (case-insensitive)."""
    files = {p.stem.upper(): p for p in FRAMES_DIR.glob("*.parquet")}
    sym = symbol.upper()
    if sym not in files:
        for k in files:
            if k.startswith(sym):
                sym = k
                break
    path = files.get(sym)
    if not path or not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)
    if "time" in df.columns:
        df["timestamp"] = pd.to_datetime(df["time"], unit="s", utc=True, errors="coerce")
        df.drop(columns=["time"], inplace=True)
    elif "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp")
    for col in ("open","high","low","close","volume","market_cap"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def load_metrics(symbol: str) -> dict:
    files = {p.stem.split("_metrics")[0].upper(): p for p in DERIVED_DIR.glob("*_metrics.json")}
    sym = symbol.upper()
    if sym not in files:
        for k in files:
            if k.startswith(sym):
                sym = k
                break
    path = files.get(sym)
    if not path or not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))

# ===== INDICATORS =====
def ensure_derived(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "close" not in df.columns:
        return df
    out = df.copy()
    price = out["close"].astype(float)

    # RSI
    d = price.diff()
    up = d.clip(lower=0)
    dn = -d.clip(upper=0).replace(0, 1e-9)
    roll_up = up.ewm(span=14, adjust=False).mean()
    roll_dn = dn.ewm(span=14, adjust=False).mean()
    rs = roll_up / roll_dn
    out["rsi"] = 100 - (100 / (1 + rs))

    # MACD
    ema12 = price.ewm(span=12, adjust=False).mean()
    ema26 = price.ewm(span=26, adjust=False).mean()
    out["macd_line"] = ema12 - ema26
    out["macd_signal"] = out["macd_line"].ewm(span=9, adjust=False).mean()

    # Bollinger
    mid = price.rolling(20, min_periods=5).mean()
    std = price.rolling(20, min_periods=5).std()
    out["bb_upper"] = mid + 2 * std
    out["bb_lower"] = mid - 2 * std
    return out

# ===== ROI =====
HOURS = {"1h":1,"4h":4,"8h":8,"12h":12,"24h":24,"7d":24*7,"30d":24*30,"1y":24*365}
def _value_at_or_before(df, hrs):
    target = now_utc() - pd.Timedelta(hours=hrs)
    older = df[df["timestamp"] <= target]
    return older["close"].iloc[-1] if not older.empty else df["close"].iloc[0]
def derive_price_change(df):
    if df.empty: return {}
    latest = df["close"].iloc[-1]
    out = {}
    for k, hrs in HOURS.items():
        base = _value_at_or_before(df, hrs)
        if base not in (None, 0):
            out[k] = round((latest / base - 1) * 100, 2)
    if len(df)>1 and df["close"].iloc[0]!=0:
        out["all"]=round((latest/df["close"].iloc[0]-1)*100,2)
    return out
def derive_investment(df):
    ch = derive_price_change(df)
    return {k: round(1000*(1+v/100),2) for k,v in ch.items()}

# ===== PLOTTING =====
def _layout(h):
    return dict(template="plotly_dark",height=h,
                margin=dict(l=24,r=18,t=22,b=10),
                plot_bgcolor="#151927",paper_bgcolor="#151927",
                legend=dict(orientation="h",y=-0.25,x=0))
def chart_main(df,symbol):
    if df.empty: return "<div>No data</div>"
    fig=go.Figure()
    fig.add_trace(go.Scatter(x=df["timestamp"],y=df["close"],mode="lines",
                             line=dict(color=C["PRICE"],width=2),name=f"{symbol} Price"))
    if {"bb_upper","bb_lower"}.issubset(df.columns):
        fig.add_trace(go.Scatter(x=df["timestamp"],y=df["bb_upper"],line=dict(color=C["BANDS"],width=1)))
        fig.add_trace(go.Scatter(x=df["timestamp"],y=df["bb_lower"],line=dict(color=C["BANDS"],width=1,dash="dot")))
    fig.update_layout(**_layout(460))
    return pio.to_html(fig,include_plotlyjs=False,full_html=False)
def chart_square(df,col,title,color):
    if df.empty or col not in df.columns:
        return f"<div>No {title}</div>"
    fig=go.Figure()
    fig.add_trace(go.Scatter(x=df["timestamp"],y=df[col],mode="lines",
                             line=dict(color=color,width=1.8)))
    fig.update_layout(**_layout(180))
    return pio.to_html(fig,include_plotlyjs=False,full_html=False)

# ===== ROUTES =====
@app.get("/")
def home():
    return ('<meta http-equiv="refresh" content="0; url=/analyze?symbol=BTC">',302)
@app.get("/api/symbols")
def api_symbols():
    return jsonify(sorted([p.stem for p in FRAMES_DIR.glob("*.parquet")]))
@app.get("/api/metrics/<symbol>")
def api_metrics(symbol): return jsonify(load_metrics(symbol))
@app.get("/api/history/<symbol>")
def api_history(symbol):
    df=load_coin_data(symbol)
    if df.empty: return jsonify({"error":f"No data for {symbol}"}),404
    return df.tail(200).to_json(orient="records")

@app.get("/analyze")
def analyze():
    coin=request.args.get("symbol","BTC").upper()
    df_raw=load_coin_data(coin)
    if df_raw.empty: return Response(f"<h3>No cached data for {coin}</h3>",mimetype="text/html")
    df=ensure_derived(df_raw)
    metrics_block=load_metrics(coin) or {}
    price_change=metrics_block.get("price_change") or derive_price_change(df)
    inv_model=metrics_block.get("investment_model",{}).get("hypothetical_1000_usd") or derive_investment(df)
    charts={"MAIN":chart_main(df,coin),"RSI":chart_square(df,"rsi","RSI",C["RSI"]),
            "MACD":chart_square(df,"macd_line","MACD",C["MACD"]),
            "BANDS":chart_square(df,"bb_upper","Bollinger",C["BANDS"])}
    return render_template("control_panel.html",coin=coin.lower(),coin_name=coin,symbol=coin,
                           updated=fmt_updated(now_utc()),metric_order=METRIC_ORDER,
                           price_change=price_change,inv_model=inv_model,charts=charts,
                           blurbs={},overview=f"{coin} loaded successfully.",
                           scores={},signals={},levels={},insights=[],strategies={})

if __name__=="__main__":
    print("✅ Control Panel running — data dir:",FRAMES_DIR)
    app.run(debug=True)
