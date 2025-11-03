# ============================================================
# server.py — Luna Cockpit (stable grid + richer expand + CSV merge)
# Brain upgrade: free/offline metrics + conversational analysis
# ============================================================

from __future__ import annotations
import os, re, json, time, random, logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Any, Tuple, Optional, List

import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv

from flask import Flask, jsonify, render_template, request, Response
from flask.json.provider import DefaultJSONProvider
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio

# ====== NEW brain modules (safe fallbacks if missing) ======
try:
    from luna_agent.metrics import enrich_with_metrics
    HAVE_METRICS = True
except Exception:
    enrich_with_metrics = None
    HAVE_METRICS = False

try:
    from luna_agent.scoring import generate_analysis
    HAVE_SCORING = True
except Exception:
    generate_analysis = None
    HAVE_SCORING = False

# (Optional legacy import; we don't rely on it)
try:
    from luna_agent import analyze_indicators as _legacy_analyze_indicators
except Exception:
    _legacy_analyze_indicators = None

# ---------- logging ----------
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
LOG = logging.getLogger("luna")

# ---------- paths ----------
ROOT = Path(__file__).parent.resolve()
TEMPLATES_DIR = ROOT / "templates"
STATIC_DIR    = ROOT / "static"

DATA_DIR   = ROOT / "luna_cache" / "data"
DERIVED    = DATA_DIR / "derived"
FRAMES_DIR = DERIVED / "frames"           # *.parquet or *.csv cache
STATE_DIR  = DATA_DIR / "state"           # TTL stamps, etc.
SESS_DIR   = DATA_DIR / "sessions"
COINS_DIR  = DATA_DIR / "coins"           # your large CSV archives (1998 coins)

for d in (FRAMES_DIR, STATE_DIR, SESS_DIR):
    d.mkdir(parents=True, exist_ok=True)

FETCH_LOG = STATE_DIR / "fetch_log.json"

# ---------- constants ----------
TTL_MINUTES = 15
TTL_SECONDS = TTL_MINUTES * 60

# ---------- env ----------
load_dotenv(ROOT / ".env")

def _parse_keys(raw: str) -> List[str]:
    return [t.strip() for t in re.split(r"[\s,;]+", raw or "") if t.strip()]

CC_KEYS = _parse_keys(os.getenv("CRYPTOCOMPARE_KEYS") or os.getenv("CRYPTOCOMPARE_KEY") or "")
CG_KEY  = (os.getenv("COINGECKO_API_KEY") or os.getenv("CG_API_KEY") or "").strip()

# ---------- Flask JSON for Plotly ----------
class PlotlyJSON(DefaultJSONProvider):
    def dumps(self, obj, **kwargs):
        from plotly.utils import PlotlyJSONEncoder
        return json.dumps(obj, cls=PlotlyJSONEncoder, **kwargs)

app = Flask(__name__, template_folder=str(TEMPLATES_DIR), static_folder=str(STATIC_DIR))
app.json = PlotlyJSON(app)

# ---------- small utils ----------
def utcnow() -> datetime: 
    return datetime.now(timezone.utc)

def _to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()

def _latest_ts(df: pd.DataFrame) -> Optional[datetime]:
    if df is None or df.empty or "timestamp" not in df.columns: 
        return None
    s = pd.to_datetime(df["timestamp"], utc=True, errors="coerce").dropna()
    return None if s.empty else s.iloc[-1].to_pydatetime()

def _fetch_log() -> dict:
    try: 
        return json.loads(FETCH_LOG.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _touch_fetch(symbol: str) -> None:
    st = _fetch_log()
    st[symbol.upper()] = _to_iso(utcnow())
    FETCH_LOG.write_text(json.dumps(st, indent=2), encoding="utf-8")

def _fresh_enough(symbol: str, ttl: int = TTL_MINUTES) -> bool:
    st = _fetch_log()
    ts = st.get(symbol.upper())
    if not ts: return False
    try:
        last = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return False
    return (utcnow() - last) < timedelta(minutes=ttl)

def money(x: Optional[float]) -> str:
    try:
        return "$" + format(float(x), ",.2f")
    except Exception:
        return "—"

def f1(x) -> str:
    try:
        if x is None or (isinstance(x, float) and (np.isnan(x) or np.isinf(x))): return "—"
        return f"{float(x):.1f}"
    except Exception:
        return "—"

def to_float(x) -> Optional[float]:
    try: 
        v = float(x)
        if np.isnan(v) or np.isinf(v): return None
        return v
    except Exception:
        return None

# ---------- cache read/write ----------
def _frame_path(symbol: str, ext: str) -> Path:
    return FRAMES_DIR / f"{symbol.upper()}.{ext}"

def save_frame(symbol: str, df: pd.DataFrame) -> None:
    if df is None or df.empty: 
        return
    p_parq = _frame_path(symbol, "parquet")
    p_csv  = _frame_path(symbol, "csv")
    try:
        df.to_parquet(p_parq, index=False)              # needs pyarrow/fastparquet
        return
    except Exception as e:
        LOG.warning("[Cache] Parquet save failed (%s), falling back to CSV.", e)
    try:
        df.to_csv(p_csv, index=False)
    except Exception as e:
        LOG.warning("[Cache] CSV save failed: %s", e)

def load_cached_frame(symbol: str) -> pd.DataFrame:
    p_parq = _frame_path(symbol, "parquet")
    p_csv  = _frame_path(symbol, "csv")
    if p_parq.exists():
        try:
            df = pd.read_parquet(p_parq)
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
            return df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
        except Exception:
            pass
    if p_csv.exists():
        try:
            df = pd.read_csv(p_csv)
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
            return df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
        except Exception:
            pass
    return pd.DataFrame()

# ---------- CryptoCompare ----------
CC_BASE = "https://min-api.cryptocompare.com/data"

class CCKeyPool:
    def __init__(self, keys: List[str]): 
        self.keys = keys or []
        self.bans: Dict[str, float] = {}
    def pick(self) -> Optional[str]:
        if not self.keys: return None
        now = time.time()
        ok = [k for k in self.keys if self.bans.get(k, 0) <= now]
        return random.choice(ok) if ok else None
    def ban(self, k: str, minutes: int = 30):
        self.bans[k] = time.time() + minutes*60

CC_POOL = CCKeyPool(CC_KEYS)

def cc_get(path: str, params: Dict[str, Any]) -> Optional[dict]:
    last_err = None
    tries = max(1, len(CC_KEYS)) + 2
    for _ in range(tries):
        k = CC_POOL.pick()
        headers = {"Apikey": k} if k else {}
        try:
            r = requests.get(f"{CC_BASE}/{path}", params=params, headers=headers, timeout=20)
            if r.status_code == 200:
                js = r.json()
                if isinstance(js, dict) and (js.get("Response") in (None, "Success")):
                    return js
                last_err = f"bad CC payload {str(js)[:180]}"
                if "limit" in str(js).lower() and k: 
                    CC_POOL.ban(k)
            else:
                txt = (r.text or "")[:200]
                last_err = f"HTTP {r.status_code} {txt}"
                if "limit" in txt.lower() and k: 
                    CC_POOL.ban(k)
        except Exception as e:
            last_err = str(e)
    LOG.warning("[CC] %s", last_err or "unknown error")
    return None

def cc_hist(symbol: str, kind: str, limit: int, aggregate: int = 1) -> pd.DataFrame:
    mp = {"minute":"v2/histominute","hour":"v2/histohour","day":"v2/histoday"}
    q  = dict(fsym=symbol.upper(), tsym="USD", limit=limit, aggregate=aggregate)
    js = cc_get(mp[kind], q)
    if not js: return pd.DataFrame()
    raw = (js.get("Data") or {}).get("Data") or []
    if not raw: return pd.DataFrame()
    df = pd.DataFrame(raw)
    if "time" in df.columns:
        df["timestamp"] = pd.to_datetime(df["time"], unit="s", utc=True, errors="coerce")
    for c in ["open","high","low","close","volumefrom","volumeto"]:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")
    df["volume"] = df["volumeto"] if "volumeto" in df.columns else pd.NA
    cols = ["timestamp","open","high","low","close","volume"]
    return df[cols].dropna(subset=["timestamp"]).sort_values("timestamp")

# ---------- CoinGecko (fallback) ----------
CG_BASE = "https://api.coingecko.com/api/v3"
CG_IDS = {
    "BTC":"bitcoin","ETH":"ethereum","SOL":"solana","BNB":"binancecoin","XRP":"ripple",
    "ADA":"cardano","DOGE":"dogecoin","LINK":"chainlink","AVAX":"avalanche-2","TON":"the-open-network"
}

def cg_get(path: str, params: dict) -> Optional[dict]:
    headers = {}
    if CG_KEY:
        headers["x-cg-demo-api-key"] = CG_KEY
    try:
        r = requests.get(f"{CG_BASE}/{path}", params=params, headers=headers, timeout=30)
        if r.status_code == 200:
            return r.json()
        LOG.warning("[CG] %s %s", r.status_code, r.text[:180])
    except Exception as e:
        LOG.warning("[CG] %s", e)
    return None

def cg_series(symbol: str, days: int = 30) -> pd.DataFrame:
    cg_id = CG_IDS.get(symbol.upper())
    if not cg_id: return pd.DataFrame()
    js = cg_get(f"coins/{cg_id}/market_chart", {"vs_currency":"usd","days":days}) \
        or cg_get(f"coins/{cg_id}/market_chart", {"vs_currency":"usd","days":max(7,days//2)})
    if not js: return pd.DataFrame()
    px   = js.get("prices") or []
    vols = js.get("total_volumes") or []
    caps = js.get("market_caps") or []

    if not px: return pd.DataFrame()
    dp = pd.DataFrame(px, columns=["ts","close"])
    dp["timestamp"] = pd.to_datetime(dp["ts"], unit="ms", utc=True, errors="coerce")
    dp["close"] = pd.to_numeric(dp["close"], errors="coerce")

    if vols:
        dv = pd.DataFrame(vols, columns=["ts","v"])
        dv["timestamp"] = pd.to_datetime(dv["ts"], unit="ms", utc=True, errors="coerce")
        dv["volume"] = pd.to_numeric(dv["v"], errors="coerce")
        dp = dp.merge(dv[["timestamp","volume"]], on="timestamp", how="left")

    if caps:
        dc = pd.DataFrame(caps, columns=["ts","mc"])
        dc["timestamp"] = pd.to_datetime(dc["ts"], unit="ms", utc=True, errors="coerce")
        dc["market_cap"] = pd.to_numeric(dc["mc"], errors="coerce")
        dp = dp.merge(dc[["timestamp","market_cap"]], on="timestamp", how="left")

    # synth OHLC for indicators
    dp["open"] = dp["close"].shift(1)
    dp["high"] = dp["close"].rolling(3, min_periods=1).max()
    dp["low"]  = dp["close"].rolling(3, min_periods=1).min()
    return dp[["timestamp","open","high","low","close","volume","market_cap"]]\
        .dropna(subset=["timestamp"]).sort_values("timestamp")

# ---------- indicators ----------
def ema(s: pd.Series, span: int) -> pd.Series:
    return s.ewm(span=span, adjust=False).mean()

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: 
        return df.copy()
    out = df.copy()

    # RSI(14)
    delta = out["close"].diff()
    up = delta.clip(lower=0.0)
    dn = -delta.clip(upper=0.0)
    rs = up.ewm(span=14, adjust=False).mean() / dn.replace(0,1e-9).ewm(span=14, adjust=False).mean()
    out["rsi"] = 100 - (100/(1+rs))

    # MACD (12,26,9)
    ema12 = ema(out["close"], 12); ema26 = ema(out["close"], 26)
    out["macd_line"]   = ema12 - ema26
    out["macd_signal"] = ema(out["macd_line"], 9)
    out["macd_hist"]   = out["macd_line"] - out["macd_signal"]

    # Bollinger (20,2) + width
    ma20  = out["close"].rolling(20).mean()
    std20 = out["close"].rolling(20).std()
    out["bb_mid"]   = ma20
    out["bb_upper"] = ma20 + 2*std20
    out["bb_lower"] = ma20 - 2*std20
    out["bb_width"] = ((out["bb_upper"] - out["bb_lower"]) / ma20.replace(0,np.nan) * 100).fillna(0)

    # ADX(14) — simplified Wilder
    try:
        hi, lo, cl = out["high"].fillna(out["close"]), out["low"].fillna(out["close"]), out["close"]
        plus_dm  = (hi.diff().where(hi.diff() > lo.diff(), 0.0)).clip(lower=0)
        minus_dm = (lo.diff().where(lo.diff() > hi.diff(), 0.0)).clip(lower=0).abs()
        tr  = pd.concat([(hi-lo),(hi-cl.shift()).abs(),(lo-cl.shift()).abs()], axis=1).max(axis=1)
        atr = tr.rolling(14).mean()
        plus_di  = 100 * (plus_dm.rolling(14).mean()  / atr.replace(0,np.nan))
        minus_di = 100 * (minus_dm.rolling(14).mean() / atr.replace(0,np.nan))
        dx  = (100 * (plus_di - minus_di).abs() / (plus_di+minus_di).replace(0,np.nan)).fillna(0)
        out["adx14"] = dx.rolling(14).mean()
    except Exception:
        out["adx14"] = np.nan

    # OBV
    try:
        vol = out["volume"].fillna(0)
        direction = np.sign(out["close"].diff()).fillna(0)
        out["obv"] = (direction * vol).cumsum()
    except Exception:
        out["obv"] = np.nan

    # ATR(14)
    try:
        high = out["high"].fillna(out["close"])
        low  = out["low"].fillna(out["close"])
        close= out["close"]
        tr = pd.concat([(high-low), (high-close.shift()).abs(), (low-close.shift()).abs()], axis=1).max(axis=1)
        out["atr14"] = tr.rolling(14, min_periods=1).mean()
    except Exception:
        out["atr14"] = np.nan

    # ALT momentum (simple EMA spread)
    try:
        out["alt_momentum"] = (ema(out["close"], 10) - ema(out["close"], 30))
    except Exception:
        out["alt_momentum"] = np.nan

    return out

# ---------- hydrate (TTL + CSV merge + CC→CG) ----------
def hydrate_symbol(symbol: str, force: bool=False) -> pd.DataFrame:
    s = symbol.upper()

    # serve cache if fresh
    if (not force) and _fresh_enough(s):
        cached = load_cached_frame(s)
        if not cached.empty:
            return cached

    LOG.info("[Hydrate] %s (force=%s, ttl=%ss)", s, force, TTL_SECONDS)

    # 0) merge local CSV archives first (once)
    df_arch = pd.DataFrame()
    local_csv = (COINS_DIR / f"{s.lower()}.csv")
    if local_csv.exists():
        try:
            df_arch = pd.read_csv(local_csv)
            if "timestamp" in df_arch.columns:
                df_arch["timestamp"] = pd.to_datetime(df_arch["timestamp"], utc=True, errors="coerce")
            elif "time" in df_arch.columns:
                df_arch["timestamp"] = pd.to_datetime(df_arch["time"], unit="s", utc=True, errors="coerce")
            rename = {}
            for a,b in [("volumeto","volume"),("volumefrom","volume"),("Vol","volume")]:
                if a in df_arch.columns: rename[a]=b
            df_arch = df_arch.rename(columns=rename)
            for c in ["open","high","low","close","volume"]:
                if c not in df_arch.columns:
                    df_arch[c] = np.nan
            df_arch = df_arch.dropna(subset=["timestamp"]).sort_values("timestamp")
        except Exception as e:
            LOG.warning("[Hydrate] Could not parse archive for %s: %s", s, e)

    # 1) live pulls (CC)
    m = cc_hist(s, "minute", limit=360)           # ~6h
    h = cc_hist(s, "hour",   limit=24*30)         # ~30d
    d = cc_hist(s, "day",    limit=365)           # ~1y

    if (m is None or m.empty) and (h is None or h.empty) and (d is None or d.empty):
        LOG.warning("[Hydrate] CC empty → CG fallback")
        df = cg_series(s, days=30)
    else:
        parts = [x for x in (d,h,m) if x is not None and not x.empty]
        df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

    # 2) add market_cap from CG if missing
    if (df is not None) and (not df.empty) and "market_cap" not in df.columns:
        caps = cg_series(s, days=30)
        if not caps.empty and "market_cap" in caps.columns:
            df = df.merge(caps[["timestamp","market_cap"]], on="timestamp", how="left")

    # 3) stitch archives + live, compute indicators + enrich
    if df is None or df.empty:
        cached = load_cached_frame(s)
        if not cached.empty:
            _touch_fetch(s); 
            return cached
        return pd.DataFrame()

    if not df_arch.empty:
        df = pd.concat([df_arch, df], ignore_index=True)

    df = df.dropna(subset=["timestamp"]).drop_duplicates(subset=["timestamp"], keep="last")\
           .sort_values("timestamp").reset_index(drop=True)

    df = compute_indicators(df)

    # ====== NEW: extra metrics (free/offline) ======
    if HAVE_METRICS and enrich_with_metrics is not None:
        try:
            df = enrich_with_metrics(df)
        except Exception as e:
            LOG.warning("[Metrics] enrich failed: %s", e)

    save_frame(s, df)
    _touch_fetch(s)
    return df

# ---------- slice & rollups ----------
LOOKBACK = {
    "1h": timedelta(hours=1), "4h": timedelta(hours=4), "8h": timedelta(hours=8),
    "12h": timedelta(hours=12), "24h": timedelta(days=1), "7d": timedelta(days=7),
    "30d": timedelta(days=30), "1y": timedelta(days=365), "all": None
}

def slice_df(df: pd.DataFrame, tf: str) -> pd.DataFrame:
    if df.empty: return df
    win = LOOKBACK.get(tf, timedelta(hours=4))
    if win is None: return df
    anchor = _latest_ts(df) or utcnow()
    cutoff = anchor - win
    return df[df["timestamp"] >= cutoff].copy()

def value_at_or_before(df: pd.DataFrame, hours: int, anchor: Optional[datetime]=None) -> Optional[float]:
    if df.empty: return None
    anchor = anchor or _latest_ts(df) or utcnow()
    t = anchor - timedelta(hours=hours)
    older = df[df["timestamp"] <= t]
    if older.empty: 
        return to_float(df["close"].iloc[0])
    return to_float(older["close"].iloc[-1])

def compute_rollups(df: pd.DataFrame) -> Tuple[Dict[str, Optional[float]], Dict[str, Optional[float]]]:
    if df.empty: return {}, {}
    ch = {}
    anchor = _latest_ts(df) or utcnow()
    last = to_float(df["close"].iloc[-1])
    for k, hrs in [("1h",1),("4h",4),("8h",8),("12h",12),("24h",24),("7d",24*7),("30d",24*30),("1y",24*365)]:
        base = value_at_or_before(df, hrs, anchor)
        ch[k] = None if (base in (None,0) or last in (None,0)) else round((last/base-1)*100, 2)
    inv = {k: (None if v is None else round(1000*(1+v/100.0),2)) for k,v in ch.items()}
    return ch, inv

# ---------- improved paragraph (keeps same output location) ----------
def luna_paragraph(symbol: str, df: pd.DataFrame, tf: str, question: str) -> str:
    if df.empty:
        return f"{symbol}: I don’t have enough fresh data yet."
    view = slice_df(df, tf)
    if view.empty:
        view = df.tail(200)
    perf, _ = compute_rollups(df)   # use full history for performance context
    ch, _ = compute_rollups(view)

    # New brain (if present)
    if HAVE_SCORING and generate_analysis is not None:
        try:
            return generate_analysis(symbol, df, view, perf, question or "")
        except Exception as e:
            LOG.warning("[Luna] generate_analysis failed: %s", e)

    # Legacy fallback, if you keep your old analyzer around
    if _legacy_analyze_indicators is not None:
        last = view.iloc[-1]
        ch_view, _ = compute_rollups(view)
        trend, reasoning = _legacy_analyze_indicators(last, ch_view)
        if trend == "bullish":
            plan = "Upside momentum should persist if volume expands and resistance breaks."
        elif trend == "bearish":
            plan = "Further downside risk unless buyers reclaim lost ground."
        else:
            plan = "Expect consolidation until a new catalyst drives direction."
        return f"{symbol}: {reasoning} {plan}"

    # Simple final fallback (never breaks)
    last = view.iloc[-1]
    price = money(last.get("close"))
    rsi   = to_float(last.get("rsi"))
    macdl = to_float(last.get("macd_line"))
    macds = to_float(last.get("macd_signal"))
    hist  = to_float(last.get("macd_hist"))
    adx   = to_float(last.get("adx14"))
    bbw   = to_float(last.get("bb_width"))
    perf_bits = [f"{k} {v:+.2f}%" for k,v in perf.items() if v is not None and k in ("1h","4h","12h","24h")]
    perf_txt  = ", ".join(perf_bits) if perf_bits else "flat"
    side = "bulls" if (macdl is not None and macds is not None and macdl > macds) else "bears" if (macdl is not None and macds is not None and macdl < macds) else "neither side"
    return (
        f"{symbol} around {price}. Over this window: {perf_txt}. "
        f"RSI {f1(rsi)}; MACD {('>' if (macdl and macds and macdl>macds) else '<' if (macdl and macds and macdl<macds) else '=')} signal "
        f"(hist {f1(hist)}). ADX {f1(adx)} (trend). Bands width {f1(bbw)}%. Short answer: "
        f"{('bulls have a small edge' if side=='bulls' else 'bears have the edge' if side=='bears' else 'range-bound')}."
    )

# ---------- modal talk ----------
def talk_indicator(key: str, df: pd.DataFrame) -> str:
    if df.empty:
        return "No data available yet. Try refreshing in a few minutes."
    last = df.iloc[-1]
    if key == "RSI":
        r = to_float(last.get("rsi"))
        if r is None:
            return "RSI is unavailable for this timeframe."
        bias = "bullish momentum building" if r > 60 else "neutral consolidation" if 40 <= r <= 60 else "bearish momentum increasing"
        return f"RSI at {r:.1f}. This suggests {bias}. Above 70 = overheated, below 30 = washed‑out."
    if key == "MACD":
        m = to_float(last.get("macd_line")); s = to_float(last.get("macd_signal")); h = to_float(last.get("macd_hist"))
        if m is None or s is None:
            return "MACD data not available yet."
        cross = "bullish crossover" if m > s else "bearish crossover" if m < s else "flat momentum"
        return f"MACD shows {cross} ({m:.2f} vs {s:.2f}); histogram {h:.2f} measures short‑term acceleration."
    if key == "ATR":
        a = to_float(last.get("atr14"))
        return f"ATR(14) = {a:.3f}. Average true range measures typical candle size; higher ATR means wider swings and more risk."
    if key == "ADX":
        a = to_float(last.get("adx14"))
        trend = "strong" if a and a >= 40 else "moderate" if a and a >= 25 else "weak or sideways"
        return f"ADX {f1(a)}. Trend strength is {trend}. Values above 25 indicate a directional move is establishing."
    if key == "OBV":
        ser = df["obv"].dropna()
        if len(ser) < 5:
            return "OBV not yet meaningful on this timeframe."
        slope = np.sign(ser.iloc[-1] - ser.iloc[-5])
        side = "accumulation (buying pressure)" if slope > 0 else "distribution (selling pressure)" if slope < 0 else "neutral flow"
        return f"On‑Balance Volume indicates {side}. Rising OBV typically confirms uptrend participation."
    if key == "BANDS":
        w = to_float(last.get("bb_width"))
        note = "tight squeeze — volatility compression" if w and w <= 1 else "normal range" if (w and w < 5) else "expanding volatility"
        return f"Bollinger width {f1(w)}. Current state: {note}."
    if key == "ALT":
        alt = to_float(last.get("alt_momentum"))
        return f"ALT momentum {f1(alt)}. Positive = recovery momentum; negative = fading drive."
    if key == "VOL":
        v = to_float(last.get("volume"))
        return f"Current volume {format(v,',.0f') if v is not None else 'n/a'}. Rising volume confirms active interest."
    if key == "LIQ":
        v = to_float(last.get("volume"))
        return f"Liquidity proxy {format(v,',.0f') if v is not None else 'n/a'}. Thin liquidity increases slippage."
    if key == "MCAP":
        mc = to_float(last.get("market_cap"))
        if mc is None:
            return "Market capitalization data not present."
        return f"Market cap {money(mc)}. Larger caps move slower; small caps amplify both gains and losses."
    return "No detailed commentary available for this indicator."

# ---------- Plotly helpers ----------
def _apply_time_axis(fig: go.Figure) -> None:
    fig.update_xaxes(
        tickformatstops=[
            dict(dtickrange=[None, 1000*60*60*24], value="%H:%M"),
            dict(dtickrange=[1000*60*60*24, None], value="%m-%d"),
        ]
    )

def fig_price(df: pd.DataFrame, symbol: str) -> go.Figure:
    # Three rows inside the same tile: Price, MACD, Volume (mini)
    fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True,
        row_heights=[0.64, 0.22, 0.14], vertical_spacing=0.03
    )
    if not df.empty:
        # Row 1: OHLC + Bollinger
        fig.add_trace(go.Candlestick(
            x=df["timestamp"], open=df["open"], high=df["high"], low=df["low"], close=df["close"],
            name=f"{symbol} OHLC", increasing_line_color="#36d399", decreasing_line_color="#f87272", opacity=0.95
        ), row=1, col=1)
        for name, col, color in [
            ("BB upper", "bb_upper", "#a78bfa"),
            ("BB mid",   "bb_mid",   "#90a4ec"),
            ("BB lower", "bb_lower", "#a78bfa"),
        ]:
            if col in df.columns:
                fig.add_trace(
                    go.Scatter(x=df["timestamp"], y=df[col], name=name, line=dict(width=1, color=color)),
                    row=1, col=1
                )

        # Row 2: MACD lines + hist
        if "macd_line" in df.columns and "macd_signal" in df.columns:
            fig.add_trace(go.Scatter(x=df["timestamp"], y=df["macd_line"],   name="MACD",  line=dict(width=1.1)),
                          row=2, col=1)
            fig.add_trace(go.Scatter(x=df["timestamp"], y=df["macd_signal"], name="Signal", line=dict(width=1, dash="dot")),
                          row=2, col=1)
        if "macd_hist" in df.columns:
            fig.add_trace(go.Bar(x=df["timestamp"], y=df["macd_hist"], name="MACD Hist"), row=2, col=1)

        # Row 3: Volume (mini) — color by candle direction
        if "volume" in df.columns:
            # green if close >= previous close else red
            diff = df["close"].diff().fillna(0)
            colors = ['#36d399' if d >= 0 else '#f87272' for d in diff.tolist()]
            fig.add_trace(
                go.Bar(x=df["timestamp"], y=df["volume"].fillna(0), name="Volume",
                       marker=dict(color=colors), opacity=0.8),
                row=3, col=1
            )

    fig.update_layout(
        template="plotly_dark", height=420,  # a touch taller to fit the mini pane
        margin=dict(l=12, r=12, t=24, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis_rangeslider_visible=False,
        barmode="relative",
    )
    _apply_time_axis(fig)
    return fig

def fig_line(df: pd.DataFrame, y: str, name: str, h: int = 155) -> go.Figure:
    fig = go.Figure()
    if not df.empty and y in df.columns and not pd.isna(df[y]).all():
        y_data = df[y].astype(float)
        fig.add_trace(go.Scatter(x=df["timestamp"], y=y_data, name=name, line=dict(width=1.6)))
        ymin, ymax = float(y_data.min()), float(y_data.max())
        if ymin == ymax:
            ymin -= 0.5
            ymax += 0.5
        fig.update_yaxes(range=[ymin, ymax], fixedrange=False)
    else:
        fig.add_annotation(text="No data for this timeframe.", showarrow=False, xref="paper", yref="paper", x=0.5, y=0.5)
    fig.update_layout(template="plotly_dark", height=h, margin=dict(l=10,r=10,t=18,b=8), showlegend=False)
    _apply_time_axis(fig)
    return fig

# ---------- routes ----------
@app.get("/")
def home():
    return ('<meta http-equiv="refresh" content="0; url=/analyze?symbol=ETH&tf=12h">', 302)

@app.get("/analyze")
def analyze():
    symbol = (request.args.get("symbol") or "ETH").upper()
    tf     = (request.args.get("tf") or "12h")

    df_full = hydrate_symbol(symbol, force=False)
    if df_full.empty:
        return Response("No data.", 500)

    df_view = slice_df(df_full, tf)
    perf, invest = compute_rollups(df_full)  # pills are based on longer history

    # tiles
    tiles: Dict[str, str] = {
        "RSI":   pio.to_html(fig_line(df_view, "rsi", "RSI"), include_plotlyjs=False, full_html=False),
        "PRICE": pio.to_html(fig_price(df_view if not df_view.empty else df_full, symbol), include_plotlyjs=False, full_html=False),
        "MCAP":  pio.to_html(fig_line(df_view if "market_cap" in df_view.columns else df_full, "market_cap", "Market Cap"), include_plotlyjs=False, full_html=False),
        "MACD":  pio.to_html(fig_line(df_view, "macd_line", "MACD"), include_plotlyjs=False, full_html=False),
        "OBV":   pio.to_html(fig_line(df_view, "obv", "OBV"), include_plotlyjs=False, full_html=False),
        "BANDS": pio.to_html(fig_line(df_view, "bb_width", "Bands Width"), include_plotlyjs=False, full_html=False),
        "VOL":   pio.to_html(fig_line(df_view, "volume", "Volume Trend"), include_plotlyjs=False, full_html=False),
        "LIQ":   pio.to_html(fig_line(df_view, "volume", "Liquidity"), include_plotlyjs=False, full_html=False),
        "ADX":   pio.to_html(fig_line(df_view, "adx14", "ADX 14"), include_plotlyjs=False, full_html=False),
        "ATR":   pio.to_html(fig_line(df_view, "atr14", "Volatility (ATR 14)"), include_plotlyjs=False, full_html=False),
        "ALT":   pio.to_html(fig_line(df_view, "alt_momentum", "ALT (Momentum)"), include_plotlyjs=False, full_html=False),
    }

    # tldr line — compact, unchanged placement
    ch_view, _ = compute_rollups(df_view if not df_view.empty else df_full)
    def pct(k):
        v = ch_view.get(k)
        return ("n/a" if v is None else f"{v:+.2f}%")
    tldr_line = (
        f"{symbol}: 1h {pct('1h')}, 4h {pct('4h')}, 12h {pct('12h')}, 24h {pct('24h')}. "
        f"Momentum/Trend vary by timeframe; bands and volume provide context."
    )

    updated = (_latest_ts(df_full) or utcnow()).strftime("UTC %Y-%m-%d %H:%M")

    return render_template(
        "control_panel.html",
        symbol=symbol, tf=tf, updated=updated,
        tiles=tiles, performance=perf, investment=invest, tldr_line=tldr_line
    )

@app.get("/expand_json")
def expand_json():
    symbol = (request.args.get("symbol") or "ETH").upper()
    tf     = (request.args.get("tf") or "12h")
    key    = (request.args.get("key") or "RSI").upper()

    df = load_cached_frame(symbol)
    if df.empty: df = hydrate_symbol(symbol, force=False)
    dfv = slice_df(df, tf)

    # choose figure
    key_map = {
        "PRICE": ("close", fig_price(dfv if not dfv.empty else df, symbol)),
        "RSI":   ("rsi",   fig_line(dfv, "rsi", "RSI")),
        "MACD":  ("macd_line", fig_line(dfv, "macd_line", "MACD")),
        "BANDS": ("bb_width", fig_line(dfv, "bb_width", "Bands Width")),
        "VOL":   ("volume", fig_line(dfv, "volume", "Volume Trend")),
        "LIQ":   ("volume", fig_line(dfv, "volume", "Liquidity")),
        "OBV":   ("obv", fig_line(dfv, "obv", "OBV")),
        "ADX":   ("adx14", fig_line(dfv, "adx14", "ADX 14")),
        "MCAP":  ("market_cap", fig_line(dfv, "market_cap", "Market Cap")),
        "ATR":   ("atr14", fig_line(dfv, "atr14", "Volatility (ATR 14)")),
        "ALT":   ("alt_momentum", fig_line(dfv, "alt_momentum", "ALT (Momentum)")),
    }

    _, fig = key_map.get(key, ("close", fig_line(dfv, "close", key)))
    talk = talk_indicator(key, dfv if not dfv.empty else df)

    return jsonify({"fig": fig.to_plotly_json(), "talk": talk, "tf": tf, "key": key})

@app.get("/api/refresh/<symbol>")
def api_refresh(symbol: str):
    df = hydrate_symbol((symbol or "ETH").upper(), force=True)
    return jsonify({"ok": (not df.empty), "rows": 0 if df is None else len(df)})

# ----- Ask-Luna (keeps same response box; smarter content) -----
def luna_answer(symbol: str, df: pd.DataFrame, tf: str, question: str = "") -> str:
    try:
        return answer_question(symbol, df, tf, question)
    except Exception as e:
        LOG.warning("answer_question fallback: %s", e)
        # Fallback to your legacy summary if anything goes wrong
        view = slice_df(df, tf)
        if view.empty:
            return f"{symbol}: I don’t have enough fresh candles for {tf} yet."
        ch, _ = compute_rollups(view)
        last  = view.iloc[-1]
        price = money(last.get("close"))
        rsi   = to_float(last.get("rsi"))
        macdl = to_float(last.get("macd_line"))
        macds = to_float(last.get("macd_signal"))
        hist  = to_float(last.get("macd_hist"))
        adx   = to_float(last.get("adx14"))
        bbw   = to_float(last.get("bb_width"))
        ser_obv = view["obv"] if "obv" in view.columns else None

        perf_bits = [f"{k} {v:+.2f}%" for k,v in ch.items() if v is not None and k in ("1h","4h","12h","24h")]
        perf_txt  = ", ".join(perf_bits) if perf_bits else "mostly flat"

        side = "bulls" if (macdl is not None and macds is not None and macdl > macds) \
               else "bears" if (macdl is not None and macds is not None and macdl < macds) else "neither side"

        obv_note = "accumulation" if (ser_obv is not None and len(ser_obv.dropna())>5 and (ser_obv.iloc[-1]-ser_obv.iloc[-5])>0) \
                   else "distribution" if (ser_obv is not None and len(ser_obv.dropna())>5 and (ser_obv.iloc[-1]-ser_obv.iloc[-5])<0) \
                   else "neutral flow"

        plan  = "If price breaks and closes beyond the recent band with rising volume, I favor follow‑through; "
        plan += "if the move fizzles and OBV diverges, I fade the breakout."

        return (
            f"{symbol} around {price}. Over this window: {perf_txt}. "
            f"RSI {f1(rsi)}; MACD {('>' if (macdl and macds and macdl>macds) else '<' if (macdl and macds and macdl<macds) else '=')} signal "
            f"(hist {f1(hist)}). ADX {f1(adx)} (trend). Bands width {f1(bbw)}%. OBV shows {obv_note}. "
            f"Short answer: {('bulls have a small edge' if side=='bulls' else 'bears have the edge' if side=='bears' else 'range‑bound')}. "
            f"{plan}"
        )

@app.post("/api/luna")
def api_luna():
    data    = request.get_json(silent=True) or {}
    symbol  = (data.get("symbol") or "ETH").upper()
    tf      = data.get("tf") or "12h"
    text    = (data.get("text") or data.get("question") or "").strip()

    df = load_cached_frame(symbol)
    if df.empty:
        df = hydrate_symbol(symbol, force=False)

    reply = luna_answer(symbol, df, tf, text) if not df.empty else f"{symbol}: I don’t have enough fresh data yet."
    return jsonify({"symbol": symbol, "reply": reply})

# --- helpers: enumerate cached symbols + map queries -------------------------
def _list_cached_symbols(maxn=500) -> list[str]:
    """Symbols we already have locally (csv/parquet) plus known CG_IDS keys."""
    out = set()
    for p in (FRAMES_DIR.glob("*.csv")):
        out.add(p.stem.upper())
    for p in (FRAMES_DIR.glob("*.parquet")):
        out.add(p.stem.upper())
    try:
        for k in (CG_IDS.keys() if 'CG_IDS' in globals() else []):
            out.add(k.upper())
    except Exception:
        pass
    syms = sorted(out)
    return syms[:maxn]

def _resolve_symbol(query: str) -> str | None:
    """Very lightweight resolver: direct symbol hit, or CG_IDS reverse by id/name."""
    if not query:
        return None
    q = query.strip()

    syms = _list_cached_symbols()
    if q.upper() in syms:
        return q.upper()

    try:
        if 'CG_IDS' in globals():
            for sym, cid in CG_IDS.items():
                if cid.lower() == q.lower():
                    return sym.upper()
    except Exception:
        pass

    if q.startswith("0x") and len(q) >= 6:
        return q
    return None

# --- API: suggestions --------------------------------------------------------
@app.get("/api/suggest")
def api_suggest():
    q = (request.args.get("q") or "").strip().upper()
    syms = _list_cached_symbols()
    if q:
        syms = [s for s in syms if q in s]
    return jsonify({"symbols": syms[:20]})

# --- API: resolve query -> symbol -------------------------------------------
@app.get("/api/resolve")
def api_resolve():
    q = (request.args.get("query") or "").strip()
    sym = _resolve_symbol(q)
    if not sym:
        return jsonify({"ok": False, "error": "not found"}), 404
    return jsonify({"ok": True, "symbol": sym})

# ---------- run ----------
if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)
