# luna_engine/features.py
import numpy as np
import pandas as pd

def _ema(s: pd.Series, span: int) -> pd.Series:
    return s.ewm(span=span, adjust=False, min_periods=max(3, span//3)).mean()

def _rsi(close: pd.Series, period: int = 14) -> pd.Series:
    d = close.diff()
    up = d.clip(lower=0.0)
    dn = -d.clip(upper=0.0)
    rs = up.ewm(span=period, adjust=False).mean() / (dn.ewm(span=period, adjust=False).mean().replace(0, 1e-9))
    return 100 - (100 / (1 + rs))

def compute_features(df: pd.DataFrame):
    out = df.copy().sort_values("timestamp")
    for c in ("price","volume","volume_24h","market_cap","fdv","liquidity_usd"):
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    if "volume" not in out.columns and "volume_24h" in out.columns:
        out["volume"] = out["volume_24h"]

    close = out["price"].astype(float)
    out["ret_1h"] = close.pct_change(1)
    out["ret_24h"] = close.pct_change(24)
    out["ret_7d"] = close.pct_change(24*7)

    # EMAs
    out["ema20"]  = _ema(close, 20)
    out["ema50"]  = _ema(close, 50)
    out["ema200"] = _ema(close, 200)

    # Bollinger
    mid = close.rolling(20, min_periods=5).mean()
    sd  = close.rolling(20, min_periods=5).std()
    if "bb_upper" not in out.columns:
        out["bb_upper"] = mid + 2*sd
    if "bb_lower" not in out.columns:
        out["bb_lower"] = mid - 2*sd
    out["bb_mid"]   = mid
    out["bb_width"] = (out["bb_upper"] - out["bb_lower"]).abs() / mid.replace(0, np.nan)

    # MACD
    ema12 = _ema(close, 12)
    ema26 = _ema(close, 26)
    out["macd_line"]   = ema12 - ema26
    out["macd_signal"] = _ema(out["macd_line"], 9)
    out["macd_hist"]   = out["macd_line"] - out["macd_signal"]

    # RSI
    if "rsi" not in out.columns:
        out["rsi"] = _rsi(close, 14)

    # Volume z-score
    if "volume" in out.columns:
        mu  = out["volume"].rolling(48, min_periods=10).mean()
        sig = out["volume"].rolling(48, min_periods=10).std()
        out["vol_z"] = ((out["volume"] - mu) / sig).clip(-5, 5)

    # Donchian highs/lows
    out["high_20d"] = close.rolling(24*20, min_periods=24).max()
    out["low_20d"]  = close.rolling(24*20, min_periods=24).min()

    # Realized vol, drawdown
    out["rv_7d"]  = (out["ret_1h"].rolling(24*7, min_periods=24).std()*np.sqrt(24)).clip(0, np.inf)
    out["rv_30d"] = (out["ret_1h"].rolling(24*30, min_periods=24).std()*np.sqrt(24)).clip(0, np.inf)
    roll_max = close.rolling(24*30, min_periods=24).max()
    out["dd_30d"] = (close / roll_max - 1.0).fillna(0.0)

    latest = {
        "price": float(close.iloc[-1]),
        "ema20": float(out["ema20"].iloc[-1]),
        "ema50": float(out["ema50"].iloc[-1]),
        "ema200": float(out["ema200"].iloc[-1]),
        "bb_width": float(out["bb_width"].iloc[-1]),
        "rsi": float(out["rsi"].iloc[-1]),
        "macd_hist": float(out["macd_hist"].iloc[-1]),
        "vol_z": float(out["vol_z"].iloc[-1]) if "vol_z" in out.columns else None,
        "rv_30d": float(out["rv_30d"].iloc[-1]) if "rv_30d" in out.columns else None,
        "dd_30d": float(out["dd_30d"].iloc[-1]) if "dd_30d" in out.columns else None,
        "high_20d": float(out["high_20d"].iloc[-1]),
        "low_20d": float(out["low_20d"].iloc[-1]),
    }
    return latest, out
