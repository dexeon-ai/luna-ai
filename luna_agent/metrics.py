# luna_agent/metrics.py
# ----------------------------------------------------
# Enrich frames with additional technical indicators (free, offline).
# Uses pandas-ta if available; otherwise falls back to pure pandas math.
# ----------------------------------------------------
from __future__ import annotations
import numpy as np
import pandas as pd

try:
    import pandas_ta as ta
    HAS_PTA = True
except Exception:
    ta = None
    HAS_PTA = False

try:
    import talib
    HAS_TALIB = True
except Exception:
    talib = None
    HAS_TALIB = False


def _roll(s: pd.Series, n: int) -> pd.core.window.Rolling:
    return s.rolling(n, min_periods=max(2, n // 2))


def _safe_div(a, b):
    with np.errstate(divide="ignore", invalid="ignore"):
        out = np.where(b == 0, np.nan, a / b)
    if isinstance(a, pd.Series):
        return pd.Series(out, index=a.index)
    return pd.Series(out)


def enrich_with_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Appends many indicators to the SAME df you're already caching.
    Assumes cols: timestamp, open, high, low, close, volume.
    Returns a new DataFrame with added columns.
    """
    if df is None or df.empty:
        return df

    out = df.copy()
    for c in ("open", "high", "low", "close", "volume"):
        if c not in out.columns:
            out[c] = np.nan
    open_ = out["open"].astype(float)
    high  = out["high"].astype(float)
    low   = out["low"].astype(float)
    close = out["close"].astype(float)
    vol   = out["volume"].fillna(0).astype(float)

    # ------------ Moving Averages ------------
    for L in (5, 10, 20, 50, 100, 200):
        if HAS_PTA:
            out[f"sma_{L}"] = ta.sma(close, length=L)
        else:
            out[f"sma_{L}"] = _roll(close, L).mean()

    for L in (9, 21, 55, 144):
        if HAS_PTA:
            out[f"ema_{L}"] = ta.ema(close, length=L)
        else:
            out[f"ema_{L}"] = close.ewm(span=L, adjust=False).mean()

    # ------------ Stochastic (14,3) ------------
    if HAS_PTA:
        stoch = ta.stoch(high=high, low=low, close=close, k=14, d=3, smooth_k=1)
        if isinstance(stoch, pd.DataFrame) and not stoch.empty:
            # Try to pick the expected columns; fall back by position
            k_col = next((c for c in stoch.columns if "K" in c or "k" in c), stoch.columns[0])
            d_col = next((c for c in stoch.columns if "D" in c or "d" in c), stoch.columns[min(1, len(stoch.columns)-1)])
            out["stoch_k"] = stoch[k_col]
            out["stoch_d"] = stoch[d_col]
    else:
        hh = _roll(high, 14).max()
        ll = _roll(low, 14).min()
        k = _safe_div((close - ll) * 100.0, (hh - ll))
        out["stoch_k"] = k
        out["stoch_d"] = _roll(k, 3).mean()

    # ------------ RSI(7) as a complement to your RSI(14) ------------
    if HAS_TALIB:
        out["rsi7"] = talib.RSI(close.values, timeperiod=7)
    elif HAS_PTA:
        out["rsi7"] = ta.rsi(close, length=7)
    else:
        delta = close.diff()
        up = delta.clip(lower=0.0)
        dn = -delta.clip(upper=0.0)
        rs = up.ewm(span=7, adjust=False).mean() / dn.replace(0, 1e-9).ewm(span=7, adjust=False).mean()
        out["rsi7"] = 100 - (100 / (1 + rs))

    # ------------ ROC ------------
    for L in (5, 14, 30):
        if HAS_PTA:
            out[f"roc_{L}"] = ta.roc(close, length=L)
        else:
            out[f"roc_{L}"] = _safe_div(close, close.shift(L)) - 1.0

    # ------------ Donchian 20 ------------
    out["donchian_h20"] = _roll(high, 20).max()
    out["donchian_l20"] = _roll(low, 20).min()

    # ------------ Volatility measures ------------
    out["std14"] = _roll(close, 14).std()
    out["cv14"]  = _safe_div(out["std14"], _roll(close, 14).mean())

    # Rough HV over last 30 days of returns (constant across index for speed)
    ts = pd.to_datetime(out["timestamp"], utc=True, errors="coerce")
    out["_ts"] = ts
    if not ts.isna().all():
        anchor = ts.iloc[-1]
        recent = out[ts >= (anchor - pd.Timedelta(days=30))]["close"].pct_change().dropna()
        hv30 = float(recent.std() * 100.0) if len(recent) > 5 else np.nan
    else:
        hv30 = np.nan
    out["hv30"] = hv30

    # ------------ Volume / Liquidity ------------
    out["vol_ma20"] = _roll(vol, 20).mean()
    out["vol_ratio"] = _safe_div(vol, out["vol_ma20"]).replace([np.inf, -np.inf], np.nan)
    # OBV slope (5)
    if "obv" in out.columns and not out["obv"].isna().all():
        obv = out["obv"].fillna(0.0)
    else:
        direction = np.sign(close.diff()).fillna(0.0)
        obv = (direction * vol).cumsum()
        out["obv"] = obv
    out["obv_slope_5"] = np.sign(obv - obv.shift(5)).fillna(0.0)

    # ------------ Candle structure ------------
    rng = (high - low).replace(0, np.nan)
    body = (close - open_).abs()
    out["candle_body_ratio"] = (body / rng).clip(0, 1)

    prev_open = open_.shift(1)
    prev_close = close.shift(1)
    out["pat_engulf_bull"] = ((close > open_) & (prev_close < prev_open) & (close >= prev_open) & (open_ <= prev_close)).astype(int)
    out["pat_engulf_bear"] = ((close < open_) & (prev_close > prev_open) & (close <= prev_open) & (open_ >= prev_close)).astype(int)

    lower_shadow = (np.minimum(open_, close) - low).abs()
    upper_shadow = (high - np.maximum(open_, close)).abs()
    with np.errstate(divide="ignore", invalid="ignore"):
        out["pat_pin_bull"] = ((lower_shadow / rng) > 0.6).astype(int)
        out["pat_pin_bear"] = ((upper_shadow / rng) > 0.6).astype(int)

    # ------------ MA alignment (20-50-200) ------------
    def _ma_align(row):
        pos = 0
        if row.get("sma_20", np.nan) > row.get("sma_50", np.nan):  pos += 1
        if row.get("sma_50", np.nan) > row.get("sma_200", np.nan): pos += 1
        return pos  # 0..2
    out["ma_align_score"] = out.apply(_ma_align, axis=1)

    # Breakouts vs Donchian
    out["breakout_up20"] = (close > out["donchian_h20"]).astype(int)
    out["breakout_dn20"] = (close < out["donchian_l20"]).astype(int)

    out.drop(columns=["_ts"], errors="ignore", inplace=True)
    return out
