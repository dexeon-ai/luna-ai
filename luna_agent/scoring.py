# luna_agent/scoring.py
# ----------------------------------------------------
# Turn indicators into a conversational, useful paragraph for Luna.
# No paid services. Deterministic with slight phrasing variety.
# ----------------------------------------------------
from __future__ import annotations
import math
import random
from typing import Dict, Optional, Tuple
import numpy as np
import pandas as pd


def _clip(x, lo=-1.0, hi=1.0):
    return max(lo, min(hi, float(x)))


def _nz(x, val=0.0):
    try:
        if x is None or (isinstance(x, float) and (math.isnan(x) or math.isinf(x))):
            return val
        return float(x)
    except Exception:
        return val


def _last(series: pd.Series):
    if series is None or len(series) == 0:
        return None
    v = series.iloc[-1]
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


def _fmt_pct(x: Optional[float]) -> str:
    return "n/a" if x is None else f"{x:+.2f}%"


def _perf_line(perf: Dict[str, Optional[float]], keys=("1h", "4h", "12h", "24h")) -> str:
    return ", ".join(f"{k} {_fmt_pct(perf.get(k))}" for k in keys)


def _momentum_score(row: pd.Series) -> float:
    rsi14 = _nz(row.get("rsi"), 50.0)
    rsi7  = _nz(row.get("rsi7"), 50.0)
    macd  = _nz(row.get("macd_line"))
    sig   = _nz(row.get("macd_signal"))
    roc14 = _nz(row.get("roc_14"))  # present if metrics.py ran; else 0
    rsi_part  = (rsi14 - 50.0) / 50.0
    macd_part = 0.6 if macd > sig else -0.6 if macd < sig else 0.0
    roc_part  = np.tanh(roc14 * 3.0)
    rsi7_part = 0.25 * ((rsi7 - 50.0) / 50.0)
    return _clip(0.45 * rsi_part + 0.35 * macd_part + 0.20 * roc_part + rsi7_part)


def _trend_score(row: pd.Series) -> float:
    adx = _nz(row.get("adx14"), 15.0)
    ma_align = _nz(row.get("ma_align_score"))
    ma_part = (ma_align - 1.0)  # -1..+1
    dmi_dir = 1.0 if _nz(row.get("macd_line")) > _nz(row.get("macd_signal")) else -1.0
    adx_part = min(adx / 50.0, 1.0) * dmi_dir
    return _clip(0.55 * adx_part + 0.45 * ma_part)


def _structure_score(row: pd.Series) -> float:
    brk = _nz(row.get("breakout_up20")) - _nz(row.get("breakout_dn20"))
    engulf = _nz(row.get("pat_engulf_bull")) - _nz(row.get("pat_engulf_bear"))
    pin = _nz(row.get("pat_pin_bull")) - _nz(row.get("pat_pin_bear"))
    body = _nz(row.get("candle_body_ratio"))
    body_part = (body - 0.5) * 0.6
    patt_part = 0.4 * brk + 0.2 * engulf + 0.1 * pin
    return _clip(patt_part + body_part)


def _liquidity_score(row: pd.Series) -> float:
    vol_ratio = _nz(row.get("vol_ratio"), 1.0)
    obv_slope = _nz(row.get("obv_slope_5"))
    vol_part = np.tanh((vol_ratio - 1.0) * 1.5)
    obv_part = 0.5 * obv_slope
    return _clip(0.7 * vol_part + 0.3 * obv_part)


def _volatility_penalty(row: pd.Series) -> float:
    atr = _nz(row.get("atr14"))
    price = _nz(row.get("close"))
    if price <= 0:
        return 0.2
    atrp = min(atr / price, 0.2)
    return float(atrp)


def _consensus(df_full: pd.DataFrame) -> float:
    try:
        macd = df_full["macd_line"].tail(120)
        sig  = df_full["macd_signal"].tail(120)
        sign = np.sign((macd - sig).rolling(5, min_periods=3).mean())
        out = float(sign.tail(20).mean())
        if math.isnan(out) or math.isinf(out):
            return 0.0
        return _clip(out)
    except Exception:
        return 0.0


def _tone(bias: float) -> str:
    if bias >= 0.15: return "bullish"
    if bias <= -0.15: return "bearish"
    return "neutral"


def _confidence(bias: float, row: pd.Series, df_full: pd.DataFrame) -> int:
    adx = _nz(row.get("adx14"), 15.0)
    adx_f = min(adx / 50.0, 1.0)
    cons = (1.0 + _consensus(df_full)) / 2.0
    vol_pen = _volatility_penalty(row)
    raw = 0.55 * abs(bias) + 0.30 * adx_f + 0.25 * cons - 0.25 * vol_pen
    return int(round(_clip(raw, 0.0, 1.0) * 100))


def _projection_probs(bias: float, row: pd.Series) -> Tuple[int, int, int]:
    vol_pen = _volatility_penalty(row)
    p_up = 0.5 + 0.35 * _clip(bias)
    p_down = 1.0 - p_up
    flat = min(0.35 + 1.5 * vol_pen, 0.6)
    p_up *= (1.0 - flat)
    p_down *= (1.0 - flat)
    total = p_up + p_down + flat
    p_up, p_down, flat = (p_up/total, p_down/total, flat/total)
    return (int(round(p_up*100)), int(round(flat*100)), int(round(p_down*100)))


def generate_analysis(
    symbol: str,
    df_full: pd.DataFrame,
    df_view: pd.DataFrame,
    perf: Dict[str, Optional[float]],
    question: str
) -> str:
    if df_view is None or df_view.empty:
        return f"{symbol}: I don’t have enough fresh data yet."

    row = df_view.iloc[-1]
    price = row.get("close", np.nan)
    price_txt = f"${float(price):,.2f}" if isinstance(price, (int, float)) and math.isfinite(price) else "—"

    momentum = _momentum_score(row)
    trend    = _trend_score(row)
    struct   = _structure_score(row)
    liquid   = _liquidity_score(row)

    bias = _clip(0.35 * momentum + 0.30 * trend + 0.20 * struct + 0.10 * liquid)
    tone = _tone(bias)
    conf = _confidence(bias, row, df_full)
    up_p, flat_p, dn_p = _projection_probs(bias, row)

    perf_line = _perf_line(perf, keys=("1h","4h","12h","24h"))

    q = (question or "").lower()
    horizon = "near term"
    if "tomorrow" in q:
        horizon = "tomorrow"
    elif "week" in q or "7" in q:
        horizon = "this week"
    elif "month" in q or "30" in q:
        horizon = "this month"

    mom_txt = "Momentum is leaning positive" if momentum > 0.15 else "Momentum is fading" if momentum < -0.15 else "Momentum looks mixed"
    trd_txt = "trend structure is improving" if trend > 0.15 else "trend pressure is down" if trend < -0.15 else "trend looks range‑bound"
    str_txt = "structure favors continuation" if struct > 0.15 else "structure hints at a potential reversal" if struct < -0.15 else "structure is balanced"
    liq_txt = "liquidity is above average" if liquid > 0.15 else "liquidity is light" if liquid < -0.15 else "liquidity is typical"

    bbw = row.get("bb_width", np.nan)
    tips = []
    if _nz(row.get("breakout_up20")) > 0:
        tips.append("Watch for a retest of the breakout level; holding above suggests follow‑through.")
    elif _nz(row.get("breakout_dn20")) > 0:
        tips.append("A failed breakdown (fast reclaim) would signal sellers losing grip.")
    if liquid > 0.15:
        tips.append("Sustained bid with rising volume is a healthy tell.")
    elif liquid < -0.15:
        tips.append("Thin books raise fake‑out risk; confirm moves with volume.")
    if isinstance(bbw, (int, float)) and math.isfinite(bbw):
        if bbw < 1.0:  tips.append("Bands are tight; volatility expansion likely.")
        if bbw > 6.0:  tips.append("Wide bands; expect chop until energy resets.")

    tips_txt = " ".join(tips[:2]) if tips else ""

    base = (
        f"{symbol} around {price_txt}. {mom_txt}, {trd_txt}, {str_txt}; {liq_txt}. "
        f"Recent: {perf_line}. Overall bias: **{tone}**, confidence ~{conf}%. "
        f"Projection ({horizon}): ↑ {up_p}%, → {flat_p}%, ↓ {dn_p}%. {tips_txt} "
        "This is educational analysis; conditions can change quickly."
    )
    flavor = random.choice([
        "I’ll keep watching momentum vs. volume into the next few candles.",
        "Key levels matter more than headlines right now.",
        "Let’s see if buyers defend the last higher low.",
        "Breakouts without volume rarely stick; confirmation is everything."
    ])
    return f"{base} {flavor}"
