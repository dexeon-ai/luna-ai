# luna_engine/signals_scores.py
import math

def _state_rsi(rsi: float) -> str:
    if rsi is None or math.isnan(rsi): return "neutral"
    if rsi >= 70: return "overbought"
    if rsi <= 30: return "oversold"
    return "neutral"

def compute_signals(latest: dict, df) -> dict:
    price   = latest["price"]
    ema20   = latest["ema20"]
    ema50   = latest["ema50"]
    ema200  = latest["ema200"]
    bbw     = latest["bb_width"]
    vol_z   = latest.get("vol_z")

    # Regime
    if price > ema200 and ema20 > ema50 > ema200:
        regime = "uptrend"
    elif price < ema200 and ema20 < ema50 < ema200:
        regime = "downtrend"
    else:
        regime = "range"

    # Breakout / squeeze
    high20  = latest["high_20d"]
    breakout = (price > 1.005 * high20) if high20 else False

    try:
        recent_bbw = df["bb_width"].tail(24*90).dropna()
        if len(recent_bbw) >= 60:
            threshold = recent_bbw.quantile(0.20)
            squeeze = bbw <= threshold
        else:
            squeeze = False
    except Exception:
        squeeze = False

    # MACD cross
    macd_hist = df["macd_hist"].tail(3).tolist()
    if len(macd_hist) >= 2:
        if macd_hist[-2] < 0 and macd_hist[-1] > 0: macd_cross = "bull"
        elif macd_hist[-2] > 0 and macd_hist[-1] < 0: macd_cross = "bear"
        else: macd_cross = "none"
    else:
        macd_cross = "none"

    return {
        "regime": regime,
        "breakout": breakout,
        "squeeze": squeeze,
        "volume_spike": bool(vol_z is not None and vol_z >= 2.0),
        "rsi_state": _state_rsi(latest["rsi"]),
        "macd_cross": macd_cross
    }

def compute_scores(latest: dict, signals: dict, mcap: float|None, fdv: float|None, liq: float|None) -> dict:
    # Momentum score
    m_score = 0
    if latest["price"] > latest["ema20"]: m_score += 20
    if latest["price"] > latest["ema50"]: m_score += 20
    if latest["rsi"] > 55:               m_score += 25
    if latest["macd_hist"] > 0:          m_score += 15
    if signals["regime"] == "uptrend":   m_score += 15
    momentum = min(100, m_score)

    # Breakout score
    breakout = 60 if signals["breakout"] else 0
    if signals["volume_spike"]: breakout += 20
    if signals["squeeze"]:      breakout += 20
    breakout = min(100, breakout)

    # Mean reversion score
    mr_score = 0
    if signals["rsi_state"] == "oversold": mr_score += 40
    if latest["price"] < latest["ema20"] and latest["price"] < latest["ema50"]: mr_score += 30
    mean_reversion = min(100, mr_score)

    # Risk (volatility + drawdown)
    rv = latest.get("rv_30d") or 0
    dd = abs(latest.get("dd_30d") or 0)
    risk = max(0, min(100, 20 + 50*rv + 30*dd))

    # Liquidity score (placeholder)
    liquidity = 60 if liq else 40

    # Valuation (FDV vs MCAP)
    val = 60
    try:
        if mcap and fdv:
            ratio = fdv / mcap if mcap > 0 else 1.0
            if ratio <= 1.2:  val = 75
            elif ratio <= 2:  val = 60
            else:             val = 45
    except Exception:
        pass

    return {
        "momentum": int(momentum),
        "breakout": int(breakout),
        "mean_reversion": int(mean_reversion),
        "risk": int(risk),
        "liquidity": int(liquidity),
        "valuation": int(val)
    }
