# luna_engine/insights_engine.py
def write_insights(symbol: str, latest: dict, signals: dict, scores: dict) -> list[dict]:
    out = []
    if signals["regime"] == "uptrend" and scores["momentum"] >= 60:
        out.append({"title":"Momentum improving",
                    "detail":"Price above EMA20/EMA50, MACD histogram positive; RSI > 55 supports trend continuation.",
                    "severity":"bullish"})
    elif signals["regime"] == "downtrend":
        out.append({"title":"Macro pressure",
                    "detail":"Below EMA200 with short MAs aligned lower. Rallies may fade unless EMA50 recaptures.",
                    "severity":"bearish"})
    else:
        out.append({"title":"Range conditions",
                    "detail":"Mixed MAs; expect chop unless a breakout clears recent highs.",
                    "severity":"info"})

    if signals["breakout"]:
        out.append({"title":"Breakout",
                    "detail":"Price cleared the prior 20‑day high. Acceptance above level favors continuation.",
                    "severity":"bullish"})
    if signals["squeeze"]:
        out.append({"title":"Compression forming",
                    "detail":"Bollinger width near lowest quintile of 90d; breakouts often follow low‑volatility clusters.",
                    "severity":"info"})

    if signals["rsi_state"] == "oversold":
        out.append({"title":"Mean‑reversion setup",
                    "detail":"RSI < 30; relief rallies toward RSI 50–55 are common.",
                    "severity":"info"})
    elif signals["rsi_state"] == "overbought":
        out.append({"title":"Extended momentum",
                    "detail":"RSI > 70; pullback risk increases, but overbought can persist in strong trends.",
                    "severity":"info"})

    if scores["valuation"] >= 70:
        out.append({"title":"Valuation fair",
                    "detail":"FDV vs Market Cap within balanced range; unlock risk moderate.",
                    "severity":"info"})
    else:
        out.append({"title":"Dilution risk",
                    "detail":"FDV materially exceeds Market Cap; watch unlock schedules.",
                    "severity":"warning"})

    if scores["risk"] >= 70:
        out.append({"title":"Volatility elevated",
                    "detail":"Realized volatility and drawdown are high; consider sizing and wider stops.",
                    "severity":"warning"})
    return out

def support_resistance(latest: dict) -> dict:
    return {
        "support": round(latest["low_20d"], 6) if latest.get("low_20d") else None,
        "resistance": round(latest["high_20d"], 6) if latest.get("high_20d") else None,
        "ema20": round(latest["ema20"], 6),
        "ema50": round(latest["ema50"], 6),
        "ema200": round(latest["ema200"], 6),
    }
