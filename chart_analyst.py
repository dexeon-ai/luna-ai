# chart_analyst.py — lightweight chart intelligence engine for Luna AI
import os, csv, statistics
from pathlib import Path

DATA_DIR = Path("luna_cache/data/coins")

def read_prices(symbol):
    path = DATA_DIR / f"{symbol.lower()}.csv"
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    prices = []
    for r in rows:
        try:
            p = float(r.get("price") or 0)
            if p > 0:
                prices.append(p)
        except:
            pass
    return prices[-72:]  # last 3 days of hourly data (if available)

def analyze_chart(symbol):
    prices = read_prices(symbol)
    if len(prices) < 4:
        return {"ok": False, "error": "not enough data"}

    last = prices[-1]
    avg = statistics.mean(prices)
    stdev = statistics.pstdev(prices)
    change_pct = ((last - prices[0]) / prices[0]) * 100 if prices[0] else 0
    volatility = (stdev / avg) * 100 if avg else 0

    # --- choose overlay tools based on behavior ---
    indicators = []
    insight = ""

    if abs(change_pct) > 5:
        indicators.append("trend_line")
        insight += "Strong directional move detected. "
    if volatility > 3:
        indicators.append("fib_retracement")
        insight += "High volatility, Fibonacci retracement applied. "
    if change_pct > 0 and volatility < 2:
        indicators.append("vwap")
        insight += "Stable uptrend, applying VWAP for fair value zone. "
    if change_pct < 0 and volatility < 2:
        indicators.append("support_resistance")
        insight += "Gradual decline, watching support levels. "

    if not indicators:
        indicators.append("neutral")
        insight = "Flat or low-activity market conditions."

    mood = "bullish" if change_pct > 0 else "bearish" if change_pct < 0 else "neutral"

    return {
        "ok": True,
        "symbol": symbol.upper(),
        "change_pct": round(change_pct, 2),
        "volatility": round(volatility, 2),
        "mood": mood,
        "indicators": indicators,
        "insight": insight.strip()
    }

if __name__ == "__main__":
    import sys, json
    sym = sys.argv[1] if len(sys.argv) > 1 else "bitcoin"
    print(json.dumps(analyze_chart(sym), indent=2))
