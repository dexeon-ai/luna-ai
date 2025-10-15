# trend_agent_v2.py — full analytics & meme/core trend scoring
import json, os

BRAIN_JSON = "luna_cache/luna_brain.json"
TRENDS_JSON = "luna_cache/luna_trends.json"

def safe_float(x):
    try:
        return float(x or 0)
    except:
        return 0.0

def calc_delta(new, old, key):
    n, o = safe_float(new.get(key)), safe_float(old.get(key))
    if o == 0:
        return None
    return round((n - o) / o * 100, 2)

def calc_risk(price_delta, vol_delta, liq_delta):
    """Simple composite risk scoring — 0 (safe) to 100 (high risk)."""
    risk = 50
    if price_delta is None or vol_delta is None or liq_delta is None:
        return risk
    if abs(price_delta) > 20:
        risk += 15
    if abs(vol_delta) > 40:
        risk += 10
    if liq_delta < -30:
        risk += 10
    return min(100, max(0, risk))

def classify_trend(price_delta):
    if price_delta is None:
        return "neutral"
    if price_delta > 3:
        return "bullish"
    if price_delta < -3:
        return "bearish"
    return "neutral"

def main():
    if not os.path.exists(BRAIN_JSON):
        print("❌ No brain file found.")
        return

    with open(BRAIN_JSON, "r", encoding="utf-8") as f:
        brain = json.load(f)

    stamps = sorted(brain.keys())
    if len(stamps) < 2:
        print("⚠️ Need at least 2 snapshots.")
        return

    latest, prev = stamps[-1], stamps[-2]
    latest_snap = brain.get(latest, {})
    prev_snap = brain.get(prev, {})

    trends = {}
    meme_trends = []

    for key, cur in latest_snap.items():
        old = prev_snap.get(key, {})
        price_delta = calc_delta(cur, old, "price")
        vol_delta = calc_delta(cur, old, "volume_24h")
        liq_delta = calc_delta(cur, old, "liquidity_usd")
        fdv_delta = calc_delta(cur, old, "fdv")
        risk_score = calc_risk(price_delta, vol_delta, liq_delta)
        trend = classify_trend(price_delta)

        symbol = cur.get("symbol") or key.upper()
        name = cur.get("name") or key

        trends[key] = {
            "name": name,
            "symbol": symbol,
            "timestamp": latest,
            "delta_price_pct": price_delta,
            "delta_vol_pct": vol_delta,
            "delta_liq_pct": liq_delta,
            "delta_fdv_pct": fdv_delta,
            "price": cur.get("price"),
            "market_cap": cur.get("market_cap"),
            "liquidity_usd": cur.get("liquidity_usd"),
            "volume_24h": cur.get("volume_24h"),
            "fdv": cur.get("fdv"),
            "risk_score": risk_score,
            "trend": trend
        }

        # Track meme coins separately for Top 10 printout
        if key.lower() in ("meme", "pepe", "bonk", "floki", "dogecoin", "shiba", "wif") or "meme" in name.lower():
            meme_trends.append({
                "symbol": symbol,
                "name": name,
                "price_delta": price_delta or 0,
                "vol_delta": vol_delta or 0,
                "trend": trend
            })

    with open(TRENDS_JSON, "w", encoding="utf-8") as f:
        json.dump(trends, f, indent=2)

    print(f"✅ Wrote {TRENDS_JSON} with {len(trends)} assets analyzed.")
    print("\n📊 Top 10 Trending Meme Coins (by price Δ%)")
    meme_trends_sorted = sorted(meme_trends, key=lambda x: x["price_delta"], reverse=True)
    for m in meme_trends_sorted[:10]:
        print(f"{m['symbol']:10} {m['price_delta']:>8.2f}%  | Vol Δ {m['vol_delta']:>8.2f}% | Trend: {m['trend']}")

if __name__ == "__main__":
    main()
