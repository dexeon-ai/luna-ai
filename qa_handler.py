# qa_handler.py — Luna QA router (CoinGecko-based technical analysis)
from plot_engine import build_tech_panel

def handle_question(data):
    """
    Handles user question and returns metrics + overlay trigger info.
    Uses CoinGecko technical chart engine (no Dexscreener dependency).
    """

    q = (data.get("question") or "").lower()
    # Default to Bitcoin if the question doesn't specify a token
    if "eth" in q or "ethereum" in q:
        symbol, cg_id = "ETH", "ethereum"
    elif "sol" in q or "solana" in q:
        symbol, cg_id = "SOL", "solana"
    else:
        symbol, cg_id = "BTC", "bitcoin"

    # Generate chart & metrics
    try:
        tech = build_tech_panel(symbol=symbol, cg_id=cg_id, short_days=7)
        m = tech["metrics"]
        summary = (
            f"{symbol} — price ${m['price']:.2f} "
            f"({m['pct_24h']:+.2f}% 24h / {m['pct_7d']:+.2f}% 7d)\n"
            f"Market cap ${m['market_cap']:.0f}, 24h vol ${m['vol_24h']:.0f}.\n"
            f"{abs(m['from_ath_pct']):.1f}% from ATH. "
            f"Nearest support {m['nearest_support']:.2f} | "
            f"resistance {m['nearest_resistance']:.2f}."
        )
        return {
            "ok": True,
            "symbol": symbol,
            "chain": "mainnet",
            "summary": summary,
            "market": m,
        }

    except Exception as e:
        return {"ok": False, "error": f"Chart engine error: {e}"}
