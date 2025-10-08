# snapshot.py — Luna Global Snapshot Engine (Dexscreener removed)
# Data from CoinGecko (primary) and CoinPaprika (fallback)
# Returns full token snapshot compatible with overlay_card.py
# Updated: 2025-10-08

from plot_engine import build_tech_panel

def build_snapshot(chain_input: str, contract: str):
    """
    Unified global snapshot:
      - Ignores Dexscreener (Cloudflare blocked)
      - Pulls all data + chart from CoinGecko/CoinPaprika
      - Works for BTC, ETH, SOL, or any symbol
    """

    symbol = (chain_input or "BTC").upper()
    print(f"[Snapshot] Building chart snapshot for {symbol}")

    try:
        # Always use chart engine (no Dexscreener)
        tech = build_tech_panel(symbol=symbol, out_path="/tmp/tech_panel.png")
        metrics = tech.get("metrics", {})

        return {
            "ok": True,
            "chain": chain_input,
            "contract": contract,
            "token": {"symbol": symbol},
            "market": {
                "price_usd": metrics.get("price"),
                "change_24h": metrics.get("pct_24h"),
                "volume_24h_usd": metrics.get("vol_24h"),
                "fdv_usd": metrics.get("market_cap"),
                "liquidity_usd": metrics.get("nearest_support"),
            },
            "tldr": (
                f"{symbol}: ${metrics.get('price', 0):,.4f} | "
                f"24h {metrics.get('pct_24h', 0):+.2f}% | "
                f"MCap {metrics.get('market_cap', 0):,.0f}"
            ),
        }

    except Exception as e:
        print("[Snapshot Error]", e)
        return {"ok": False, "error": str(e)}
