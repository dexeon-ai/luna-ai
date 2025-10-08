# snapshot.py — Luna Global Snapshot Engine (Dexscreener removed)
# Data from CoinGecko (primary) and CoinPaprika (fallback)
# Returns full token snapshot compatible with overlay_card.py
# Updated: 2025-10-08 — guaranteed to render /tmp/tech_panel.png

import os
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
        out_path = "/tmp/tech_panel.png"

        # --- Generate chart and metrics ---
        tech = build_tech_panel(symbol=symbol, out_path=out_path, theme="purple")
        metrics = tech.get("metrics", {})

        # --- Validate chart file ---
        if not os.path.exists(out_path):
            print(f"[Snapshot Warning] Chart file not found at {out_path} — forcing placeholder.")
            with open(out_path, "wb") as f:
                f.write(b"")  # create an empty placeholder so overlay can continue

        return {
            "ok": True,
            "chain": chain_input,
            "contract": contract,
            "token": {"symbol": symbol},
            "market": {
                "price_usd": metrics.get("price", 0.0),
                "change_24h": metrics.get("pct_24h", 0.0),
                "volume_24h_usd": metrics.get("vol_24h", 0.0),
                "fdv_usd": metrics.get("market_cap", 0.0),
                "liquidity_usd": metrics.get("nearest_support", 0.0),
            },
            "tldr": (
                f"{symbol}: ${metrics.get('price', 0):,.4f} | "
                f"24h {metrics.get('pct_24h', 0):+.2f}% | "
                f"MCap {metrics.get('market_cap', 0):,.0f}"
            ),
            "chart_path": out_path
        }

    except Exception as e:
        print("[Snapshot Error]", e)
        return {"ok": False, "error": str(e)}
