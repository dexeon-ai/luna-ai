# snapshot.py — Luna Global Snapshot (Dexscreener removed)
# All data + chart from plot_engine (CoinGecko primary)
# Updated: 2025-10-08

from plot_engine import build_tech_panel

def build_snapshot(chain_input: str, contract: str):
    """
    Snapshot for broadcast overlay:
      - Ignores Dexscreener completely
      - Uses our chart engine for metrics + image
      - chain_input is treated as symbol hint (e.g., 'BTC', 'ETH', 'SOL')
    """
    symbol = (chain_input or "BTC").upper()
    print(f"[Snapshot] Building chart snapshot for {symbol}")

    try:
        tech = build_tech_panel(symbol=symbol, out_path="/tmp/tech_panel.png")
        M = tech.get("metrics", {})

        return {
            "ok": True,
            "chain": symbol.title(),
            "contract": contract,
            "token": {"symbol": symbol},
            "market": {
                "price_usd": M.get("price"),
                "change_24h": M.get("pct_24h"),
                "volume_24h_usd": M.get("vol_24h"),
                "fdv_usd": M.get("market_cap"),
                "liquidity_usd": M.get("nearest_support"),  # placeholder for LP-like metric
            },
            "tldr": (
                f"{symbol}: ${M.get('price', 0):,.2f} | "
                f"24h {M.get('pct_24h', 0):+.2f}% | "
                f"MC {_fmt_usd(M.get('market_cap', 0))} | "
                f"From ATH {M.get('from_ath_pct', 0):+.2f}%"
            ),
        }
    except Exception as e:
        print("[Snapshot Error]", e)
        return {"ok": False, "error": str(e)}

def _fmt_usd(v):
    try:
        f = float(v)
        if f >= 1_000_000_000: return f"{f/1_000_000_000:.2f}B"
        if f >= 1_000_000:     return f"{f/1_000_000:.2f}M"
        if f >= 1_000:         return f"{f/1_000:.2f}K"
        return f"{f:,.2f}"
    except:
        return str(v)
