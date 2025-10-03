def pct_str(x):
    try:
        if x is None:
            return "n/a"
        val = float(x)
        sign = "+" if val >= 0 else ""
        return f"{sign}{val:.2f}%"
    except Exception:
        return "n/a"

def usd(x, decimals=2):
    try:
        if x is None:
            return "n/a"
        val = float(x)
        if val >= 1_000_000_000:
            return f"${val/1_000_000_000:.2f}B"
        if val >= 1_000_000:
            return f"${val/1_000_000:.2f}M"
        if val >= 1_000:
            return f"${val/1_000:.2f}K"
        return f"${val:.{decimals}f}"
    except Exception:
        return "n/a"

def smart_price(price, fdv=None):
    """Format price. If tiny, emphasize market cap instead."""
    try:
        val = float(price)
        if val >= 0.01:
            return f"${val:.2f}"
        elif val >= 0.0001:
            return f"${val:.6f}"
        elif val > 0:
            cap_str = usd(fdv) if fdv else "?"
            return f"${val:.8f} (focus: MC {cap_str})"
        return "n/a"
    except Exception:
        return "n/a"

def pick_best_pair(pairs, wanted_chain=None):
    if not pairs:
        return None
    candidates = [p for p in pairs if (wanted_chain is None or p.get("chainId") == wanted_chain)]
    if not candidates:
        candidates = pairs
    def liq_usd(p):
        return float(((p.get("liquidity") or {}).get("usd")) or 0.0)
    return max(candidates, key=liq_usd)

def chain_slug(chain: str) -> str:
    c = (chain or "").strip().lower()
    mapping = {
        "sol": "solana", "solana": "solana",
        "eth": "ethereum", "ethereum": "ethereum",
        "bsc": "bsc", "binance": "bsc", "bnb": "bsc",
        "arb": "arbitrum", "arbitrum": "arbitrum",
        "base": "base",
        "opt": "optimism", "optimism": "optimism",
        "avax": "avalanche", "avalanche": "avalanche",
        "polygon": "polygon", "matic": "polygon",
        "fantom": "fantom", "ftm": "fantom",
        "blast": "blast",
        "linea": "linea"
    }
    return mapping.get(c, c)
