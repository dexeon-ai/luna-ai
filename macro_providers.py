import requests
from typing import Dict, Any, Optional

COINGECKO_API = "https://api.coingecko.com/api/v3"

def fetch_global_data() -> Optional[Dict[str, Any]]:
    """
    Fetch global crypto market data from CoinGecko.
    Returns dict with total market cap, volume, BTC dom, stables %, ETH/BTC ratio.
    """
    try:
        r = requests.get(f"{COINGECKO_API}/global", timeout=20)
        r.raise_for_status()
        data = r.json().get("data", {})
        out = {
            "total_mcap_usd": data.get("total_market_cap", {}).get("usd"),
            "total_vol_usd": data.get("total_volume", {}).get("usd"),
            "btc_dom_pct": data.get("market_cap_percentage", {}).get("btc"),
            "eth_dom_pct": data.get("market_cap_percentage", {}).get("eth"),
            "active_cryptos": data.get("active_cryptocurrencies"),
            "markets": data.get("markets"),
            "update_at": data.get("updated_at")
        }
        return out
    except Exception:
        return None

def fetch_trending() -> Optional[Dict[str, Any]]:
    """
    Fetch trending coins from CoinGecko.
    Returns top 5 trending by search popularity.
    """
    try:
        r = requests.get(f"{COINGECKO_API}/search/trending", timeout=20)
        r.raise_for_status()
        coins = r.json().get("coins", [])
        out = []
        for c in coins[:5]:
            item = c.get("item", {})
            out.append({
                "id": item.get("id"),
                "symbol": item.get("symbol"),
                "name": item.get("name"),
                "market_cap_rank": item.get("market_cap_rank")
            })
        return {"top_trending": out}
    except Exception:
        return None

def build_macro_summary() -> Dict[str, Any]:
    """
    Build a macro market context summary with BTC dom, stables %, ETH/BTC.
    """
    global_data = fetch_global_data()
    trending = fetch_trending()
    if not global_data:
        return {"ok": False, "error": "Failed to fetch macro data"}

    total_mcap = global_data.get("total_mcap_usd")
    total_vol = global_data.get("total_vol_usd")
    btc_dom = global_data.get("btc_dom_pct")
    eth_dom = global_data.get("eth_dom_pct")

    summary = f"Global cap {total_mcap/1e9:.1f}B, vol {total_vol/1e9:.1f}B. "
    if btc_dom and eth_dom:
        summary += f"BTC dom {btc_dom:.1f}%, ETH dom {eth_dom:.1f}%. "
        if btc_dom > 55:
            summary += "BTC dominance high → defensive tone. "
        elif btc_dom < 45:
            summary += "BTC dominance low → altcoins outperforming. "
    if trending and trending.get("top_trending"):
        names = ", ".join([t['symbol'].upper() for t in trending["top_trending"]])
        summary += f"Trending: {names}."

    return {
        "ok": True,
        "macro": {
            "global_data": global_data,
            "trending": trending,
            "summary": summary
        }
    }
