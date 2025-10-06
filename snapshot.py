# snapshot.py — real Dexscreener fetch tuned for Solana contracts
import requests

def build_snapshot(chain_input: str, contract: str):
    url = f"https://api.dexscreener.com/latest/dex/pairs/{chain_input}/{contract}"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code != 200:
            return {"ok": False, "error": f"HTTP {r.status_code}"}
        data = r.json()
    except Exception as e:
        return {"ok": False, "error": str(e)}

    pairs = data.get("pairs") or []
    if not pairs:
        return {"ok": False, "error": "Pair not found."}

    p = pairs[0]
    base = p.get("baseToken", {})
    market = p.get("market", {})
    return {
        "ok": True,
        "chain": chain_input,
        "contract": contract,
        "token": {"symbol": base.get("symbol", "—")},
        "market": {
            "price_usd": p.get("priceUsd"),
            "change_24h": (p.get("priceChange") or {}).get("h24"),
            "volume_24h_usd": (p.get("volume") or {}).get("h24"),
            "fdv_usd": p.get("fdv"),
            "liquidity_usd": (p.get("liquidity") or {}).get("usd"),
        },
        "tldr": f"{base.get('symbol')} on {chain_input}: price ${p.get('priceUsd')} | 24h {p.get('priceChange',{}).get('h24')}%",
    }
