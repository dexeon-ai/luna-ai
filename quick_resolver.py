# quick_resolver.py — one-shot resolver for unknown coins (fast + safe)
import os, json, time, random, requests, re
from datetime import datetime, timezone

CACHE_DIR = "luna_cache"
NEW_PATH  = os.path.join(CACHE_DIR, "new_contracts.json")
CG_BASE   = "https://api.coingecko.com/api/v3"
DX_BASE   = "https://api.dexscreener.io/latest/dex/search/?q="

os.makedirs(CACHE_DIR, exist_ok=True)

def _safe_get(url, retries=5, backoff=4):
    for i in range(retries):
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 429:
                wait = backoff * (i + 1)
                time.sleep(wait)
                continue
            txt = r.text.strip()
            if not txt:
                time.sleep(backoff * (i + 1))
                continue
            return r.json()
        except Exception:
            time.sleep(backoff * (i + 1) + random.uniform(0,1.5))
    return None

def _normalize_key(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s)
    return s.strip("-")

def cg_search(query):
    d = _safe_get(f"{CG_BASE}/search?query={query}")
    return d.get("coins", []) if d else []

def cg_coin(cg_id):
    d = _safe_get(f"{CG_BASE}/coins/{cg_id}?localization=false&tickers=false&market_data=false")
    return d or {}

def dx_search(q):
    try:
        r = requests.get(f"{DX_BASE}{q}", timeout=10)
        return r.json().get("pairs", [])
    except Exception:
        return []

def best_match(name, symbol=None):
    coins = cg_search(name)
    if not coins: return None
    sym_low = (symbol or "").lower()
    name_low = (name or "").lower()
    for c in coins:
        if (c.get("symbol") or "").lower() == sym_low:
            return c
    for c in coins:
        if (c.get("name") or "").lower() == name_low:
            return c
    return coins[0]

NATIVE_IDS = {
    "bitcoin","ethereum","solana","ripple","binancecoin",
    "tron","cardano","dogecoin","toncoin","litecoin"
}

def quick_resolve(query_name_or_symbol: str):
    """Resolve unknown coin quickly; add/update new_contracts.json, return dict."""
    q = (query_name_or_symbol or "").strip()
    if not q:
        return None

    # Try "NAME (SYMBOL)" → split if present
    name, symbol = q, None
    if "(" in q and q.endswith(")"):
        try:
            name = q.split("(", 1)[0].strip()
            symbol = q.rsplit("(", 1)[1].strip(") ").upper()
        except:
            name, symbol = q, None

    cg = best_match(name, symbol)
    if not cg:
        return None

    cg_id  = cg.get("id")
    names  = cg.get("name") or name
    sym    = (symbol or cg.get("symbol") or "").upper()

    meta = cg_coin(cg_id)
    platforms = meta.get("platforms") if isinstance(meta.get("platforms"), dict) else {}
    chain, contract = None, None
    for ch, addr in (platforms or {}).items():
        if addr:
            chain, contract = ch.lower(), addr
            break

    verified = False
    if contract:
        pairs = dx_search(contract) or dx_search(f"{names} {sym}")
        for p in pairs:
            base = (p.get("baseToken") or {})
            if base.get("address") and base["address"].lower() == contract.lower():
                verified = True
                break

    if contract is None and cg_id in NATIVE_IDS:
        chain, verified = cg_id, True

    # Persist in new_contracts.json
    now = datetime.now(timezone.utc).isoformat()
    blob = {"tokens": {}, "updated_at": now}
    if os.path.exists(NEW_PATH):
        try:
            with open(NEW_PATH, "r", encoding="utf-8") as f:
                blob = json.load(f)
        except Exception:
            pass

    key = _normalize_key(names) or cg_id or sym.lower()
    blob["tokens"][key] = {
        "name": names,
        "symbol": sym,
        "id": cg_id,
        "chain": chain,
        "contract": contract,
        "verified": bool(verified),
        "source": "coingecko+dexscreener",
        "last_resolved": now
    }

    with open(NEW_PATH, "w", encoding="utf-8") as f:
        json.dump(blob, f, indent=2, ensure_ascii=False)

    return blob["tokens"][key]

if __name__ == "__main__":
    # Example CLI usage:
    #   python quick_resolver.py "Pepe (PEPE)"
    import sys
    q = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else ""
    info = quick_resolve(q)
    print(json.dumps(info or {"error": "not_found"}, indent=2))
