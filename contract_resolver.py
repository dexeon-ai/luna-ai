# contract_resolver.py — improved resilient version (v2)
# Resolves Coingecko ID, chain, and contract for each token safely.

import json, time, os, requests, random

SEED_PATH = "luna_cache/contracts_seed.json"
OUT_PATH  = "luna_cache/contracts.json"

CG_BASE = "https://api.coingecko.com/api/v3"
DX_BASE = "https://api.dexscreener.io/latest/dex/search/?q="

# --------------------------------------------------------
# SAFE REQUEST HELPERS
# --------------------------------------------------------
def safe_get(url, retries=6, backoff=5):
    """Call API with retries/back-off when body is empty, 429, or invalid JSON."""
    for attempt in range(retries):
        try:
            r = requests.get(url, timeout=12)
            if r.status_code == 429:  # Rate limit hit
                wait = backoff * (attempt + 1)
                print(f"[CG 429] Rate limit — sleeping {wait}s ...")
                time.sleep(wait)
                continue
            text = r.text.strip()
            if not text:
                wait = backoff * (attempt + 1)
                print(f"[EMPTY BODY] Waiting {wait}s ...")
                time.sleep(wait)
                continue
            data = r.json()
            return data
        except Exception as e:
            wait = backoff * (attempt + 1)
            print(f"[CG RETRY {attempt+1}/{retries}] {e} → waiting {wait}s")
            time.sleep(wait + random.uniform(0, 2))
    return None

def cg_search(query):
    data = safe_get(f"{CG_BASE}/search?query={query}")
    if not data:
        return []
    return data.get("coins", [])

def cg_coin(id_):
    data = safe_get(f"{CG_BASE}/coins/{id_}?localization=false&tickers=false&market_data=false")
    return data or {}

def dx_search(query):
    try:
        r = requests.get(f"{DX_BASE}{query}", timeout=10)
        return r.json().get("pairs", [])
    except Exception as e:
        print("[DX SEARCH ERR]", e)
        return []

# --------------------------------------------------------
# COINGECKO RESOLUTION
# --------------------------------------------------------
def best_cg_match(name, symbol):
    """Prefer exact symbol match, then name match, then top search result."""
    coins = cg_search(name)
    if not coins:
        return None
    sym_low = (symbol or "").lower()
    name_low = (name or "").lower()
    for c in coins:
        if (c.get("symbol") or "").lower() == sym_low:
            return c
    for c in coins:
        if (c.get("name") or "").lower() == name_low:
            return c
    return coins[0]

def resolve_one(token):
    """Return (id, chain, contract, verified)"""
    name, sym = token["name"], token["symbol"]
    cg = best_cg_match(name, sym)
    if not cg:
        return None, None, None, False

    cg_id = cg.get("id")
    meta = cg_coin(cg_id)
    if not meta:
        return cg_id, None, None, False

    platforms = (meta.get("platforms") or {}) if isinstance(meta.get("platforms"), dict) else {}
    chain, contract = None, None
    for ch, addr in platforms.items():
        if addr:
            chain, contract = ch.lower(), addr
            break

    # Cross-check with Dexscreener if possible
    verified = False
    if contract:
        pairs = dx_search(contract)
        if not pairs:
            pairs = dx_search(f"{name} {sym}")
        for p in pairs:
            base = (p.get("baseToken") or {})
            if base.get("address") and base["address"].lower() == contract.lower():
                verified = True
                break

    # Native L1s with no contract
    if contract is None and (cg_id in {
        "bitcoin","ethereum","solana","ripple","binancecoin",
        "tron","cardano","dogecoin","toncoin","litecoin"
    }):
        chain, verified = cg_id, True

    return cg_id, chain, contract, verified

# --------------------------------------------------------
# MAIN LOOP
# --------------------------------------------------------
def main():
    os.makedirs("luna_cache", exist_ok=True)
    with open(SEED_PATH, "r", encoding="utf-8") as f:
        seed = json.load(f)

    tokens = seed["tokens"]
    done, total = 0, len(tokens)

    for key, t in tokens.items():
        # Skip if already resolved
        if t.get("id") and (t.get("verified") or t.get("contract") or t.get("chain")):
            done += 1
            continue

        cg_id, chain, contract, verified = resolve_one(t)
        t["id"] = cg_id
        t["chain"] = chain
        t["contract"] = contract
        t["verified"] = bool(verified)

        done += 1
        print(f"[{done}/{total}] {t['name']} ({t['symbol']}): id={cg_id} chain={chain} contract={contract} verified={t['verified']}")

        # polite pacing with random jitter
        time.sleep(1.5 + random.uniform(0, 1.0))

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(seed, f, indent=2, ensure_ascii=False)
    print(f"\n✅ Wrote resolved contracts → {OUT_PATH}")

if __name__ == "__main__":
    main()
