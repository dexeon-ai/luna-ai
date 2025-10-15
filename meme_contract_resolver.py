# meme_contract_resolver.py — conservative threaded resolver (8 threads)
import os, csv, json, time, random, requests, threading
from queue import Queue

# ---------------------------------------------------
# CONFIG
# ---------------------------------------------------
INFILE  = "TOP 1000 MEMECOINS 11pm PM OCT 9 2025 - Sheet1.csv"
SEED_JSON = "luna_cache/meme_contracts_seed.json"
OUT_JSON  = "luna_cache/meme_contracts.json"
THREADS = 8                   # safe concurrency
SAVE_INTERVAL = 50            # write progress every N tokens
CG_BASE = "https://api.coingecko.com/api/v3"
DX_BASE = "https://api.dexscreener.io/latest/dex/search/?q="

os.makedirs("luna_cache", exist_ok=True)

# ---------------------------------------------------
# SAFE HTTP HELPERS
# ---------------------------------------------------
def safe_get(url, retries=5, backoff=5):
    for attempt in range(retries):
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 429:
                wait = backoff * (attempt + 1)
                print(f"[429] sleeping {wait}s")
                time.sleep(wait)
                continue
            txt = r.text.strip()
            if not txt:
                time.sleep(backoff * (attempt + 1))
                continue
            return r.json()
        except Exception as e:
            wait = backoff * (attempt + 1)
            print(f"[GET RETRY {attempt+1}/{retries}] {e} → {wait}s")
            time.sleep(wait + random.uniform(0,2))
    return None

def cg_search(q):
    d = safe_get(f"{CG_BASE}/search?query={q}")
    return d.get("coins", []) if d else []

def cg_coin(i):
    d = safe_get(f"{CG_BASE}/coins/{i}?localization=false&tickers=false&market_data=false")
    return d or {}

def dx_search(q):
    try:
        r = requests.get(f"{DX_BASE}{q}", timeout=10)
        return r.json().get("pairs", [])
    except:
        return []

# ---------------------------------------------------
# RESOLVE LOGIC
# ---------------------------------------------------
def best_match(name, symbol):
    coins = cg_search(name)
    sym_low, name_low = symbol.lower(), name.lower()
    for c in coins:
        if (c.get("symbol") or "").lower() == sym_low:
            return c
    for c in coins:
        if (c.get("name") or "").lower() == name_low:
            return c
    return coins[0] if coins else None

def resolve_one(t):
    name, sym = t["name"], t["symbol"]
    cg = best_match(name, sym)
    if not cg: return None, None, None, False
    cgid = cg.get("id")
    meta = cg_coin(cgid) or {}
    plats = (meta.get("platforms") or {}) if isinstance(meta.get("platforms"), dict) else {}
    chain, contract = None, None
    for ch, addr in plats.items():
        if addr:
            chain, contract = ch.lower(), addr
            break
    verified = False
    if contract:
        pairs = dx_search(contract) or dx_search(f"{name} {sym}")
        for p in pairs:
            base = (p.get("baseToken") or {})
            if base.get("address") and base["address"].lower() == contract.lower():
                verified = True
                break
    if contract is None and (cgid in {"bitcoin","ethereum","solana","ripple","binancecoin",
                                      "tron","cardano","dogecoin","toncoin","litecoin"}):
        chain, verified = cgid, True
    return cgid, chain, contract, verified

# ---------------------------------------------------
# CSV PARSER
# ---------------------------------------------------
def parse_csv(path):
    import pandas as pd
    try:
        df = pd.read_csv(path)
    except Exception:
        # fallback for Excel-style export with semicolons or weird encoding
        df = pd.read_csv(path, sep=";", encoding="utf-8", engine="python")
    
    names = []
    for _, row in df.iterrows():
        # try to find a column that contains both name and symbol
        combined = None
        for col in df.columns:
            val = str(row[col])
            if any(x in val for x in ["\n", "Buy", " "]) and not val.lower().startswith("nan"):
                combined = val
                break
        if combined:
            parts = [x.strip() for x in combined.replace("Buy","").split("\n") if x.strip()]
            # most rows look like "1   Pepe\nPEPE"
            name = None
            symbol = None
            if len(parts) >= 2:
                # handle "rank name" pattern
                bits = parts[0].split()
                if bits and bits[0].isdigit():
                    bits = bits[1:]
                name = " ".join(bits)
                symbol = parts[1].upper()
            elif len(parts) == 1:
                name = parts[0]
                symbol = parts[0].upper()
            if name and symbol and name.lower() != "name":
                names.append({"name": name, "symbol": symbol})
    print(f"✅ Parsed {len(names)} meme tokens from CSV")
    return names

# ---------------------------------------------------
# THREAD WORKER
# ---------------------------------------------------
def worker():
    while True:
        item = q.get()
        if item is None:
            break
        key, token = item
        try:
            cg_id, chain, contract, verified = resolve_one(token)
            token.update({"id": cg_id, "chain": chain, "contract": contract, "verified": verified})
            results[key] = token
            print(f"[{len(results)}/{total}] {token['name']} ({token['symbol']}): id={cg_id}, chain={chain}, verified={verified}")
        except Exception as e:
            print(f"[ERR] {token['name']} → {e}")
        finally:
            if len(results) % SAVE_INTERVAL == 0:
                save_progress()
            time.sleep(random.uniform(0.8, 1.2))
            q.task_done()

def save_progress():
    tmp = {"tokens": results}
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(tmp, f, indent=2, ensure_ascii=False)
    print(f"💾 progress saved ({len(results)}/{total})")

# ---------------------------------------------------
# MAIN
# ---------------------------------------------------
if __name__ == "__main__":
    tokens = parse_csv(INFILE)
    total = len(tokens)
    results = {}
    print(f"Parsed {total} meme tokens")

    # preload seed for reference
    seed = {"tokens": {t["symbol"].lower(): t for t in tokens}}
    with open(SEED_JSON, "w", encoding="utf-8") as f:
        json.dump(seed, f, indent=2, ensure_ascii=False)

    q = Queue()
    threads = []
    for i in range(THREADS):
        t = threading.Thread(target=worker)
        t.daemon = True
        t.start()
        threads.append(t)

    for i, token in enumerate(tokens):
        q.put((i, token))

    q.join()
    for _ in range(THREADS):
        q.put(None)
    for t in threads:
        t.join()

    save_progress()
    print(f"\n✅ Completed meme_contract_resolver: {len(results)}/{total} tokens resolved.")
