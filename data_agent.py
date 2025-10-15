# data_agent_v3.py — staggered hourly raw data collector for core + meme assets
import os, json, time, csv, random, requests, argparse
from datetime import datetime, timezone
from symbol_buckets import BUCKETS   # auto-generated bucket file

# === Coingecko Pro key wiring ===
CG_API_KEY = "CG-mVmG196hWJwz9tjqLZvmmTSQ"  # CoinGecko Pro API key

# === Inputs ===
CONTRACTS_JSON = "luna_cache/contracts.json"          # output of contract_resolver.py
SEED_JSON      = "luna_cache/contracts_seed.json"     # fallback if not yet resolved

# === Outputs ===
CACHE_DIR  = "luna_cache"
BRAIN_JSON = os.path.join(CACHE_DIR, "luna_brain.json")
DATA_DIR   = os.path.join(CACHE_DIR, "data", "coins")   # core cryptos
MEME_DIR   = os.path.join(CACHE_DIR, "data", "memes")   # meme coins

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(MEME_DIR, exist_ok=True)

CG_BASE = "https://pro-api.coingecko.com/api/v3" if CG_API_KEY else "https://api.coingecko.com/api/v3"
DX_SEARCH = "https://api.dexscreener.io/latest/dex/search/?q="

# Revised schedule: alternating CORE and MEME buckets to prevent overlap
SCHEDULE_MINUTES = {
     0: "CORE_1",
     3: "MEME_1",
     7: "CORE_2",
    10: "MEME_2",
    14: "CORE_3",
    17: "MEME_3",
    21: "CORE_4",
    24: "MEME_4",
    28: "CORE_5",
    31: "MEME_5",
    35: "CORE_6",
    38: "MEME_6",
    42: "CORE_7",
    45: "MEME_7",
    49: "CORE_8",
    52: "MEME_8"
}

def now_utc_hour_str():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:00 UTC")

def load_contracts():
    path = CONTRACTS_JSON if os.path.exists(CONTRACTS_JSON) else SEED_JSON
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

# ---- Fetchers ---------------------------------------------------------------
def fetch_cg_by_id(cg_id):
    if not cg_id:
        return None
    url = f"{CG_BASE}/coins/{cg_id}?localization=false&tickers=false&market_data=true"
    headers = {"x-cg-pro-api-key": CG_API_KEY} if CG_API_KEY else {}
    for attempt in range(6):
        try:
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code == 429 or not r.text.strip():
                wait = 5 * (attempt + 1)
                print(f"[CG 429] Rate limit, sleeping {wait}s…")
                time.sleep(wait)
                continue
            data = r.json()
            m = data.get("market_data") or {}
            return {
                "price": (m.get("current_price") or {}).get("usd"),
                "volume_24h": (m.get("total_volume") or {}).get("usd"),
                "market_cap": (m.get("market_cap") or {}).get("usd"),
            }
        except Exception as e:
            wait = 5 * (attempt + 1)
            print(f"[CG RETRY {attempt+1}] {e} → wait {wait}s")
            time.sleep(wait + random.uniform(0, 2))
    return None

def fetch_dx(query):
    try:
        r = requests.get(f"{DX_SEARCH}{query}", timeout=10)
        pairs = r.json().get("pairs") or []
        if not pairs:
            return None
        p = pairs[0]
        return {
            "price_dex": float(p.get("priceUsd") or 0) if p.get("priceUsd") else None,
            "liquidity_usd": (p.get("liquidity") or {}).get("usd"),
            "fdv": p.get("fdv"),
            "volume_dex_24h": (p.get("volume") or {}).get("h24"),
        }
    except Exception as e:
        print("[DX ERR]", e)
        return None

# ---- Core brain updater -----------------------------------------------------
def update_brain(bucket_key):
    contracts_blob = load_contracts()
    token_map = contracts_blob["tokens"]
    bucket = BUCKETS.get(bucket_key, [])
    timestamp = now_utc_hour_str()

    # decide which output folder to use
    out_dir = MEME_DIR if "meme" in bucket_key.lower() else DATA_DIR

    # load existing brain
    brain = {}
    if os.path.exists(BRAIN_JSON):
        try:
            with open(BRAIN_JSON, "r", encoding="utf-8") as f:
                brain = json.load(f)
        except Exception:
            brain = {}
    if timestamp not in brain:
        brain[timestamp] = {}

    print(f"\n🧠 Updating bucket {bucket_key} @ {timestamp} — {len(bucket)} assets")

    for entry in bucket:
        key = entry["key"]
        tok = token_map.get(key) or {}
        cg_id = tok.get("id")
        contract = tok.get("contract")
        name = tok.get("name") or entry["name"]
        symbol = tok.get("symbol") or entry["symbol"]

        cg = fetch_cg_by_id(cg_id) if cg_id else None
        dx = None
        if contract:
            dx = fetch_dx(contract)
            if dx is None:
                dx = fetch_dx(f"{name} {symbol}")
        else:
            dx = fetch_dx(f"{name} {symbol}")

        merged = {}
        if cg: merged.update(cg)
        if dx: merged.update(dx)

        brain[timestamp][key] = {
            "name": name,
            "symbol": symbol,
            "id": cg_id,
            "contract": contract,
            **merged
        }

        # write per-asset CSV
        path = os.path.join(out_dir, f"{symbol.lower()}.csv")
        new = not os.path.exists(path)
        with open(path, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if new:
                w.writerow(["timestamp","price","volume_24h","market_cap",
                            "price_dex","liquidity_usd","fdv","volume_dex_24h"])
            w.writerow([
                timestamp,
                merged.get("price"),
                merged.get("volume_24h"),
                merged.get("market_cap"),
                merged.get("price_dex"),
                merged.get("liquidity_usd"),
                merged.get("fdv"),
                merged.get("volume_dex_24h"),
            ])
        time.sleep(1.0 + random.uniform(0.2, 0.8))  # polite delay

    with open(BRAIN_JSON, "w", encoding="utf-8") as f:
        json.dump(brain, f, indent=2)
    print(f"✅ Saved {BRAIN_JSON} for {bucket_key}")

# ---- Main scheduler --------------------------------------------------------- 
def main_loop():
    print("⏱️ data_agent_v3 started. Buckets will run at minutes:", sorted(SCHEDULE_MINUTES.keys()))
    while True:
        now = datetime.now(timezone.utc)
        minute = now.minute
        if minute in SCHEDULE_MINUTES:
            bucket_key = SCHEDULE_MINUTES[minute]
            update_brain(bucket_key)
            time.sleep(60)  # avoid double-run
        else:
            time.sleep(5)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", help="Run one specific bucket (e.g. CORE_1 or MEME_1)", default=None)
    args = parser.parse_args()

    if args.bucket:
        update_brain(args.bucket)
    else:
        main_loop()