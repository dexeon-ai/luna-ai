# ============================================================
# Luna AI - Active Coin Filter (market data validator)
# Filters down the clean coin map to only active, tradable coins
# ============================================================

import json
import requests
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# --- Paths ---
ROOT = Path(r"C:\Users\jmpat\Desktop\Luna AI\luna_cache\data")
SRC = ROOT / "coin_map_clean.json"
DST = ROOT / "coin_map_active.json"

# --- API CONFIG ---
CRYPTOCOMPARE_KEYS = [
    "7c245d724f73ccdbef6deeca0575ba037b60dfb60ac3cc9c217cab98d3fbfd4a",
    "4aad2203de2b6107448f88ed88e12547773e7f092ce4c15f96dc08dc9f4c7fbd",
    "467b0aea4b1d0adefd689480d0ce2a4a2590b4d5e8cb91a374781037fef42752",
    "078de3acce4d54f7eb7bf933461998f6834a63fa69bcf2aa6c250176a90aab54"
]
API_BASE = "https://min-api.cryptocompare.com/data/pricemultifull"
MAX_WORKERS = 10

# --- Helpers ---
def get_api_key(i):
    """Rotate API keys to prevent throttling."""
    return CRYPTOCOMPARE_KEYS[i % len(CRYPTOCOMPARE_KEYS)]

def check_coin(symbol, i):
    """Check if coin has valid live market data."""
    key = get_api_key(i)
    params = {"fsyms": symbol, "tsyms": "USD", "api_key": key}
    try:
        r = requests.get(API_BASE, params=params, timeout=10)
        if r.status_code != 200:
            return False
        j = r.json().get("RAW", {}).get(symbol, {}).get("USD", {})
        price = j.get("PRICE")
        volume = j.get("TOTALVOLUME24H")
        mcap = j.get("MKTCAP")

        # Coin must have valid data
        if price and volume and mcap and volume > 1000:
            return True
        return False
    except Exception:
        return False

# --- Load coin map ---
with open(SRC, "r", encoding="utf-8") as f:
    data = json.load(f)

coins = list(data.items())
total = len(coins)
active = {}

print(f"[Luna] 🧠 Checking {total:,} coins for live market activity...")

start = time.time()
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
    futures = {
        exe.submit(check_coin, meta.get("symbol"), i): (cid, meta)
        for i, (cid, meta) in enumerate(coins)
    }

    for n, fut in enumerate(as_completed(futures), 1):
        cid, meta = futures[fut]
        symbol = meta.get("symbol")
        try:
            ok = fut.result()
            status = "✅" if ok else "⚠️"
            print(f"[{n}/{total}] {status} {symbol}")
            if ok:
                active[cid] = meta
        except Exception as e:
            print(f"[{n}/{total}] ❌ {symbol} - error {e}")

elapsed = (time.time() - start) / 60
print(f"\n[Luna] ✅ Active coin filter complete — {len(active):,} / {total:,} valid ({elapsed:.1f} min)")

# --- Save output ---
with open(DST, "w", encoding="utf-8") as f:
    json.dump(active, f, indent=2)

print(f"[Luna] 💾 Saved → {DST}")
