# ============================================================
# Luna AI — Contract Lookup Utility (Smart Fetch)
# ============================================================

import requests, json, time
from pathlib import Path

DATA_PATH = Path(r"C:\Users\jmpat\Desktop\Luna AI\luna_cache\data")
COINS_DIR = DATA_PATH / "coins"
ANALYSIS_DIR = DATA_PATH / "analysis"
COIN_MAP = DATA_PATH / "coin_map_active.json"

DEX_API = "https://api.dexscreener.com/latest/dex/search?q="

def identify_chain(addr):
    """Simple chain inference."""
    if addr.startswith("0x"): return "evm"
    if len(addr) == 44: return "solana"
    if addr.startswith("base"): return "base"
    return "unknown"

def fetch_dex(addr):
    """Fetch token data from DexScreener."""
    try:
        r = requests.get(DEX_API + addr, timeout=10)
        if r.status_code == 200:
            data = r.json().get("pairs", [])
            if not data: return None
            token = data[0].get("baseToken", {})
            quote = data[0].get("quoteToken", {})
            price = float(data[0].get("priceUsd", 0))
            volume = float(data[0].get("volume", {}).get("h24", 0))
            liquidity = float(data[0].get("liquidity", {}).get("usd", 0))
            name = token.get("name")
            symbol = token.get("symbol")
            return {
                "address": addr,
                "name": name,
                "symbol": symbol,
                "price": price,
                "volume": volume,
                "liquidity": liquidity,
                "chain": identify_chain(addr)
            }
    except Exception as e:
        print(f"[DexFetch] error: {e}")
    return None

def save_to_luna(data):
    cid = data["name"].replace(" ", "-").lower()
    coin_csv = COINS_DIR / f"{cid}.csv"
    coin_json = ANALYSIS_DIR / f"{cid}.json"
    cmap = json.load(open(COIN_MAP, "r", encoding="utf-8"))

    # write CSV snapshot
    with open(coin_csv, "w", encoding="utf-8") as f:
        f.write("timestamp,price,volume_24h,liquidity_usd\n")
        f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')},{data['price']},{data['volume']},{data['liquidity']}\n")

    # write analysis stub
    analysis = {
        "coin_id": cid,
        "symbol": data["symbol"],
        "name": data["name"],
        "chain": data["chain"],
        "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"),
        "overview": {"summary": f"{data['name']} contract lookup added."}
    }
    with open(coin_json, "w", encoding="utf-8") as f:
        json.dump(analysis, f, indent=2)

    # update map
    cmap[cid] = {"symbol": data["symbol"], "name": data["name"]}
    json.dump(cmap, open(COIN_MAP, "w", encoding="utf-8"), indent=2)
    print(f"[Luna] ✅ Saved {data['name']} to Luna cache.")

def lookup(addr):
    print(f"[Luna] 🔍 Searching for {addr} ...")
    data = fetch_dex(addr)
    if not data:
        print(f"[Luna] ⚠️ No data found for {addr}")
        return
    print(f"[Luna] ✅ Found {data['name']} ({data['symbol']}) on {data['chain'].upper()} — ${data['price']:.6f}")
    save_to_luna(data)

if __name__ == "__main__":
    addr = input("Enter contract address: ").strip()
    lookup(addr)
