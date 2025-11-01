import json, requests, os
DATA_DIR = "luna_cache/data"
os.makedirs(DATA_DIR, exist_ok=True)
out_file = os.path.join(DATA_DIR, "coin_map.json")

print("Fetching full coin list from CoinGecko...")
r = requests.get("https://api.coingecko.com/api/v3/coins/list?include_platform=false", timeout=60)
r.raise_for_status()
coins = r.json()

coin_map = {}
for c in coins:
    coin_id = c["id"]
    symbol  = c["symbol"].upper()
    # build standard icon URL pattern
    icon_url = f"https://assets.coingecko.com/coins/images/{c['id']}/small/{c['id']}.png"
    coin_map[coin_id] = {"symbol": symbol, "icon_url": icon_url}

json.dump(coin_map, open(out_file, "w", encoding="utf-8"), indent=2)
print(f"Wrote {len(coin_map)} entries to {out_file}")
