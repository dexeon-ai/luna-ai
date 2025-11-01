import json, requests, time, os
from pathlib import Path

ROOT = Path(r"C:\Users\jmpat\Desktop\Luna AI")
DATA = ROOT / "luna_cache" / "data"
HIST = DATA / "historical"
LOG = ROOT / "logs" / "delta_update.log"

# Read API keys from .env
from dotenv import dotenv_values
keys = dotenv_values(ROOT / ".env").get("CRYPTOCOMPARE_KEYS", "").split(",")
if not keys or not keys[0]:
    raise SystemExit("No CryptoCompare keys found in .env")

key_index = 0
def get_key():
    global key_index
    key = keys[key_index % len(keys)].strip()
    key_index += 1
    return key

def fetch_latest(symbol):
    url = f"https://min-api.cryptocompare.com/data/histoday"
    params = {"fsym": symbol.upper(), "tsym": "USD", "limit": 1, "api_key": get_key()}
    try:
        r = requests.get(url, params=params, timeout=20)
        j = r.json()
        if j.get("Response") != "Success":
            return None
        return j["Data"][-1]
    except Exception as e:
        print("Error", symbol, e)
        return None

def append_bar(symbol, bar):
    path = HIST / f"{symbol.upper()}.json"
    if not path.exists():
        return
    j = json.load(open(path))
    existing = [d["time"] for d in j["data"]]
    if bar["time"] in existing:
        return
    j["data"].append(bar)
    j["last_updated"] = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
    with open(path, "w", encoding="utf-8") as f:
        json.dump(j, f, indent=2)
    with open(LOG, "a", encoding="utf-8") as log:
        log.write(f"{symbol}: added {bar['time']} close={bar['close']}\n")

def main():
    active_path = DATA / "coin_map_active.json"
    active = json.load(open(active_path, "r", encoding="utf-8"))

    # Handle both formats: ["BTC", "ETH", ...] or [{"Symbol":"BTC"}, ...]
    symbols = []
    if isinstance(active, list):
        if all(isinstance(x, dict) for x in active):
            symbols = [x.get("Symbol") or x.get("symbol") or x.get("ticker") for x in active if (x.get("Symbol") or x.get("symbol") or x.get("ticker"))]
        elif all(isinstance(x, str) for x in active):
            symbols = active
        else:
            print("⚠️ Unrecognized coin_map_active.json structure.")
            return
    else:
        print("⚠️ coin_map_active.json is not a list.")
        return

    print(f"Loaded {len(symbols)} active symbols for delta update.")

    for sym in symbols:
        sym = sym.strip().upper()
        bar = fetch_latest(sym)
        if bar:
            append_bar(sym, bar)
        time.sleep(0.8)  # safe rate limit
    print("✅ Delta update complete.")

if __name__ == "__main__":
    main()
