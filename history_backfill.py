# ============================================================
# history_backfill.py — 180d hourly for ALL + lifetime daily for LEGACY
# Sources: CryptoCompare primary; CoinGecko fallback (free)
# Output:
#   luna_cache/history/hourly/<coin>.csv  (ts, price, volume_usd, mcap?)
#   luna_cache/history/daily/<coin>.csv   (ts, price, volume_usd, mcap?)
# Safe to stop/restart — overwrites atomically.
# ============================================================
import os, time, json, math, csv, requests
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).parent.resolve()
DATA_DIR = ROOT / "luna_cache" / "data"
COINS_DIR = DATA_DIR / "coins"
ANALYSIS_DIR = DATA_DIR / "analysis"
HIST_DIR = DATA_DIR / "history"
HH_DIR = HIST_DIR / "hourly"
DD_DIR = HIST_DIR / "daily"
for d in (HIST_DIR, HH_DIR, DD_DIR): d.mkdir(parents=True, exist_ok=True)

COIN_MAP = DATA_DIR / "coin_map.json"
CC_KEY = os.getenv("CRYPTOCOMPARE_KEY", "")
# CryptoCompare (primary)
CC_HISTO_HOUR = "https://min-api.cryptocompare.com/data/v2/histohour"
CC_HISTO_DAY  = "https://min-api.cryptocompare.com/data/v2/histoday"
# CoinGecko (fallback)
GECKO_CHART = "https://api.coingecko.com/api/v3/coins/{id}/market_chart"

# How we qualify “legacy”
LEGACY_COINS = {
    "bitcoin","ethereum","solana","cardano","xrp","dogecoin","litecoin","tron",
    "polkadot","chainlink","binancecoin","bittorrent","stellar","monero"
}

# ----- helpers
def now_utc(): return datetime.now(timezone.utc)
def ts_to_iso(ts:int): return datetime.utcfromtimestamp(int(ts)).replace(tzinfo=timezone.utc).isoformat()

def load_coin_map():
    if COIN_MAP.exists():
        try: return json.loads(COIN_MAP.read_text(encoding="utf-8"))
        except: pass
    return {}

COINMAP = load_coin_map()

def cc_symbol(coin_id:str)->str|None:
    # Try explicit mapping first
    key = coin_id.lower()
    entry = COINMAP.get(key) or COINMAP.get(key.replace("-","")) or {}
    sym = entry.get("symbol") or entry.get("Symbol")
    if sym: return sym.upper()
    # fallback guess
    return coin_id[:6].upper()

def coin_universe():
    # Always use full coin_map if available, even if local CSVs exist
    if COINMAP:
        return sorted(COINMAP.keys())
    if COINS_DIR.exists():
        return sorted([p.stem for p in COINS_DIR.glob("*.csv") if p.stem])
    return []

def safe_get(d,k,default=None):
    if d is None: return default
    v = d.get(k)
    return v if v not in (None,"","NaN") else default

def write_csv_atomic(path:Path, rows:list[dict]):
    if not rows: return
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["timestamp","price","volume_usd","market_cap"])
        w.writeheader()
        for r in rows: w.writerow(r)
    tmp.replace(path)

# ----- CryptoCompare pulls
def cc_histohour_paged(symbol:str, needed:int=4320):  # 180d * 24h
    rows=[]; to_ts=None
    while len(rows) < needed:
        lim = min(2000, needed-len(rows))
        params = {"fsym":symbol,"tsym":"USD","limit":lim,"aggregate":1}
        if to_ts: params["toTs"]=to_ts
        if CC_KEY: params["api_key"]=CC_KEY
        r = requests.get(CC_HISTO_HOUR, params=params, timeout=20)
        r.raise_for_status()
        data = (r.json().get("Data") or {}).get("Data") or []
        if not data: break
        # prepend batch to build oldest->newest
        batch=[]
        for d in data:
            if not d or d.get("close") in (None,""): continue
            rows_dict = {
                "timestamp": ts_to_iso(d["time"]),
                "price": float(d["close"]),
                "volume_usd": float(d.get("volumeto") or 0.0),
                "market_cap": ""
            }
            batch.append(rows_dict)
        if not batch: break
        rows = batch + rows
        to_ts = int(data[0]["time"]) - 3600  # step earlier
        time.sleep(0.12)  # respect rate limits
    return rows

def cc_histoday_paged(symbol:str, days:int=5000):
    rows=[]; to_ts=None; remaining=days
    while remaining>0:
        lim = min(2000, remaining)
        params = {"fsym":symbol,"tsym":"USD","limit":lim,"aggregate":1}
        if to_ts: params["toTs"]=to_ts
        if CC_KEY: params["api_key"]=CC_KEY
        r = requests.get(CC_HISTO_DAY, params=params, timeout=20)
        r.raise_for_status()
        data = (r.json().get("Data") or {}).get("Data") or []
        if not data: break
        batch=[]
        for d in data:
            if not d or d.get("close") in (None,""): continue
            batch.append({
                "timestamp": ts_to_iso(d["time"]),
                "price": float(d["close"]),
                "volume_usd": float(d.get("volumeto") or 0.0),
                "market_cap": ""
            })
        if not batch: break
        rows = batch + rows
        to_ts = int(data[0]["time"]) - 86400
        remaining -= len(batch)
        time.sleep(0.12)
    return rows

# ----- CoinGecko fallback
def gecko_chart(coin_id:str, days:str, interval:str):
    url = GECKO_CHART.format(id=coin_id)
    r = requests.get(url, params={"vs_currency":"usd","days":days,"interval":interval}, timeout=20)
    if r.status_code!=200: return []
    j = r.json()
    prices = j.get("prices") or []
    caps   = j.get("market_caps") or []
    vols   = j.get("total_volumes") or []
    # align by index if lengths match; otherwise price only
    rows=[]
    for i,pp in enumerate(prices):
        ts_ms, price = pp
        mcap = caps[i][1] if i < len(caps) else ""
        vol  = vols[i][1] if i < len(vols) else ""
        rows.append({
            "timestamp": ts_to_iso(int(ts_ms/1000)),
            "price": float(price),
            "volume_usd": float(vol) if vol not in ("",None) else "",
            "market_cap": float(mcap) if mcap not in ("",None) else ""
        })
    return rows

def gecko_hourly_180(coin_id:str):
    return gecko_chart(coin_id, days="180", interval="hourly")

def gecko_daily_max(coin_id:str):
    return gecko_chart(coin_id, days="max", interval="daily")

# ----- main work
def backfill_for_coin(coin_id:str):
    sym = cc_symbol(coin_id)

    # Hourly 180d
    hh_path = HH_DIR / f"{coin_id}.csv"
    if not hh_path.exists():
        rows = []
        try:
            rows = cc_histohour_paged(sym, needed=4320)
        except Exception as e:
            print(f"[{coin_id}] CC histohour fail ({e}); trying Gecko hourly…")
        if not rows:
            try:
                rows = gecko_hourly_180(coin_id)
            except Exception as e:
                print(f"[{coin_id}] Gecko hourly fail ({e})")
        if rows:
            write_csv_atomic(hh_path, rows)

    # Daily lifetime for LEGACY only
    if coin_id in LEGACY_COINS:
        dd_path = DD_DIR / f"{coin_id}.csv"
        if not dd_path.exists():
            rows = []
            try:
                # 20 years ~= 7300 days
                rows = cc_histoday_paged(sym, days=7300)
            except Exception as e:
                print(f"[{coin_id}] CC histoday fail ({e}); trying Gecko daily…")
            if not rows:
                try:
                    rows = gecko_daily_max(coin_id)
                except Exception as e:
                    print(f"[{coin_id}] Gecko daily fail ({e})")
            if rows:
                write_csv_atomic(dd_path, rows)

def main():
    coins = coin_universe()
    total = len(coins)
    print(f"[History] Backfill start — {total} coins")
    done = 0
    for c in coins:
        try:
            backfill_for_coin(c)
        except Exception as e:
            print(f"[WARN] {c}: {e}")
        done += 1
        if done % 200 == 0:
            print(f"  …{done}/{total}")
    print(f"[History] ✅ Done at {now_utc().strftime('UTC %Y-%m-%d %H:%M')}")

if __name__ == "__main__":
    main()
