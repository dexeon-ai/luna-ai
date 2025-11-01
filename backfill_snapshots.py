# backfill_snapshots.py  —  Daily snapshots for 30d/1y/ATH + analysis updates
import os, json, time, math, requests
from pathlib import Path
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT = Path(__file__).parent.resolve()
DATA_DIR = ROOT / "luna_cache" / "data"
COINS_DIR = DATA_DIR / "coins"
ANALYSIS_DIR = DATA_DIR / "analysis"
SNAP_DIR = DATA_DIR / "snapshots"
COIN_MAP = DATA_DIR / "coin_map.json"

SNAP_DIR.mkdir(parents=True, exist_ok=True)
ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)

CC_KEY = os.getenv("CRYPTOCOMPARE_KEY", "")
CC_HISTO = "https://min-api.cryptocompare.com/data/v2/histoday"
GECKO_CHART = "https://api.coingecko.com/api/v3/coins/{id}/market_chart?vs_currency=usd&days=max&interval=daily"

HEADERS = {}
TIMEOUT = 20

def now_utc():
    return datetime.now(timezone.utc)

def load_coin_map():
    if COIN_MAP.exists():
        try:
            return json.loads(COIN_MAP.read_text(encoding="utf-8"))
        except:
            pass
    return {}

COINMAP = load_coin_map()

def sym_for(coin_id: str) -> str:
    entry = COINMAP.get(coin_id, {})
    sym = entry.get("symbol") or entry.get("Symbol") or coin_id[:5]
    return sym.upper()

def fetch_cc_histoday(symbol: str, limit: int = 400, to_ts: int | None = None):
    params = {
        "fsym": symbol.upper(),
        "tsym": "USD",
        "limit": limit,
        "aggregate": 1
    }
    if to_ts: params["toTs"] = to_ts
    if CC_KEY: params["api_key"] = CC_KEY
    r = requests.get(CC_HISTO, params=params, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    j = r.json()
    # Structure: {"Data":{"Data":[{"time":..., "close":...}, ...]}}
    data = (j.get("Data") or {}).get("Data") or []
    # keep only items with price
    return [d for d in data if isinstance(d.get("close"), (int,float))]

def fetch_cc_histoday_paged(symbol: str, days_needed: int, max_pages: int = 5):
    out = []
    to_ts = None
    while days_needed > 0 and max_pages > 0:
        batch = fetch_cc_histoday(symbol, limit=min(2000, days_needed), to_ts=to_ts)
        if not batch:
            break
        out[:0] = batch  # prepend older candles at front
        to_ts = batch[0]["time"] - 86400  # page backward
        days_needed -= len(batch)
        max_pages -= 1
        time.sleep(0.15)  # gentle
    return out

def fetch_gecko_daily(coin_id: str):
    url = GECKO_CHART.format(id=coin_id)
    r = requests.get(url, timeout=TIMEOUT)
    if r.status_code != 200:
        return []
    # structure: {"prices":[[ms, price], ...], ...}
    prices = (r.json() or {}).get("prices") or []
    out = []
    for ms, price in prices:
        try:
            out.append({"time": int(ms/1000), "close": float(price)})
        except:
            pass
    return out

def first_price_on_or_after(candles, ts):
    for c in candles:
        if c["time"] >= ts:
            return float(c["close"])
    return None

def candle_max(candles):
    if not candles:
        return None, None
    m = max(candles, key=lambda c: c["close"])
    return float(m["close"]), int(m["time"])

def ensure_snapshots_for_coin(coin_id: str):
    symbol = sym_for(coin_id)
    # Use CC if possible for daily data
    candles = []
    try:
        candles = fetch_cc_histoday(symbol, limit=420)  # ~14 months
        if not candles:
            # fallback: gecko
            candles = fetch_gecko_daily(coin_id)
    except Exception as e:
        # fallback to gecko on any CC error
        candles = fetch_gecko_daily(coin_id)

    # For ATH we try a few CC pages (light), otherwise accept max from the above
    ath_price = ath_ts = None
    try:
        cc_more = fetch_cc_histoday_paged(symbol, days_needed=365*5, max_pages=4)  # up to ~5 years
        use = cc_more or candles
        ath_price, ath_ts = candle_max(use)
    except:
        ath_price, ath_ts = candle_max(candles)

    if not candles:
        # write an empty snapshot so we know we tried
        snap = {
            "coin_id": coin_id, "symbol": symbol,
            "month_open": None, "year_open": None,
            "ath": {"price": ath_price, "ts": datetime.utcfromtimestamp(ath_ts).isoformat()+"Z" if ath_ts else None},
            "source": "none", "last_updated": now_utc().isoformat()
        }
        (SNAP_DIR / f"{coin_id}.json").write_text(json.dumps(snap, indent=2), encoding="utf-8")
        return snap

    # Compute anchors
    now = now_utc()
    ts_30d = int((now - timedelta(days=30)).timestamp())
    ts_1y  = int((now - timedelta(days=365)).timestamp())
    month_open = first_price_on_or_after(candles, ts_30d)
    year_open  = first_price_on_or_after(candles, ts_1y)

    snap = {
        "coin_id": coin_id, "symbol": symbol,
        "month_open": month_open, "year_open": year_open,
        "ath": {"price": ath_price, "ts": datetime.utcfromtimestamp(ath_ts).isoformat()+"Z" if ath_ts else None},
        "source": "cryptocompare" if candles and "time" in candles[0] else "coingecko",
        "last_updated": now.isoformat()
    }
    (SNAP_DIR / f"{coin_id}.json").write_text(json.dumps(snap, indent=2), encoding="utf-8")

    # Merge into analysis JSON so UI gets numbers immediately
    update_analysis_with_snapshots(coin_id, snap)
    return snap

def load_latest_price(coin_id: str):
    p = COINS_DIR / f"{coin_id}.csv"
    if not p.exists(): return None
    try:
        # read last non-empty line efficiently
        *_, last = p.read_text(encoding="utf-8").strip().splitlines()
        cols = last.split(",")
        # naive header handling
        if "timestamp" in last.lower():
            return None
        # price is the 2nd column per your CSV example: timestamp,price,volume_24h,market_cap,...
        return float(cols[1])
    except:
        return None

def pct(latest, base):
    if latest is None or base in (None, 0):
        return None
    return round((latest/base - 1) * 100.0, 2)

def update_analysis_with_snapshots(coin_id: str, snap: dict):
    latest = load_latest_price(coin_id)
    price_change = {}
    inv = {}
    if latest is not None:
        if snap.get("month_open"):
            pc = pct(latest, snap["month_open"]); 
            if pc is not None: 
                price_change["30d"] = pc; inv["30d"] = round(1000*(1+pc/100),2)
        if snap.get("year_open"):
            pc = pct(latest, snap["year_open"]);
            if pc is not None:
                price_change["1y"] = pc; inv["1y"] = round(1000*(1+pc/100),2)
        if snap.get("ath",{}).get("price"):
            pc = pct(latest, snap["ath"]["price"]);
            if pc is not None:
                price_change["all"] = pc; inv["all"] = round(1000*(1+pc/100),2)

    # open or create analysis file
    a_path = ANALYSIS_DIR / f"{coin_id}.json"
    try:
        a = json.loads(a_path.read_text(encoding="utf-8")) if a_path.exists() else {}
    except:
        a = {}
    a.setdefault("coin_id", coin_id)
    a.setdefault("symbol", snap.get("symbol") or coin_id.upper())
    a.setdefault("name", coin_id.capitalize())
    a.setdefault("last_updated", now_utc().isoformat())
    a.setdefault("metrics", {})
    a.setdefault("investment_model", {})
    a["metrics"].setdefault("price_change", {})
    a["metrics"]["price_change"].update(price_change)
    a["investment_model"].setdefault("hypothetical_1000_usd", {})
    a["investment_model"]["hypothetical_1000_usd"].update(inv)

    a_path.write_text(json.dumps(a, indent=2), encoding="utf-8")

def coin_universe():
    # If coin_map.json exists, use its keys; else infer from coin CSV files.
    if COINMAP:
        return sorted(COINMAP.keys())
    if COINS_DIR.exists():
        return sorted([p.stem for p in COINS_DIR.glob("*.csv")])
    return ["bitcoin", "ethereum"]  # minimal fallback

def main(workers=6):
    coins = coin_universe()
    print(f"[Luna Backfill] Snapshotting {len(coins)} coins…")
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(ensure_snapshots_for_coin, c): c for c in coins}
        ok = 0
        for f in as_completed(futs):
            c = futs[f]
            try:
                f.result()
                ok += 1
                if ok % 25 == 0:
                    print(f"  …{ok}/{len(coins)} done")
            except Exception as e:
                print(f"[WARN] {c}: {e}")
    print(f"[Luna Backfill] Done at {now_utc().strftime('UTC %Y-%m-%d %H:%M')}")

if __name__ == "__main__":
    main()
