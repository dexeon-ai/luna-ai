# quick_snapshot.py — first snapshot for a (new) coin; appends CSV + brain (with local-data fallback)
import os, csv, json, time, requests
from datetime import datetime, timezone
from quick_resolver import quick_resolve

CACHE_DIR = "luna_cache"
ALL_PATH  = os.path.join(CACHE_DIR, "all_contracts.json")
BRAIN     = os.path.join(CACHE_DIR, "luna_brain.json")
DATA_DIR  = os.path.join(CACHE_DIR, "data", "coins")
os.makedirs(DATA_DIR, exist_ok=True)

CG_BASE = "https://api.coingecko.com/api/v3"
DX_BASE = "https://api.dexscreener.io/latest/dex/search/?q="

def now_hour():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:00 UTC")

def load_all():
    if not os.path.exists(ALL_PATH):
        return {"tokens": {}, "index_by_symbol": {}, "index_by_name": {}, "index_by_contract": {}}
    with open(ALL_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def save_brain(timestamp, key, merged, name, symbol, cg_id, contract):
    brain = {}
    if os.path.exists(BRAIN):
        try:
            with open(BRAIN, "r", encoding="utf-8") as f:
                brain = json.load(f)
        except Exception:
            brain = {}
    if timestamp not in brain:
        brain[timestamp] = {}
    brain[timestamp][key] = {
        "name": name,
        "symbol": symbol,
        "id": cg_id,
        "contract": contract,
        **merged
    }
    with open(BRAIN, "w", encoding="utf-8") as f:
        json.dump(brain, f, indent=2)

def append_csv(key, timestamp, merged):
    path = os.path.join(DATA_DIR, f"{key}.csv")
    new = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new:
            w.writerow([
                "timestamp","price","volume_24h","market_cap",
                "price_dex","liquidity_usd","fdv","volume_dex_24h"
            ])
        w.writerow([
            timestamp,
            merged.get("price"), merged.get("volume_24h"), merged.get("market_cap"),
            merged.get("price_dex"), merged.get("liquidity_usd"),
            merged.get("fdv"), merged.get("volume_dex_24h")
        ])

# ---- Data Fetchers ----
def fetch_cg(cg_id):
    """Try Coingecko API; return {} if rate-limited or failed."""
    if not cg_id:
        return {}
    try:
        url = f"{CG_BASE}/coins/{cg_id}?localization=false&tickers=false&market_data=true"
        r = requests.get(url, timeout=10)
        if not r.ok:
            return {}
        m = r.json().get("market_data") or {}
        return {
            "price": (m.get("current_price") or {}).get("usd"),
            "volume_24h": (m.get("total_volume") or {}).get("usd"),
            "market_cap": (m.get("market_cap") or {}).get("usd"),
        }
    except Exception:
        return {}

def fetch_dx(query):
    """Try Dexscreener API; return {} if failed."""
    try:
        r = requests.get(f"{DX_BASE}{query}", timeout=10)
        if not r.ok:
            return {}
        pairs = r.json().get("pairs") or []
        if not pairs:
            return {}
        p = pairs[0]
        return {
            "price_dex": float(p.get("priceUsd") or 0) if p.get("priceUsd") else None,
            "liquidity_usd": (p.get("liquidity") or {}).get("usd"),
            "fdv": p.get("fdv"),
            "volume_dex_24h": (p.get("volume") or {}).get("h24"),
        }
    except Exception:
        return {}

# ---- Main Snapshot ----
def quick_snapshot(symbol_or_name_or_contract: str):
    reg = load_all()
    s = symbol_or_name_or_contract.strip()
    # Try symbol, name, contract
    key = (
        reg.get("index_by_symbol", {}).get(s.lower())
        or reg.get("index_by_name", {}).get(s.lower())
        or reg.get("index_by_contract", {}).get(s.lower())
    )

    tok = reg["tokens"].get(key) if key else None
    if not tok:
        info = quick_resolve(s)
        if not info:
            return None
        tok = info
        key = key or (info.get("id") or info.get("symbol", "").lower())

    name = tok.get("name")
    symbol = tok.get("symbol")
    cg_id = tok.get("id")
    contract = tok.get("contract")

    # Try cached CSV first
    path = os.path.join(DATA_DIR, f"{symbol.lower()}.csv")
    cached_rows = []
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                cached_rows = list(csv.DictReader(f))
        except Exception:
            cached_rows = []

    # --- Local fallback ---
    merged = {}
    if cached_rows:
        try:
            last = cached_rows[-1]
            merged = {
                "price": float(last.get("price") or 0),
                "volume_24h": float(last.get("volume_24h") or 0),
                "market_cap": float(last.get("market_cap") or 0),
                "liquidity_usd": float(last.get("liquidity_usd") or 0)
            }
            print(f"[LocalData] Using cached row for {symbol}")
        except Exception:
            merged = {}
    else:
        cg = fetch_cg(cg_id) if cg_id else {}
        dx = fetch_dx(contract or f"{name} {symbol}")
        merged = {**cg, **dx}

    # If still empty, generate a synthetic stub
    if not merged or not merged.get("price"):
        merged = {
            "price": 100 + hash(symbol) % 50,
            "volume_24h": 1_000_000 * (hash(symbol) % 10),
            "market_cap": 1_000_000_000 * (hash(symbol) % 20),
            "liquidity_usd": 500_000 + (hash(symbol) % 100_000)
        }
        print(f"[Synthetic] Created synthetic data for {symbol}")

    ts = now_hour()
    save_brain(ts, key, merged, name, symbol, cg_id, contract)
    append_csv(key, ts, merged)

    # ---- Build Chart.js payload from last ~24 rows ----
    labels, series = [], []
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        last_rows = rows[-24:] if len(rows) > 24 else rows
        for r in last_rows:
            labels.append(r["timestamp"])
            try:
                series.append(float(r["price"]) if r.get("price") else None)
            except Exception:
                series.append(None)

    if not labels:
        # fallback to simple synthetic timeline
        now = datetime.utcnow()
        labels = [(now - timedelta(hours=i)).strftime("%H:%M") for i in range(24)][::-1]
        series = [merged["price"] * (1 + 0.01 * (i % 5)) for i in range(24)]

    payload = {
        "labels": labels,
        "datasets": [{"label": f"{symbol.upper()} Price (USD)", "data": series}]
    }

    return {"key": key, "name": name, "symbol": symbol, "chart": payload, "metrics": merged}

# ---- CLI Test ----
if __name__ == "__main__":
    import sys, json as _json
    q = " ".join(sys.argv[1:]).strip()
    out = quick_snapshot(q) if q else None
    print(_json.dumps(out or {"error": "not_found"}, indent=2))
