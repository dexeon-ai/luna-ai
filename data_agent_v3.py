# ============================================================
# data_agent_v3.py — Luna Hybrid Data Engine (v4: symbol-cache)
# ============================================================
# - Resolves proper trading symbols for coin IDs (e.g., "akash-network" -> "AKT")
# - Caches those mappings in luna_cache/data/symbols.json
# - Pulls ~7d hourly data from CryptoCompare (verify=False for Windows SSL)
# - Skips junk CSVs and throttles requests to avoid rate limits
# - Fetches Fear & Greed Index into luna_cache/data/fear_greed.csv
# ============================================================

from __future__ import annotations
import os, csv, math, time, json, logging, requests, urllib3, re
from datetime import datetime, timezone
from typing import Dict, List, Optional

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ---------- Paths ----------
BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(BASE_DIR, "luna_cache")
DATA_DIR  = os.path.join(CACHE_DIR, "data")
COIN_DIR  = os.path.join(DATA_DIR, "coins")
SYMBOLS_JSON = os.path.join(DATA_DIR, "symbols.json")
os.makedirs(COIN_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

# ---------- Keys ----------
CC_KEY  = os.getenv("CRYPTOCOMPARE_KEY") or ""

# ---------- Logger ----------
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger("LunaDataAgent")

UTC = timezone.utc

# ---------- Config ----------
PER_REQUEST_SLEEP = 1.5      # seconds between API calls to be polite
MAX_HOURS = 168              # ~7 days
KEEP_POINTS = 240            # keep ~10 days hourly
# Optional: cap coins per run for faster iteration (None = all)
MAX_COINS_PER_RUN = None     # e.g., set to 200 to test faster

# ============================================================
# Utilities
# ============================================================
def _ts(dtobj: datetime) -> str:
    return dtobj.replace(tzinfo=UTC).strftime("%Y-%m-%d %H:%M UTC")

def _safe_float(x):
    try:
        f = float(x)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None

def _read_csv(path: str) -> List[Dict[str, str]]:
    if not os.path.exists(path): return []
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def _write_csv(path: str, rows: List[Dict[str, str]]):
    hdr = ["timestamp","price","volume_24h","market_cap",
           "price_dex","liquidity_usd","fdv","volume_dex_24h"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=hdr)
        w.writeheader()
        for r in rows:
            w.writerow({k: str(r.get(k, "")) for k in hdr})

def _merge_dedupe(old_rows, new_rows):
    seen = {r["timestamp"]: r for r in old_rows}
    for r in new_rows:
        seen[r["timestamp"]] = r
    merged = sorted(seen.values(), key=lambda r: r["timestamp"])
    return merged[-KEEP_POINTS:]

def _wait():
    time.sleep(PER_REQUEST_SLEEP)

# ============================================================
# Symbol cache + resolution
# ============================================================
def _load_symbols_cache() -> Dict[str, str]:
    if os.path.exists(SYMBOLS_JSON):
        try:
            return json.load(open(SYMBOLS_JSON, "r", encoding="utf-8"))
        except Exception:
            return {}
    return {}

def _save_symbols_cache(cache: Dict[str, str]):
    try:
        with open(SYMBOLS_JSON, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

SYMBOLS_CACHE = _load_symbols_cache()

def gecko_symbol_for(coin_id: str) -> Optional[str]:
    """Resolve trading symbol using CoinGecko (verify=False to avoid SSL issues)."""
    try:
        url = f"https://api.coingecko.com/api/v3/coins/{coin_id.lower()}"
        r = requests.get(url, timeout=10, verify=False)
        if r.status_code != 200:
            return None
        sym = r.json().get("symbol", "").upper()
        return sym or None
    except Exception:
        return None

_SUFFIX_RE = re.compile(r"-(network|protocol|finance|token|dao|ai|coin|org|ecosystem)$", re.IGNORECASE)

def _smart_guess_symbol(coin_id: str) -> str:
    """Heuristic: strip common suffixes, remove dashes -> uppercase."""
    cid = coin_id.lower()
    cid = _SUFFIX_RE.sub("", cid)
    cid = cid.replace("-", "")
    # Keep only letters/numbers; truncate a reasonable length
    cid = re.sub(r"[^a-z0-9]", "", cid).upper()
    if len(cid) == 0:
        cid = coin_id.upper()[:5]
    return cid[:10]

def resolve_symbol(coin_id: str, csv_exists: bool) -> Optional[str]:
    """Resolve symbol for a coin_id using cache -> gecko -> smart guess + verify with CC."""
    # 1) cache
    cached = SYMBOLS_CACHE.get(coin_id.lower())
    if cached:
        return cached

    # 2) gecko for new or if no CSV yet (fastest coverage)
    sym = None
    if not csv_exists:
        sym = gecko_symbol_for(coin_id)
        if sym:
            SYMBOLS_CACHE[coin_id.lower()] = sym
            _save_symbols_cache(SYMBOLS_CACHE)
            return sym

    # 3) smart guess, then probe CC quickly (limit=3 points to validate)
    guess = _smart_guess_symbol(coin_id)
    if _cc_has_data(guess):
        SYMBOLS_CACHE[coin_id.lower()] = guess
        _save_symbols_cache(SYMBOLS_CACHE)
        return guess

    # 4) if gecko not used yet (csv exists but we still failed), try it now
    if csv_exists and not sym:
        sym = gecko_symbol_for(coin_id)
        if sym and _cc_has_data(sym):
            SYMBOLS_CACHE[coin_id.lower()] = sym
            _save_symbols_cache(SYMBOLS_CACHE)
            return sym

    return None

# ============================================================
# CryptoCompare connector (with verify=False)
# ============================================================
CC_BASE = "https://min-api.cryptocompare.com/data/v2"

def _cc_get(url: str, params: dict) -> Optional[dict]:
    headers = {"authorization": f"Apikey {CC_KEY}"} if CC_KEY else {}
    for attempt in range(3):
        r = requests.get(url, headers=headers, params=params, timeout=20, verify=False)
        if r.status_code == 200:
            return r.json()
        if r.status_code == 429:
            logger.warning("CryptoCompare rate limit hit; waiting...")
            time.sleep(5)
            continue
        logger.warning(f"[CryptoCompare] HTTP {r.status_code} at {url}")
        time.sleep(1)
    return None

def _cc_has_data(symbol: str) -> bool:
    """Quick probe: ask for a tiny history (3 points)."""
    js = _cc_get(f"{CC_BASE}/histohour", {"fsym": symbol.upper(), "tsym": "USD", "limit": 3})
    if not js: return False
    arr = js.get("Data", {}).get("Data", [])
    return bool(arr)

def cc_hist_hour(symbol: str, hours: int = MAX_HOURS) -> List[Dict]:
    js = _cc_get(f"{CC_BASE}/histohour", {"fsym": symbol.upper(), "tsym": "USD", "limit": hours})
    out: List[Dict] = []
    if js:
        arr = js.get("Data", {}).get("Data", [])
        for d in arr:
            t = datetime.fromtimestamp(int(d["time"]), tz=UTC)
            out.append({
                "t": t,
                "price": _safe_float(d.get("close")),
                "vol": _safe_float(d.get("volumeto")),
            })
    return out

# ============================================================
# Main fetch logic
# ============================================================
def fetch_hourly_7d(coin_id: str) -> List[Dict[str, str]]:
    """Return normalized hourly rows for ~7 days using CC; resolve symbol robustly."""
    csv_path = os.path.join(COIN_DIR, f"{coin_id.lower()}.csv")
    csv_exists = os.path.exists(csv_path)

    # Resolve the trading symbol for this coin_id
    sym = resolve_symbol(coin_id, csv_exists)
    if not sym:
        logger.warning(f"[Luna] {coin_id}: cannot resolve a trading symbol; skipping")
        return []

    _wait()
    cc = cc_hist_hour(sym, hours=MAX_HOURS)
    if not cc:
        logger.warning(f"[Luna] {coin_id}: symbol {sym} returned no hourly data on CC; skipping")
        return []

    rows: List[Dict[str, str]] = []
    for c in cc:
        rows.append({
            "timestamp": _ts(c["t"]),
            "price": c.get("price"),
            "volume_24h": c.get("vol"),
            "market_cap": "",
            "price_dex": "",
            "liquidity_usd": "",
            "fdv": "",
            "volume_dex_24h": "",
        })
    logger.info(f"[Luna] {coin_id}: CC OK ({len(cc)} pts) sym={sym}")
    return rows

# ============================================================
# Cache updater
# ============================================================
def update_coin_csv(coin_id: str) -> str:
    """Fetch+merge+write CSV for a coin ID."""
    path = os.path.join(COIN_DIR, f"{coin_id.lower()}.csv")
    new_rows = fetch_hourly_7d(coin_id)
    if not new_rows:
        logger.warning(f"[Luna] No data for {coin_id}")
        return path
    old_rows = _read_csv(path)
    merged = _merge_dedupe(old_rows, new_rows)
    _write_csv(path, merged)
    return path

# ============================================================
# Fear & Greed Index
# ============================================================
def fetch_fear_greed(limit: int = 30) -> List[Dict[str, str]]:
    try:
        url = f"https://api.alternative.me/fng/?limit={limit}&format=json"
        r = requests.get(url, timeout=15, verify=False)
        if r.status_code != 200:
            logger.warning(f"[FNG] HTTP {r.status_code}")
            return []
        arr = r.json().get("data", [])
        arr.reverse()
        rows = []
        for d in arr:
            t = datetime.fromtimestamp(int(d["timestamp"]), tz=UTC)
            rows.append({
                "timestamp": _ts(t),
                "value": d["value"],
                "label": d["value_classification"]
            })
        out = os.path.join(DATA_DIR, "fear_greed.csv")
        with open(out, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["timestamp", "value", "label"])
            w.writeheader()
            for r_ in rows:
                w.writerow(r_)
        logger.info("[Luna] Fear & Greed updated.")
        return rows
    except Exception as e:
        logger.warning(f"[FNG] Failed: {e}")
        return []

# ============================================================
# Discover coin IDs
# ============================================================
def discover_coins() -> List[str]:
    """
    Discover coin IDs from cache folder.
    Skip junk filenames (e.g., '4.csv', '.csv', one-char names).
    """
    files = [f for f in os.listdir(COIN_DIR) if f.endswith(".csv") and not f.startswith(".")]
    ids: List[str] = []
    for f in files:
        name = os.path.splitext(f)[0]
        if len(name.strip()) <= 1:
            continue
        # keep alnum/dash IDs only
        if not re.match(r"^[a-z0-9-]+$", name):
            continue
        ids.append(name)
    ids = sorted(list(set(ids)))
    # Optional: limit per run for speed
    if MAX_COINS_PER_RUN and len(ids) > MAX_COINS_PER_RUN:
        ids = ids[:MAX_COINS_PER_RUN]
    logger.info(f"[Luna] Discovered {len(ids)} coin files.")
    return ids if ids else ["bitcoin", "ethereum", "solana", "dogecoin"]
