# ==============================================================
# Luna AI — Full Historical Data Pull (CryptoCompare + Rotation)
# ==============================================================

import os
import json
import time
import math
import argparse
import itertools
import traceback
from pathlib import Path
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

# ---------- PATHS / ENV ----------
ROOT = Path(r"C:\Users\jmpat\Desktop\Luna AI")
load_dotenv(ROOT / ".env")

# crypto compare key rotation (comma-separated list in .env)
API_KEYS = [k.strip() for k in os.getenv("CRYPTOCOMPARE_KEYS", "").split(",") if k.strip()]
if not API_KEYS:
    raise SystemExit("❌ No CryptoCompare keys found in .env (CRYPTOCOMPARE_KEYS).")

api_cycle = itertools.cycle(API_KEYS)
SESSION = requests.Session()

# data sources
COIN_MAP_ACTIVE = ROOT / "luna_cache" / "data" / "coin_map_active.json"
COIN_MAP_FALLBACK = ROOT / "luna_cache" / "data" / "coin_map_clean.json"

# output + logs
OUT_DIR = ROOT / "luna_cache" / "data" / "historical"
LOG_DIR = ROOT / "logs"
OUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOG_DIR / "historical_pull.log"
BAD_FILE = LOG_DIR / "rejected_symbols.txt"

# ---------- API CONFIG ----------
BASE_URL = "https://min-api.cryptocompare.com/data/v2/histoday"
LIMIT = 2000               # CryptoCompare returns limit+1 points (2001)
TSYM = "USD"
TIMEOUT = 20
RETRIES_PER_CALL = 6
SLEEP_BETWEEN_CALLS = 0.25  # polite pause between page requests
ONE_DAY = 86400

# ---------- UTIL ----------
def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def log(msg: str) -> None:
    print(msg, flush=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

def append_bad(symbol: str) -> None:
    with open(BAD_FILE, "a", encoding="utf-8") as f:
        f.write(symbol + "\n")

# ---------- DATA PULL ----------
def fetch_hist(symbol: str, to_ts: int | None, verbose: bool=False):
    """
    Fetch one page (≈2001 daily bars) for symbol.
    Uses rotating API keys and exponential backoff.
    Returns list of bars (ascending by time) or [] on error.
    """
    params = {
        "fsym": symbol,
        "tsym": TSYM,
        "limit": LIMIT,
        "api_key": next(api_cycle),
    }
    if to_ts is not None:
        params["toTs"] = to_ts

    for attempt in range(1, RETRIES_PER_CALL + 1):
        if verbose:
            log(f"[{symbol}] 🔵 Attempt {attempt}/{RETRIES_PER_CALL} — fetching history...")

        try:
            r = SESSION.get(BASE_URL, params=params, timeout=TIMEOUT)
            status = r.status_code

            if status == 200:
                data = r.json()
                if data.get("Response") == "Success":
                    bars = data.get("Data", {}).get("Data", []) or []
                    # sometimes the API returns the last candle duplicated with zeroes — filter nonsense
                    cleaned = [b for b in bars if b.get("time") and isinstance(b.get("time"), int)]
                    if verbose:
                        log(f"[{symbol}] HTTP 200")
                        log(f"[{symbol}] ✅ Success ({len(cleaned)} bars)")
                    return cleaned

                # non-success but 200
                message = (data.get("Message") or "").lower()
                if "market does not exist" in message:
                    log(f"[{symbol}] ⏩ Market does not exist on CryptoCompare — skipping.")
                    append_bad(symbol)
                    return []
                if "limit" in message or "rate" in message:
                    # rotate key + cooldown
                    if verbose:
                        log(f"[{symbol}] ⚠️ Rate limit — rotating key & backing off.")
                    time.sleep(0.8)
                    params["api_key"] = next(api_cycle)
                    continue

                # other API message
                log(f"[{symbol}] ⚠️ API message: {data.get('Message')}")
                time.sleep(2)
                continue

            if status == 429:
                log(f"[{symbol}] 🚫 429 Too Many Requests — rotating key & backing off.")
                time.sleep(15)
                params["api_key"] = next(api_cycle)
                continue

            # other HTTP codes
            log(f"[{symbol}] ⚠️ HTTP {status}: {r.text[:120]}")
            time.sleep(2 + attempt)
        except requests.Timeout:
            log(f"[{symbol}] ⏱️ Timeout — retrying.")
            time.sleep(2 + attempt)
        except Exception as e:
            log(f"[{symbol}] ⚠️ Exception: {e}")
            time.sleep(2 + attempt)

    log(f"[{symbol}] ❌ Exhausted retries for this page.")
    return []

def pull_full(symbol: str, debug: bool=False):
    """
    Pull entire daily history by walking backward using toTs.
    - Sets toTs = oldest - 86400 (include boundary avoidance)
    - Stops when:
        * page returns 0/1 bars
        * oldest timestamp stops decreasing (duplicate detection)
        * max pages hit (failsafe)
    Returns sorted list of unique bars.
    """
    allbars: dict[int, dict] = {}
    to_ts = None
    oldest_seen = math.inf
    stale_count = 0
    max_pages = 2000  # ~2000 * 2000 days is far beyond any coin lifetime

    for page in range(1, max_pages + 1):
        bars = fetch_hist(symbol, to_ts, verbose=debug)
        if not bars or len(bars) <= 1:
            if debug:
                log(f"[{symbol}] 🛑 No more data (page {page}).")
            break

        # Merge (deduplicate by timestamp)
        for b in bars:
            t = b["time"]
            allbars[t] = b

        oldest = bars[0]["time"]
        newest = bars[-1]["time"]

        # progress logging
        if page == 1 or page % 5 == 0 or debug:
            log(f"[{symbol}] 📦 page {page} | {len(bars)} bars | range: {oldest} → {newest} | total unique: {len(allbars)}")

        # detect progress
        if oldest >= oldest_seen:
            stale_count += 1
            if stale_count >= 2:
                log(f"[{symbol}] 🌀 Duplicate earliest timestamp detected twice — stopping.")
                break
        else:
            stale_count = 0
            oldest_seen = oldest

        # walk back one more day to avoid including `oldest` again
        to_ts = oldest - ONE_DAY
        time.sleep(SLEEP_BETWEEN_CALLS)

    # return bars sorted ascending
    return [allbars[t] for t in sorted(allbars.keys())]

# ---------- MAIN ----------
def load_coin_map():
    """
    Prefer coin_map_active.json; if missing, fall back to coin_map_clean.json.
    Returns an ordered list of (coin_id, symbol) sorted by symbol ascending.
    """
    path = COIN_MAP_ACTIVE if COIN_MAP_ACTIVE.exists() else COIN_MAP_FALLBACK
    if not path.exists():
        raise SystemExit(f"❌ Missing coin map at {path}. Generate it first.")

    data = json.load(open(path, "r", encoding="utf-8"))

    # normalize and filter
    items = []
    for cid, meta in data.items():
        sym = (meta.get("symbol") or "").upper().strip()
        if not sym:
            continue
        if not sym.isalnum():
            continue
        if len(sym) > 10:
            continue
        if sym in {"000", "GIB"}:
            continue
        items.append((cid, sym))

    # sort by symbol
    items.sort(key=lambda x: x[1])
    return items

def main():
    parser = argparse.ArgumentParser(description="Pull full historical daily data for all active coins.")
    parser.add_argument("--start", type=str, default="", help="Start from this SYMBOL (inclusive), e.g., --start ONDO")
    parser.add_argument("--debug", action="store_true", help="Verbose page logs for the current coin.")
    args = parser.parse_args()

    coins = load_coin_map()
    if args.start:
        start_sym = args.start.upper()
        coins = [(cid, sym) for (cid, sym) in coins if sym >= start_sym]
        log(f"[Luna] 🔁 Starting from {start_sym} onward...")

    total = len(coins)
    log(f"[Luna] 🌙 Pulling full history for {total:,} coins at {now_utc()}")

    for idx, (cid, sym) in enumerate(coins, start=1):
        out_file = OUT_DIR / f"{sym}.json"

        # Skip if already cached
        if out_file.exists():
            # uncomment next line if you want a visible confirmation
            # log(f"[{idx}/{total}] ✅ {sym} already cached — skipping.")
            continue

        try:
            bars = pull_full(sym, debug=args.debug)
            if not bars:
                log(f"[{idx}/{total}] ⚠️ No data for {sym}")
                append_bad(sym)
                continue

            payload = {
                "coin_id": cid,
                "symbol": sym,
                "records": len(bars),
                "source": "cryptocompare_histoday_full",
                "last_updated": now_utc(),
                "data": bars,
            }
            out_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            log(f"[{idx}/{total}] ✅ {sym} ({len(bars)} days) → {out_file}")

        except KeyboardInterrupt:
            log("⛔ Interrupted by user.")
            break
        except Exception as e:
            log(f"[{idx}/{total}] ❌ {sym}: {e}")
            traceback.print_exc()
            append_bad(sym)
            # small pause to avoid hot-loop on fatal errors
            time.sleep(1)

    log(f"[Luna] ✅ Completed at {now_utc()}")

if __name__ == "__main__":
    main()
