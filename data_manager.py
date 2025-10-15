# ===========================================================
# data_manager.py — Luna Data Cache Manager
# Handles tiered caching for historical market data (CoinGecko + fallback)
# Stores JSONs in /tmp/luna_cache and auto-prunes older files.
# Updated: 2025-10-13
# ===========================================================

import os, json, time
from pathlib import Path
from plot_engine import fetch_market_chart, _resolve_cg_id

# -----------------------------------------------------------
# Cache setup
# -----------------------------------------------------------
CACHE_DIR = Path("/tmp/luna_cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
MAX_CACHE_FILES = 200  # cap for Render disk
STALE_AFTER = 12 * 3600  # 12 hours

# Tiered fetch duration
TOP10 = ["BTC","ETH","BNB","SOL","XRP","ADA","DOGE","AVAX","MATIC","DOT"]

def _get_days(symbol: str):
    """Return fetch window depending on asset importance."""
    s = symbol.upper()
    if s in TOP10:
        return 180  # cap at 6 months for speed
    elif len(s) <= 5:
        return 730  # ~2 years
    else:
        return 90   # 3 months

def _cache_path(symbol: str):
    return CACHE_DIR / f"{symbol.lower()}.json"

# -----------------------------------------------------------
# Utility helpers
# -----------------------------------------------------------
def compress_timeseries(ts, px, max_points=5000):
    """Downsample old data to keep files lightweight."""
    if not ts or not px:
        return [], []
    if len(ts) <= max_points:
        return ts, px
    step = max(1, len(ts) // max_points)
    return ts[::step], px[::step]

def prune_old_cache():
    """Delete oldest cache files if over limit or older than 30 days."""
    files = sorted(CACHE_DIR.glob("*.json"), key=lambda f: f.stat().st_mtime)
    # Remove oldest beyond limit
    for f in files[:-MAX_CACHE_FILES]:
        try:
            f.unlink()
        except Exception:
            pass
    # Remove files older than 30 days
    cutoff = time.time() - 30*86400
    for f in CACHE_DIR.glob("*.json"):
        try:
            if f.stat().st_mtime < cutoff:
                f.unlink()
        except Exception:
            pass

def get_cached(symbol: str):
    """Return cached data if it exists and is fresh."""
    path = _cache_path(symbol)
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return None
    if time.time() - data.get("timestamp", 0) > STALE_AFTER:
        return None
    return data

def save_cache(symbol: str, ts, px):
    """Save compressed data to cache."""
    ts, px = compress_timeseries(ts, px)
    path = _cache_path(symbol)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump({
                "symbol": symbol.upper(),
                "timestamp": time.time(),
                "points": list(zip(ts, px))
            }, f)
    except Exception as e:
        print(f"[Cache Save ERR] {symbol}: {e}")
    prune_old_cache()

# -----------------------------------------------------------
# Core interface
# -----------------------------------------------------------
def get_or_fetch(symbol: str):
    """Fetch data from cache or API if not cached."""
    # 1. Try cache
    data = get_cached(symbol)
    if data:
        return data

    # 2. Resolve and fetch
    cg_id = _resolve_cg_id(symbol)
    if not cg_id:
        raise RuntimeError(f"Cannot resolve CoinGecko ID for {symbol}")

    days = _get_days(symbol)
    prices, volumes, mcap, timestamps = fetch_market_chart(symbol, days=days)

    if not timestamps or not prices:
        raise RuntimeError(f"No data returned for {symbol}")

    save_cache(symbol, timestamps, prices)
    return {
        "symbol": symbol.upper(),
        "timestamp": time.time(),
        "points": list(zip(timestamps, prices))
    }

# -----------------------------------------------------------
# Dashboard summary
# -----------------------------------------------------------
def list_cache_summary():
    """Return high-level cache stats for dashboard."""
    files = sorted(CACHE_DIR.glob("*.json"), key=lambda f: f.stat().st_mtime, reverse=True)
    total = len(files)
    size = sum(f.stat().st_size for f in files) / 1024 / 1024
    newest = files[0].stat().st_mtime if files else 0
    oldest = files[-1].stat().st_mtime if files else 0
    return {
        "count": total,
        "size_mb": round(size, 2),
        "newest_age_min": round((time.time() - newest) / 60, 1) if newest else None,
        "oldest_age_hr": round((time.time() - oldest) / 3600, 1) if oldest else None,
        "dir": str(CACHE_DIR)
    }

# -----------------------------------------------------------
# Exports
# -----------------------------------------------------------
__all__ = ["CACHE_DIR", "get_or_fetch", "list_cache_summary"]
