# data_manager.py — Luna Data Cache Manager
# Handles tiered caching for historical market data (CoinGecko + fallback)
# Stores JSONs in /tmp/luna_cache and auto-prunes older files.
# Updated: 2025-10-08

import os, json, time
from pathlib import Path
from plot_engine import _fetch_prices, _resolve_cg_id

CACHE_DIR = Path("/tmp/luna_cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
MAX_CACHE_FILES = 200  # cap for Render disk
STALE_AFTER = 12 * 3600  # 12 hours

# Tiered fetch duration
TOP10 = ["BTC","ETH","BNB","SOL","XRP","ADA","DOGE","AVAX","MATIC","DOT"]

def _get_days(symbol: str):
    s = symbol.upper()
    if s in TOP10:
        return "max"
    elif len(s) <= 5:
        return 730  # ~2 years
    else:
        return 90   # 3 months

def _cache_path(symbol: str):
    return CACHE_DIR / f"{symbol.lower()}.json"

def compress_timeseries(ts, px, max_points=5000):
    """Downsample old data to keep files lightweight."""
    if len(ts) <= max_points:
        return ts, px
    step = len(ts) // max_points
    return ts[::step], px[::step]

def prune_old_cache():
    """Delete oldest cache files if over limit or older than 30 days."""
    files = sorted(CACHE_DIR.glob("*.json"), key=lambda f: f.stat().st_mtime)
    for f in files[:-MAX_CACHE_FILES]:
        f.unlink()
    cutoff = time.time() - 30*86400
    for f in CACHE_DIR.glob("*.json"):
        if f.stat().st_mtime < cutoff:
            f.unlink()

def get_cached(symbol: str):
    """Return cached data if it exists and is fresh."""
    path = _cache_path(symbol)
    if not path.exists(): return None
    with open(path) as f:
        data = json.load(f)
    if time.time() - data.get("timestamp", 0) > STALE_AFTER:
        return None
    return data

def save_cache(symbol: str, ts, px):
    """Save compressed data to cache."""
    ts, px = compress_timeseries(ts, px)
    path = _cache_path(symbol)
    json.dump({
        "symbol": symbol.upper(),
        "timestamp": time.time(),
        "points": list(zip(ts, px))
    }, open(path, "w"))
    prune_old_cache()

def get_or_fetch(symbol: str):
    """Fetch data from cache or API if not cached."""
    data = get_cached(symbol)
    if data:
        return data
    cg_id = _resolve_cg_id(symbol)
    days = _get_days(symbol)
    ts, px = _fetch_prices(cg_id, days=days)
    if not ts:
        raise RuntimeError(f"No data for {symbol}")
    save_cache(symbol, ts, px)
    return {"symbol": symbol, "points": list(zip(ts, px))}

# ===========================================================
# Shared interface for other modules (server, dashboard, etc.)
# ===========================================================
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

# ✅ expose CACHE_DIR at module level for imports
__all__ = ["CACHE_DIR", "get_or_fetch", "list_cache_summary"]
