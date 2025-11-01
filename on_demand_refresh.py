# ============================================================
# on_demand_refresh.py — Fetch only the missing hours for a coin
# Uses CryptoCompare histohour to append deltas to CSV, then
# regenerates analysis JSON via luna_analyzer.analyze_coin.
# ============================================================

import os, json, time, math, pathlib, requests
from datetime import datetime, timezone
import pandas as pd

ROOT = pathlib.Path(__file__).parent.resolve()
DATA_DIR = ROOT / "luna_cache" / "data"
COINS_DIR = DATA_DIR / "coins"
ANALYSIS_DIR = DATA_DIR / "analysis"
MAP_PATH = DATA_DIR / "coin_map.json"

CRYPTOCOMPARE_KEY = os.getenv("CRYPTOCOMPARE_KEY", "").strip()
CC_HISTO = "https://min-api.cryptocompare.com/data/v2/histohour"

# ---- utilities ------------------------------------------------

def _now():
    return datetime.now(timezone.utc)

def _load_coin_map():
    if MAP_PATH.exists():
        try:
            return json.loads(MAP_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}

def resolve_symbol(coin_id: str) -> str | None:
    """
    Resolve trading symbol for CryptoCompare.
    Prefers coin_map.json: { coin_id: {"symbol": "BTC", ...} } or { "symbol": "WIF" }.
    Falls back to uppercase name without hyphens.
    """
    m = _load_coin_map().get(coin_id.lower())
    if isinstance(m, dict):
        # try the obvious keys
        for k in ("symbol", "cc_symbol", "ticker"):
            s = m.get(k)
            if isinstance(s, str) and len(s) >= 2:
                return s.upper().replace(" ", "")
    # fallback: uppercase slug
    return coin_id.replace("-", "").upper()

def _fetch_histohour(symbol: str, limit: int, to_ts: int | None) -> list[dict]:
    params = {
        "fsym": symbol,
        "tsym": "USD",
        "limit": min(2000, max(1, int(limit))),  # CC caps at 2000
    }
    if CRYPTOCOMPARE_KEY:
        params["api_key"] = CRYPTOCOMPARE_KEY
    if to_ts:
        params["toTs"] = int(to_ts)
    r = requests.get(CC_HISTO, params=params, timeout=20)
    r.raise_for_status()
    j = r.json()
    if j.get("Response") != "Success":
        raise RuntimeError(f"CC histohour error: {j.get('Message')}")
    return j["Data"]["Data"]  # list of bars

def _compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Small, self‑contained indicators (consistent with your stack)."""
    if df.empty or "price" not in df.columns:
        return df
    out = df.copy().sort_values("timestamp")
    close = out["price"].astype(float)

    # RSI(14)
    d = close.diff()
    up = d.clip(lower=0.0)
    dn = -d.clip(upper=0.0)
    ru = up.ewm(span=14, adjust=False).mean()
    rd = dn.ewm(span=14, adjust=False).mean().replace(0, 1e-9)
    rs = ru / rd
    out["rsi"] = 100 - (100 / (1 + rs))

    # MACD (12,26,9)
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26
    out["macd_line"] = macd_line
    out["macd_signal"] = macd_line.ewm(span=9, adjust=False).mean()
    out["macd_hist"] = out["macd_line"] - out["macd_signal"]

    # Bollinger (20,2)
    mid = close.rolling(20, min_periods=5).mean()
    sd = close.rolling(20, min_periods=5).std()
    out["bb_upper"] = mid + 2 * sd
    out["bb_lower"] = mid - 2 * sd

    # Volume trend (24h MA on "volume")
    if "volume" in out.columns:
        out["volume_trend"] = out["volume"].rolling(24, min_periods=6).mean()

    # Sentiment proxy (24h return)
    out["sentiment"] = close.pct_change(24) * 100.0

    # Fear/Greed proxy (RSI + volume)
    base = out["rsi"].clip(0, 100).fillna(50)
    vt = out.get("volume_trend", 0).fillna(0)
    out["fear_greed"] = ((base * 0.7) + 15 + (vt.clip(-1, 1) * 20)).clip(0, 100)
    out["whale_txn_count"] = 0  # placeholder
    return out

# ---- core -----------------------------------------------------

def ensure_fresh_coin(coin_id: str,
                      stale_minutes: int = 90,
                      max_back_hours: int = 720) -> bool:
    """
    Ensures CSV is up‑to‑date enough for rendering.
    If last point older than 'stale_minutes', fetches ONLY the missing hours
    from CryptoCompare (up to max_back_hours) and appends to CSV.
    Then rebuilds analysis JSON via luna_analyzer.analyze_coin.
    Returns True if it updated, False if already fresh or fetch failed.
    """
    csv_path = COINS_DIR / f"{coin_id}.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    if not csv_path.exists():
        # No baseline: we need at least a seed. Try a small backfill (e.g., last 168h).
        print(f"[Fresh] {coin_id}: no CSV; bootstrapping last 168h …")
        return _append_histohours(coin_id, hours_needed=168)

    df = pd.read_csv(csv_path)
    if "timestamp" not in df.columns or df.empty:
        print(f"[Fresh] {coin_id}: CSV empty/malformed; bootstrapping 168h …")
        return _append_histohours(coin_id, hours_needed=168)

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp")
    if df.empty:
        print(f"[Fresh] {coin_id}: CSV empty after cleaning; bootstrapping 168h …")
        return _append_histohours(coin_id, hours_needed=168)

    last_ts = df["timestamp"].iloc[-1]
    age_min = (_now() - last_ts).total_seconds() / 60.0

    if age_min < stale_minutes:
        # Already fresh enough
        return False

    # Compute missing hours (cap by max_back_hours)
    hours_needed = math.ceil(age_min / 60.0)
    hours_needed = max(1, min(int(hours_needed), int(max_back_hours)))
    return _append_histohours(coin_id, hours_needed=hours_needed)

def _append_histohours(coin_id: str, hours_needed: int) -> bool:
    symbol = resolve_symbol(coin_id)
    if not symbol:
        print(f"[Fresh] {coin_id}: cannot resolve symbol; skip")
        return False

    # Download in chunks of <=2000 hours
    remaining = hours_needed
    to_ts = int(_now().timestamp())
    rows = []

    try:
        while remaining > 0:
            batch = min(2000, remaining)
            data = _fetch_histohour(symbol, batch, to_ts)
            if not data:
                break
            # CryptoCompare returns oldest->newest for histo; last point is toTs
            for item in data:
                t = int(item.get("time", 0))
                if not t:
                    continue
                rows.append({
                    "timestamp": datetime.fromtimestamp(t, tz=timezone.utc),
                    "price": float(item.get("close", 0.0) or 0.0),
                    # use volumeto as proxy for USD volume in that hour
                    "volume_24h": float(item.get("volumeto", 0.0) or 0.0)
                })
            remaining -= batch
            # next chunk ends where the last one started
            to_ts = int(data[0]["time"]) - 1
            # be nice to rate limits
            time.sleep(0.4)

        if not rows:
            print(f"[Fresh] {coin_id}: no bars fetched; skip")
            return False

        # Append to CSV and recompute indicators
        csv_path = COINS_DIR / f"{coin_id}.csv"
        new_df = pd.DataFrame(rows).drop_duplicates(subset=["timestamp"]).sort_values("timestamp")
        if csv_path.exists():
            old = pd.read_csv(csv_path)
            if "timestamp" in old.columns:
                old["timestamp"] = pd.to_datetime(old["timestamp"], utc=True, errors="coerce")
                old = old.dropna(subset=["timestamp"]).sort_values("timestamp")
            df = pd.concat([old, new_df], ignore_index=True)
            df = df.drop_duplicates(subset=["timestamp"], keep="last").sort_values("timestamp")
        else:
            df = new_df

        # Normalize columns expected by the rest of the stack
        if "volume" not in df.columns and "volume_24h" in df.columns:
            df["volume"] = pd.to_numeric(df["volume_24h"], errors="coerce")

        df = _compute_indicators(df)
        df.to_csv(csv_path, index=False)
        print(f"[Fresh] {coin_id}: +{len(new_df)} hours appended → {csv_path}")

        # Immediately rebuild analysis JSON
        try:
            from luna_analyzer import analyze_coin
            analyze_coin(coin_id)
        except Exception as e:
            print(f"[Fresh] {coin_id}: analysis rebuild failed: {e}")

        return True

    except requests.HTTPError as e:
        print(f"[Fresh] {coin_id}: HTTP error: {e}")
        return False
    except Exception as e:
        print(f"[Fresh] {coin_id}: error: {e}")
        return False

# ---- CLI -----------------------------------------------------

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Append missing hourly bars for a coin and rebuild analysis.")
    ap.add_argument("coin", help="coin id, e.g., bitcoin")
    ap.add_argument("--hours", type=int, default=720, help="max hours to pull (default 720)")
    ap.add_argument("--stale", type=int, default=90, help="stale minutes threshold (default 90)")
    args = ap.parse_args()
    updated = ensure_fresh_coin(args.coin, stale_minutes=args.stale, max_back_hours=args.hours)
    print("[Fresh] Updated." if updated else "[Fresh] Already fresh or failed.")
