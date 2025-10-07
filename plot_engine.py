# updated manually on 2025-10-07
# plot_engine.py — Luna Technical Chart Engine v1
# - Data source: CoinGecko (no CAPTCHA)
# - Caching: /tmp/luna_cache (short TTL for 7d data, longer for lifetime)
# - Indicators: short-term trendline, trend-based Fibonacci extensions,
#               long-term support zones (density-based)

import os, time, json, math, hashlib
import requests
import numpy as np
import matplotlib
matplotlib.use("Agg")  # headless
import matplotlib.pyplot as plt
from pathlib import Path

# Cache and retry configuration
CACHE_DIR = Path("/tmp/luna_cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_TTL = 15 * 60        # 15-minute cache
MAX_RETRIES = 3
RETRY_WAIT = 3
SHORT_TTL = 4 * 60         # 4 minutes for 7d prices
LONG_TTL = 6 * 60 * 60    # 6 hours for lifetime prices
INFO_TTL = 15 * 60        # 15 minutes for coin market_data

CG_API = "https://api.coingecko.com/api/v3/coins"

# ------------------------------------------------------------
# Data fetch + caching
# ------------------------------------------------------------
def _fetch_json(url: str):
    """Fetch JSON with retry + caching (handles 429s)"""
    key = hashlib.sha1(url.encode()).hexdigest()
    fp = CACHE_DIR / f"{key}.json"

    # valid cache?
    if fp.exists() and (time.time() - fp.stat().st_mtime) < CACHE_TTL:
        with open(fp) as f:
            return json.load(f)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, timeout=12)
            if r.status_code == 429:
                print(f"[Retry {attempt}] 429 Too Many Requests – sleeping {RETRY_WAIT*attempt}s")
                time.sleep(RETRY_WAIT * attempt)
                continue
            r.raise_for_status()
            data = r.json()
            with open(fp, "w") as f:
                json.dump(data, f)
            return data
        except Exception as e:
            print(f"[Retry {attempt}] {e}")
            time.sleep(RETRY_WAIT)
    # fallback to stale cache if exists
    if fp.exists():
        print("[Fetch] Using stale cache for", url)
        with open(fp) as f:
            return json.load(f)
    return {}

def _resolve_cg_id(symbol: str) -> str:
    s = (symbol or "").upper()
    known = {
        "BTC": "bitcoin",
        "ETH": "ethereum",
        "SOL": "solana",
        "BNB": "binancecoin",
        "ADA": "cardano",
        "XRP": "ripple",
        "DOGE": "dogecoin",
        "AVAX": "avalanche-2",
        "MATIC": "matic-network",
    }
    return known.get(s, s.lower())

def _fetch_prices(cg_id: str, days="max", ttl=LONG_TTL):
    url = f"{CG_API}/{cg_id}/market_chart?vs_currency=usd&days={days}"
    data = _fetch_json(url)

    prices = data.get("prices", [])
    if not prices:
        return [], []
    ts = [p[0] / 1000.0 for p in prices]
    px = [float(p[1]) for p in prices]
    return ts, px

def _fetch_info(cg_id: str, ttl=INFO_TTL):
    url = f"{CG_API}/{cg_id}?localization=false&tickers=false&market_data=true&community_data=false&developer_data=false&sparkline=false"
    data = _fetch_json(url)
    return data

# ------------------------------------------------------------
# Public entry point
# ------------------------------------------------------------
def build_tech_panel(
    symbol: str = "BTC",
    cg_id: str = None,
    short_days: int = 7,
    out_path: str = "/tmp/tech_panel.png",
    theme: str = "purple"
):
    """
    Returns: dict with:
      - chart_path (str)
      - metrics (dict): price, pct_24h, pct_7d, mcap, vol_24h, from_ath_pct,
                        nearest_support, nearest_resistance, ath_price,
                        circ_supply, total_supply, btc_dominance, pct_30d
      - pivots (dict): A,B used for fib
    """

    coin_id = cg_id or _resolve_cg_id(symbol)

    # Fetch & cache data
    short_ts, short_px = _fetch_prices(coin_id, days=short_days, ttl=SHORT_TTL)    # 7d
    long_ts, long_px = _fetch_prices(coin_id, days="max", ttl=LONG_TTL)            # lifetime
    info = _fetch_info(coin_id, ttl=INFO_TTL)                                      # market_data

    if len(short_px) < 10 or len(long_px) < 50:
        raise RuntimeError("Insufficient data for chart rendering")

    # Metrics
    price_now = float(info.get("market_data", {}).get("current_price", {}).get("usd", short_px[-1]))
    pct_24h = float(info.get("market_data", {}).get("price_change_percentage_24h", _percent_change(short_px, hours=24)))
    mcap = float(info.get("market_data", {}).get("market_cap", {}).get("usd", 0.0))
    vol_24h = float(info.get("market_data", {}).get("total_volume", {}).get("usd", 0.0))
    ath_usd = float(info.get("market_data", {}).get("ath", {}).get("usd", max(long_px)))
    circ_supply = float(info.get("market_data", {}).get("circulating_supply", 0.0))
    total_supply = float(info.get("market_data", {}).get("total_supply", 0.0))
    ath_price = ath_usd
    btc_dom = float(info.get("market_data", {}).get("market_cap_percentage", {}).get("btc", 0.0))
    pct_30d = float(info.get("market_data", {}).get("price_change_percentage_30d", 0.0))
    from_ath_pct = ((price_now - ath_usd) / ath_usd) * 100.0 if ath_usd else 0.0
    pct_7d = _percent_change(short_px, hours=24*7)

    # Indicators (short-term)
    trendline = _ols_trendline(short_px)          # y_hat across [0..N-1]
    A, B = _recent_pivots(short_px)               # swing low/high (A,B) of freshest move
    fib_exts = _fib_extensions(A, B)              # list of (level_name, price)

    # Support zones (long-term)
    supports, resistances = _support_resistance_zones(long_px, bins=40, top_k=4)
    nearest_sup, nearest_res = _nearest_levels(price_now, supports, resistances)

    # Render dual panel (short-term top, long-term bottom)
    _render_dual(
        short_px=short_px, long_px=long_px,
        trendline=trendline, fib_exts=fib_exts,
        supports=supports, resistances=resistances,
        price_now=price_now, theme=theme, out_path=out_path
    )

    return {
        "chart_path": out_path,
        "metrics": {
            "price": price_now,
            "pct_24h": pct_24h,
            "pct_7d": pct_7d,
            "market_cap": mcap,
            "vol_24h": vol_24h,
            "from_ath_pct": from_ath_pct,
            "nearest_support": nearest_sup,
            "nearest_resistance": nearest_res,
            "ath_price": ath_price,
            "circ_supply": circ_supply,
            "total_supply": total_supply,
            "btc_dominance": btc_dom,
            "pct_30d": pct_30d,
        },
        "pivots": {"A": A, "B": B}
    }

# ------------------------------------------------------------
# Indicators
# ------------------------------------------------------------
def _percent_change(prices, hours=24):
    if len(prices) < 2: return 0.0
    # Approximate: use last-N samples over the whole window.
    # market_chart returns ~hourly resolution for small 'days'.
    n = max(1, min(len(prices) - 1, hours))
    old = prices[-1 - n]
    cur = prices[-1]
    return (cur - old) / old * 100.0 if old else 0.0

def _ols_trendline(prices):
    y = np.array(prices, dtype=float)
    x = np.arange(len(y), dtype=float)
    if len(y) < 3: return y
    A = np.vstack([x, np.ones_like(x)]).T
    slope, intercept = np.linalg.lstsq(A, y, rcond=None)[0]
    yhat = slope * x + intercept
    return yhat.tolist()

def _recent_pivots(prices):
    """
    Find last significant swing A -> B:
    - scan for local extrema with a small window
    - pick the final two alternating pivots (low->high or high->low)
    Returns tuple (A_price, B_price)
    """
    y = np.array(prices, dtype=float)
    n = len(y)
    if n < 10:
        return (y[0], y[-1])

    k = max(2, n // 30)  # local neighborhood
    mins = []
    maxs = []
    for i in range(k, n - k):
        window = y[i - k:i + k + 1]
        if y[i] == window.min(): mins.append(i)
        if y[i] == window.max(): maxs.append(i)

    pivots = sorted([(i, "min") for i in mins] + [(i, "max") for i in maxs])
    # keep the last two alternates
    last_two = []
    for i, t in reversed(pivots):
        if not last_two:
            last_two.append((i, t))
        elif last_two[-1][1] != t:
            last_two.append((i, t))
            break

    if len(last_two) < 2:
        return (y[0], y[-1])

    last_two.sort(key=lambda x: x[0])
    iA, tA = last_two[0]
    iB, tB = last_two[1]
    return (float(y[iA]), float(y[iB]))

def _fib_extensions(A, B):
    """
    Trend-based Fib extension levels from move A->B
    If B > A (up move): levels above B
    If B < A (down move): levels below B
    """
    r = [1.272, 1.414, 1.618, 2.0]
    levels = []
    move = (B - A)
    if move == 0:
        return levels
    up = move > 0
    for rr in r:
        if up:
            levels.append((f"{rr:.3f}x", B + (move * (rr - 1.0))))
        else:
            levels.append((f"{rr:.3f}x", B - (abs(move) * (rr - 1.0))))
    return levels

def _support_resistance_zones(prices, bins=40, top_k=4):
    """
    Density-based levels using histogram of lifetime closes.
    Returns two lists: supports[], resistances[] as price floats.
    """
    y = np.array(prices, dtype=float)
    if len(y) < 50:
        return [], []

    hist, edges = np.histogram(y, bins=bins)
    # Select largest bins as zones; convert bin centers to price levels.
    idx = np.argsort(hist)[-top_k:]
    centers = (edges[idx] + edges[idx + 1]) / 2.0
    centers = sorted(centers)

    # Split around current price later; for now return all sorted centers.
    # Caller will choose nearest support/resistance relative to price_now.
    return centers, centers

def _nearest_levels(price_now, supports, resistances):
    sup = max([s for s in supports if s <= price_now], default=None)
    res = min([r for r in resistances if r >= price_now], default=None)
    return sup, res

# ------------------------------------------------------------
# Rendering
# ------------------------------------------------------------
def _render_dual(short_px, long_px, trendline, fib_exts, supports, resistances, price_now, theme, out_path):
    plt.figure(figsize=(7.2, 4.2), facecolor="#0b0f28")
    # Color theme
    col_price_s = "#60d4ff"  # short
    col_price_l = "#b084ff"  # long
    col_trend = "#9cff9c"
    col_fib = "#ffd166"  # amber
    col_zone = "#8be9fd"  # cyan-ish (transparent bands)

    # ----- Short-term panel -----
    ax1 = plt.subplot(2, 1, 1, facecolor="#0f1433")
    x1 = np.arange(len(short_px))
    ax1.plot(x1, short_px, color=col_price_s, linewidth=2, label="Price (7d)")
    if len(trendline) == len(short_px):
        ax1.plot(x1, trendline, color=col_trend, linewidth=1.8, linestyle="--", label="Trendline")

    # Fib extension lines drawn at right side (full width)
    if fib_exts:
        ymin, ymax = min(short_px), max(short_px)
        for name, lvl in fib_exts:
            if lvl is None: continue
            ax1.axhline(lvl, color=col_fib, linewidth=1.1, linestyle=":", alpha=0.9)
            ax1.text(x1[-1], lvl, f"  {name}  {lvl:,.2f}", color=col_fib, fontsize=8,
                     va="center", ha="left")

    ax1.set_title("Short-Term (7d) — Price, Trendline & Fib Extensions", color="w", fontsize=10, pad=8)
    ax1.set_xticks([]); ax1.tick_params(axis='y', colors='w', labelsize=8)
    ax1.grid(alpha=0.08)

    # ----- Long-term panel -----
    ax2 = plt.subplot(2, 1, 2, facecolor="#0f1433")
    x2 = np.arange(len(long_px))
    ax2.plot(x2, long_px, color=col_price_l, linewidth=1.6, label="Price (All time)")

    # Support/resistance zones (as translucent bands)
    if supports:
        ymin, ymax = ax2.get_ylim()
        span = (ymax - ymin) * 0.01  # band thickness
        for lvl in supports:
            ax2.axhspan(lvl - span, lvl + span, color=col_zone, alpha=0.08)
    # Emphasize nearest support/resistance around current price
    ax2.axhline(price_now, color="#ffffff", linewidth=1.0, alpha=0.25, linestyle="--")

    ax2.set_title("Long-Term (All time) — Support Density Zones", color="w", fontsize=10, pad=8)
    ax2.set_xticks([]); ax2.tick_params(axis='y', colors='w', labelsize=8)
    ax2.grid(alpha=0.08)

    plt.tight_layout(pad=1.2)
    plt.savefig(out_path, dpi=180, bbox_inches="tight", pad_inches=0.1, transparent=False)
    plt.close()