# plot_engine.py — Luna Technical Chart Engine v3
# - Primary data: CoinGecko (no captcha)
# - Fallback (only if CG returns empty): CoinPaprika (daily)
# - Per-endpoint caching/backoff to avoid 429s
# - Indicators:
#     • Short-term OLS trend line
#     • Trend-based Fib extensions (A→B, last move)
#     • Long-term density support zones
#     • Projection: linear forecast + ±1σ band (next N samples)
# - Render: dual panels (7d / lifetime), white ticks, dark theme
# Updated: 2025-10-08

import time, json, hashlib
from pathlib import Path

import requests
import numpy as np

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

# -------------------------------
# Cache / backoff config
# -------------------------------
CACHE_DIR = Path("/tmp/luna_cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Global defaults
CACHE_TTL   = 15 * 60     # generic 15m
SHORT_TTL   = 4 * 60      # ~7d prices refresh
LONG_TTL    = 6 * 60 * 60 # lifetime refresh
INFO_TTL    = 15 * 60     # market info refresh
MAX_RETRIES = 3
RETRY_WAIT  = 3

CG_API = "https://api.coingecko.com/api/v3/coins"

# -------------------------------
# Utilities: cache + fetch
# -------------------------------
def _fetch_json(url: str, ttl: int = CACHE_TTL):
    """
    Fetch JSON with retry/backoff + per-URL file cache.
    ttl controls how 'fresh' a cached file must be to be reused.
    """
    key = hashlib.sha1(url.encode()).hexdigest()
    fp = CACHE_DIR / f"{key}.json"

    # Fresh cache?
    if fp.exists():
        age = time.time() - fp.stat().st_mtime
        if age < ttl:
            try:
                with open(fp) as f:
                    return json.load(f)
            except Exception:
                pass  # fall through to refetch

    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, timeout=12)
            if r.status_code == 429:
                sleep_for = RETRY_WAIT * attempt
                print(f"[Backoff] 429 on {url} — sleeping {sleep_for}s")
                time.sleep(sleep_for)
                continue
            r.raise_for_status()
            data = r.json()
            try:
                with open(fp, "w") as f:
                    json.dump(data, f)
            except Exception:
                pass
            return data
        except Exception as e:
            last_err = e
            time.sleep(RETRY_WAIT)

    # Fallback to stale cache if we have it
    if fp.exists():
        try:
            print(f"[Fetch] Using stale cache for {url} after error: {last_err}")
            with open(fp) as f:
                return json.load(f)
        except Exception:
            pass

    print(f"[Fetch] failed for {url}: {last_err}")
    return {}

def _resolve_cg_id(symbol: str) -> str:
    """Resolve a ticker symbol to a CoinGecko coin id."""
    s = (symbol or "").upper()
    mapping = {
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
    return mapping.get(s, s.lower())

# Public alias so other modules can import either name
resolve_cg_id = _resolve_cg_id

def _fetch_prices_cg(cg_id: str, days="max"):
    # Per-endpoint TTL: 7d window refreshes more often than lifetime
    ttl = SHORT_TTL if (days != "max") else LONG_TTL
    url = f"{CG_API}/{cg_id}/market_chart?vs_currency=usd&days={days}"
    data = _fetch_json(url, ttl=ttl)
    prices = data.get("prices") or []
    ts = [p[0] / 1000.0 for p in prices]
    px = [float(p[1]) for p in prices]
    return ts, px

def _fetch_prices_fallback_daily(cg_id: str):
    # CoinPaprika uses ids like "btc-bitcoin"
    pid = "btc-bitcoin" if cg_id == "bitcoin" else cg_id
    try:
        r = requests.get(
            f"https://api.coinpaprika.com/v1/tickers/{pid}/historical?start=2013-04-28&interval=1d",
            timeout=12
        )
        if not r.ok:
            return [], []
        hist = r.json()
        if not isinstance(hist, list):
            return [], []
        ts, px = [], []
        # Approximate timestamps to daily spacing (seconds); avoids timezone noise
        t0 = int(time.time()) - 86400 * len(hist)
        for i, h in enumerate(hist):
            if "close" in h:
                ts.append(t0 + i * 86400)
                px.append(float(h["close"]))
        return ts, px
    except Exception as e:
        print("[Fallback error CoinPaprika]", e)
        return [], []

def _fetch_prices(cg_id: str, days="max"):
    # Try CoinGecko; fallback to daily history if empty
    ts, px = _fetch_prices_cg(cg_id, days=days)
    if not px:
        print("[Prices] Falling back to daily data")
        ts, px = _fetch_prices_fallback_daily(cg_id)
    return ts, px

def _fetch_info(cg_id: str):
    url = (
        f"{CG_API}/{cg_id}"
        "?localization=false&tickers=false&market_data=true"
        "&community_data=false&developer_data=false&sparkline=false"
    )
    return _fetch_json(url, ttl=INFO_TTL)

# -------------------------------
# Public entry
# -------------------------------
def build_tech_panel(
    symbol: str = "BTC",
    cg_id: str | None = None,   # optional, kept for backward-compat
    short_days: int = 7,
    out_path: str = "/tmp/tech_panel.png",
    theme: str = "purple",
    **_ignore,                 # swallow stray kwargs safely
):
    """
    Build chart image and return metrics.
    Returns:
      {
        "chart_path": <str>,
        "metrics": {
           price, pct_24h, pct_7d, market_cap, vol_24h,
           from_ath_pct, nearest_support, nearest_resistance,
           ath_price, circ_supply, total_supply, pct_30d
        },
        "pivots": {"A": A, "B": B}
      }
    """
    coin_id = cg_id or _resolve_cg_id(symbol)

    # data
    _, short_px = _fetch_prices(coin_id, days=short_days)
    _, long_px  = _fetch_prices(coin_id, days="max")
    info        = _fetch_info(coin_id)

    if len(short_px) < 10 and long_px:
        # synthesize a short window from tail of long series if CG throttled
        short_px = long_px[-180:]  # ~last 180 samples

    if len(short_px) < 10 or len(long_px) < 50:
        raise RuntimeError("Insufficient data for chart rendering")

    md = info.get("market_data", {}) if isinstance(info, dict) else {}
    price_now = float(md.get("current_price", {}).get("usd", short_px[-1]))
    pct_24h   = float(md.get("price_change_percentage_24h", _percent_change(short_px, hours=24)))
    market_cap= float(md.get("market_cap", {}).get("usd", 0.0))
    vol_24h   = float(md.get("total_volume", {}).get("usd", 0.0))
    ath_price = float(md.get("ath", {}).get("usd", max(long_px)))
    circ      = float(md.get("circulating_supply", 0.0))
    total_sup = float(md.get("total_supply", 0.0))
    pct_30d   = float(md.get("price_change_percentage_30d", 0.0))
    from_ath_pct = ((price_now - ath_price) / ath_price * 100.0) if ath_price else 0.0
    pct_7d = _percent_change(short_px, hours=24*7)

    # indicators
    trendline = _ols_trendline(short_px)
    A, B      = _recent_pivots(short_px)
    fib_exts  = _fib_extensions(A, B)

    supports, resistances = _support_resistance_zones(long_px, bins=40, top_k=4)
    nearest_sup, nearest_res = _nearest_levels(price_now, supports, resistances)

    # projection over short window (next 24 samples)
    proj = _forecast_linear(short_px, forecast_n=24)

    _render_dual(
        short_px=short_px,
        long_px=long_px,
        trendline=trendline,
        fib_exts=fib_exts,
        supports=supports,
        resistances=resistances,
        price_now=price_now,
        projection=proj,
        out_path=out_path,
    )

    return {
        "chart_path": out_path,
        "metrics": {
            "price": price_now,
            "pct_24h": pct_24h,
            "pct_7d": pct_7d,
            "market_cap": market_cap,
            "vol_24h": vol_24h,
            "from_ath_pct": from_ath_pct,
            "nearest_support": nearest_sup,
            "nearest_resistance": nearest_res,
            "ath_price": ath_price,
            "circ_supply": circ,
            "total_supply": total_sup,
            "pct_30d": pct_30d,
        },
        "pivots": {"A": A, "B": B},
    }

# -------------------------------
# Indicators / helpers
# -------------------------------
def _percent_change(prices, hours=24):
    if len(prices) < 2:
        return 0.0
    n = max(1, min(len(prices) - 1, hours))
    old = prices[-1 - n]
    cur = prices[-1]
    return ((cur - old) / old) * 100.0 if old else 0.0

def _ols_trendline(prices):
    y = np.array(prices, dtype=float)
    x = np.arange(len(y), dtype=float)
    if len(y) < 3:
        return y.tolist()
    A = np.vstack([x, np.ones_like(x)]).T
    slope, intercept = np.linalg.lstsq(A, y, rcond=None)[0]
    return (slope * x + intercept).tolist()

def _recent_pivots(prices):
    y = np.array(prices, dtype=float)
    n = len(y)
    if n < 10:
        return float(y[0]), float(y[-1])
    k = max(2, n // 30)
    mins, maxs = [], []
    for i in range(k, n - k):
        w = y[i-k:i+k+1]
        if y[i] == w.min():
            mins.append(i)
        if y[i] == w.max():
            maxs.append(i)
    pivots = sorted([(i, "min") for i in mins] + [(i, "max") for i in maxs])
    last_two = []
    for i, t in reversed(pivots):
        if not last_two:
            last_two.append((i, t))
        elif last_two[-1][1] != t:
            last_two.append((i, t))
            break
    if len(last_two) < 2:
        return float(y[0]), float(y[-1])
    last_two.sort(key=lambda z: z[0])
    iA, _ = last_two[0]
    iB, _ = last_two[1]
    return float(y[iA]), float(y[iB])

def _fib_extensions(A, B):
    r = [1.272, 1.414, 1.618, 2.0]
    out = []
    move = (B - A)
    if move == 0:
        return out
    up = move > 0
    for rr in r:
        val = B + (move * (rr - 1.0)) if up else B - (abs(move) * (rr - 1.0))
        out.append((f"{rr:.3f}x", float(val)))
    return out

def _support_resistance_zones(prices, bins=40, top_k=4):
    y = np.array(prices, dtype=float)
    if len(y) < 50:
        return [], []
    hist, edges = np.histogram(y, bins=bins)
    idx = np.argsort(hist)[-top_k:]
    centers = (edges[idx] + edges[idx+1]) / 2.0
    centers = sorted(centers)
    return centers, centers

def _nearest_levels(price_now, supports, resistances):
    sup = max([s for s in supports if s <= price_now], default=None)
    res = min([r for r in resistances if r >= price_now], default=None)
    return sup, res

def _forecast_linear(prices, forecast_n=24):
    """
    Simple linear projection using last third of short series.
    Returns dict with x (indices), y (forecast), y_hi, y_lo bands.
    """
    y = np.array(prices, dtype=float)
    n = len(y)
    if n < 12:
        return None
    tail = max(12, n // 3)
    x = np.arange(n - tail, n, dtype=float)
    A = np.vstack([x, np.ones_like(x)]).T
    slope, intercept = np.linalg.lstsq(A, y[-tail:], rcond=None)[0]
    # residual std
    y_hat_tail = slope * x + intercept
    resid = y[-tail:] - y_hat_tail
    sigma = np.std(resid) if len(resid) > 1 else 0.0

    x_future = np.arange(n, n + forecast_n, dtype=float)
    y_future = slope * x_future + intercept
    return {
        "x0": n,                    # index where projection begins
        "x": x_future.tolist(),
        "y": y_future.tolist(),
        "y_hi": (y_future + sigma).tolist(),
        "y_lo": (y_future - sigma).tolist(),
    }

def _fmt_usd(val, _pos=None):
    v = float(val)
    if abs(v) >= 1_000_000_000:
        return f"${v/1_000_000_000:.1f}B"
    if abs(v) >= 1_000_000:
        return f"${v/1_000_000:.1f}M"
    if abs(v) >= 1_000:
        return f"${v/1_000:.1f}K"
    if abs(v) >= 1:
        return f"${v:,.2f}"
    return f"${v:.6f}"

# -------------------------------
# Rendering
# -------------------------------
def _render_dual(short_px, long_px, trendline, fib_exts, supports, resistances, price_now, projection, out_path):
    # Make text/ticks **white** for readability on the dark background
    plt.rcParams.update({
        "axes.edgecolor": "#3a3f66",
        "axes.labelcolor": "#FFFFFF",
        "xtick.color": "#FFFFFF",
        "ytick.color": "#FFFFFF",
        "text.color":  "#FFFFFF",
        "font.size": 9,
    })

    fig = plt.figure(figsize=(7.2, 4.2), facecolor="#0b0f28")

    # colors
    col_short  = "#70e1ff"  # short price line
    col_long   = "#b084ff"  # long price line
    col_trend  = "#9cff9c"
    col_fib    = "#ffd166"
    col_zone   = "#8be9fd"
    col_proj   = "#ff9ee6"  # projection (pink)
    col_band   = "#ff9ee6"

    # ---- Short-term panel ----
    ax1 = plt.subplot(2, 1, 1, facecolor="#101538")
    x1 = np.arange(len(short_px))
    ax1.plot(x1, short_px, color=col_short, linewidth=2.0, label="Price (7d)")

    if len(trendline) == len(short_px):
        ax1.plot(x1, trendline, color=col_trend, linewidth=1.6, linestyle="--", label="Trend")

    # Fib levels
    if fib_exts:
        for name, lvl in fib_exts:
            ax1.axhline(lvl, color=col_fib, linewidth=0.9, linestyle=":", alpha=0.85)
            ax1.text(x1[-1], lvl, f"  {name} {lvl:,.0f}", color=col_fib, fontsize=8,
                     va="center", ha="left")

    # Projection
    if projection:
        xf = projection["x"]
        yf = projection["y"]
        yhi = projection["y_hi"]
        ylo = projection["y_lo"]
        # fill band first
        ax1.fill_between(xf, ylo, yhi, color=col_band, alpha=0.08, linewidth=0)
        ax1.plot([x1[-1], xf[0]], [short_px[-1], yf[0]], color=col_proj, linewidth=1.2, linestyle="--")
        ax1.plot(xf, yf, color=col_proj, linewidth=1.4, linestyle="--", label="Projection")

    ax1.yaxis.set_major_formatter(FuncFormatter(_fmt_usd))
    ax1.set_xticks([])  # hide ticks; index scale not meaningful to users
    ax1.grid(alpha=0.10, color="#2a2f55")
    ax1.legend(facecolor="#141a40", edgecolor="#343a66", labelcolor="#FFFFFF", loc="upper left", fontsize=8)

    # ---- Long-term panel ----
    ax2 = plt.subplot(2, 1, 2, facecolor="#101538")
    x2 = np.arange(len(long_px))
    ax2.plot(x2, long_px, color=col_long, linewidth=1.6, label="Price (All time)")

    if supports:
        ymin, ymax = ax2.get_ylim()
        span = (ymax - ymin) * 0.012
        for lvl in supports:
            ax2.axhspan(lvl - span, lvl + span, color=col_zone, alpha=0.08)

    ax2.axhline(price_now, color="#ffffff", linewidth=1.0, alpha=0.35, linestyle="--")
    ax2.yaxis.set_major_formatter(FuncFormatter(_fmt_usd))
    ax2.set_xticks([])
    ax2.grid(alpha=0.10, color="#2a2f55")

    plt.tight_layout(pad=1.2)
    fig.savefig(out_path, dpi=180, bbox_inches="tight", pad_inches=0.15)
    plt.close(fig)
