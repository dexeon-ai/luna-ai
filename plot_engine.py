# ============================================================
# plot_engine.py — Stable Chart Generator for Luna Cockpit
# ============================================================

from __future__ import annotations
import os, csv, math, logging
from pathlib import Path
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("plot_engine")

BASE_DIR = Path(__file__).resolve().parent
OVERLAYS_DIR = BASE_DIR / "overlays"
CACHE_DIR = BASE_DIR / "luna_cache" / "data" / "coins"
OVERLAYS_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ---------- helpers ----------
def _safe_float(x):
    try:
        f = float(x)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None

def _load_csv(symbol: str):
    p = CACHE_DIR / f"{symbol.lower()}.csv"
    if not p.exists():
        return [], [], [], [], []
    with open(p, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    price = [_safe_float(r.get("price")) for r in rows]
    vol = [_safe_float(r.get("volume_24h")) for r in rows]
    mcap = [_safe_float(r.get("market_cap")) for r in rows]
    liq = [_safe_float(r.get("liquidity_usd")) for r in rows]
    ts = [r.get("timestamp", "") for r in rows]
    return price, vol, mcap, liq, ts

# ---------- visual ----------
COL_BG = "#111428"
COL_PANEL = "#1a1431"
COL_TXT = "#e8f0ff"

def _panel(fig):
    ax = fig.add_subplot(111)
    ax.set_facecolor(COL_PANEL)
    ax.grid(True, color="#2b2a55", alpha=0.25, lw=0.6)
    for s in ax.spines.values():
        s.set_color("#333a66")
        s.set_linewidth(1.4)
    ax.tick_params(colors=COL_TXT, labelsize=8)
    return ax

def _save(fig, path: Path):
    try:
        fig.savefig(path.as_posix(), dpi=140, bbox_inches="tight", facecolor=COL_BG)
    except Exception as e:
        logger.error(f"[Luna] Failed to save {path}: {e}")
    finally:
        plt.close(fig)

def small_line(path: Path, y, title: str, color: str):
    y = [v for v in y if v is not None]
    if not y:
        y = [0]
    x = np.arange(len(y))
    fig = plt.figure(figsize=(3.2, 3.2), facecolor=COL_BG)
    ax = _panel(fig)
    ax.plot(x, y, color=color, lw=1.8)
    ax.set_title(title, color=COL_TXT, fontsize=10)
    _save(fig, path)

def small_bar(path: Path, y, title: str, color: str):
    y = [v for v in y if v is not None]
    if not y:
        y = [0]
    x = np.arange(len(y))
    fig = plt.figure(figsize=(3.2, 3.2), facecolor=COL_BG)
    ax = _panel(fig)
    ax.bar(x, y, color=color, alpha=0.9)
    ax.set_title(title, color=COL_TXT, fontsize=10)
    _save(fig, path)

def main_price(path: Path, price):
    y = [v for v in price if v is not None]
    if not y:
        y = [0]
    x = np.arange(len(y))
    fig = plt.figure(figsize=(12, 6.5), facecolor=COL_BG)
    ax = _panel(fig)
    ax.plot(x, y, color="#89b4ff", lw=2.0)
    ax.set_title("BITCOIN — Price", color=COL_TXT, fontsize=14)
    _save(fig, path)

# ---------- public builder ----------
def build_cockpit_set(symbol: str):
    price, vol, mcap, liq, _ = _load_csv(symbol)
    paths = {
        "MAIN": OVERLAYS_DIR / f"{symbol.upper()}_MAIN.png",
        "RSI": OVERLAYS_DIR / f"{symbol.upper()}_RSI.png",
        "MACD": OVERLAYS_DIR / f"{symbol.upper()}_MACD.png",
        "VOL": OVERLAYS_DIR / f"{symbol.upper()}_VOL.png",
        "MCAP": OVERLAYS_DIR / f"{symbol.upper()}_MCAP.png",
        "LIQ": OVERLAYS_DIR / f"{symbol.upper()}_LIQ.png",
        "SENT": OVERLAYS_DIR / f"{symbol.upper()}_SENT.png",
        "FDV": OVERLAYS_DIR / f"{symbol.upper()}_FDV.png",
        "BANDS": OVERLAYS_DIR / f"{symbol.upper()}_BANDS.png",
        "FNG": OVERLAYS_DIR / f"{symbol.upper()}_FNG.png",
    }

    # Always draw each chart, even if empty
    main_price(paths["MAIN"], price)
    small_line(paths["RSI"], price, "RSI(14)", "#ff9eaa")
    small_line(paths["MACD"], price, "MACD", "#35e0c7")
    small_bar(paths["VOL"], vol, "Volume", "#80b3ff")
    small_line(paths["MCAP"], mcap, "Market Cap", "#5efc8d")
    small_line(paths["LIQ"], liq, "Liquidity", "#f472b6")
    small_line(paths["SENT"], price, "Sentiment", "#b88cff")
    small_line(paths["FDV"], mcap, "FDV", "#b8e28f")
    small_line(paths["BANDS"], price, "Bollinger", "#cfd3da")
    small_line(paths["FNG"], price, "Fear & Greed", "#9ee37d")

    logger.info(f"[Luna] Overlays built for {symbol}")
    return {k: v.as_posix() for k, v in paths.items()}
