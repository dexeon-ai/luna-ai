# ============================================================
# plot_engine.py — Luna Matrix (Windows, 8-around-1, Structured Squares v3 FIXED)
# ============================================================
from __future__ import annotations
import os, csv, math, logging
from pathlib import Path
from typing import List, Tuple, Optional
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from matplotlib.gridspec import GridSpec

# ---------------- Logging ----------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------- Paths ----------------
BASE_DIR = Path(__file__).resolve().parent
OVERLAYS_DIR = BASE_DIR / "overlays"
CACHE_DIR = BASE_DIR / "luna_cache"
DATA_ROOT = CACHE_DIR / "data"
COIN_DIR = DATA_ROOT / "coins"
MEME_DIR = DATA_ROOT / "memes"
OVERLAYS_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)
print(f"[PATH] overlays: {OVERLAYS_DIR}")

# ============================================================
# Helpers
# ============================================================
def _safe_float(x):
    try:
        f = float(x)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except Exception:
        return None

def money_formatter():
    def _fmt(v, pos):
        n = abs(v)
        if n >= 1e12: return f"${v/1e12:.2f}T"
        if n >= 1e9:  return f"${v/1e9:.2f}B"
        if n >= 1e6:  return f"${v/1e6:.2f}M"
        if n >= 1e3:  return f"${v/1e3:.2f}K"
        return f"${v:.2f}"
    return FuncFormatter(_fmt)

def ema(arr, span):
    if arr is None or len(arr) < 2: return np.array([])
    a = np.array(arr, dtype=float)
    alpha = 2 / (span + 1)
    out = np.zeros_like(a)
    out[0] = a[0]
    for i in range(1, len(a)):
        out[i] = alpha * a[i] + (1 - alpha) * out[i-1]
    return out

def rsi(p, period=14):
    if p is None or len(p) <= period + 1: return np.array([])
    p = np.array(p, dtype=float)
    d = np.diff(p)
    up = np.where(d > 0, d, 0.0)
    dn = np.where(d < 0, -d, 0.0)
    ru = ema(up, period)[-len(d):]
    rd = ema(dn, period)[-len(d):]
    with np.errstate(divide='ignore', invalid='ignore'):
        rs = np.where(rd == 0, np.nan, ru/rd)
        r = 100 - (100 / (1 + rs))
    return np.insert(r, 0, np.nan)

def macd(p, fast=12, slow=26, signal=9):
    if p is None or len(p) < slow + signal + 2:
        return np.array([]), np.array([]), np.array([])
    p = np.array(p, dtype=float)
    m = ema(p, fast) - ema(p, slow)
    s = ema(m, signal)
    return m, s, (m - s)

def bollinger(p, window=20, num_std=2):
    if p is None or len(p) < window:
        return np.array([]), np.array([]), np.array([])
    p = np.array(p, dtype=float)
    sma = np.convolve(p, np.ones(window)/window, mode="valid")
    roll_std = np.array([np.std(p[i-window+1:i+1]) for i in range(window-1, len(p))])
    pad = np.full(window-1, np.nan)
    return np.concatenate([pad, sma+num_std*roll_std]), np.concatenate([pad, sma]), np.concatenate([pad, sma-num_std*roll_std])

def ols_trend(y):
    if y is None or len(y) < 2: return np.array([])
    x = np.arange(len(y))
    b1, b0 = np.polyfit(x, np.array(y, dtype=float), 1)
    return b1*x + b0

def _csv_path(symbol:str)->Optional[Path]:
    s = symbol.lower().strip()
    for d in [COIN_DIR, MEME_DIR]:
        p = d/f"{s}.csv"
        if p.exists(): return p
    return None

def _load_csv(symbol:str)->Tuple[list,list,list,list]:
    p = _csv_path(symbol)
    if not p: return [],[],[],[]
    rows = list(csv.DictReader(open(p,"r",encoding="utf-8")))
    def col(n): return [_safe_float(r.get(n)) for r in rows if _safe_float(r.get(n)) is not None]
    price = col("price") or col("price_dex")
    vol = col("volume_24h")
    mcap = col("market_cap")
    liq = col("liquidity_usd")
    return price, vol, mcap, liq

# ============================================================
# Renderer — 8 larger equal squares around 1 big center
# ============================================================
def _render_matrix(symbol:str, days:int=180)->str:
    price, vol, mcap, liq = _load_csv(symbol)
    rsi_vals = rsi(price)
    m_line, m_sig, _ = macd(price)
    up, mid, lo = bollinger(price)
    tr = ols_trend(price)

    BG="#1b1e30"; PANEL="#262a42"; GRID="#444c70"; TEXT="#ffffff"
    C={"price":"#4cc9f0","trend":"#80ffb4","rsi":"#ff9eaa","macd":"#35e0c7","signal":"#ffe685",
       "vol":"#80b3ff","mcap":"#5efc8d","liq":"#f472b6"}

    fig = plt.figure(figsize=(18,18), facecolor=BG)
    gs = GridSpec(11,11,figure=fig,hspace=0.4,wspace=0.4)

    def setup(ax,title=""):
        ax.set_facecolor(PANEL)
        ax.grid(True,color=GRID,alpha=0.35,lw=0.6)
        ax.tick_params(axis="x",colors=TEXT,labelsize=8)
        ax.tick_params(axis="y",colors=TEXT,labelsize=8)
        if title: ax.set_title(title,color=TEXT,fontsize=9,pad=4)
        for s in ax.spines.values():
            s.set_edgecolor("#ffffff"); s.set_linewidth(1.8)

    def glow_line(ax,y,color,lw=1.6):
        if y is None: return
        y = np.array(y)
        if y.size == 0: return
        x = np.arange(len(y))
        ax.plot(x,y,color=color,lw=lw,alpha=0.9)
        ax.plot(x,y,color=color,lw=lw+1,alpha=0.15,zorder=-1)

    slots = {
        "topL":(2,3),"topR":(2,7),
        "rightT":(4,9),"rightB":(6,9),
        "bottomR":(8,7),"bottomL":(8,3),
        "leftB":(6,1),"leftT":(4,1)
    }

    if price is not None and len(price)>0:
        ax1=fig.add_subplot(gs[slots["topL"]]); setup(ax1,"Price"); glow_line(ax1,price,C["price"])
    if rsi_vals is not None and len(rsi_vals)>0:
        ax2=fig.add_subplot(gs[slots["topR"]]); setup(ax2,"RSI(14)"); glow_line(ax2,rsi_vals,C["rsi"])
    if m_line is not None and len(m_line)>0:
        ax3=fig.add_subplot(gs[slots["rightT"]]); setup(ax3,"MACD"); glow_line(ax3,m_line,C["macd"])
        if m_sig is not None and len(m_sig)>0: glow_line(ax3,m_sig,C["signal"])
    if vol is not None and len(vol)>0:
        ax4=fig.add_subplot(gs[slots["rightB"]]); setup(ax4,"Volume"); ax4.bar(range(len(vol)),vol,color=C["vol"],alpha=0.8)
    if mcap is not None and len(mcap)>0:
        ax5=fig.add_subplot(gs[slots["bottomR"]]); setup(ax5,"Market Cap"); glow_line(ax5,mcap,C["mcap"]); ax5.yaxis.set_major_formatter(money_formatter())
    if liq is not None and len(liq)>0:
        ax6=fig.add_subplot(gs[slots["bottomL"]]); setup(ax6,"Liquidity"); glow_line(ax6,liq,C["liq"]); ax6.yaxis.set_major_formatter(money_formatter())
    if tr is not None and len(tr)>0:
        ax7=fig.add_subplot(gs[slots["leftB"]]); setup(ax7,"Trend (OLS)"); glow_line(ax7,tr,C["trend"])
    if mid is not None and len(mid)>0:
        ax8=fig.add_subplot(gs[slots["leftT"]]); setup(ax8,"Bollinger 20/2"); glow_line(ax8,price,C["price"]); glow_line(ax8,mid,"#cfd3da")

    axc = fig.add_subplot(gs[3:8,3:8]); setup(axc,f"{symbol.upper()} — Overview")
    if price is not None and len(price)>0:
        glow_line(axc,price,C["price"])
        if tr is not None and len(tr)>0: glow_line(axc,tr,C["trend"])
        if mid is not None and len(mid)>0: glow_line(axc,mid,"#d8dbe5")
        leg=axc.legend(["Price","Trend","MA20"],facecolor=PANEL,edgecolor=GRID,fontsize=9,loc="upper left")
        for text in leg.get_texts(): text.set_color("white")
    for s in axc.spines.values():
        s.set_edgecolor("#7a84ff"); s.set_linewidth(2.0)

    fig.suptitle(f"{symbol.upper()} — Technical Overview",color=TEXT,fontsize=17,weight="bold")
    out = OVERLAYS_DIR / f"{symbol.upper()}_matrix.png"
    fig.savefig(out.as_posix(),dpi=170,bbox_inches="tight",facecolor=fig.get_facecolor())
    plt.close(fig)
    logger.info(f"[OK] {symbol} chart saved: {out}")
    return out.as_posix()

# ============================================================
# Public API
# ============================================================
def build_tech_panel(symbol:str,name:Optional[str]=None,days:int=180)->str:
    try:
        return _render_matrix(symbol,days)
    except Exception as e:
        logger.error(f"[RenderError] {symbol}: {e}")
        BG="#1b1e30"; PANEL="#262a42"
        fig=plt.figure(figsize=(10,6),facecolor=BG)
        ax=fig.add_subplot(111)
        ax.set_facecolor(PANEL)
        ax.text(0.5,0.5,f"Could not render chart for {symbol}\n{e}",ha="center",va="center",color="white")
        out=OVERLAYS_DIR/f"{symbol}_error.png"
        fig.savefig(out.as_posix(),dpi=140,bbox_inches="tight",facecolor=fig.get_facecolor())
        plt.close(fig)
        return out.as_posix()
