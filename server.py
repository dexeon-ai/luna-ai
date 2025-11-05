# ============================================================
# server.py — Luna Cockpit (stable grid + DS/GT/CG routing + cap-mode)
# Implements Grok-confirmed clarifications with zero grid changes
# ============================================================

from __future__ import annotations
import os, re, io, json, time, random, logging, math
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Any, Tuple, Optional, List, Callable

import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv

from flask import Flask, jsonify, render_template, request, Response
from flask.json.provider import DefaultJSONProvider
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio

# ---------- build tag ----------
BUILD_TAG = "LUNA-PROD-DS_GT_BIRD-CAPMODE-2025-11-04-locked"

# ---------- logging ----------
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
LOG = logging.getLogger("luna")
LOG.info("Starting Luna backend build=%s", BUILD_TAG)

# ---------- paths ----------
ROOT = Path(__file__).parent.resolve()
TEMPLATES_DIR = ROOT / "templates"
STATIC_DIR    = ROOT / "static"

DATA_DIR   = ROOT / "luna_cache" / "data"
DERIVED    = DATA_DIR / "derived"
FRAMES_DIR = DERIVED / "frames"
STATE_DIR  = DATA_DIR / "state"
SESS_DIR   = DATA_DIR / "sessions"
COINS_DIR  = DATA_DIR / "coins"

for d in (FRAMES_DIR, STATE_DIR, SESS_DIR):
    d.mkdir(parents=True, exist_ok=True)

FETCH_LOG = STATE_DIR / "fetch_log.json"
HOTSET    = STATE_DIR / "hotset.txt"

# ---------- constants ----------
TTL_MINUTES = 15
TTL_SECONDS = TTL_MINUTES * 60

COIN_LIST_PATH = STATE_DIR / "cg_coin_list.json"
COIN_LIST_TTL  = 60 * 60 * 24   # 24h

# ---------- env ----------
load_dotenv(ROOT / ".env")

def _parse_keys(raw: str) -> List[str]:
    return [t.strip() for t in re.split(r"[\s,;]+", raw or "") if t.strip()]

CC_KEYS = _parse_keys(os.getenv("CRYPTOCOMPARE_KEYS") or os.getenv("CRYPTOCOMPARE_KEY") or "")
CG_KEY  = (os.getenv("COINGECKO_API_KEY") or os.getenv("CG_API_KEY") or "").strip()
BIRDEYE_KEY = (os.getenv("BIRDEYE_KEY") or "public").strip()

# ---------- Flask JSON for Plotly ----------
class PlotlyJSON(DefaultJSONProvider):
    def dumps(self, obj, **kwargs):
        from plotly.utils import PlotlyJSONEncoder
        return json.dumps(obj, cls=PlotlyJSONEncoder, **kwargs)

app = Flask(__name__, template_folder=str(TEMPLATES_DIR), static_folder=str(STATIC_DIR))
app.json = PlotlyJSON(app)

# ---------- misc utils ----------
def utcnow() -> datetime: return datetime.now(timezone.utc)
def _to_iso(dt: datetime) -> str: return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()

def _fetch_log() -> dict:
    try:
        return json.loads(FETCH_LOG.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _touch_fetch(symbol: str) -> None:
    st = _fetch_log()
    st[symbol] = _to_iso(utcnow())
    FETCH_LOG.write_text(json.dumps(st, indent=2), encoding="utf-8")

def _fresh_enough(symbol: str, ttl_min: int = TTL_MINUTES) -> bool:
    st = _fetch_log()
    ts = st.get(symbol)
    if not ts: return False
    try:
        last = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return False
    return (utcnow() - last) < timedelta(minutes=ttl_min)

def money(x: Optional[float]) -> str:
    try:
        if x is None: return "—"
        v = float(x)
        if math.isfinite(v) is False: return "—"
        if abs(v) >= 1000000000: return f"${v/1e9:.2f}B"
        if abs(v) >= 1000000:    return f"${v/1e6:.2f}M"
        if abs(v) >= 1000:       return f"${v/1e3:.2f}K"
        return "$" + format(v, ",.2f")
    except Exception:
        return "—"

def f1(x) -> str:
    try:
        if x is None or (isinstance(x, float) and (np.isnan(x) or np.isinf(x))): return "—"
        return f"{float(x):.1f}"
    except Exception:
        return "—"

def to_float(x) -> Optional[float]:
    try:
        v = float(x)
        if np.isnan(v) or np.isinf(v): return None
        return v
    except Exception:
        return None

# ---------- rate limit (lightweight; no new deps) ----------
_RL_BUCKETS: Dict[str, Dict[str, List[float]]] = {}
def allow_rate(ip: str, key: str, limit: int, window_sec: int) -> bool:
    now = time.time()
    bucket = _RL_BUCKETS.setdefault(key, {}).setdefault(ip, [])
    # drop old
    while bucket and (now - bucket[0] > window_sec):
        bucket.pop(0)
    if len(bucket) >= limit:
        return False
    bucket.append(now)
    return True

# ---------- address detection / canonicalization ----------
_BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_BASE58_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")  # tightened

def _b58decode_ok(s: str) -> bool:
    # very small validator (no actual numeric conversion necessary)
    if not (32 <= len(s) <= 44): return False
    for ch in s:
        if ch not in _BASE58_ALPHABET: return False
    return True

def is_address(q: str) -> bool:
    if not isinstance(q, str) or not q: return False
    s = q.strip()
    if re.fullmatch(r"0x[a-fA-F0-9]{40}", s): return True  # EVM
    if _BASE58_RE.match(s) and _b58decode_ok(s): return True  # Solana base58 lookalike
    if re.fullmatch(r"(?i)^(eq|uf)[a-z0-9_-]{46}$", s): return True  # TON-ish
    return False

def canonicalize_address(raw: str) -> str:
    s = (raw or "").strip()
    if s.startswith("0X"): s = "0x" + s[2:]
    if s.lower().startswith("0x"):
        return s.lower()
    # Base58 / TON: ask DexScreener search for canonical case
    try:
        r = requests.get("https://api.dexscreener.com/latest/dex/search", params={"q": s}, timeout=10)
        if r.ok:
            js = r.json() or {}
            pairs = js.get("pairs") or []
            ql = s.lower()
            # prefer chain by tiebreaker (handled later), here just recover case of the token addr
            for p in pairs:
                bt = (((p.get("baseToken") or {}).get("address")) or "")
                qt = (((p.get("quoteToken") or {}).get("address")) or "")
                if bt and bt.lower() == ql: return bt
                if qt and qt.lower() == ql: return qt
    except Exception as e:
        LOG.warning("[Canon] DS search failed: %s", e)
    return s

def canonicalize_query(q: str) -> str:
    s = (q or "").strip()
    return canonicalize_address(s) if is_address(s) else s

def _norm_for_cache(s: str) -> str:
    s = (s or "").strip()
    if s.lower().startswith("0x"): return s.lower()
    if is_address(s): return s  # keep base58/ton case as from DS
    return s.upper()

def _disp_symbol(s: str) -> str:
    return s if is_address(s) else s.upper().strip()

# NEW: treat long alphanumerics as “contract‑ish” slugs so we try DS→GT before CC
LONG_ALNUM_RE = re.compile(r"^[A-Za-z0-9]{30,}$")
def looks_contractish(q: str) -> bool:
    s = (q or "").strip()
    if not s: return False
    if is_address(s): return True
    return bool(LONG_ALNUM_RE.match(s))

# ---------- cache io ----------
def _frame_path(symbol: str, ext: str) -> Path:
    return FRAMES_DIR / f"{_norm_for_cache(symbol)}.{ext}"

def save_frame(symbol: str, df: pd.DataFrame) -> None:
    if df is None or df.empty: return
    p_parq = _frame_path(symbol, "parquet")
    p_csv  = _frame_path(symbol, "csv")
    try:
        df.to_parquet(p_parq, index=False)
        return
    except Exception as e:
        LOG.warning("[Cache] Parquet save failed (%s), fallback CSV.", e)
    try:
        df.to_csv(p_csv, index=False)
    except Exception as e:
        LOG.warning("[Cache] CSV save failed: %s", e)

def load_cached_frame(symbol: str) -> pd.DataFrame:
    p_parq = _frame_path(symbol, "parquet")
    p_csv  = _frame_path(symbol, "csv")
    if p_parq.exists():
        try:
            df = pd.read_parquet(p_parq)
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
            return df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
        except Exception:
            pass
    if p_csv.exists():
        try:
            df = pd.read_csv(p_csv)
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
            return df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
        except Exception:
            pass
    return pd.DataFrame()

# ---------- CryptoCompare (symbols only) ----------
CC_BASE = "https://min-api.cryptocompare.com/data"

class CCKeyPool:
    def __init__(self, keys: List[str]):
        self.keys = keys or []
        self.bans: Dict[str, float] = {}
    def pick(self) -> Optional[str]:
        if not self.keys: return None
        now = time.time()
        ok = [k for k in self.keys if self.bans.get(k, 0) <= now]
        return random.choice(ok) if ok else None
    def ban(self, k: str, minutes: int = 30):
        self.bans[k] = time.time() + minutes*60

CC_POOL = CCKeyPool(CC_KEYS)

def cc_get(path: str, params: Dict[str, Any]) -> Optional[dict]:
    last_err = None
    tries = max(1, len(CC_KEYS)) + 1
    for _ in range(tries):
        k = CC_POOL.pick()
        headers = {"Apikey": k} if k else {}
        try:
            r = requests.get(f"{CC_BASE}/{path}", params=params, headers=headers, timeout=15)
            if r.status_code == 200:
                js = r.json()
                if isinstance(js, dict) and (js.get("Response") in (None, "Success")):
                    return js
                last_err = f"bad CC payload {str(js)[:180]}"
                if "limit" in str(js).lower() and k:
                    CC_POOL.ban(k)
            else:
                txt = (r.text or "")[:160]
                last_err = f"HTTP {r.status_code} {txt}"
                if "limit" in txt.lower() and k:
                    CC_POOL.ban(k)
        except Exception as e:
            last_err = str(e)
    LOG.warning("[CC] %s", last_err or "unknown error")
    return None

def cc_hist(symbol: str, kind: str, limit: int, aggregate: int = 1) -> pd.DataFrame:
    if is_address(symbol):  # never hit CC for addresses
        return pd.DataFrame()
    mp = {"minute":"v2/histominute","hour":"v2/histohour","day":"v2/histoday"}
    q  = dict(fsym=_norm_for_cache(symbol), tsym="USD", limit=limit, aggregate=aggregate)
    js = cc_get(mp[kind], q)
    if not js: return pd.DataFrame()
    raw = (js.get("Data") or {}).get("Data") or []
    if not raw: return pd.DataFrame()
    df = pd.DataFrame(raw)
    if "time" in df.columns:
        df["timestamp"] = pd.to_datetime(df["time"], unit="s", utc=True, errors="coerce")
    for c in ["open","high","low","close","volumefrom","volumeto"]:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")
    df["volume"] = df["volumeto"] if "volumeto" in df.columns else pd.NA
    cols = ["timestamp","open","high","low","close","volume"]
    return df[cols].dropna(subset=["timestamp"]).sort_values("timestamp")

# ---------- CoinGecko ----------
CG_BASE = "https://api.coingecko.com/api/v3"
def cg_headers() -> dict:
    return {"x-cg-demo-api-key": CG_KEY} if CG_KEY else {}

def cg_get(path: str, params: dict) -> Optional[dict]:
    try:
        r = requests.get(f"{CG_BASE}/{path}", params=params, headers=cg_headers(), timeout=20)
        if r.status_code == 200:
            return r.json()
        LOG.warning("[CG] %s %s", r.status_code, r.text[:160])
    except Exception as e:
        LOG.warning("[CG] %s", e)
    return None

def cg_fetch_coin_list() -> list:
    try:
        if COIN_LIST_PATH.exists():
            age = time.time() - COIN_LIST_PATH.stat().st_mtime
            if age < COIN_LIST_TTL:
                return json.loads(COIN_LIST_PATH.read_text(encoding="utf-8"))
        LOG.info("[CG] fetching /coins/list?include_platform=true")
        r = requests.get(f"{CG_BASE}/coins/list", params={"include_platform": "true"}, headers=cg_headers(), timeout=30)
        if r.status_code == 200:
            coins = r.json()
            COIN_LIST_PATH.write_text(json.dumps(coins), encoding="utf-8")
            return coins
        LOG.warning("[CG] list HTTP %s", r.status_code)
    except Exception as e:
        LOG.warning("[CG] coin list failed: %s", e)
    try:
        if COIN_LIST_PATH.exists():
            return json.loads(COIN_LIST_PATH.read_text(encoding="utf-8"))
    except Exception:
        pass
    return []

CG_IDS = {
    "BTC":"bitcoin","ETH":"ethereum","SOL":"solana","BNB":"binancecoin","XRP":"ripple",
    "ADA":"cardano","DOGE":"dogecoin","LINK":"chainlink","AVAX":"avalanche-2","TON":"the-open-network"
}

def cg_id_for_symbol_or_contract(q: str) -> Optional[str]:
    if not q: return None
    q_raw = q.strip()
    if not is_address(q_raw):
        if q_raw.upper() in CG_IDS:
            return CG_IDS[q_raw.upper()]
    coins = cg_fetch_coin_list()
    if not coins:
        return CG_IDS.get(q_raw.upper()) if not is_address(q_raw) else None

    ql = q_raw.lower()

    # Contract resolution across known platforms
    if is_address(q_raw):
        for c in coins:
            plats = c.get("platforms") or {}
            for _, addr in plats.items():
                if addr and str(addr).lower() == ql:
                    return c["id"]
        return None

    # ID exact
    for c in coins:
        if (c.get("id","") or "").lower() == ql:
            return c["id"]

    # Symbol match (prefer mapped first)
    matches = [c for c in coins if (c.get("symbol","") or "").lower() == ql]
    if matches:
        pref = CG_IDS.get(q_raw.upper())
        if pref and any(c["id"] == pref for c in matches):
            return pref
        # prefer those with ethereum platform
        for c in matches:
            plats = (c.get("platforms") or {})
            if any(k.lower() == "ethereum" and (v or "").strip() for k,v in plats.items()):
                return c["id"]
        return matches[0]["id"]

    # Name
    for c in coins:
        if (c.get("name","") or "").lower() == ql:
            return c["id"]

    return CG_IDS.get(q_raw.upper())

def cg_series(symbol_or_contract: str, days: int = 30) -> pd.DataFrame:
    cg_id = cg_id_for_symbol_or_contract(symbol_or_contract)
    if not cg_id:
        return pd.DataFrame()
    js = cg_get(f"coins/{cg_id}/market_chart", {"vs_currency":"usd","days":days}) \
        or cg_get(f"coins/{cg_id}/market_chart", {"vs_currency":"usd","days":max(7,days//2)})
    if not js: return pd.DataFrame()
    px   = js.get("prices") or []
    vols = js.get("total_volumes") or []
    caps = js.get("market_caps") or []

    if not px: return pd.DataFrame()
    dp = pd.DataFrame(px, columns=["ts","close"])
    dp["timestamp"] = pd.to_datetime(dp["ts"], unit="ms", utc=True, errors="coerce")
    dp["close"] = pd.to_numeric(dp["close"], errors="coerce")

    if vols:
        dv = pd.DataFrame(vols, columns=["ts","v"])
        dv["timestamp"] = pd.to_datetime(dv["ts"], unit="ms", utc=True, errors="coerce")
        dv["volume"] = pd.to_numeric(dv["v"], errors="coerce")
        dp = dp.merge(dv[["timestamp","volume"]], on="timestamp", how="left")

    if caps:
        dc = pd.DataFrame(caps, columns=["ts","mc"])
        dc["timestamp"] = pd.to_datetime(dc["ts"], unit="ms", utc=True, errors="coerce")
        dc["market_cap"] = pd.to_numeric(dc["mc"], errors="coerce")
        dp = dp.merge(dc[["timestamp","market_cap"]], on="timestamp", how="left")

    # synth OHLC for indicators (price)
    dp["open"] = dp["close"].shift(1)
    dp["high"] = dp["close"].rolling(3, min_periods=1).max()
    dp["low"]  = dp["close"].rolling(3, min_periods=1).min()
    return dp[["timestamp","open","high","low","close","volume","market_cap"]].dropna(subset=["timestamp"]).sort_values("timestamp")

# ---------- DexScreener + GeckoTerminal + Birdeye ----------
DS_BASE = "https://api.dexscreener.com"
GT_BASE = "https://api.geckoterminal.com/api/v2"

# authoritative mapping (confirmed via Grok)
DS_TO_GT = {
    "ethereum": "eth",
    "bsc": "bsc",
    "polygon": "polygon",
    "arbitrum": "arbitrum",
    "base": "base",
    "optimism": "optimism",
    "avalanche": "avalanche",
    "fantom": "fantom",
    "linea": "linea",
    "zk_sync_era": "zk_sync_era",
    "zksync": "zk_sync_era",
    "polygon_pos": "polygon",
    "solana": "solana",
    "sui": "sui",
    "ton": "ton",
    "blast": "blast",
    "pulsechain": "pulsechain",        # ✅
}

PREFERRED_QUOTES = {"USDC","USDT","SOL","ETH","WETH","USD"}

def safe_fetch(url: str, params: dict | None = None, retries: int = 3, timeout: int = 12) -> Optional[requests.Response]:
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params or {}, timeout=timeout, headers={"Accept":"application/json"})
            if r.status_code == 429:
                sleep = [1,4,16][min(attempt,2)]
                time.sleep(sleep)
                continue
            return r
        except requests.Timeout:
            time.sleep([1,4,16][min(attempt,2)])
        except Exception as e:
            LOG.warning("[fetch] %s", e)
            time.sleep(1)
    return None

def safe_resample(df: pd.DataFrame, tf: str) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    return resample_for_tf(df, tf)

def ds_get(path: str, params: Optional[dict] = None) -> Optional[dict | list]:
    r = safe_fetch(f"{DS_BASE}{path}", params=params)
    if r and r.status_code == 200:
        try:
            return r.json()
        except Exception as e:
            LOG.warning("[DS] JSON decode failed: %s...", e)
            return None
    if r:
        LOG.warning("[DS] HTTP %s for %s (%s)", r.status_code, path, (r.text or "")[:160])
    return None

def gt_get(path: str, params: Optional[dict] = None) -> Optional[dict]:
    r = safe_fetch(f"{GT_BASE}{path}", params=params)
    if r and r.status_code == 200:
        try:
            return r.json()
        except Exception as e:
            LOG.warning("[GT] JSON decode failed: %s", e)
            return None
    if r:
        LOG.warning("[GT] %s %s", r.status_code, (r.text or "")[:160])
    return None

def _collect_pairs_from_ds_payload(js) -> List[dict]:
    pairs: List[dict] = []
    if isinstance(js, list):
        pairs.extend([p for p in js if isinstance(p, dict)])
    elif isinstance(js, dict):
        if isinstance(js.get("pairs"), list):
            pairs.extend([p for p in js.get("pairs") if isinstance(p, dict)])
        if isinstance(js.get("data"), dict) and isinstance(js["data"].get("pairs"), list):
            pairs.extend([p for p in js["data"]["pairs"] if isinstance(p, dict)])
    return pairs

def ds_pairs_for_token(addr: str) -> List[dict]:
    pairs: List[dict] = []
    # token-pairs on known chains first (fast)
    for ch in ["solana","ethereum","base","arbitrum","bsc","polygon","optimism","avalanche","fantom","linea","zksync","blast","sui","ton"]:
        js1 = ds_get(f"/token-pairs/v1/{ch}/{addr}")
        pairs.extend(_collect_pairs_from_ds_payload(js1))
    # plus search as catch-all
    js2 = ds_get("/latest/dex/search", params={"q": addr})
    cand = _collect_pairs_from_ds_payload(js2)
    if cand:
        ql = addr.lower()
        for p in cand:
            try:
                bt = (((p.get("baseToken") or {}).get("address") or "")).lower()
                qt = (((p.get("quoteToken") or {}).get("address") or "")).lower()
                if ql in (bt, qt):
                    pairs.append(p)
            except Exception:
                continue
    return pairs

def pick_chain_tiebreaker(pairs: List[dict]) -> Optional[str]:
    """Decide which chain to use:
       - Always pick highest 24h volume unless priority chain is within 2x (respect priority then).
    """
    if not pairs: return None
    # group by chain highest volume
    by_chain: Dict[str, dict] = {}
    for p in pairs:
        ch = p.get("chainId")
        if not ch: continue
        best = by_chain.get(ch)
        if not best or (p.get("volume", {}).get("h24") or 0) > (best.get("volume", {}).get("h24") or 0):
            by_chain[ch] = p
    if not by_chain: return None
    # compute highest vol across chains
    vols = [(ch, by_chain[ch].get("volume", {}).get("h24") or 0) for ch in by_chain]
    ch_max, v_max = max(vols, key=lambda x: x[1])
    # chain priority list (only used if within 2x)
    PRIORITY = ["solana","ethereum","base","arbitrum","bsc","polygon","optimism","avalanche","fantom","linea","zksync","blast","sui","ton"]
    # within 2x? respect priority order
    within_2x = {ch: v for ch, v in vols if v >= (v_max / 2.0)}
    if within_2x and len(within_2x) > 1:
        for ch in PRIORITY:
            if ch in within_2x:
                return ch
    # else pick highest vol chain
    return ch_max

def score_pair(p: dict) -> float:
    try:
        liq = float(((p.get("liquidity") or {}).get("usd")) or 0.0)
        vol_h24 = float(((p.get("volume") or {}).get("h24")) or 0.0)
        quote = (((p.get("quoteToken") or {}).get("symbol")) or "").upper()
        dexid = (p.get("dexId") or "").lower()
    except Exception:
        return 0.0
    if vol_h24 < 5000:  # discard ultra-low activity
        return 0.0
    if liq < 10000:
        return 0.0
    score = liq
    if quote in PREFERRED_QUOTES: score *= 2.0
    if vol_h24 >= 50000: score += (vol_h24 / 1000.0)
    if dexid in ("raydium","uniswap"): score *= 1.2
    return float(score)

def pick_best_pair(pairs: List[dict]) -> Optional[dict]:
    if not pairs: return None
    # pick chain first (tiebreaker)
    target_chain = pick_chain_tiebreaker(pairs)
    cand = [p for p in pairs if p.get("chainId") == target_chain] if target_chain else pairs
    scored = [(score_pair(p), p) for p in cand]
    scored = [t for t in scored if t[0] > 0]
    if not scored:
        # relax min liq to 2500 if nothing qualified
        def score_relax(p):
            try:
                liq = float(((p.get("liquidity") or {}).get("usd")) or 0.0)
                vol_h24 = float(((p.get("volume") or {}).get("h24")) or 0.0)
                if vol_h24 < 2500 or liq < 2500: return 0.0
                base = liq + vol_h24/2000.0
                return base
            except Exception:
                return 0.0
        scored = [(score_relax(p), p) for p in cand]
        scored = [t for t in scored if t[0] > 0]
        if scored:
            LOG.info("[DS] Low liquidity pool selected (relaxed)")
    if not scored:
        return None
    scored.sort(key=lambda t: t[0], reverse=True)
    best = scored[0][1]
    return best

def token_meta_for(q: str) -> dict:
    """DS first → pick best pair; accept quoteToken if necessary. Enrich with CG if missing name/symbol."""
    meta = {
        "name":"Unknown","symbol":"UNK","chain":"","dexId":"","pairAddress":"",
        "decimals": None, "label": q[:10]+"...", "marketCap": None, "fdv": None,
        "liq_usd": None, "vol_h24": None
    }
    try:
        js = ds_get("/latest/dex/search", params={"q": q})
        pairs = _collect_pairs_from_ds_payload(js)
        if pairs:
            best = pick_best_pair(pairs)
            if best:
                chain = best.get("chainId") or ""
                token = None
                ql = q.lower()
                bt = (((best.get("baseToken") or {}).get("address") or "")).lower()
                qt = (((best.get("quoteToken") or {}).get("address") or "")).lower()
                if ql == bt:
                    token = best.get("baseToken") or {}
                elif ql == qt:
                    token = best.get("quoteToken") or {}
                else:
                    # prefer baseToken in the chosen chain
                    token = best.get("baseToken") or {}
                sym = token.get("symbol") or "UNK"
                name = token.get("name") or "Unknown"
                decs = token.get("decimals")
                liq  = (best.get("liquidity") or {}).get("usd")
                vol  = (best.get("volume") or {}).get("h24")
                meta.update({
                    "name": name, "symbol": sym, "chain": chain, "dexId": best.get("dexId") or "",
                    "pairAddress": best.get("pairAddress") or "", "decimals": decs,
                    "marketCap": best.get("marketCap"), "fdv": best.get("fdv"),
                    "liq_usd": liq, "vol_h24": vol,
                })
                meta["label"] = f"{name} ({sym}) — {meta['dexId'].capitalize()}/{meta['chain'].capitalize()}"
                return meta
    except Exception as e:
        LOG.warning("[Meta] DS failed: %s", e)
    # CG enrich name/symbol if possible (best-effort)
    try:
        cg_id = cg_id_for_symbol_or_contract(q)
        if cg_id:
            data = cg_get(f"coins/{cg_id}", {
                "localization":"false","tickers":"false","market_data":"false",
                "community_data":"false","developer_data":"false","sparkline":"false",
            })
            if data:
                name = data.get("name") or meta["name"]
                sym  = (data.get("symbol") or meta["symbol"] or "").upper()
                meta.update({"name":name, "symbol":sym})
                meta["label"] = f"{name} ({sym})"
    except Exception:
        pass
    return meta

def gt_ohlcv_by_pool(network: str, pool_id: str, tf: str, limit: int = 500) -> pd.DataFrame:
    js = gt_get(f"/networks/{network}/pools/{pool_id}/ohlcv/{tf}", params={"limit": limit})
    if not js: return pd.DataFrame()
    # GT returns {data:{attributes:{ohlcv_list:[[ts, o,h,l,c, v],...]}}}
    data = js.get("data")
    attrs = None
    if isinstance(data, dict):
        attrs = data.get("attributes") or {}
    elif isinstance(data, list) and data:
        attrs = data[0].get("attributes") or {}
    rows = (attrs or {}).get("ohlcv_list") or []
    recs = []
    for row in rows:
        if not isinstance(row, (list,tuple)) or len(row) < 6: continue
        ts_raw = int(row[0])
        ts = pd.to_datetime(ts_raw if ts_raw < 10**12 else ts_raw/1000, unit="s", utc=True, errors="coerce")
        recs.append({
            "timestamp": ts,
            "open": float(row[1]), "high": float(row[2]), "low": float(row[3]), "close": float(row[4]),
            "volume": float(row[5]),
        })
    if not recs: return pd.DataFrame()
    return pd.DataFrame.from_records(recs).dropna(subset=["timestamp"]).sort_values("timestamp")

def gt_find_token_pools(network: str, addr: str) -> List[str]:
    js = gt_get(f"/networks/{network}/tokens/{addr}/pools", params={"include":"base_token,quote_token"})
    ids = []
    try:
        data = js.get("data") if js else None
        if isinstance(data, list):
            # prefer by 24h volume if attribute present, otherwise order given
            sorted_ids = []
            for d in data:
                pid = d.get("id")
                vol = (((d.get("attributes") or {}).get("volume_usd") or {}).get("h24")) or 0
                sorted_ids.append((vol or 0, pid))
            sorted_ids.sort(key=lambda t: t[0], reverse=True)
            ids = [pid for _, pid in sorted_ids]
    except Exception:
        pass
    return ids[:3]  # limit to top 3

def birdeye_ohlc_solana(addr: str, tf: str) -> pd.DataFrame:
    """
    Birdeye public history_price returns items[{unixTime, value}] (close only). We convert to OHLC=close and volume=0.
    tf map: 1T/5T/15T -> '1m','5m','15m'; 1H->'1h'; 4H->'4h'; 1D->'1d'
    """
    tf_map = {"1T":"1m","5T":"5m","15T":"15m","1H":"1h","4H":"4h","1D":"1d"}
    birdeye_tf = tf_map.get(tf, "1h")
    now = int(time.time())
    # 2 days window for minute, 7d for hour, 30d for 4h, 365d day
    if birdeye_tf in ("1m","5m","15m"): start = now - 2*24*3600
    elif birdeye_tf == "1h": start = now - 7*24*3600
    elif birdeye_tf == "4h": start = now - 30*24*3600
    else: start = now - 365*24*3600
    url = "https://public-api.birdeye.so/defi/history_price"
    headers = {"X-API-KEY": BIRDEYE_KEY, "accept": "application/json"}
    params  = {"address": addr, "address_type":"token", "type": birdeye_tf, "time_from": start, "time_to": now}
    r = safe_fetch(url, params=params, timeout=12)
    if not (r and r.ok): return pd.DataFrame()
    js = r.json() or {}
    items = (js.get("data") or {}).get("items") or []
    if not items: return pd.DataFrame()
    recs = []
    for it in items:
        ts = pd.to_datetime(int(it.get("unixTime") or 0), unit="s", utc=True, errors="coerce")
        close = float(it.get("value") or 0)
        if not ts or close <= 0: continue
        recs.append({"timestamp": ts, "open": close, "high": close, "low": close, "close": close, "volume": 0.0})
    return pd.DataFrame.from_records(recs).dropna(subset=["timestamp"]).sort_values("timestamp")

# ---------- supply / decimals ----------
_SUPPLY_CACHE: Dict[str, dict] = {}  # key: f"{chain}:{addr}" -> {"ts":..., "decimals":..., "supply":...}

def _cache_supply_get(chain: str, addr: str) -> Optional[dict]:
    k = f"{chain}:{addr}"
    item = _SUPPLY_CACHE.get(k)
    if not item: return None
    if time.time() - item["ts"] > 3600:  # 1h TTL
        return None
    return item

def _cache_supply_put(chain: str, addr: str, dec: Optional[int], supply: Optional[float]):
    k = f"{chain}:{addr}"
    _SUPPLY_CACHE[k] = {"ts": time.time(), "decimals": dec, "supply": supply}

def get_evm_supply_decimals(addr: str, chain: str) -> Tuple[int, Optional[float]]:
    cached = _cache_supply_get(chain, addr)
    if cached: return cached["decimals"] or 18, cached["supply"]
    rpc_map = {
        "ethereum": "https://rpc.ankr.com/eth",
        "bsc":      "https://bsc-dataseed.binance.org",
        "polygon":  "https://polygon-rpc.com",
        "base":     "https://mainnet.base.org",
        "arbitrum": "https://arb1.arbitrum.io/rpc",
        "optimism": "https://mainnet.optimism.io",
        "fantom":   "https://rpc.ftm.tools",
        "avalanche":"https://api.avax.network/ext/bc/C/rpc",
        "linea":    "https://rpc.linea.build",
        "zksync":   "https://mainnet.era.zksync.io",
        "zk_sync_era":"https://mainnet.era.zksync.io",
        "blast":    "https://rpc.blast.io"
    }
    rpc = rpc_map.get(chain)
    if not rpc:
        _cache_supply_put(chain, addr, 18, None)
        return 18, None
    try:
        # decimals()
        payload = {"jsonrpc":"2.0","method":"eth_call","params":[{"to":addr,"data":"0x313ce567"}, "latest"],"id":1}
        r = requests.post(rpc, json=payload, timeout=12)
        dec = 18
        if r.ok and isinstance(r.json(), dict) and r.json().get("result"):
            dec = int(r.json()["result"], 16)
        # totalSupply()
        payload["params"][0]["data"] = "0x18160ddd"
        r2 = requests.post(rpc, json=payload, timeout=12)
        supply = None
        if r2.ok and isinstance(r2.json(), dict) and r2.json().get("result"):
            raw = int(r2.json()["result"], 16)
            supply = raw / (10**dec) if dec is not None else None
        _cache_supply_put(chain, addr, dec, supply)
        return dec, supply
    except Exception as e:
        LOG.warning("[EVM supply] %s", e)
        _cache_supply_put(chain, addr, 18, None)
        return 18, None

def get_solana_supply_decimals(addr: str) -> Tuple[int, Optional[float]]:
    cached = _cache_supply_get("solana", addr)
    if cached: return cached["decimals"] or 9, cached["supply"]
    try:
        r = safe_fetch("https://api.solscan.io/token/meta", params={"token": addr}, timeout=12)
        if r and r.ok:
            js = r.json() or {}
            data = js.get("data") or {}
            dec = int(data.get("decimals") or 9)
            supply_raw = float(data.get("supply") or 0.0)
            supply = supply_raw / (10**dec) if dec else None
            _cache_supply_put("solana", addr, dec, supply)
            return dec, supply
    except Exception as e:
        LOG.warning("[Solana supply] %s", e)
    _cache_supply_put("solana", addr, 9, None)
    return 9, None

# ---------- indicators ----------
def ema(s: pd.Series, span: int) -> pd.Series:
    return s.ewm(span=span, adjust=False).mean()

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df.copy()
    out = df.copy()

    # RSI(14)
    delta = out["close"].diff()
    up = delta.clip(lower=0.0)
    dn = -delta.clip(upper=0.0)
    rs = up.ewm(span=14, adjust=False).mean() / dn.replace(0,1e-9).ewm(span=14, adjust=False).mean()
    out["rsi"] = 100 - (100/(1+rs))

    # MACD (12,26,9)
    ema12 = ema(out["close"], 12); ema26 = ema(out["close"], 26)
    out["macd_line"]   = ema12 - ema26
    out["macd_signal"] = ema(out["macd_line"], 9)
    out["macd_hist"]   = out["macd_line"] - out["macd_signal"]

    # Bollinger (20,2) + width
    ma20  = out["close"].rolling(20).mean()
    std20 = out["close"].rolling(20).std()
    out["bb_mid"]   = ma20
    out["bb_upper"] = ma20 + 2*std20
    out["bb_lower"] = ma20 - 2*std20
    out["bb_width"] = ((out["bb_upper"] - out["bb_lower"]) / ma20.replace(0,np.nan) * 100).fillna(0)

    # ADX(14)
    try:
        hi, lo, cl = out["high"].fillna(out["close"]), out["low"].fillna(out["close"]), out["close"]
        plus_dm  = (hi.diff().where(hi.diff() > lo.diff(), 0.0)).clip(lower=0)
        minus_dm = (lo.diff().where(lo.diff() > hi.diff(), 0.0)).clip(lower=0).abs()
        tr  = pd.concat([(hi-lo),(hi-cl.shift()).abs(),(lo-cl.shift()).abs()], axis=1).max(axis=1)
        atr = tr.rolling(14).mean()
        plus_di  = 100 * (plus_dm.rolling(14).mean()  / atr.replace(0,np.nan))
        minus_di = 100 * (minus_dm.rolling(14).mean() / atr.replace(0,np.nan))
        dx  = (100 * (plus_di - minus_di).abs() / (plus_di+minus_di).replace(0,np.nan)).fillna(0)
        out["adx14"] = dx.rolling(14).mean()
    except Exception:
        out["adx14"] = np.nan

    # OBV
    try:
        vol = out["volume"].fillna(0)
        direction = np.sign(out["close"].diff()).fillna(0)
        out["obv"] = (direction * vol).cumsum()
    except Exception:
        out["obv"] = np.nan

    # ATR(14)
    try:
        high = out["high"].fillna(out["close"])
        low  = out["low"].fillna(out["close"])
        close= out["close"]
        tr = pd.concat([(high-low), (high-close.shift()).abs(), (low-close.shift()).abs()], axis=1).max(axis=1)
        out["atr14"] = tr.rolling(14, min_periods=1).mean()
    except Exception:
        out["atr14"] = np.nan

    # ALT momentum (simple EMA spread)
    try:
        out["alt_momentum"] = (ema(out["close"], 10) - ema(out["close"], 30))
    except Exception:
        out["alt_momentum"] = np.nan

    return out

# ---------- timeframe windows & resample ----------
LOOKBACK = {
    "1h": timedelta(hours=1), "4h": timedelta(hours=4), "8h": timedelta(hours=8),
    "12h": timedelta(hours=12), "24h": timedelta(days=1), "7d": timedelta(days=7),
    "30d": timedelta(days=30), "1y": timedelta(days=365), "all": None
}
RESAMPLE_BY_TF = {
    "1h": "1T",   # 1 minute
    "4h": "5T",
    "8h": "5T",
    "12h": "15T",
    "24h": "15T",
    "7d": "1H",
    "30d": "4H",
    "1y": "1D",
    "all": "1D",
}

def slice_df(df: pd.DataFrame, tf: str) -> pd.DataFrame:
    if df.empty: return df
    win = LOOKBACK.get(tf, timedelta(hours=4))
    if win is None: return df
    anchor = pd.to_datetime(df["timestamp"]).max()
    cutoff = (anchor - win) if pd.notna(anchor) else (utcnow() - win)
    out = df[df["timestamp"] >= cutoff].copy()
    return out

def resample_for_tf(df: pd.DataFrame, tf: str) -> pd.DataFrame:
    """
    Robust resampler for any candle shape:
      - Works if some columns are missing (e.g., no market_cap, no volume)
      - Synthesizes O/H/L if only 'close' is present
      - Chooses frequency by UI timeframe
      - Never raises KeyError if columns are absent
    """
    if df is None or df.empty:
        return df

    if "timestamp" not in df.columns:
        return df

    out = df.copy()

    # Normalize timestamps (supports seconds or ms)
    def _to_ms(ts):
        try:
            t = float(ts)
            return int(t * 1000) if t < 1e10 else int(t)
        except Exception:
            return ts

    out["timestamp"] = pd.to_datetime(out["timestamp"].apply(_to_ms), unit="ms", utc=True, errors="coerce")
    out = out.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    if out.empty:
        return out

    # If only 'close' exists, synthesize minimal OHLC for plotting/indicators
    if "close" in out.columns:
        if "open" not in out.columns or out["open"].isna().all():
            out["open"] = out["close"].shift(1).fillna(out["close"])
        if "high" not in out.columns:
            out["high"] = out[["open", "close"]].max(axis=1)
        if "low" not in out.columns:
            out["low"] = out[["open", "close"]].min(axis=1)

    # Pick resample frequency by UI timeframe
    FREQ_MAP = {
        "1h": "1T",   # 1 minute
        "4h": "5T",   # 5 minutes
        "8h": "5T",
        "12h": "15T", # 15 minutes
        "24h": "15T",
        "7d": "1H",
        "30d": "4H",
        "1y": "1D",
        "all": "1D",
    }
    freq = FREQ_MAP.get(tf, "1H")

    idx = out.set_index("timestamp")

    # Build aggregation dict ONLY for columns that exist
    agg_dict = {}
    if "open" in idx.columns:  agg_dict["open"]  = "first"
    if "high" in idx.columns:  agg_dict["high"]  = "max"
    if "low"  in idx.columns:  agg_dict["low"]   = "min"
    if "close" in idx.columns: agg_dict["close"] = "last"
    if "volume" in idx.columns: agg_dict["volume"] = "sum"
    if "market_cap" in idx.columns: agg_dict["market_cap"] = "last"

    # If somehow nothing to aggregate, return original data
    if not agg_dict:
        return out

    res = idx.resample(freq).agg(agg_dict)

    # Forward-fill price fields; keep volume as-is, cap as last
    for col in ("open", "high", "low", "close"):
        if col in res.columns:
            res[col] = res[col].ffill()

    if "market_cap" in res.columns:
        res["market_cap"] = res["market_cap"].ffill()

    # If after resample we got nothing, fall back to original
    if res is None or res.empty:
        return out

    res = res.ffill().reset_index()
    return res

def value_at_or_before(df: pd.DataFrame, hours: int, anchor: Optional[datetime]=None) -> Optional[float]:
    if df.empty: return None
    anchor = anchor or (pd.to_datetime(df["timestamp"]).max().to_pydatetime() if not df.empty else utcnow())
    t = anchor - timedelta(hours=hours)
    older = df[df["timestamp"] <= t]
    if older.empty:
        return to_float(df["close"].iloc[0])
    return to_float(older["close"].iloc[-1])

def compute_rollups(df: pd.DataFrame) -> Tuple[Dict[str, Optional[float]], Dict[str, Optional[float]]]:
    if df.empty: return {}, {}
    ch = {}
    anchor = pd.to_datetime(df["timestamp"]).max().to_pydatetime() if not df.empty else utcnow()
    last = to_float(df["close"].iloc[-1])
    for k, hrs in [("1h",1),("4h",4),("8h",8),("12h",12),("24h",24),("7d",24*7),("30d",24*30),("1y",24*365)]:
        base = value_at_or_before(df, hrs, anchor)
        ch[k] = None if (base in (None,0) or last in (None,0)) else round((last/base-1)*100, 2)
    inv = {k: (None if v is None else round(1000*(1+v/100.0),2)) for k,v in ch.items()}
    return ch, inv

# ---------- DS/GT hydrate for addresses ----------
def _gt_tf_for_days(days: int) -> str:
    if days <= 2: return "5m"
    if days <= 7: return "15m"
    if days <= 30: return "1h"
    if days <= 120: return "4h"
    return "1d"

def _tf_to_gt(tf: str) -> str:
    # Map UI tf to GT timeframe for pool fetch
    mp = {"1h":"5m","4h":"15m","8h":"15m","12h":"1h","24h":"1h","7d":"1h","30d":"4h","1y":"1d","all":"1d"}
    return mp.get(tf, "1h")

def ds_series_via_gt(addr: str, tf: str, days_hint: int = 365) -> Tuple[pd.DataFrame, Optional[dict]]:
    pairs = ds_pairs_for_token(addr)
    best = pick_best_pair(pairs)
    if not best:
        LOG.info("[DS] no pairs for %s", addr)
        return pd.DataFrame(), None
    chain  = (best.get("chainId") or "").lower().strip()
    pair   = best.get("pairAddress") or ""
    net    = DS_TO_GT.get(chain, chain)
    if not net or not pair:
        LOG.info("[DS→GT] missing net/pair for %s (chain=%s, pair=%s)", addr, chain, pair)
        return pd.DataFrame(), best
    gt_tf = _tf_to_gt(tf) if tf in RESAMPLE_BY_TF else _gt_tf_for_days(days_hint)
    LOG.info("[DS→GT] GT OHLCV net=%s pair=%s tf=%s", net, pair, gt_tf)
    df = gt_ohlcv_by_pool(net, pair, gt_tf, limit=500)
    if (df is None or df.empty):
        # Try GT token pools (limit 3), then Birdeye (Solana only)
        pools = gt_find_token_pools(net, addr)
        for pid in pools:
            LOG.info("[DS→GT] trying token pool id %s", pid)
            df = gt_ohlcv_by_pool(net, pid, gt_tf, limit=500)
            if not df.empty: break
        if (df is None or df.empty) and chain == "solana":
            tf_resample = RESAMPLE_BY_TF.get(tf, "1H")
            LOG.info("[DS→GT] GT empty, trying Birdeye for %s (%s)", addr, tf_resample)
            df = birdeye_ohlc_solana(addr, tf_resample)
    if df is not None and not df.empty and "market_cap" not in df.columns:
        df["market_cap"] = np.nan
    return df, best

# ---------- cap/FDV derivation ----------
def derive_cap_or_fdv(meta: dict, ds_pair: Optional[dict], cg_token: Optional[dict]) -> Tuple[Optional[float], str]:
    # precedence: CG.cap → DS.marketCap → CG.FDV → DS.fdv → GT (ignored here)
    try:
        if cg_token:
            mc = (((cg_token.get("market_data") or {}).get("market_cap") or {}).get("usd"))
            if mc: return float(mc), "Market Cap"
        if ds_pair and ds_pair.get("marketCap"):
            return float(ds_pair["marketCap"]), "Market Cap"
        if cg_token:
            fdv = (((cg_token.get("market_data") or {}).get("fully_diluted_valuation") or {}).get("usd"))
            if fdv: return float(fdv), "FDV (est.) — supply unverified"
        if ds_pair and ds_pair.get("fdv"):
            return float(ds_pair["fdv"]), "FDV (est.) — supply unverified"
    except Exception:
        pass
    return None, "n/a"

def should_use_cap_view(price: Optional[float], decimals: Optional[int]) -> bool:
    if price is None: return True
    if price <= 0.10: return True
    if decimals is not None and decimals >= 9: return True
    return False

# ---------- master hydrate ----------
META_CACHE: Dict[str, dict] = {}  # normalized key -> meta

def hydrate_symbol(query: str, force: bool=False, tf_for_fetch: str="12h") -> pd.DataFrame:
    raw_in = (query or "").strip()
    raw = canonicalize_query(raw_in)
    s_for_cache = _norm_for_cache(raw)

    # cache
    if (not force) and _fresh_enough(s_for_cache):
        cached = load_cached_frame(s_for_cache)
        if not cached.empty:
            LOG.info("[Hydrate] %s served from fresh cache", s_for_cache)
            return cached

    LOG.info("[Hydrate] %s (force=%s, ttl=%ss)", s_for_cache, force, TTL_SECONDS)

    is_addr = is_address(s_for_cache)
    df = pd.DataFrame()

    # meta (name/symbol/etc.) for labels & facts
    meta = token_meta_for(raw)
    META_CACHE[s_for_cache] = meta

    if is_addr:
        # DS -> GT -> Birdeye
        df, best_pair = ds_series_via_gt(raw, tf_for_fetch)
        # Try to add CG caps (point series) if missing market_cap
        if (df is None or df.empty) and best_pair is None:
            LOG.info("[Hydrate] DS/GT empty; trying CG series for %s", raw)
            tmp = cg_series(raw, days=365)
            if tmp is None or tmp.empty:
                tmp = cg_series(raw, days=30)
            df = tmp
        # compute indicators later after optional cap scaling
    else:
        # NEW: if it looks like a contract slug (long alnum), try DS→GT first
        if looks_contractish(s_for_cache):
            LOG.info("[Hydrate] %s looks contract-ish → DS/GT first", s_for_cache)
            df_try, best_try = ds_series_via_gt(raw, tf_for_fetch)
            if df_try is not None and not df_try.empty:
                df = df_try
            else:
                LOG.info("[Hydrate] DS/GT empty for %s → continuing to CC", s_for_cache)

        # If still empty, do the CC→CG path for real symbols
        if df is None or df.empty:
            m = cc_hist(s_for_cache, "minute", limit=360)
            h = cc_hist(s_for_cache, "hour",   limit=24*30)
            d = cc_hist(s_for_cache, "day",    limit=365)
            if (m is None or m.empty) and (h is None or h.empty) and (d is None or d.empty):
                LOG.info("[Hydrate] CC empty → CG fallback for %s", s_for_cache)
                tmp = cg_series(raw, days=365)
                if tmp is None or tmp.empty:
                    tmp = cg_series(raw, days=30)
                df = tmp
            else:
                parts = [x for x in (d,h,m) if x is not None and not x.empty]
                df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

    if df is None or df.empty:
        cached = load_cached_frame(s_for_cache)
        if not cached.empty:
            _touch_fetch(s_for_cache)
            LOG.info("[Hydrate] %s returning from older cache", s_for_cache)
            return cached
        LOG.warning("[Hydrate] %s no data after routing", s_for_cache)
        return pd.DataFrame()

    # add cap series (cap-mode) for addresses when we can estimate supply
    if is_addr:
        chain = (meta.get("chain") or "").lower()
        addr  = raw
        decs  = meta.get("decimals")
        supply = None
        try:
            if chain == "solana":
                decs2, supply = get_solana_supply_decimals(addr)
                decs = decs if decs is not None else decs2
            elif chain:
                decs2, supply = get_evm_supply_decimals(addr, chain)
                decs = decs if decs is not None else decs2
        except Exception:
            pass
        # if supply is available, compute market_cap per candle
        if supply and "close" in df.columns:
            df["market_cap"] = df["close"] * float(supply)

    # indicators (computed on price-close)
    df = compute_indicators(df)

    # save
    save_frame(s_for_cache, df)
    _touch_fetch(s_for_cache)

    # record hotset
    try:
        HOTSET.parent.mkdir(parents=True, exist_ok=True)
        with HOTSET.open("a", encoding="utf-8") as f:
            f.write(s_for_cache + "\n")
    except Exception:
        pass

    return df

# ---------- figures ----------
def _apply_time_axis(fig: go.Figure) -> None:
    fig.update_xaxes(
        tickformatstops=[
            dict(dtickrange=[None, 1000*60*60*24], value="%H:%M"),
            dict(dtickrange=[1000*60*60*24, None], value="%m-%d"),
        ]
    )

def _add_fibs(fig: go.Figure, series: pd.Series, use_extension: bool=False) -> None:
    if series is None or series.empty: return
    high, low = float(series.max()), float(series.min())
    diff = high - low if high >= low else 0.0
    levels = [(low, "Fib 0%"), (low + 0.236*diff, "Fib 23.6%"), (low + 0.5*diff, "Fib 50%"),
              (low + 0.618*diff, "Fib 61.8%"), (high, "Fib 100%")]
    if use_extension:
        levels.append((high + 0.618*diff, "Fib 161.8% Ext"))
    for y, label in levels:
        fig.add_hline(y=y, line_dash="dot", opacity=0.25, annotation_text=label, annotation_position="top right")

def fig_price(df: pd.DataFrame, symbol: str, y_mode: str, log_scale: bool) -> go.Figure:
    """
    y_mode: 'price' or 'cap'
    """
    fig = make_subplots(rows=3, cols=1, shared_xaxes=True, row_heights=[0.64, 0.22, 0.14], vertical_spacing=0.03)

    # pick series
    if y_mode == "cap" and "market_cap" in df.columns and not pd.isna(df["market_cap"]).all():
        o = df["open"] * (df["market_cap"]/df["close"]).replace([np.inf, -np.inf], np.nan).fillna(method="ffill")
        h = df["high"] * (df["market_cap"]/df["close"]).replace([np.inf, -np.inf], np.nan).fillna(method="ffill")
        l = df["low"]  * (df["market_cap"]/df["close"]).replace([np.inf, -np.inf], np.nan).fillna(method="ffill")
        c = df["market_cap"]
        y_title = "Market Cap (USD)"
    else:
        o, h, l, c = df["open"], df["high"], df["low"], df["close"]
        y_title = "Price (USD)"
        y_mode = "price"

    # Row1: Candles + Bollinger on series c
    fig.add_trace(go.Candlestick(
        x=df["timestamp"], open=o, high=h, low=l, close=c,
        name=f"{symbol} {y_mode.upper()}",
        increasing_line_color="#36d399", decreasing_line_color="#f87272", opacity=0.95
    ), row=1, col=1)

    # if price mode: show price BB; if cap mode and cap exists: compute ad-hoc BB on cap-close
    if y_mode == "price":
        series_for_bb = df["close"]
    else:
        series_for_bb = df["market_cap"]

    if series_for_bb is not None and not pd.isna(series_for_bb).all():
        bb_ma = series_for_bb.rolling(20).mean()
        bb_sd = series_for_bb.rolling(20).std()
        bb_u = bb_ma + 2*bb_sd
        bb_l = bb_ma - 2*bb_sd
        for name, series, color in [("BB upper", bb_u, "#a78bfa"), ("BB mid", bb_ma, "#90a4ec"), ("BB lower", bb_l, "#a78bfa")]:
            fig.add_trace(go.Scatter(x=df["timestamp"], y=series, name=name, line=dict(width=1, color=color)), row=1, col=1)

    # add fibs (extension if ADX>25)
    adx_last = to_float(df["adx14"].iloc[-1]) if "adx14" in df.columns and not df.empty else None
    _add_fibs(fig, c, use_extension=bool(adx_last and adx_last > 25))

    # Row2: MACD
    if "macd_line" in df.columns and "macd_signal" in df.columns:
        fig.add_trace(go.Scatter(x=df["timestamp"], y=df["macd_line"],   name="MACD",  line=dict(width=1.1)), row=2, col=1)
        fig.add_trace(go.Scatter(x=df["timestamp"], y=df["macd_signal"], name="Signal", line=dict(width=1, dash="dot")), row=2, col=1)
    if "macd_hist" in df.columns:
        fig.add_trace(go.Bar(x=df["timestamp"], y=df["macd_hist"], name="MACD Hist"), row=2, col=1)

    # Row3: Volume
    if "volume" in df.columns:
        diff = df["close"].diff().fillna(0)
        colors = ['#36d399' if d >= 0 else '#f87272' for d in diff.tolist()]
        fig.add_trace(go.Bar(x=df["timestamp"], y=df["volume"].fillna(0), name="Volume", marker=dict(color=colors), opacity=0.8), row=3, col=1)
        if df["volume"].fillna(0).sum() == 0:
            fig.add_annotation(text="Volume n/a (source fallback)", showarrow=False, xref="paper", yref="paper", x=0.95, y=0.33)

    fig.update_layout(
        template="plotly_dark", height=420,
        margin=dict(l=12, r=12, t=24, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis_rangeslider_visible=False, barmode="relative",
        yaxis_title=y_title
    )
    _apply_time_axis(fig)
    if log_scale:
        fig.update_yaxes(type="log", row=1, col=1)
    return fig

def fig_line(df: pd.DataFrame, y: str, name: str, h: int = 155) -> go.Figure:
    fig = go.Figure()
    if not df.empty and y in df.columns and not pd.isna(df[y]).all():
        y_data = df[y].astype(float)
        fig.add_trace(go.Scatter(x=df["timestamp"], y=y_data, name=name, line=dict(width=1.6)))
        ymin, ymax = float(y_data.min()), float(y_data.max())
        if ymin == ymax:
            ymin -= 0.5; ymax += 0.5
        fig.update_yaxes(range=[ymin, ymax], fixedrange=False)
    else:
        fig.add_annotation(text="No data for this timeframe.", showarrow=False, xref="paper", yref="paper", x=0.5, y=0.5)
    fig.update_layout(template="plotly_dark", height=h, margin=dict(l=10,r=10,t=18,b=8), showlegend=False)
    _apply_time_axis(fig)
    return fig

# ---------- Ask-Luna ----------
INTRO_LINES = ["Here’s the read:","Quick take:","Alright — chart check:","Let’s keep it real:"]
def intro_line() -> str: return random.choice(INTRO_LINES)

def _fmt_pct(v: Optional[float]) -> str: return "n/a" if v is None else f"{v:+.2f}%"
def _safe_tail(series: pd.Series, n: int) -> pd.Series:
    try: return series.dropna().tail(n)
    except Exception: return pd.Series(dtype="float64")

def classify_bias_metrics(df: pd.DataFrame) -> Tuple[str, dict]:
    """Compute bias using thresholds: RSI >60/<40; 24h >+5%/<-5%; MACD hist slope across last 3 bars; ADX >25 gate."""
    if df.empty: return "range", {}
    # 24h delta
    ch, _ = compute_rollups(df)
    delta_24h = ch.get("24h") or 0.0
    # RSI / MACD / ADX
    last = df.iloc[-1]
    rsi = to_float(last.get("rsi"))
    adx = to_float(last.get("adx14"))
    hist = df["macd_hist"] if "macd_hist" in df.columns else pd.Series(dtype=float)
    macd_slope = None
    if len(hist.dropna()) >= 3:
        macd_slope = float(hist.dropna().iloc[-1] - hist.dropna().iloc[-3]) / 3.0
    score = 0; reasons={}
    if rsi is not None:
        if rsi > 60: score += 1; reasons["rsi"] = ">60"
        elif rsi < 40: score -= 1; reasons["rsi"] = "<40"
    if macd_slope is not None:
        if macd_slope > 0 and (hist.iloc[-1] if len(hist)>0 else 0) > 0: score += 1; reasons["macd"] = "pos+rising"
        elif macd_slope < 0 and (hist.iloc[-1] if len(hist)>0 else 0) < 0: score -= 1; reasons["macd"] = "neg+falling"
    if delta_24h > 5: score += 1; reasons["24h"] = f"+{delta_24h:.1f}%"
    elif delta_24h < -5: score -= 1; reasons["24h"] = f"{delta_24h:.1f}%"
    if adx is not None and adx > 25:
        reasons["adx"] = f"{adx:.1f}"
        if score >= 2: return "bullish", reasons
        if score <= -2: return "bearish", reasons
    return "range", reasons

def build_header_facts(meta: dict) -> str:
    parts = []
    cap = meta.get("marketCap")
    fdv = meta.get("fdv")
    liq = meta.get("liq_usd")
    volh= meta.get("vol_h24")
    if cap: parts.append(f"Cap: {money(cap)}")
    elif fdv: parts.append(f"FDV (est.): {money(fdv)}")
    if liq: parts.append(f"Liq: {money(liq)}")
    if volh: parts.append(f"24h Vol: {money(volh)}")
    if (meta.get("chain") and meta.get("pairAddress")):
        parts.append(f"Pair: https://dexscreener.com/{meta['chain']}/{meta['pairAddress']}")
    return " | ".join(parts)

def luna_answer(symbol: str, df: pd.DataFrame, tf: str, question: str = "", meta: Optional[dict]=None) -> str:
    view = slice_df(df, tf)
    if view.empty: view = df.tail(200)
    view = resample_for_tf(view, tf)

    bias, reasons = classify_bias_metrics(view)
    ch, _ = compute_rollups(view)
    price = money(to_float(view["close"].iloc[-1]) if not view.empty else None)
    perf = f"1h {_fmt_pct(ch.get('1h'))}, 4h {_fmt_pct(ch.get('4h'))}, 12h {_fmt_pct(ch.get('12h'))}, 24h {_fmt_pct(ch.get('24h'))}"
    header = build_header_facts(meta or {})

    # sentiment proxy (volume vs EMA)
    sentiment = ""
    try:
        vol = view["volume"].fillna(0)
        if len(vol.dropna()) >= 20:
            ema20 = vol.ewm(span=20).mean().iloc[-1]
            vnow = vol.iloc[-1]
            if ema20 and vnow >= 1.3*ema20: sentiment = "Hype picking up (volume spike). "
            elif ema20 and vnow <= 0.7*ema20: sentiment = "Fading interest (volume cooling). "
    except Exception:
        pass

    # intent-aware templates (simple, direct)
    ql = (question or "").lower()
    def line_levels():
        try:
            high = money(float(view["high"].tail(60).max()))
            low  = money(float(view["low"].tail(60).min()))
            return f"Levels: ↑ {high}, ↓ {low}."
        except Exception:
            return ""
    if any(k in ql for k in ["up", "bull", "rally", "pump"]):
        txt = f"{intro_line()} {symbol} around {price}. {header}\n"
        txt += f"Bias: {'bullish' if bias=='bullish' else 'not strongly bullish'}; {perf}. "
        txt += sentiment
        txt += "Why: "
        if 'rsi' in reasons and reasons['rsi'].startswith(">"): txt += "RSI elevated; "
        if reasons.get('macd') == "pos+rising": txt += "MACD turning up; "
        if '24h' in reasons and reasons['24h'].startswith('+'): txt += "24h momentum positive; "
        txt += line_levels()
        return txt.strip()

    if any(k in ql for k in ["down", "bear", "dump", "red"]):
        txt = f"{intro_line()} {symbol} around {price}. {header}\n"
        txt += f"Bias: {'bearish' if bias=='bearish' else 'risk of pullback'}; {perf}. "
        txt += sentiment
        txt += "Watch: "
        if 'rsi' in reasons and reasons['rsi'].startswith("<"): txt += "RSI weak; "
        if reasons.get('macd') == "neg+falling": txt += "MACD sliding; "
        if '24h' in reasons and reasons['24h'].startswith('-'): txt += "24h pressure; "
        txt += line_levels()
        return txt.strip()

    if any(k in ql for k in ["vol", "range", "choppy", "squeeze"]):
        txt = f"{intro_line()} {symbol} around {price}. {header}\n"
        txt += f"{perf}. "
        bbw = to_float(view["close"].rolling(20).std().iloc[-1] / (view['close'].rolling(20).mean().iloc[-1] or 1) * 100) if len(view)>=20 else None
        state = "tight (squeeze)" if (bbw is not None and bbw <= 1.0) else "normal" if (bbw is not None and bbw < 5.0) else "expanding"
        txt += f"Volatility: {state}. {sentiment}{line_levels()}"
        return txt.strip()

    if any(k in ql for k in ["buy", "entry", "accum"]):
        txt = f"{intro_line()} {symbol} around {price}. {header}\n"
        txt += f"{perf}. "
        txt += "Consider entries on dips into prior support with rising volume; manage risk — small caps can swing fast. "
        txt += line_levels()
        return txt.strip()

    # general / reasons
    txt = f"{intro_line()} {symbol} around {price}. {header}\n"
    txt += f"{perf}. "
    if bias == "bullish":
        txt += f"Lean: bullish. "
    elif bias == "bearish":
        txt += f"Lean: bearish. "
    else:
        txt += f"Lean: range‑bound. "
    txt += sentiment + line_levels()
    return txt.strip()

# ---------- routes ----------
@app.get("/")
def home():
    return ('<meta http-equiv="refresh" content="0; url=/analyze?symbol=ETH&tf=12h">', 302)

def sanitize_query(q: str) -> str:
    """Allow-list for symbols/ids. Addresses bypass sanitization."""
    if is_address(q): return q
    if len(q) > 100: return q[:100]
    return re.sub(r'[^a-zA-Z0-9_:/.\- ]', '', q)

@app.get("/analyze")
def analyze():
    symbol_raw = sanitize_query(request.args.get("symbol") or "ETH")
    tf = (request.args.get("tf") or "12h")
    # view + log toggles (appended by JS; safe defaults)
    view_pref = (request.args.get("view") or "").lower()   # 'cap' or 'price'
    yscale    = (request.args.get("scale") or "").lower()  # 'log' or 'lin'

    df_full = hydrate_symbol(symbol_raw, force=False, tf_for_fetch=tf)
    if df_full.empty:
        return Response("No data.", 500)

    # resample for tf
    df_view = slice_df(df_full, tf)
    df_view = resample_for_tf(df_view, tf)

    # meta for label + facts
    meta = META_CACHE.get(_norm_for_cache(canonicalize_query(symbol_raw))) or {}
    name_sym = meta.get("label")
    symbol_disp = name_sym if (name_sym and is_address(symbol_raw)) else _disp_symbol(symbol_raw)

    # cap-mode default
    last_price = to_float(df_view["close"].iloc[-1]) if not df_view.empty else None
    cap_default = should_use_cap_view(last_price, meta.get("decimals"))
    y_mode = view_pref if view_pref in ("cap","price") else ("cap" if cap_default else "price")
    log_scale = True if yscale == "log" else False

    perf, invest = compute_rollups(df_full)
    # tiles — keys unchanged
    tiles: Dict[str, str] = {
        "RSI":   pio.to_html(fig_line(df_view, "rsi", "RSI"), include_plotlyjs=False, full_html=False),
        "PRICE": pio.to_html(fig_price(df_view if not df_view.empty else df_full, symbol_disp, y_mode, log_scale), include_plotlyjs=False, full_html=False),
        "MCAP":  pio.to_html(fig_line(df_view if "market_cap" in df_view.columns else df_full, "market_cap", "Market Cap"), include_plotlyjs=False, full_html=False),
        "MACD":  pio.to_html(fig_line(df_view, "macd_line", "MACD"), include_plotlyjs=False, full_html=False),
        "OBV":   pio.to_html(fig_line(df_view, "obv", "OBV"), include_plotlyjs=False, full_html=False),
        "BANDS": pio.to_html(fig_line(df_view, "bb_width", "Bands Width"), include_plotlyjs=False, full_html=False),
        "VOL":   pio.to_html(fig_line(df_view, "volume", "Volume Trend"), include_plotlyjs=False, full_html=False),
        "LIQ":   pio.to_html(fig_line(df_view, "volume", "Liquidity"), include_plotlyjs=False, full_html=False),
        "ADX":   pio.to_html(fig_line(df_view, "adx14", "ADX 14"), include_plotlyjs=False, full_html=False),
        "ATR":   pio.to_html(fig_line(df_view, "atr14", "Volatility (ATR 14)"), include_plotlyjs=False, full_html=False),
        "ALT":   pio.to_html(fig_line(df_view, "alt_momentum", "ALT (Momentum)"), include_plotlyjs=False, full_html=False),
    }

    # tldr line (unchanged container; we prepend header facts subtly)
    facts = build_header_facts(meta)
    def pct(v): return ("n/a" if v is None else f"{v:+.2f}%")
    tldr_line = f"{symbol_disp}: 1h {pct(perf.get('1h'))}, 4h {pct(perf.get('4h'))}, 12h {pct(perf.get('12h'))}, 24h {pct(perf.get('24h'))}."
    if facts: tldr_line = f"{facts} — " + tldr_line

    updated = (pd.to_datetime(df_full["timestamp"]).max().strftime("UTC %Y-%m-%d %H:%M") if not df_full.empty else _to_iso(utcnow()))

    return render_template("control_panel.html", symbol=symbol_disp, tf=tf, updated=updated,
                           tiles=tiles, performance=perf, investment=invest, tldr_line=tldr_line)

@app.get("/expand_json")
def expand_json():
    # basic rate limit 10/min per IP
    ip = request.headers.get("X-Forwarded-For", request.remote_addr or "na").split(",")[0].strip()
    if not allow_rate(ip, "/expand_json", limit=10, window_sec=60):
        return jsonify({"error":"rate limit"}), 429

    symbol_raw = sanitize_query(request.args.get("symbol") or "ETH")
    tf     = (request.args.get("tf") or "12h")
    key    = (request.args.get("key") or "RSI").upper()
    view_pref = (request.args.get("view") or "").lower()
    yscale    = (request.args.get("scale") or "").lower()

    df = load_cached_frame(symbol_raw)
    if df.empty: df = hydrate_symbol(symbol_raw, force=False, tf_for_fetch=tf)
    dfv = slice_df(df, tf); dfv = resample_for_tf(dfv, tf)
    meta = META_CACHE.get(_norm_for_cache(canonicalize_query(symbol_raw))) or {}
    last_price = to_float(dfv["close"].iloc[-1]) if not dfv.empty else None
    cap_default = should_use_cap_view(last_price, meta.get("decimals"))
    y_mode = view_pref if view_pref in ("cap","price") else ("cap" if cap_default else "price")
    log_scale = True if yscale == "log" else False

    if key == "PRICE":
        fig = fig_price(dfv if not dfv.empty else df, _disp_symbol(symbol_raw), y_mode, log_scale)
    else:
        key_map = {
            "RSI":   ("rsi","RSI"),
            "MACD":  ("macd_line","MACD"),
            "BANDS": ("bb_width","Bands Width"),
            "VOL":   ("volume","Volume Trend"),
            "LIQ":   ("volume","Liquidity"),
            "OBV":   ("obv","OBV"),
            "ADX":   ("adx14","ADX 14"),
            "MCAP":  ("market_cap","Market Cap"),
            "ATR":   ("atr14","Volatility (ATR 14)"),
            "ALT":   ("alt_momentum","ALT (Momentum)")
        }
        ycol, title = key_map.get(key, ("close", key))
        fig = fig_line(dfv, ycol, title)

    talk = "Tap a tile for details."
    return jsonify({"fig": fig.to_plotly_json(), "talk": talk, "tf": tf, "key": key})

@app.get("/api/refresh/<symbol>")
def api_refresh(symbol: str):
    df = hydrate_symbol(symbol, force=True)
    return jsonify({"ok": (not df.empty), "rows": 0 if df is None else len(df)})

@app.post("/api/luna")
def api_luna():
    # rate limit 5/min per IP
    ip = request.headers.get("X-Forwarded-For", request.remote_addr or "na").split(",")[0].strip()
    if not allow_rate(ip, "/api/luna", limit=5, window_sec=60):
        return jsonify({"symbol":"", "reply":"Hold up—too many requests; try again in a minute."}), 429

    data    = request.get_json(silent=True) or {}
    symbol  = sanitize_query((data.get("symbol") or "ETH").strip())
    tf      = (data.get("tf") or "12h")
    text    = (data.get("text") or data.get("question") or "").strip()

    df = load_cached_frame(symbol)
    if df.empty:
        df = hydrate_symbol(symbol, force=False, tf_for_fetch=tf)

    meta = META_CACHE.get(_norm_for_cache(canonicalize_query(symbol))) or {}
    disp = meta.get("label") if (meta.get("label") and is_address(symbol)) else _disp_symbol(symbol)
    reply = luna_answer(disp, df, tf, text, meta) if not df.empty else f"{disp}: I don’t have enough fresh data yet."
    return jsonify({"symbol": disp, "reply": reply})

# --- helpers: suggestions & resolve -----------------------------------------
def _list_cached_symbols(maxn=500) -> list[str]:
    out = set()
    for p in (FRAMES_DIR.glob("*.csv")): out.add(_disp_symbol(p.stem))
    for p in (FRAMES_DIR.glob("*.parquet")): out.add(_disp_symbol(p.stem))
    try:
        for k in (CG_IDS.keys() if 'CG_IDS' in globals() else []): out.add(k.upper())
    except Exception: pass
    try:
        coins = cg_fetch_coin_list()
        for c in coins[:2000]:
            sym = c.get("symbol")
            if sym: out.add(sym.upper())
    except Exception: pass
    return sorted(out)[:maxn]

def _resolve_symbol(query: str) -> str | None:
    if not query: return None
    q = query.strip()
    if is_address(q): return canonicalize_query(q)
    syms = _list_cached_symbols(maxn=2000)
    if q.upper() in syms: return q.upper()
    cg_id = cg_id_for_symbol_or_contract(q)
    if cg_id:
        coins = cg_fetch_coin_list()
        for c in coins:
            if (c.get("id","") or "").lower() == cg_id.lower():
                sym = c.get("symbol") or ""
                return sym.upper() if sym else cg_id
        return cg_id
    return None

@app.get("/api/suggest")
def api_suggest():
    q = (request.args.get("q") or "").strip().upper()
    syms = _list_cached_symbols()
    if q:
        syms = [s for s in syms if q in s]
    return jsonify({"symbols": syms[:20]})

@app.get("/api/resolve")
def api_resolve():
    q = (request.args.get("query") or "").strip()
    sym = _resolve_symbol(q)
    if not sym: return jsonify({"ok": False, "error": "not found"}), 404
    return jsonify({"ok": True, "symbol": sym})

# --- debug endpoints ---------------------------------------------------------
@app.get("/debug/resolve")
def debug_resolve():
    q = (request.args.get("q") or "").strip()
    can = canonicalize_query(q)
    pairs = ds_pairs_for_token(q) if is_address(q) else []
    return jsonify({
        "input": q, "is_addr": is_address(q),
        "canonical": can, "pairs_found": len(pairs),
        "build": BUILD_TAG
    })

@app.get("/debug/ds_pairs")
def debug_ds_pairs():
    q = (request.args.get("q") or "").strip()
    pairs = ds_pairs_for_token(q)
    return jsonify({"count": len(pairs), "pairs": pairs[:3]})

# --- healthz -----------------------------------------------------------------
@app.get("/healthz")
def healthz():
    def ping(url, params=None): 
        try:
            r = safe_fetch(url, params=params or {}, timeout=6); 
            return "ok" if (r and r.ok) else "down"
        except Exception: return "down"
    vendors = {
        "cc": ping("https://min-api.cryptocompare.com/data/price", {"fsym":"ETH","tsyms":"USD"}),
        "cg": ping("https://api.coingecko.com/api/v3/ping"),
        "ds": ping("https://api.dexscreener.com/latest/dex/search", {"q":"eth"}),
        "gt": ping("https://api.geckoterminal.com/api/v2/networks/eth/tokens/0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
    }
    st = _fetch_log()
    return jsonify({
        "vendors": vendors,
        "cache": {
            "frames_files": len(list(FRAMES_DIR.glob("*.*"))),
        },
        "last_fetches": st,
        "build": BUILD_TAG
    })

# ---------- run ----------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
