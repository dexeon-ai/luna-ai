from __future__ import annotations
import os, re, json, time, random, logging, math
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Any, Tuple, Optional, List

import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv

from flask import Flask, jsonify, render_template, request, Response
from flask.json.provider import DefaultJSONProvider

import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio

import pytz
USER_TZ = pytz.timezone(os.getenv("LUNA_TZ", "America/Chicago"))

# ---------- build tag ----------
BUILD_TAG = "LUNA-PROD-DS_GT_BIRD-CAPMODE-2025-11-06-SAFE_TILE"

# ---------- logging ----------
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
LOG = logging.getLogger("luna")

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
        if abs(v) >= 1_000_000_000: return f"${v/1e9:.2f}B"
        if abs(v) >= 1_000_000:     return f"${v/1e6:.2f}M"
        if abs(v) >= 1_000:         return f"${v/1e3:.2f}K"
        return "$" + format(v, ",.2f")
    except Exception:
        return "—"

# --- smarter money for micro-prices and clean sig figs
def money_smart(x: Optional[float]) -> str:
    try:
        if x is None: return "—"
        v = float(x)
        if not np.isfinite(v): return "—"
        if abs(v) >= 1.0:   # dollars and above
            return f"${v:,.2f}"
        # micro pricing: show up to 8 decimals but trim trailing zeros
        s = f"{v:.8f}".rstrip("0").rstrip(".")
        return f"${s}"
    except Exception:
        return "—"

def fmt_sig(x: Optional[float], default="0.00"):
    try:
        x = float(x)
        if x == 0: return "0.00"
        m = abs(x)
        if m >= 1:     return f"{x:.2f}"
        if m >= 1e-2:  return f"{x:.4f}"
        if m >= 1e-4:  return f"{x:.6f}"
        return f"{x:.2e}"
    except Exception:
        return default

# --- fast intent parser so answers react to the question
_INTENTS = {
    "breakout": ["breakout","break out","pop","rip","pump","explode"],
    "volatility": ["volatility","atr","chop","wild","calm","range"],
    "fib": ["fib","fibonacci","1.618","0.618","levels","extensions","retracement"],
    "support": ["support","resistance","level","floor","ceiling","supply","demand"],
    "direction": ["up","down","bullish","bearish","trend","direction","bias"],
    "risk": ["risk","stop","invalid","invalidate","drawdown"],
}

def parse_intents(q: str) -> set:
    ql = (q or "").lower()
    hits = set()
    for tag, keys in _INTENTS.items():
        if any(k in ql for k in keys):
            hits.add(tag)
    return hits

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
    if not (32 <= len(s) <= 44): return False
    for ch in s:
        if ch not in _BASE58_ALPHABET: return False
    return True

def is_address(q: str) -> bool:
    if not isinstance(q, str) or not q: return False
    s = q.strip()
    if re.fullmatch(r"0x[a-fA-F0-9]{40}", s): return True  # EVM
    if _BASE58_RE.match(s) and _b58decode_ok(s): return True  # Solana base58
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
    if is_address(s): return s
    return s.upper()

def _disp_symbol(s: str) -> str:
    return s if is_address(s) else s.upper().strip()

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

    if is_address(q_raw):
        for c in coins:
            plats = c.get("platforms") or {}
            for _, addr in plats.items():
                if addr and str(addr).lower() == ql:
                    return c["id"]
        return None

    for c in coins:
        if (c.get("id","") or "").lower() == ql:
            return c["id"]

    matches = [c for c in coins if (c.get("symbol","") or "").lower() == ql]
    if matches:
        pref = CG_IDS.get(q_raw.upper())
        if pref and any(c["id"] == pref for c in matches):
            return pref
        for c in matches:
            plats = (c.get("platforms") or {})
            if any(k.lower() == "ethereum" and (v or "").strip() for k,v in plats.items()):
                return c["id"]
        return matches[0]["id"]

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

# authoritative mapping
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
    "pulsechain": "pulsechain",
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
    for ch in ["solana","ethereum","base","arbitrum","bsc","polygon","optimism","avalanche","fantom","linea","zksync","blast","sui","ton","pulsechain"]:
        js1 = ds_get(f"/token-pairs/v1/{ch}/{addr}")
        pairs.extend(_collect_pairs_from_ds_payload(js1))
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
    if not pairs: return None
    by_chain: Dict[str, dict] = {}
    for p in pairs:
        ch = p.get("chainId")
        if not ch: continue
        best = by_chain.get(ch)
        if best or (p.get("volume", {}) and best and isinstance(best.get("volume"), dict)):
            pass
        best = by_chain.get(ch)
        if not best or (p.get("volume", {}).get("h24") or 0) > (best.get("volume", {}).get("h24") or 0):
            by_chain[ch] = p
    if not by_chain: return None
    vols = [(ch, by_chain[ch].get("volume", {}).get("h24") or 0) for ch in by_chain]
    ch_max, v_max = max(vols, key=lambda x: x[1])
    PRIORITY = ["solana","ethereum","base","arbitrum","bsc","polygon","optimism","avalanche","fantom","linea","zksync","blast","sui","ton","pulsechain"]
    within_2x = {ch: v for ch, v in vols if v_max <= 2*(v_max if ch==ch_max else v)}
    if within_2x and len(within_2x) > 1:
        for ch in PRIORITY:
            if ch in within_2x:
                return ch
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
    target_chain = pick_chain_tiebreaker(pairs)
    cand = [p for p in pairs if p.get("chainId") == target_chain] if target_chain else pairs
    scored = [(score_pair(p), p) for p in cand]
    scored = [t for t in scored if t[0] > 0]
    if not scored:
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

def gt_ohlcv_by_pool(network: str, pool_id: str, timeframe: str, aggregate: int = 1, limit: int = 500) -> pd.DataFrame:
    # timeframe in {'minute','hour','day'}; aggregate >=1
    js = gt_get(f"/networks/{network}/pools/{pool_id}/ohlcv/{timeframe}", params={"aggregate": aggregate, "limit": limit})
    if not js: return pd.DataFrame()
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
            sorted_ids = []
            for d in data:
                pid = d.get("id")
                vol = (((d.get("attributes") or {}).get("volume_usd") or {}).get("h24")) or 0
                sorted_ids.append((vol or 0, pid))
            sorted_ids.sort(key=lambda t: t[0], reverse=True)
            ids = [pid for _, pid in sorted_ids]
    except Exception:
        pass
    return ids[:3]

def birdeye_ohlc_solana(addr: str, tf: str) -> pd.DataFrame:
    # tf given as pandas alias; map to Birdeye
    tf_map = {"1T":"1m","5T":"5m","15T":"15m","1H":"1h","4H":"4h","1D":"1d"}
    birdeye_tf = tf_map.get(tf, "1h")
    now = int(time.time())
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

    # ALT momentum (simple EMA spread surrogate)
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

    if "close" in out.columns:
        if "open" not in out.columns or out["open"].isna().all():
            out["open"] = out["close"].shift(1).fillna(out["close"])
        if "high" not in out.columns:
            out["high"] = out[["open", "close"]].max(axis=1)
        if "low" not in out.columns:
            out["low"] = out[["open", "close"]].min(axis=1)

    freq = RESAMPLE_BY_TF.get(tf, "1H")
    idx = out.set_index("timestamp")

    agg_dict = {}
    if "open" in idx.columns:  agg_dict["open"]  = "first"
    if "high" in idx.columns:  agg_dict["high"]  = "max"
    if "low"  in idx.columns:  agg_dict["low"]   = "min"
    if "close" in idx.columns: agg_dict["close"] = "last"
    if "volume" in idx.columns: agg_dict["volume"] = "sum"
    if "market_cap" in idx.columns: agg_dict["market_cap"] = "last"

    if not agg_dict:
        return out

    res = idx.resample(freq).agg(agg_dict)

    for col in ("open", "high", "low", "close"):
        if col in res.columns:
            res[col] = res[col].ffill()
    if "market_cap" in res.columns:
        res["market_cap"] = res["market_cap"].ffill()

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
def _tf_to_gt(tf: str) -> Tuple[str, int]:
    """
    Map UI TF → (GT timeframe, aggregate).
    """
    if tf in ("1h",):      return ("minute", 1)   # ~60 points
    if tf in ("4h","8h"):  return ("minute", 5)   # 5m agg
    if tf in ("12h","24h"):return ("minute", 15)  # 15m agg
    if tf == "7d":         return ("hour", 1)
    if tf == "30d":        return ("hour", 4)
    return ("day", 1)      # 1y/all

def ds_series_via_gt(addr: str, tf: str) -> Tuple[pd.DataFrame, Optional[dict]]:
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
    gt_tf, agg = _tf_to_gt(tf)
    LOG.info("[DS→GT] GT OHLCV net=%s pair=%s %s agg=%s", net, pair, gt_tf, agg)
    df = gt_ohlcv_by_pool(net, pair, gt_tf, aggregate=agg, limit=500)
    if (df is None or df.empty):
        # Try GT token pools (limit 3), then Birdeye (Solana only)
        pools = gt_find_token_pools(net, addr)
        for pid in pools:
            LOG.info("[DS→GT] trying token pool id %s", pid)
            df = gt_ohlcv_by_pool(net, pid, gt_tf, aggregate=agg, limit=500)
            if not df.empty: break
        if (df is None or df.empty) and chain == "solana":
            tf_resample = RESAMPLE_BY_TF.get(tf, "1H")
            LOG.info("[DS→GT] GT empty, trying Birdeye for %s (%s)", addr, tf_resample)
            df = birdeye_ohlc_solana(addr, tf_resample)
    if df is not None and not df.empty and "market_cap" not in df.columns:
        df["market_cap"] = np.nan
    return df, best

# ---------- supply / decimals (cap mode) ----------
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
        "blast":    "https://rpc.blast.io",
        "pulsechain":"https://rpc.pulsechain.com"
    }
    rpc = rpc_map.get(chain)
    if not rpc:
        _cache_supply_put(chain, addr, 18, None)
        return 18, None
    try:
        payload = {"jsonrpc":"2.0","method":"eth_call","params":[{"to":addr,"data":"0x313ce567"}, "latest"],"id":1}  # decimals
        r = requests.post(rpc, json=payload, timeout=12)
        dec = 18
        if r.ok and isinstance(r.json(), dict) and r.json().get("result"):
            dec = int(r.json()["result"], 16)
        payload["params"][0]["data"] = "0x18160ddd"  # totalSupply
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

# ---------- cap/FDV derivation (labels) ----------
def derive_cap_or_fdv_from_meta(meta: dict) -> Optional[float]:
    # prefer marketCap; then fdv
    if meta.get("marketCap"): return to_float(meta.get("marketCap"))
    if meta.get("fdv"): return to_float(meta.get("fdv"))
    return None

# ---------- master hydrate ----------
META_CACHE: Dict[str, dict] = {}  # normalized key -> meta
# --- helpers for odd contract-like inputs (Hyperliquid/Sui/Aptos etc.)
_CONTRACTISH = re.compile(r"^(0x[a-fA-F0-9]{8,64}|[A-Za-z0-9]{32,}|.+::.+)$")

def looks_contractish(s: str) -> bool:
    s = (s or "").strip()
    if is_address(s): 
        return True
    if _CONTRACTISH.match(s):
        return True
    return False

def token_meta_for(q: str) -> dict:
    meta = {
        "name":"Unknown","symbol":"UNK","chain":"","dexId":"","pairAddress":"",
        "decimals": None, "label": q[:10]+"...", "marketCap": None, "fdv": None,
        "liq_usd": None, "vol_h24": None, "tokenAddress": None
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
                    "tokenAddress": (token.get("address") or None),
                })
                meta["label"] = f"{name} ({sym}) — {meta['dexId'].capitalize()}/{meta['chain'].capitalize()}"
                return meta
    except Exception as e:
        LOG.warning("[Meta] DS failed: %s", e)
    return meta

# --- tiny format helpers for small indicator numbers
def fmt_sig(x, default="0.00"):
    try:
        x = float(x)
        if x == 0: return "0.00"
        m = abs(x)
        if m >= 1:       return f"{x:.2f}"
        if m >= 1e-2:    return f"{x:.4f}"
        if m >= 1e-4:    return f"{x:.6f}"
        return f"{x:.2e}"
    except Exception:
        return default

def hydrate_symbol(query: str, force: bool=False, tf_for_fetch: str="12h") -> pd.DataFrame:
    raw_in = (query or "").strip()
    raw = canonicalize_query(raw_in)
    s_for_cache = _norm_for_cache(raw)

    if (not force) and _fresh_enough(s_for_cache):
        cached = load_cached_frame(s_for_cache)
        if not cached.empty:
            LOG.info("[Hydrate] %s served from fresh cache", s_for_cache)
            return cached

    LOG.info("[Hydrate] %s (force=%s, ttl=%ss)", s_for_cache, force, TTL_SECONDS)

    is_addr = is_address(s_for_cache)
    df = pd.DataFrame()

    meta = token_meta_for(raw) if is_addr else {}
    META_CACHE[s_for_cache] = meta

    if (not is_addr) and looks_contractish(raw):
        meta_guess = token_meta_for(raw)
        addr_guess = (meta_guess or {}).get("tokenAddress")
        if addr_guess:
            LOG.info("[Hydrate] contract-like '%s' resolved to %s on %s", raw, addr_guess, meta_guess.get("chain"))
            META_CACHE[s_for_cache] = meta_guess
            df, _ = ds_series_via_gt(addr_guess, tf_for_fetch)
            if df is not None and not df.empty:
                is_addr = True  # we have a real address now
                meta = meta_guess

    if is_addr:
        df, best_pair = ds_series_via_gt(raw, tf_for_fetch)
        if (df is None or df.empty):
            LOG.info("[Hydrate] DS/GT empty for %s → continuing to CC", raw)
        else:
            # cap series if we have supply
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
            if supply and "close" in df.columns:
                df["market_cap"] = df["close"] * float(supply)

    if df.empty:  # ticker path or address fallback
        m = cc_hist(s_for_cache, "minute", limit=360)
        h = cc_hist(s_for_cache, "hour",   limit=24*30)
        d = cc_hist(s_for_cache, "day",    limit=365)
        if (m is None or m.empty) and (h is None or h.empty) and (d is None or d.empty):
            LOG.info("[Hydrate] CC empty → CG fallback for %s", s_for_cache)
            df365 = cg_series(raw, days=365)
            df = df365 if (df365 is not None and not df365.empty) else cg_series(raw, days=30)
        else:
            parts = [x for x in (d,h,m) if x is not None and not x.empty]
            df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

    if df is None or df.empty:
        cached = load_cached_frame(s_for_cache)
        if not cached.empty:
            _touch_fetch(s_for_cache)
            LOG.info("[Hydrate] %s returning from older cache", s_for_cache)
            return cached
    if df is None or df.empty:
        LOG.warning("[Hydrate] %s no data after routing", s_for_cache)
        return pd.DataFrame()

    df = compute_indicators(df)

    save_frame(s_for_cache, df)
    _touch_fetch(s_for_cache)

    try:
        HOTSET.parent.mkdir(parents=True, exist_ok=True)
        with HOTSET.open("a", encoding="utf-8") as f:
            f.write(s_for_cache + "\n")
    except Exception:
        pass

    return df

def _directional_tilt(view: pd.DataFrame) -> tuple[str, int]:
    """
    Build a directional tilt ('mild ↑', 'modest ↓', etc.) and a confidence score (0–100)
    using MACD slope, RSI location, ADX strength and band width.
    """
    if view.empty:
        return ("flat", 0)

    score = 0
    parts = []

    # MACD histogram slope
    if "macd_hist" in view.columns:
        h = view["macd_hist"].dropna()
        if len(h) >= 4:
            slope = float(h.iloc[-1] - h.iloc[-4])
            if slope > 0: score += 2; parts.append("MACD rising")
            elif slope < 0: score -= 2; parts.append("MACD falling")

    # RSI location
    rsi = to_float(view.get("rsi", pd.Series(dtype=float)).iloc[-1]) if "rsi" in view.columns and len(view) else None
    if rsi is not None:
        if rsi >= 60: score += 1; parts.append("RSI>60")
        elif rsi <= 40: score -= 1; parts.append("RSI<40")

    # ADX strength
    adx = to_float(view.get("adx14", pd.Series(dtype=float)).iloc[-1]) if "adx14" in view.columns and len(view) else None
    if adx is not None:
        if adx >= 30: score += 1; parts.append("ADX strong")
        elif adx <= 15: score -= 1; parts.append("ADX weak")

    # Bollinger width contraction/expansion
    bw = to_float(view.get("bb_width", pd.Series(dtype=float)).iloc[-1]) if "bb_width" in view.columns and len(view) else None
    if bw is not None:
        # expansion with positive MACD slope favors up continuation, contraction favors breakout risk
        if bw >= 3 and score > 0: score += 1; parts.append("bands expanding")
        elif bw <= 1 and abs(score) == 0: parts.append("bands tight")

    # Map score to label and confidence
    if score >= 3:   return ("modest ↑", 70)
    if score == 2:   return ("mild ↑", 60)
    if score == 1:   return ("slight ↑", 55)
    if score == 0:   return ("flat", 50)
    if score == -1:  return ("slight ↓", 55)
    if score == -2:  return ("mild ↓", 60)
    return ("modest ↓", 70)

def _summarize_24h(view: pd.DataFrame, ch: dict) -> str:
    """1–2 sentence recap for the past 24 hours."""
    d = ch.get("24h")
    if d is None:
        return "Past 24h: not enough clean data on this timeframe."
    sign = "up" if d >= 0 else "down"
    hi = float(view["high"].tail(96).max()) if "high" in view.columns else None
    lo = float(view["low"].tail(96).min())  if "low"  in view.columns else None
    bits = [f"Past 24h: {d:+.2f}% ({sign})."]
    if hi and lo:
        bits.append(f"Range ≈ {money_smart(lo)} → {money_smart(hi)}.")
    return " ".join(bits)

# ---------- figures ----------
import pytz
# Default timezone — you can change this or make it dynamic later
USER_TZ = pytz.timezone(os.getenv("LUNA_TZ", "America/Chicago"))

def _apply_time_axis(fig: go.Figure) -> None:
    """Applies clean time axis labels and consistent formatting."""
    fig.update_xaxes(
        tickformatstops=[
            dict(dtickrange=[None, 1000 * 60 * 60 * 24], value="%H:%M"),   # hourly
            dict(dtickrange=[1000 * 60 * 60 * 24, None], value="%m-%d"),   # daily
        ],
        showgrid=True,
        gridcolor="#1a2b3e",
        tickfont=dict(size=10, color="#9eb3c9"),
    )

def fig_price(df: pd.DataFrame, symbol: str) -> go.Figure:
    """Builds the main candlestick + MACD chart and localizes timestamps."""
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        row_heights=[0.78, 0.22], vertical_spacing=0.04
    )

    # --- timezone localization ---
    if not df.empty:
        if "timestamp" in df.columns:
            df = df.copy()
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce").dt.tz_convert(USER_TZ)

        fig.add_trace(go.Candlestick(
            x=df["timestamp"], open=df["open"], high=df["high"], low=df["low"], close=df["close"],
            name=f"{symbol} OHLC", increasing_line_color="#36d399", decreasing_line_color="#f87272", opacity=0.95
        ), row=1, col=1)

        if "bb_upper" in df.columns:
            fig.add_trace(go.Scatter(x=df["timestamp"], y=df["bb_upper"],
                                     name="BB upper", line=dict(width=1)), row=1, col=1)
        if "bb_mid" in df.columns:
            fig.add_trace(go.Scatter(x=df["timestamp"], y=df["bb_mid"],
                                     name="BB mid", line=dict(width=1, dash="dot")), row=1, col=1)
        if "bb_lower" in df.columns:
            fig.add_trace(go.Scatter(x=df["timestamp"], y=df["bb_lower"],
                                     name="BB lower", line=dict(width=1)), row=1, col=1)
        if "macd_line" in df.columns and "macd_signal" in df.columns:
            fig.add_trace(go.Scatter(x=df["timestamp"], y=df["macd_line"],
                                     name="MACD", line=dict(width=1.1)), row=2, col=1)
            fig.add_trace(go.Scatter(x=df["timestamp"], y=df["macd_signal"],
                                     name="Signal", line=dict(width=1, dash="dot")), row=2, col=1)
        if "macd_hist" in df.columns:
            fig.add_trace(go.Bar(x=df["timestamp"], y=df["macd_hist"], name="MACD Hist"), row=2, col=1)

    fig.update_layout(
        template="plotly_dark",
        height=400,
        margin=dict(l=12, r=12, t=24, b=36),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis_rangeslider_visible=False,
    )
    _apply_time_axis(fig)
    return fig

def fig_line(df: pd.DataFrame, y: str, name: str, h: int = 155) -> go.Figure:
    """Builds mini line charts for indicators with timezone conversion."""
    fig = go.Figure()

    if not df.empty and y in df.columns and not pd.isna(df[y]).all():
        df = df.copy()
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce").dt.tz_convert(USER_TZ)

        y_data = df[y].astype(float)
        fig.add_trace(go.Scatter(x=df["timestamp"], y=y_data, name=name, line=dict(width=1.6)))
        ymin, ymax = float(y_data.min()), float(y_data.max())
        if ymin == ymax:
            ymin -= 0.5
            ymax += 0.5
        fig.update_yaxes(range=[ymin, ymax], fixedrange=False)
    else:
        fig.add_annotation(
            text="No data for this timeframe.",
            showarrow=False,
            xref="paper", yref="paper", x=0.5, y=0.5,
            font=dict(color="#9eb3c9")
        )

    fig.update_layout(
        template="plotly_dark",
        height=h,
        margin=dict(l=10, r=10, t=18, b=8),
        showlegend=False
    )
    _apply_time_axis(fig)
    return fig

# ---------- Ask-Luna ----------
def intro_line() -> str:
    return random.choice(["Here’s the read:","Quick take:","Alright — chart check:","Let’s keep it real:"])

def _fmt_pct(v: Optional[float]) -> str: return "n/a" if v is None else f"{v:+.2f}%"
def _safe_tail(series: pd.Series, n: int) -> pd.Series:
    try: return series.dropna().tail(n)
    except Exception: return pd.Series(dtype="float64")

def classify_bias_metrics(df: pd.DataFrame) -> Tuple[str, dict]:
    if df.empty: return "range", {}
    ch, _ = compute_rollups(df)
    delta_24h = ch.get("24h") or 0.0
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

from openai import OpenAI

def make_conversational(text: str, symbol: str, question: str = "") -> str:
    """
    Context-aware rewrite using the new OpenAI API.
    The LLM sees both the question and the data summary,
    so responses are more targeted and relevant.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        LOG.warning("[Luna conversational rewrite skipped] No OPENAI_API_KEY set")
        return text

    try:
        client = OpenAI(api_key=api_key)

        system_prompt = (
            "You are Luna, a friendly crypto analyst who explains charts "
            "and market behavior clearly to retail traders. "
            "Your job is to answer the user's question using the provided data summary. "
            "Be confident but not absolute—describe what the data *suggests*, not certainties. "
            "Be concise (3–6 sentences). Avoid repeating identical numeric stats each time."
        )

        user_prompt = (
            f"User question: {question}\n\n"
            f"Technical summary data for {symbol}:\n{text}\n\n"
            "Please answer the question directly, interpreting the data in plain English. "
            "If the question asks about a cause, speculate responsibly based on volume, volatility, or momentum shifts. "
            "If it asks for a prediction, frame it as probability or potential scenarios, not financial advice."
        )

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.8,
            max_tokens=260,
        )

        reply = response.choices[0].message.content.strip()
        return reply if reply else text

    except Exception as e:
        LOG.warning("[Luna conversational rewrite failed] %s", e)
        return text

# ---- Question classifier (maps user phrasing to a topic bucket)
QUESTION_TYPES = {
    "trend":       ["trend","direction","bias","bullish","bearish","up or down","up/down","tilt","next 24h","24 hours","coming day","today"],
    "support":     ["support","resistance","levels","floor","ceiling","s/r","local high","local low","last high","last low","swing","range high","range low"],
    "volume":      ["volume","liquidity","hype","interest","vol","spike","jump","surge","why the volume","00:00"],
    "volatility":  ["atr","volatility","wild","calm","range","choppy","band","bollinger","tight","squeeze"],
    "fundamental": ["market cap","mcap","fdv","supply","tokenomics","holders"],
    "history24h":  ["past 24","last 24","previous 24","what happened","recap"],
    "history1m":   ["month","30d","last month","a month ago"],
    "prediction":  ["future","next","soon","where","price","move","forecast","breakout","break out","rip","pump","explode","projection","increase or decrease"]
}
def classify_question(q: str) -> str:
    ql = (q or "").lower()
    for k, words in QUESTION_TYPES.items():
        if any(w in ql for w in words):
            return k
    return "general"

def _last_local_extrema(series: pd.Series, window: int = 20) -> tuple[str, Optional[pd.Timestamp], Optional[float]]:
    """Find last local high/low in a simple way: lookback N bars and report max/min + time."""
    try:
        s = series.dropna()
        if s.empty: return ("none", None, None)
        tail = s.tail(window)
        if tail.empty: return ("none", None, None)
        hi_val = float(tail.max()); hi_ts = tail.idxmax()
        lo_val = float(tail.min()); lo_ts = tail.idxmin()
        # Decide which is more "recent"
        if hi_ts > lo_ts: return ("high", hi_ts, hi_val)
        else:             return ("low",  lo_ts, lo_val)
    except Exception:
        return ("none", None, None)

def _recent_volume_spike(df: pd.DataFrame, lookback: int = 60, ema_span: int = 20) -> Optional[str]:
    try:
        if "volume" not in df.columns or df.empty: return None
        vol = df["volume"].dropna()
        if len(vol) < max(lookback, ema_span): return None
        tail = vol.tail(lookback)
        ema = tail.ewm(span=ema_span).mean()
        last = float(tail.iloc[-1]); ema_last = float(ema.iloc[-1])
        if ema_last <= 0: return None
        ratio = last / ema_last
        ts = df["timestamp"].iloc[-1]
        # If the last bar is an outlier, report it; otherwise report max in window
        if ratio >= 1.5:
            when = ts.to_pydatetime()
            return f"Recent volume spike ~{ratio:.2f}× typical; last bar {when:%Y-%m-%d %H:%M UTC}."
        # else find the biggest spike
        abs_ratio = (tail / ema).fillna(0.0)
        imax = int(abs_ratio.idxmax())
        # idxmax returns absolute index; we need timestamp at that position
        peak_idx = abs_ratio.values.argmax()
        peak_ts  = df["timestamp"].tail(lookback).iloc[peak_idx].to_pydatetime()
        peak_mul = float(abs_ratio.max())
        if peak_mul >= 1.5:
            return f"Largest recent spike ~{peak_mul:.2f}× typical at {peak_ts:%Y-%m-%d %H:%M UTC}."
        return None
    except Exception:
        return None

def extract_data_summary(df: pd.DataFrame, ch: dict) -> str:
    """Compile a compact snapshot of technical metrics for Luna's context."""
    if df.empty:
        return "No chart data available."

    latest = df.iloc[-1]
    summary = {}

    summary["last_price"] = to_float(latest.get("close"))
    summary["volume"] = to_float(latest.get("volume"))
    summary["rsi"] = to_float(latest.get("rsi"))
    summary["adx"] = to_float(latest.get("adx14"))
    summary["atr"] = to_float(latest.get("atr14"))
    summary["macd_hist"] = to_float(latest.get("macd_hist"))
    summary["bb_width"] = to_float(latest.get("bb_width"))
    summary["market_cap"] = to_float(latest.get("market_cap"))

    # 24h delta for price and volume
    vol_24h = None
    try:
        tail = df.tail(96)
        if len(tail) > 2:
            vol_24h = float(tail["volume"].iloc[-1] - tail["volume"].iloc[0])
    except Exception:
        vol_24h = None

    lines = [
        f"Last price: {money_smart(summary['last_price']) if summary['last_price'] else 'n/a'}",
        f"RSI: {summary['rsi']:.1f}" if summary["rsi"] else "RSI: n/a",
        f"ADX: {summary['adx']:.1f}" if summary["adx"] else "ADX: n/a",
        f"ATR: {summary['atr']:.4f}" if summary["atr"] else "ATR: n/a",
        f"MACD hist: {summary['macd_hist']:+.4f}" if summary["macd_hist"] else "MACD hist: n/a",
        f"Bollinger width: {summary['bb_width']:.2f}%" if summary["bb_width"] else "BB width: n/a",
        f"Market cap: {money(summary['market_cap'])}" if summary["market_cap"] else "",
        f"24h price change: {_fmt_pct(ch.get('24h'))}",
        f"24h volume delta: {vol_24h:+,.0f}" if vol_24h else ""
    ]
    return "; ".join([x for x in lines if x])

def luna_answer(symbol: str, df: pd.DataFrame, tf: str, question: str = "", meta: Optional[dict] = None) -> str:
    """
    Enhanced Luna response:
    - Answers by question intent first
    - Keeps one-line metric recap for context
    - Reduces numeric overload, adds plain-English commentary
    """

    # --- Data prep
    view = slice_df(df, tf)
    if view.empty: view = df.tail(200)
    view = resample_for_tf(view, tf)
    view = compute_indicators(view)
    q_type = classify_question(question)

    bias, _ = classify_bias_metrics(view)
    ch, _   = compute_rollups(view)
    price   = to_float(view["close"].iloc[-1]) if not view.empty else None
    rsi     = to_float(view["rsi"].iloc[-1]) if "rsi" in view.columns else None
    adx     = to_float(view["adx14"].iloc[-1]) if "adx14" in view.columns else None
    tilt, conf = _directional_tilt(view)

    def fmt_pct(v): 
        return "n/a" if v is None else f"{v:+.2f}%"

    perf_str = f"1h {fmt_pct(ch.get('1h'))}, 12h {fmt_pct(ch.get('12h'))}, 24h {fmt_pct(ch.get('24h'))}"

    where = f"{symbol} is trading around {money_smart(price)}" if price else symbol

    # --- Interpret indicators in human style
    rsi_text = (
        "Momentum looks overheated — near-term pullback likely."
        if rsi and rsi >= 70 else
        "Momentum looks washed-out — possible bounce ahead."
        if rsi and rsi <= 30 else
        "Momentum is neutral and drifting sideways."
    )

    adx_text = (
        "Trend strength is solid; strong moves may continue."
        if adx and adx >= 40 else
        "Trend is building but not yet decisive."
        if adx and adx >= 25 else
        "No strong directional trend right now."
    )

    # --- Core analysis text (by question)
    answer = ""

    if q_type in ("history24h", "trend"):
        answer = _summarize_24h(view, ch) + " "
        answer += f"Bias shows {bias}; the tape leans {tilt.replace('↑','up').replace('↓','down')} with about {conf}% confidence. "
        answer += rsi_text + " " + adx_text

    elif q_type in ("prediction",):
        answer = (
            f"In the next 24 hours, {symbol} may tilt {tilt.replace('↑','upward').replace('↓','downward')} "
            f"(confidence ~{conf}%). {rsi_text} {adx_text}"
        )

    elif q_type in ("support",):
        kind, ts, val = _last_local_extrema(view.set_index("timestamp")["close"], window=60)
        if val:
            answer = (
                f"The last local {kind} was near {money_smart(val)} on "
                f"{ts:%b %d, %H:%M UTC}. {adx_text}"
            )
        else:
            answer = "No clear local highs or lows detected on this timeframe."

    elif q_type in ("volume",):
        vs = _recent_volume_spike(view, lookback=96, ema_span=20)
        answer = vs or "Volume activity appears typical—no unusual spikes recently."

    elif q_type in ("volatility",):
        atr = to_float(view.get("atr14", pd.Series(dtype=float)).iloc[-1])
        if atr:
            answer = (
                f"Volatility is moderate. Average candle range (ATR14) is {atr:.4g}, "
                "suggesting measured but not extreme price swings."
            )
        else:
            answer = "No reliable ATR data for this symbol."

    elif q_type in ("history1m",):
        d30 = ch.get("30d")
        answer = (
            f"Over the past month, {symbol} moved {fmt_pct(d30)}. "
            + ("Momentum remains constructive." if (d30 or 0) > 0 else "Overall tone has softened.")
        )

    else:
        answer = (
            f"{symbol} is moving sideways with {bias} conditions. "
            + rsi_text + " " + adx_text
        )

    # --- Compose final readable output
    reply = (
        f"{where}. {answer.strip()} "
        f"Performance snapshot: {perf_str}. "
        "Note—this is observation, not financial advice."
    )

    data_context = extract_data_summary(view, ch)
    prompt_context = f"{reply}\n\nChart data summary:\n{data_context}"
    return make_conversational(prompt_context, symbol, question)

# ---------- safe tile placeholder ----------
def safe_tile(html_block, label="No data for this timeframe."):
    """
    Ensures any empty/annotation-only Plotly tiles render as a fixed-height
    placeholder so the grid remains perfectly aligned for low-cap tokens.
    """
    try:
        if (not html_block) or ("No data for this timeframe" in html_block):
            return f"<div class='chart-missing'>{label}</div>"
    except Exception:
        return f"<div class='chart-missing'>{label}</div>"
    return html_block

# ---------- routes ----------
@app.get("/")
def home():
    return ('<meta http-equiv="refresh" content="0; url=/analyze?symbol=ETH&tf=12h">', 302)

def sanitize_query(q: str) -> str:
    if is_address(q): return q
    if len(q) > 100: return q[:100]
    return re.sub(r'[^a-zA-Z0-9_:/.\- ]', '', q)

@app.get("/analyze")
def analyze():
    symbol_raw = (request.args.get("query") or request.args.get("symbol") or "ETH").strip()
    symbol_raw = sanitize_query(symbol_raw)
    tf = (request.args.get("tf") or "12h")

    df_full = hydrate_symbol(symbol_raw, force=False, tf_for_fetch=tf)
    if df_full.empty:
        # return a non-crashing placeholder without breaking layout
        LOG.warning("[Analyze] %s returned empty frame — rendering placeholder.", symbol_raw)
        now = utcnow()
        df_full = pd.DataFrame({
            "timestamp": [now - timedelta(minutes=1), now],
            "open":[0,0],"high":[0,0],"low":[0,0],"close":[0,0],"volume":[0,0]
        })

    df_view = slice_df(df_full, tf)
    df_view = resample_for_tf(df_view, tf)
    df_view = compute_indicators(df_view)  # CRITICAL: indicators after resample so small tiles aren’t empty/same

    meta = META_CACHE.get(_norm_for_cache(canonicalize_query(symbol_raw))) or {}
    name_sym = meta.get("label")
    symbol_disp = name_sym if (name_sym and is_address(symbol_raw)) else _disp_symbol(symbol_raw)

    perf, invest = compute_rollups(df_full)

    tiles: Dict[str, str] = {
        "PRICE": safe_tile(pio.to_html(fig_price(df_view if not df_view.empty else df_full, symbol_disp), include_plotlyjs=False, full_html=False)),
        "RSI":   safe_tile(pio.to_html(fig_line(df_view, "rsi", "RSI"), include_plotlyjs=False, full_html=False)),
        "MCAP":  safe_tile(pio.to_html(
                    fig_line(df_view if "market_cap" in df_view.columns else df_full, "market_cap", "Market Cap"),
                    include_plotlyjs=False, full_html=False)),
        "MACD":  safe_tile(pio.to_html(fig_line(df_view, "macd_line", "MACD"), include_plotlyjs=False, full_html=False)),
        "OBV":   safe_tile(pio.to_html(fig_line(df_view, "obv", "OBV"), include_plotlyjs=False, full_html=False)),
        "ATR":   safe_tile(pio.to_html(fig_line(df_view, "atr14", "ATR 14"), include_plotlyjs=False, full_html=False)),
        "BANDS": safe_tile(pio.to_html(fig_line(df_view, "bb_width", "Bands Width"), include_plotlyjs=False, full_html=False)),
        "VOL":   safe_tile(pio.to_html(fig_line(df_view, "volume", "Volume Trend"), include_plotlyjs=False, full_html=False)),
        "LIQ":   safe_tile(pio.to_html(fig_line(df_view, "volume", "Liquidity"), include_plotlyjs=False, full_html=False)),
        "ADX":   safe_tile(pio.to_html(fig_line(df_view, "adx14", "ADX 14"), include_plotlyjs=False, full_html=False)),
        "ALT":   safe_tile(pio.to_html(fig_line(df_view, "alt_momentum", "ALT (Momentum)"), include_plotlyjs=False, full_html=False)),
    }

    def pct(v): return ("n/a" if v is None else f"{v:+.2f}%")
    tldr_line = f"{symbol_disp}: 1h {pct(perf.get('1h'))}, 4h {pct(perf.get('4h'))}, 12h {pct(perf.get('12h'))}, 24h {pct(perf.get('24h'))}."
    facts = build_header_facts(meta)
    if facts: tldr_line = f"{facts} — " + tldr_line

    updated = (pd.to_datetime(df_full["timestamp"]).max().strftime("UTC %Y-%m-%d %H:%M") if not df_full.empty else _to_iso(utcnow()))

    return render_template(
        "control_panel.html",
        symbol=symbol_disp, symbol_raw=symbol_raw, tf=tf, updated=updated,
        tiles=tiles, performance=perf, investment=invest, tldr_line=tldr_line, build=BUILD_TAG
    )

@app.get("/expand_json")
def expand_json():
    try:
        ip = request.headers.get("X-Forwarded-For", request.remote_addr or "na").split(",")[0].strip()
        if not allow_rate(ip, "/expand_json", limit=12, window_sec=60):
            tf  = (request.args.get("tf") or "12h")
            key = (request.args.get("key") or "").upper()
            empty = fig_line(pd.DataFrame(), "close", "No data", h=360)
            return jsonify({"fig": empty.to_plotly_json(), "talk": "Rate limited. Try again in a moment.", "tf": tf, "key": key}), 429

        symbol_raw = sanitize_query(request.args.get("symbol") or "ETH")
        tf     = (request.args.get("tf") or "12h")
        key    = (request.args.get("key") or "RSI").upper()

        df = load_cached_frame(symbol_raw)
        if df.empty:
            df = hydrate_symbol(symbol_raw, force=False, tf_for_fetch=tf)

        dfv = slice_df(df, tf)
        dfv = resample_for_tf(dfv, tf)
        dfv = compute_indicators(dfv)  # ensure tiles have their values

        # choose figure
        if   key == "PRICE":  fig = fig_price(dfv if not dfv.empty else df, _disp_symbol(symbol_raw))
        elif key == "RSI":    fig = fig_line(dfv, "rsi", "RSI", h=360)
        elif key == "MACD":   fig = fig_line(dfv, "macd_line", "MACD", h=360)
        elif key == "MCAP":   fig = fig_line(dfv if "market_cap" in dfv.columns else df, "market_cap", "Market Cap", h=360)
        elif key == "BANDS":  fig = fig_line(dfv, "bb_width", "Bollinger Width", h=360)
        elif key == "VOL":    fig = fig_line(dfv, "volume", "Volume Trend", h=360)
        elif key == "LIQ":    fig = fig_line(dfv, "volume", "Liquidity", h=360)
        elif key == "OBV":    fig = fig_line(dfv, "obv", "OBV", h=360)
        elif key == "ADX":    fig = fig_line(dfv, "adx14", "ADX 14", h=360)
        elif key == "ATR":    fig = fig_line(dfv, "atr14", "ATR 14", h=360)
        elif key == "ALT":    fig = fig_line(dfv, "alt_momentum", "ALT momentum", h=360)
        else:                 fig = fig_line(dfv, "close", key, h=360)

        talk = f"{key} — " + talk_for_key(key, dfv if not dfv.empty else df)
        return jsonify({"fig": fig.to_plotly_json(), "talk": talk, "tf": tf, "key": key})

    except Exception as e:
        LOG.exception("[expand_json] failed: %s", e)
        # return a harmless payload so the modal never blanks
        tf  = (request.args.get("tf") or "12h")
        key = (request.args.get("key") or "").upper()
        empty = fig_line(pd.DataFrame(), "close", "No data", h=360)
        return jsonify({"fig": empty.to_plotly_json(), "talk": "Error loading chart.", "tf": tf, "key": key}), 200

def talk_for_key(key: str, df: pd.DataFrame) -> str:
    """
    Longer, plain‑English explanations with thresholds and 'what it means'.
    Always returns a multi‑sentence blurb.
    """
    if df is None or df.empty:
        return "No data available for this timeframe."

    last = df.iloc[-1]
    try:
        if key == "RSI":
            v = to_float(last.get("rsi"))
            if v is None: return "RSI — unavailable on this timeframe."
            if v >= 70:
                return f"RSI — {v:.1f} in the hot zone. Momentum is stretched; pullbacks are common after extended runs."
            if v <= 30:
                return f"RSI — {v:.1f} in oversold territory. Mean‑reversion bounces are plausible if buyers show up."
            return f"RSI — {v:.1f}: neutral zone; gauges momentum speed. Watch for moves through 60 (strength) or under 40 (weakness)."

        if key == "MACD":
            ml = to_float(last.get("macd_line"))
            sg = to_float(last.get("macd_signal"))
            hs = to_float(last.get("macd_hist"))
            if (ml is None) or (sg is None):
                return "MACD — not available."
            cross = "above" if ml > sg else "below"
            bias  = "bullish" if ml > sg else "bearish"
            hist  = "" if hs is None else f" (hist {hs:+.4f})"
            return (f"MACD — line is {cross} signal → {bias} bias{hist}. "
                    "Upward crosses often precede momentum builds; downward crosses hint at short‑term weakness.")

        if key == "ADX":
            a = to_float(last.get("adx14"))
            if a is None: return "ADX — not available."
            if a >= 40: return f"ADX — {a:.1f}: strong trend in play; continuation moves often extend."
            if a >= 25: return f"ADX — {a:.1f}: trend forming; confirm with volume and higher‑highs."
            return f"ADX — {a:.1f}: weak trend; expect chop unless fresh volume arrives."

        if key == "ATR":
            a = to_float(last.get("atr14"))
            if a is None: return "ATR — not available."
            return f"ATR(14) — {a:.4g}: average candle range. Bigger ATR = faster tape; size positions accordingly."

        if key == "BANDS":
            w = to_float(last.get("bb_width"))
            if w is None: return "Bollinger Bands — unavailable."
            if w <= 1:
                return f"Bollinger width — {w:.2f}%: bands are tight (squeeze). Breakout risk rises if volume expands."
            if w >= 5:
                return f"Bollinger width — {w:.2f}%: bands are wide. Volatility is high; fades and whipsaws are common."
            return f"Bollinger width — {w:.2f}%: normal volatility; look to momentum and volume for direction."

        if key in ("VOL", "LIQ"):
            v = to_float(last.get("volume"))
            if v is None: return "Volume — not available."
            return f"Volume — {v:,.0f}. Surges alongside breakouts validate moves; drying volume into highs can precede pullbacks."

        if key == "MCAP":
            mc = to_float(last.get("market_cap"))
            return ("Market Cap — not available on this feed."
                    if mc is None else f"Market Cap — ≈ {money(mc)}. Rising cap with rising volume suggests growing participation.")

        if key == "OBV":
            s = df["obv"].dropna()
            if len(s) < 3: return "OBV — not enough points."
            slope = float(s.iloc[-1] - s.iloc[-3])
            dirn  = "accumulation" if slope > 0 else "distribution"
            return f"OBV — recent slope points to {dirn}. Pair with price trend to judge confirmation vs divergence."

        if key == "ALT":
            a = to_float(last.get("alt_momentum"))
            if a is None: return "ALT momentum — not available."
            trend = "positive" if a > 0 else "negative"
            return f"ALT momentum — {a:+.4f}: {trend} impulse; rising values reflect acceleration, falling values show fading push."

    except Exception as e:
        LOG.warning("[Talk] error %s", e)

    return "Data available; interpret with volume and context."

@app.get("/api/refresh/<symbol>")
def api_refresh(symbol: str):
    df = hydrate_symbol(symbol, force=True)
    return jsonify({"ok": (not df.empty), "rows": 0 if df is None else len(df)})

@app.post("/api/luna")
def api_luna():
    ip = request.headers.get("X-Forwarded-For", request.remote_addr or "na").split(",")[0].strip()
    if not allow_rate(ip, "/api_luna", limit=5, window_sec=60):
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