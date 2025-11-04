# ============================================================
# server.py — Luna Cockpit
# - CryptoCompare primary for majors
# - CoinGecko fallback
# - DexScreener resolver + GeckoTerminal OHLCV when CC/CG can't
# - Market‑cap‑first view for low‑priced coins (price < $0.10)
# - Conversational, question‑aware answers (speech friendly)
# - QA layout unchanged on the backend (front-end swap below)
# ============================================================

from __future__ import annotations
import os, re, json, time, random, logging
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

# ---------- build tag ----------
BUILD_TAG = "LUNA-MCAP-FIRST-2025-11-04"

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

# ---------- constants ----------
TTL_MINUTES = 15
TTL_SECONDS = TTL_MINUTES * 60

# CoinGecko coin list cache for universal symbol/contract lookup
COIN_LIST_PATH = STATE_DIR / "cg_coin_list.json"
COIN_LIST_TTL  = 60 * 60 * 24   # 24h

# ---------- env ----------
load_dotenv(ROOT / ".env")

def _parse_keys(raw: str) -> List[str]:
    return [t.strip() for t in re.split(r"[\s,;]+", raw or "") if t.strip()]

CC_KEYS = _parse_keys(os.getenv("CRYPTOCOMPARE_KEYS") or os.getenv("CRYPTOCOMPARE_KEY") or "")
CG_KEY  = (os.getenv("COINGECKO_API_KEY") or os.getenv("CG_API_KEY") or "").strip()

# ---------- Flask JSON ----------
class PlotlyJSON(DefaultJSONProvider):
    def dumps(self, obj, **kwargs):
        from plotly.utils import PlotlyJSONEncoder
        return json.dumps(obj, cls=PlotlyJSONEncoder, **kwargs)

app = Flask(__name__, template_folder=str(TEMPLATES_DIR), static_folder=str(STATIC_DIR))
app.json = PlotlyJSON(app)

# ---------- small utils ----------
def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def _to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()

def _latest_ts(df: pd.DataFrame) -> Optional[datetime]:
    if df is None or df.empty or "timestamp" not in df.columns:
        return None
    s = pd.to_datetime(df["timestamp"], utc=True, errors="coerce").dropna()
    return None if s.empty else s.iloc[-1].to_pydatetime()

def _fetch_log() -> dict:
    try:
        return json.loads(FETCH_LOG.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _touch_fetch(symbol: str) -> None:
    st = _fetch_log()
    st[symbol] = _to_iso(utcnow())
    FETCH_LOG.write_text(json.dumps(st, indent=2), encoding="utf-8")

def _fresh_enough(symbol: str, ttl: int = TTL_MINUTES) -> bool:
    st = _fetch_log()
    ts = st.get(symbol)
    if not ts: return False
    try:
        last = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return False
    return (utcnow() - last) < timedelta(minutes=ttl)

def to_float(x) -> Optional[float]:
    try:
        v = float(x)
        if np.isnan(v) or np.isinf(v): return None
        return v
    except Exception:
        return None

def money(x: Optional[float]) -> str:
    try:
        return "$" + format(float(x), ",.2f")
    except Exception:
        return "—"

def money_compact(v: Optional[float]) -> str:
    try:
        n = float(v)
        neg = n < 0
        n = abs(n)
        if n >= 1_000_000_000:
            s = f"{n/1_000_000_000:.2f}B"
        elif n >= 1_000_000:
            s = f"{n/1_000_000:.2f}M"
        elif n >= 1_000:
            s = f"{n/1_000:.2f}k"
        else:
            s = f"{n:.0f}"
        return ("-$" if neg else "$") + s
    except Exception:
        return "—"

# --- address detection / normalization (EVM + Solana/base58) -----
_BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_BASE58_RE = re.compile(r"^[%s]{32,64}$" % re.escape(_BASE58_ALPHABET))

def _is_evm_addr(s: str) -> bool:
    return isinstance(s, str) and re.fullmatch(r"0[xX][a-fA-F0-9]{40}", s.strip()) is not None

def _is_solana_addr(s: str) -> bool:
    return isinstance(s, str) and (_BASE58_RE.match(s.strip()) is not None)

def _is_address_like(s: str) -> bool:
    if not isinstance(s, str) or not s: return False
    if _is_evm_addr(s): return True
    if _is_solana_addr(s): return True
    # safety: long alnum, used by some chains / user copy-paste
    return bool(re.fullmatch(r"[A-Za-z0-9]{26,64}", s.strip()))

def _norm_for_cache(s: str) -> str:
    s = (s or "").strip()
    if _is_evm_addr(s): return s.lower()
    if _is_address_like(s): return s
    return s.upper()

def _short_addr(s: str) -> str:
    if not s: return s
    if _is_evm_addr(s):
        s = s.lower()
        return s[:6] + "…" + s[-4:]
    if _is_solana_addr(s):
        return s[:4] + "…" + s[-4:]
    return s

def _disp_symbol(s: str) -> str:
    return s.strip() if _is_address_like(s) else s.upper().strip()

# ---------- cache read/write ----------
def _frame_path(symbol: str, ext: str) -> Path:
    return FRAMES_DIR / f"{_norm_for_cache(symbol)}.{ext}"

def save_frame(symbol: str, df: pd.DataFrame) -> None:
    if df is None or df.empty:
        return
    p_parq = _frame_path(symbol, "parquet")
    p_csv  = _frame_path(symbol, "csv")
    try:
        df.to_parquet(p_parq, index=False)
        return
    except Exception as e:
        LOG.warning("[Cache] Parquet save failed (%s), falling back to CSV.", e)
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

# ---------- CryptoCompare ----------
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
    tries = max(1, len(CC_KEYS)) + 2
    for _ in range(tries):
        k = CC_POOL.pick()
        headers = {"Apikey": k} if k else {}
        try:
            r = requests.get(f"{CC_BASE}/{path}", params=params, headers=headers, timeout=20)
            if r.status_code == 200:
                js = r.json()
                if isinstance(js, dict) and (js.get("Response") in (None, "Success")):
                    return js
                last_err = f"bad CC payload {str(js)[:180]}"
                if "limit" in str(js).lower() and k:
                    CC_POOL.ban(k)
            else:
                txt = (r.text or "")[:200]
                last_err = f"HTTP {r.status_code} {txt}"
                if "limit" in txt.lower() and k:
                    CC_POOL.ban(k)
        except Exception as e:
            last_err = str(e)
    LOG.warning("[CC] %s", last_err or "unknown error")
    return None

def cc_hist(symbol: str, kind: str, limit: int, aggregate: int = 1) -> pd.DataFrame:
    # Never hit CC for addresses
    if _is_address_like(symbol):
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
CG_TIMEOUT = 30
CG_IDS = {
    "BTC":"bitcoin","ETH":"ethereum","SOL":"solana","BNB":"binancecoin","XRP":"ripple",
    "ADA":"cardano","DOGE":"dogecoin","LINK":"chainlink","AVAX":"avalanche-2","TON":"the-open-network"
}

def cg_headers() -> dict:
    headers = {}
    if CG_KEY:
        headers["x-cg-demo-api-key"] = CG_KEY
    return headers

def cg_get(path: str, params: dict) -> Optional[dict]:
    try:
        r = requests.get(f"{CG_BASE}/{path}", params=params, headers=cg_headers(), timeout=CG_TIMEOUT)
        if r.status_code == 200:
            return r.json()
        LOG.warning("[CG] %s %s", r.status_code, r.text[:180])
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
        r = requests.get(
            f"{CG_BASE}/coins/list",
            params={"include_platform": "true"},
            headers=cg_headers(),
            timeout=CG_TIMEOUT
        )
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

def cg_id_for_symbol_or_contract(q: str) -> Optional[str]:
    if not q: return None
    q_raw = q.strip()
    if not _is_address_like(q_raw):
        if q_raw.upper() in CG_IDS:
            return CG_IDS[q_raw.upper()]
    coins = cg_fetch_coin_list()
    if not coins:
        return CG_IDS.get(q_raw.upper()) if not _is_address_like(q_raw) else None

    ql = q_raw.lower()

    # Contract resolution
    if _is_address_like(q_raw):
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

    # Symbol match
    matches = [c for c in coins if (c.get("symbol","") or "").lower() == ql]
    if matches:
        pref = CG_IDS.get(q_raw.upper())
        if pref:
            for c in matches:
                if c["id"] == pref:
                    return pref
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

    # synth OHLC for indicators (we just need smoothers)
    dp["open"] = dp["close"].shift(1)
    dp["high"] = dp["close"].rolling(3, min_periods=1).max()
    dp["low"]  = dp["close"].rolling(3, min_periods=1).min()
    return dp[["timestamp","open","high","low","close","volume","market_cap"]]\
        .dropna(subset=["timestamp"]).sort_values("timestamp")

# ---------- DexScreener + GeckoTerminal ----------
DS_BASE = "https://api.dexscreener.com"
GT_BASE = "https://api.geckoterminal.com/api/v2"

PREFERRED_QUOTES = {"USDC","USDT","SOL","ETH","WETH","USD"}

DS_TO_GT = {
    "solana": "solana",
    "ethereum": "eth",
    "bsc": "bsc",
    "base": "base",
    "arbitrum": "arbitrum",
    "polygon": "polygon_pos",
    "avalanche": "avalanche",
    "optimism": "optimism",
    "fantom": "fantom",
    "linea": "linea",
    "zksync": "zksync",
    "blast": "blast",
    "ton": "ton",
    "sui": "sui",
}
EVM_CHAIN_GUESS = ["ethereum","base","arbitrum","bsc","polygon","optimism","avalanche","fantom","linea","zksync","blast"]

def ds_get(path: str, params: Optional[dict] = None) -> Optional[dict | list]:
    try:
        url = f"{DS_BASE}{path}"
        r = requests.get(url, params=params or {}, timeout=25, headers={"Accept":"application/json"})
        if r.status_code == 200:
            try:
                return r.json()
            except Exception as e:
                LOG.warning("[DS] JSON decode failed: %s ... %s", e, r.text[:160])
                return None
        LOG.warning("[DS] HTTP %s for %s (%s)", r.status_code, url, (r.text or "")[:180])
    except Exception as e:
        LOG.warning("[DS] %s", e)
    return None

def gt_get(path: str, params: Optional[dict] = None) -> Optional[dict]:
    try:
        r = requests.get(f"{GT_BASE}{path}", params=params or {}, timeout=25, headers={"Accept":"application/json"})
        if r.status_code == 200:
            return r.json()
        LOG.warning("[GT] %s %s", r.status_code, (r.text or "")[:180])
    except Exception as e:
        LOG.warning("[GT] %s", e)
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

def _guess_ds_chains(addr: str) -> List[str]:
    if _is_solana_addr(addr):
        return ["solana"]
    if _is_evm_addr(addr):
        return EVM_CHAIN_GUESS
    return ["solana"] + EVM_CHAIN_GUESS

def ds_pairs_for_token(addr: str) -> List[dict]:
    pairs: List[dict] = []
    for ch in _guess_ds_chains(addr):
        js1 = ds_get(f"/token-pairs/v1/{ch}/{addr}")
        pairs.extend(_collect_pairs_from_ds_payload(js1))
        if pairs: break
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
        if pairs: break
    return pairs

def _score_pair(p: dict) -> float:
    try:
        quote = (((p or {}).get("quoteToken") or {}).get("symbol") or "")
        liq = float((((p or {}).get("liquidity") or {}).get("usd")) or 0.0)
    except Exception:
        quote, liq = "", 0.0
    score = liq
    if str(quote).upper() in PREFERRED_QUOTES:
        score *= 10.0
    return float(score)

def ds_best_pair(addr: str) -> Optional[dict]:
    cand = ds_pairs_for_token(addr)
    if not cand:
        LOG.info("[DS] no pairs for %s", addr)
        return None
    cand = sorted(cand, key=_score_pair, reverse=True)
    best = cand[0]
    try:
        LOG.info("[DS] best pair for %s -> chain=%s pair=%s quote=%s liq=$%s",
                 addr,
                 (best.get("chainId") or "?"),
                 (best.get("pairAddress") or "?"),
                 (((best.get("quoteToken") or {}).get("symbol")) or "?"),
                 str((((best.get("liquidity") or {}).get("usd")) or "0")) )
    except Exception:
        pass
    return best

def _gt_timeframe_for_days(days: int) -> str:
    if days <= 2: return "5m"
    if days <= 7: return "15m"
    if days <= 30: return "1h"
    if days <= 120: return "4h"
    return "1d"

def gt_ohlcv_by_pool(network: str, pool_addr: str, timeframe: str = "1h", limit: int = 500) -> pd.DataFrame:
    js = gt_get(f"/networks/{network}/pools/{pool_addr}/ohlcv/{timeframe}", params={"limit": limit})
    if not js:
        return pd.DataFrame()
    try:
        data = js.get("data")
        if isinstance(data, dict):
            attrs = data.get("attributes") or {}
            rows = attrs.get("ohlcv_list") or []
        elif isinstance(data, list) and data:
            rows = (data[0].get("attributes") or {}).get("ohlcv_list") or []
        else:
            rows = []
        if not rows:
            return pd.DataFrame()
        recs = []
        for row in rows:
            if not isinstance(row, (list, tuple)) or len(row) < 6:
                continue
            ts_raw = int(row[0])
            ts = pd.to_datetime(ts_raw if ts_raw < 10**12 else ts_raw/1000, unit="s", utc=True, errors="coerce")
            recs.append({
                "timestamp": ts,
                "open": float(row[1]),
                "high": float(row[2]),
                "low":  float(row[3]),
                "close":float(row[4]),
                "volume": float(row[5]),
            })
        df = pd.DataFrame.from_records(recs).dropna(subset=["timestamp"]).sort_values("timestamp")
        # GT is price/volume; we won't have market_cap from GT directly
        df["market_cap"] = np.nan
        return df
    except Exception as e:
        LOG.warning("[GT] parse error: %s", e)
        return pd.DataFrame()

def ds_series_via_geckoterminal(addr: str, days: int = 30) -> pd.DataFrame:
    """Resolve addr -> best DS pair -> GT OHLCV for that pool (for charts)."""
    best = ds_best_pair(addr)
    if not best:
        return pd.DataFrame()
    chain = (best.get("chainId") or "").lower().strip()
    pair  = best.get("pairAddress") or ""
    net = DS_TO_GT.get(chain, chain)
    if not net or not pair:
        LOG.info("[DS→GT] missing mapping or pair for %s (chain=%s, pair=%s)", addr, chain, pair)
        return pd.DataFrame()
    tf = _gt_timeframe_for_days(days)
    LOG.info("[DS→GT] fetching GT OHLCV net=%s pair=%s tf=%s", net, pair, tf)
    df = gt_ohlcv_by_pool(net, pair, timeframe=tf, limit=500)
    return df

# ---------- canonicalization ----------
def ds_canonical_address(possibly_wrong_case: str) -> Optional[str]:
    q = (possibly_wrong_case or "").strip()
    js = ds_get("/latest/dex/search", params={"q": q})
    pairs = _collect_pairs_from_ds_payload(js)
    if not pairs:
        return None
    ql = q.lower()
    for p in pairs:
        try:
            bt = (((p.get("baseToken") or {}).get("address") or ""))
            qt = (((p.get("quoteToken") or {}).get("address") or ""))
            if bt and bt.lower() == ql:
                return bt
            if qt and qt.lower() == ql:
                return qt
        except Exception:
            continue
    return None

def canonicalize_query(raw: str) -> str:
    s = (raw or "").strip()
    if _is_evm_addr(s):
        if s.startswith("0X"):  # repair 0X...
            s = "0x" + s[2:]
        return s.lower()
    if _is_solana_addr(s):
        return s
    if _is_address_like(s):
        rec = ds_canonical_address(s)
        if rec:
            LOG.info("[Canon] DS recovered canonical address for '%s' -> '%s'", s, rec)
            return rec
        LOG.info("[Canon] Address-like '%s' but DS couldn't canonicalize; keeping as-is", s)
        return s
    return s

# ---------- indicators ----------
def ema(s: pd.Series, span: int) -> pd.Series:
    return s.ewm(span=span, adjust=False).mean()

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
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

    # ADX(14) — simplified
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

# ---------- hydrate (TTL + CSV merge + CC→CG→DS/GT) ----------
def hydrate_symbol(symbol: str, force: bool=False) -> pd.DataFrame:
    raw_in = (symbol or "").strip()
    raw = canonicalize_query(raw_in)
    s_for_cache = _norm_for_cache(raw)

    if (not force) and _fresh_enough(s_for_cache):
        cached = load_cached_frame(s_for_cache)
        if not cached.empty:
            LOG.info("[Hydrate] %s served from fresh cache", s_for_cache)
            return cached

    LOG.info("[Hydrate] %s (force=%s, ttl=%ss)", s_for_cache, force, TTL_SECONDS)

    # 0) local CSV append (if present)
    df_arch = pd.DataFrame()
    local_basename = s_for_cache if _is_address_like(s_for_cache) else s_for_cache.lower()
    local_csv = (COINS_DIR / f"{local_basename}.csv")
    if local_csv.exists():
        try:
            df_arch = pd.read_csv(local_csv)
            if "timestamp" in df_arch.columns:
                df_arch["timestamp"] = pd.to_datetime(df_arch["timestamp"], utc=True, errors="coerce")
            elif "time" in df_arch.columns:
                df_arch["timestamp"] = pd.to_datetime(df_arch["time"], unit="s", utc=True, errors="coerce")
            rename = {}
            for a,b in [("volumeto","volume"),("volumefrom","volume"),("Vol","volume")]:
                if a in df_arch.columns: rename[a]=b
            df_arch = df_arch.rename(columns=rename)
            for c in ["open","high","low","close","volume"]:
                if c not in df_arch.columns:
                    df_arch[c] = np.nan
            df_arch = df_arch.dropna(subset=["timestamp"]).sort_values("timestamp")
        except Exception as e:
            LOG.warning("[Hydrate] Could not parse archive for %s: %s", s_for_cache, e)

    # 1) CC for majors (skip for addresses)
    is_addr = _is_address_like(s_for_cache)
    m = cc_hist(s_for_cache, "minute", limit=360) if not is_addr else pd.DataFrame()
    h = cc_hist(s_for_cache, "hour",   limit=24*30) if not is_addr else pd.DataFrame()
    d = cc_hist(s_for_cache, "day",    limit=365)   if not is_addr else pd.DataFrame()

    if (m is None or m.empty) and (h is None or h.empty) and (d is None or d.empty):
        LOG.info("[Hydrate] CC empty/unsupported → CG fallback for %s", s_for_cache)
        df = cg_series(raw, days=365)
        if df.empty:
            df = cg_series(raw, days=30)
        if df.empty and is_addr:
            try:
                LOG.info("[Hydrate] CG empty → DexScreener+GeckoTerminal fallback for %s", raw)
                df = ds_series_via_geckoterminal(raw, days=365)
                if df.empty:
                    df = ds_series_via_geckoterminal(raw, days=30)
            except Exception as e:
                LOG.warning("[Hydrate] DS/GT fallback error for %s: %s", raw, e)
    else:
        parts = [x for x in (d,h,m) if x is not None and not x.empty]
        df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

    # 2) add market_cap from CG if missing (best-effort)
    if (df is not None) and (not df.empty) and "market_cap" not in df.columns:
        caps = cg_series(raw, days=90)
        if not caps.empty and "market_cap" in caps.columns:
            df = df.merge(caps[["timestamp","market_cap"]], on="timestamp", how="left")

    # 3) stitch + indicators
    if df is None or df.empty:
        cached = load_cached_frame(s_for_cache)
        if not cached.empty:
            _touch_fetch(s_for_cache)
            LOG.info("[Hydrate] %s returning from older cache", s_for_cache)
            return cached
        LOG.warning("[Hydrate] %s no data after CC/CG/DS-GT", s_for_cache)
        return pd.DataFrame()

    if not df_arch.empty:
        df = pd.concat([df_arch, df], ignore_index=True)

    df = df.dropna(subset=["timestamp"]).drop_duplicates(subset=["timestamp"], keep="last")\
           .sort_values("timestamp").reset_index(drop=True)

    df = compute_indicators(df)

    save_frame(s_for_cache, df)
    _touch_fetch(s_for_cache)
    return df

# ---------- slice & rollups ----------
LOOKBACK = {
    "1h": timedelta(hours=1), "4h": timedelta(hours=4), "8h": timedelta(hours=8),
    "12h": timedelta(hours=12), "24h": timedelta(days=1), "7d": timedelta(days=7),
    "30d": timedelta(days=30), "1y": timedelta(days=365), "all": None
}

def slice_df(df: pd.DataFrame, tf: str) -> pd.DataFrame:
    if df.empty: return df
    win = LOOKBACK.get(tf, timedelta(hours=4))
    if win is None: return df
    anchor = _latest_ts(df) or utcnow()
    cutoff = anchor - win
    return df[df["timestamp"] >= cutoff].copy()

def value_at_or_before(df: pd.DataFrame, hours: int, anchor: Optional[datetime]=None) -> Optional[float]:
    if df.empty: return None
    anchor = anchor or _latest_ts(df) or utcnow()
    t = anchor - timedelta(hours=hours)
    older = df[df["timestamp"] <= t]
    if older.empty:
        return to_float(df["close"].iloc[0])
    return to_float(older["close"].iloc[-1])

def compute_rollups(df: pd.DataFrame) -> Tuple[Dict[str, Optional[float]], Dict[str, Optional[float]]]:
    if df.empty: return {}, {}
    ch = {}
    anchor = _latest_ts(df) or utcnow()
    last = to_float(df["close"].iloc[-1])
    for k, hrs in [("1h",1),("4h",4),("8h",8),("12h",12),("24h",24),("7d",24*7),("30d",24*30),("1y",24*365)]:
        base = value_at_or_before(df, hrs, anchor)
        ch[k] = None if (base in (None,0) or last in (None,0)) else round((last/base-1)*100, 2)
    inv = {k: (None if v is None else round(1000*(1+v/100.0),2)) for k,v in ch.items()}
    return ch, inv

# ---------- name/label resolver ----------
def resolve_display_meta(q: str) -> dict:
    """
    Try to resolve a human label:
    - for contracts: DexScreener best pair -> base or quote token that matches address
    - CoinGecko list as backup
    """
    info = {
        "label": _disp_symbol(q),
        "symbol": None,
        "name": None,
        "address": q if _is_address_like(q) else None,
        "chain": None
    }
    if _is_address_like(q):
        best = ds_best_pair(q)
        if best:
            bt = (best.get("baseToken") or {})
            qt = (best.get("quoteToken") or {})
            addr = q.lower()
            chosen = bt if (bt.get("address","").lower()==addr) else qt if (qt.get("address","").lower()==addr) else bt
            sym = (chosen.get("symbol") or "").upper() or None
            nm  = chosen.get("name") or None
            ch  = (best.get("chainId") or None)
            info.update({"symbol": sym, "name": nm, "chain": ch})
            if sym:
                info["label"] = sym
            elif nm:
                info["label"] = nm
            else:
                info["label"] = _short_addr(q)
            return info
        # CG name try
        coins = cg_fetch_coin_list()
        if coins:
            ql = q.lower()
            for c in coins:
                plats = c.get("platforms") or {}
                for _, addr in plats.items():
                    if addr and addr.lower()==ql:
                        info["symbol"]= (c.get("symbol") or "").upper() or None
                        info["name"] = c.get("name") or None
                        info["label"]= info["symbol"] or info["name"] or _short_addr(q)
                        return info
        info["label"] = _short_addr(q)
        return info
    else:
        # symbol / id
        coins = cg_fetch_coin_list()
        if coins:
            # direct id
            for c in coins:
                if (c.get("id","") or "").lower() == q.lower():
                    info["symbol"]= (c.get("symbol") or "").upper() or None
                    info["name"]= c.get("name") or None
                    info["label"]= info["symbol"] or info["name"] or q.upper()
                    return info
            # symbol
            for c in coins:
                if (c.get("symbol","") or "").upper() == q.upper():
                    info["symbol"]= (c.get("symbol") or "").upper()
                    info["name"]= c.get("name") or None
                    info["label"]= info["symbol"]
                    return info
        info["label"] = q.upper()
        return info

# ---------- market‑cap‑first helpers ----------
def prefer_mcap(df: pd.DataFrame) -> bool:
    """Prefer market cap view when token price is tiny (price < $0.10) AND we have market_cap."""
    if df is None or df.empty or "close" not in df.columns:
        return False
    try:
        last_price = float(df["close"].dropna().iloc[-1])
    except Exception:
        return False
    if last_price < 0.10 and "market_cap" in df.columns and not pd.isna(df["market_cap"]).all():
        return True
    return False

def fib_levels(series: pd.Series) -> List[Tuple[str, float]]:
    """Return common fib levels (0, 23.6, 38.2, 50, 61.8, 100) using min/max of the visible data."""
    s = series.dropna()
    if s.empty:
        return []
    lo, hi = float(s.min()), float(s.max())
    rng = hi - lo
    levels = [
        ("0%", lo),
        ("23.6%", lo + 0.236 * rng),
        ("38.2%", lo + 0.382 * rng),
        ("50%", lo + 0.5 * rng),
        ("61.8%", lo + 0.618 * rng),
        ("100%", hi),
    ]
    return levels

# ---------- Plotly helpers ----------
def _apply_time_axis(fig: go.Figure) -> None:
    fig.update_xaxes(
        tickformatstops=[
            dict(dtickrange=[None, 1000*60*60*24], value="%H:%M"),
            dict(dtickrange=[1000*60*60*24, None], value="%m-%d"),
        ]
    )

def fig_price(df: pd.DataFrame, symbol: str, use_mcap: bool = False) -> go.Figure:
    """
    If use_mcap=True and we have market_cap, Row1 shows Market Cap line + fib levels.
    Otherwise Row1 is OHLC + Bollinger.
    Row2 still shows MACD on price; Row3 volume mini.
    """
    fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True,
        row_heights=[0.64, 0.22, 0.14], vertical_spacing=0.03
    )

    if not df.empty:
        if use_mcap and "market_cap" in df.columns and not pd.isna(df["market_cap"]).all():
            # Row 1: Market cap line + Fib levels
            y = df["market_cap"].astype(float)
            fig.add_trace(
                go.Scatter(x=df["timestamp"], y=y, name=f"{_disp_symbol(symbol)} Market Cap", line=dict(width=1.8)),
                row=1, col=1
            )
            # Fib overlays
            for label, val in fib_levels(y.tail(min(len(y), 500))):
                fig.add_hline(y=val, line_dash="dot", line_width=1, opacity=0.5, annotation_text=f"Fib {label}", annotation_position="top left", row=1, col=1)
        else:
            # Row 1: OHLC + Bollinger
            fig.add_trace(go.Candlestick(
                x=df["timestamp"], open=df["open"], high=df["high"], low=df["low"], close=df["close"],
                name=f"{_disp_symbol(symbol)} OHLC", increasing_line_color="#36d399", decreasing_line_color="#f87272", opacity=0.95
            ), row=1, col=1)
            for name, col, color in [
                ("BB upper", "bb_upper", "#a78bfa"),
                ("BB mid",   "bb_mid",   "#90a4ec"),
                ("BB lower", "bb_lower", "#a78bfa"),
            ]:
                if col in df.columns:
                    fig.add_trace(
                        go.Scatter(x=df["timestamp"], y=df[col], name=name, line=dict(width=1, color=color)),
                        row=1, col=1
                    )

        # Row 2: MACD lines + hist (on price)
        if "macd_line" in df.columns and "macd_signal" in df.columns:
            fig.add_trace(go.Scatter(x=df["timestamp"], y=df["macd_line"],   name="MACD",  line=dict(width=1.1)),
                          row=2, col=1)
            fig.add_trace(go.Scatter(x=df["timestamp"], y=df["macd_signal"], name="Signal", line=dict(width=1, dash="dot")),
                          row=2, col=1)
        if "macd_hist" in df.columns:
            fig.add_trace(go.Bar(x=df["timestamp"], y=df["macd_hist"], name="MACD Hist"), row=2, col=1)

        # Row 3: Volume mini
        if "volume" in df.columns:
            diff = df["close"].diff().fillna(0)
            colors = ['#36d399' if d >= 0 else '#f87272' for d in diff.tolist()]
            fig.add_trace(
                go.Bar(x=df["timestamp"], y=df["volume"].fillna(0), name="Volume",
                       marker=dict(color=colors), opacity=0.8),
                row=3, col=1
            )

    fig.update_layout(
        template="plotly_dark", height=420,
        margin=dict(l=12, r=12, t=24, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis_rangeslider_visible=False,
        barmode="relative",
    )
    _apply_time_axis(fig)
    return fig

def fig_line(df: pd.DataFrame, y: str, name: str, h: int = 155) -> go.Figure:
    fig = go.Figure()
    if not df.empty and y in df.columns and not pd.isna(df[y]).all():
        y_data = df[y].astype(float)
        fig.add_trace(go.Scatter(x=df["timestamp"], y=y_data, name=name, line=dict(width=1.6)))
        ymin, ymax = float(y_data.min()), float(y_data.max())
        if ymin == ymax:
            ymin -= 0.5
            ymax += 0.5
        fig.update_yaxes(range=[ymin, ymax], fixedrange=False)
    else:
        fig.add_annotation(text="No data for this timeframe.", showarrow=False, xref="paper", yref="paper", x=0.5, y=0.5)
    fig.update_layout(template="plotly_dark", height=h, margin=dict(l=10,r=10,t=18,b=8), showlegend=False)
    _apply_time_axis(fig)
    return fig

# ---------- conversational Luna ----------
INTRO_LINES = [
    "Quick take:",
    "Here’s the read:",
    "Chart check:",
    "What matters right now:",
]

def _fmt_pct(v: Optional[float]) -> str:
    return "n/a" if v is None else f"{v:+.2f}%"

def _list_pct(ch: dict, keys=("1h","4h","24h")) -> str:
    bits = [f"{k} {_fmt_pct(ch.get(k))}" for k in keys]
    return ", ".join(bits)

def classify_question(q: str) -> str:
    q = (q or "").lower()
    if any(k in q for k in ["which coin", "what coin", "who are you", "what is this"]):
        return "id"
    if "rug" in q or "scam" in q or "honeypot" in q:
        return "rug"
    if any(k in q for k in ["bearish", "down", "sell", "dump"]):
        return "bearish"
    if any(k in q for k in ["bullish", "up", "pump", "moon", "rally"]):
        return "bullish"
    if any(k in q for k in ["volatility", "range", "chop", "band"]):
        return "vol"
    return "general"

def build_friendly_summary(symbol_raw: str, meta: dict, df: pd.DataFrame, tf: str) -> Tuple[str, dict]:
    view = slice_df(df, tf) if not df.empty else df
    if view.empty:
        view = df.tail(200)
    ch, _ = compute_rollups(view)
    last = view.iloc[-1] if not view.empty else df.iloc[-1]

    price = to_float(last.get("close"))
    mcap  = to_float(last.get("market_cap"))
    use_mcap = prefer_mcap(df)

    if use_mcap and mcap:
        headline = f"{meta.get('label') or _disp_symbol(symbol_raw)} — market cap around {money_compact(mcap)} (price {money(price)})."
        perf = f"{_list_pct(ch, keys=('1h','4h','24h'))} (cap)."
    else:
        headline = f"{meta.get('label') or _disp_symbol(symbol_raw)} — price around {money(price)}."
        perf = f"{_list_pct(ch, keys=('1h','4h','24h'))}."

    # light interpretation (no heavy jargon)
    rsi   = to_float(last.get("rsi")); adx = to_float(last.get("adx14")); hist = to_float(last.get("macd_hist"))
    mood = []
    if rsi is not None:
        mood.append("oversold-ish" if rsi < 35 else "overbought-ish" if rsi > 65 else "neutral RSI")
    if adx is not None:
        mood.append("strong trend" if adx >= 25 else "choppy")
    if hist is not None:
        mood.append("momentum fading" if hist < 0 else "momentum building")
    mood_txt = ", ".join(mood) if mood else "mixed signals"

    explain = f"{mood_txt}. Think of price as the slice and market cap as the whole pizza — we’re watching the whole pie when tokens are cheap."
    return f"{headline} {perf} {explain}", {"use_mcap": use_mcap, "price":price, "mcap":mcap, "perf":ch}

def answer_by_intent(intent: str, meta: dict, snap: dict, q: str) -> str:
    ch = snap["perf"]
    price = snap["price"]; mcap = snap["mcap"]
    use_mcap = snap["use_mcap"]

    if intent == "id":
        bits = []
        if meta.get("symbol"): bits.append(meta["symbol"])
        if meta.get("name") and (meta["name"] != meta.get("symbol")): bits.append(meta["name"])
        if meta.get("chain"): bits.append(f"on {meta['chain']}")
        if meta.get("address"): bits.append(_short_addr(meta["address"]))
        core = " • ".join([b for b in bits if b])
        now_line = f"Current {'market cap' if use_mcap and mcap else 'price'}: {money_compact(mcap) if use_mcap and mcap else money(price)}."
        return f"This is {core or meta.get('label')}. {now_line}"

    if intent == "rug":
        return (
            "Red‑flag checklist (quick): "
            "• Trading active? (volume not vanishing) "
            "• Liquidity not locked/owned by deployer. "
            "• Contract not blacklisting or blocking sells. "
            "I can’t audit the contract, but I can watch behavior: "
            f"last 24h move: {_fmt_pct(ch.get('24h'))}. "
            "If you see sudden liquidity pulls or only buys/no sells, assume extreme risk."
        )

    if intent == "bearish":
        return (
            f"Leaning bearish near‑term. 4h/24h: {_fmt_pct(ch.get('4h'))}, {_fmt_pct(ch.get('24h'))}. "
            "If the next push happens on weak volume and RSI stays sub‑45, expect lower highs. "
            "Invalidation: strong close above recent highs with rising volume."
        )

    if intent == "bullish":
        return (
            f"Leaning bullish if buyers reclaim recent highs. 1h/4h: {_fmt_pct(ch.get('1h'))}, {_fmt_pct(ch.get('4h'))}. "
            "Look for OBV to rise and a clean break on above‑average volume. "
            "Invalidation: loss of recent swing‑low on heavy sell volume."
        )

    if intent == "vol":
        return (
            "Volatility view: Bollinger width shows compression/expansion regime; "
            "expect larger moves when bands re‑expand. ATR gives you a typical bar size — use it to size risk."
        )

    # general
    return (
        "In plain English: bulls need higher highs on rising volume; bears want lower highs and weak bounces. "
        "Use recent swing‑high/swing‑low as your invalidation lines."
    )

def luna_answer(symbol: str, df: pd.DataFrame, tf: str, question: str = "") -> str:
    meta = resolve_display_meta(symbol)
    if df.empty:
        return f"{meta.get('label') or _disp_symbol(symbol)}: I don’t have enough fresh data yet."

    intro = random.choice(INTRO_LINES)
    summary, snap = build_friendly_summary(symbol, meta, df, tf)
    intent = classify_question(question)
    tail = answer_by_intent(intent, meta, snap, question)
    return f"{intro} {summary}\n{tail}"

# ---------- routes ----------
@app.get("/")
def home():
    return ('<meta http-equiv="refresh" content="0; url=/analyze?symbol=ETH&tf=12h">', 302)

@app.get("/analyze")
def analyze():
    symbol_raw = (request.args.get("symbol") or "ETH").strip()
    tf     = (request.args.get("tf") or "12h")
    meta   = resolve_display_meta(symbol_raw)  # human label

    df_full = hydrate_symbol(symbol_raw, force=False)
    if df_full.empty:
        return Response("No data.", 500)

    df_view = slice_df(df_full, tf)
    perf, invest = compute_rollups(df_full)

    use_mcap = prefer_mcap(df_view if not df_view.empty else df_full)

    tiles: Dict[str, str] = {
        "RSI":   pio.to_html(fig_line(df_view, "rsi", "RSI"), include_plotlyjs=False, full_html=False),
        "PRICE": pio.to_html(fig_price(df_view if not df_view.empty else df_full, meta["label"], use_mcap=use_mcap), include_plotlyjs=False, full_html=False),
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

    ch_view, _ = compute_rollups(df_view if not df_view.empty else df_full)
    def pct(k):
        v = ch_view.get(k)
        return ("n/a" if v is None else f"{v:+.2f}%")

    # if mcap-first, say that in TLDR
    tldr_basis = "cap" if use_mcap else "price"
    tldr_line = (
        f"{meta['label']}: 1h {pct('1h')}, 4h {pct('4h')}, 12h {pct('12h')}, 24h {pct('24h')} ({tldr_basis}). "
        f"Cheap tokens are judged by market cap first — think whole pizza, not price-per-slice."
    )

    updated = (_latest_ts(df_full) or utcnow()).strftime("UTC %Y-%m-%d %H:%M")

    return render_template(
        "control_panel.html",
        symbol=meta["label"], tf=tf, updated=updated,
        tiles=tiles, performance=perf, investment=invest, tldr_line=tldr_line
    )

@app.get("/expand_json")
def expand_json():
    symbol_raw = (request.args.get("symbol") or "ETH").strip()
    tf     = (request.args.get("tf") or "12h")
    key    = (request.args.get("key") or "RSI").upper()

    df = load_cached_frame(symbol_raw)
    if df.empty: df = hydrate_symbol(symbol_raw, force=False)
    dfv = slice_df(df, tf)

    key_map = {
        "PRICE": ("close", fig_price(dfv if not dfv.empty else df, resolve_display_meta(symbol_raw)["label"], use_mcap=prefer_mcap(dfv if not dfv.empty else df))),
        "RSI":   ("rsi",   fig_line(dfv, "rsi", "RSI")),
        "MACD":  ("macd_line", fig_line(dfv, "macd_line", "MACD")),
        "BANDS": ("bb_width", fig_line(dfv, "bb_width", "Bands Width")),
        "VOL":   ("volume", fig_line(dfv, "volume", "Volume Trend")),
        "LIQ":   ("volume", fig_line(dfv, "volume", "Liquidity")),
        "OBV":   ("obv", fig_line(dfv, "obv", "OBV")),
        "ADX":   ("adx14", fig_line(dfv, "adx14", "ADX 14")),
        "MCAP":  ("market_cap", fig_line(dfv, "market_cap", "Market Cap")),
        "ATR":   ("atr14", fig_line(dfv, "atr14", "Volatility (ATR 14)")),
        "ALT":   ("alt_momentum", fig_line(dfv, "alt_momentum", "ALT (Momentum)")),
    }

    _, fig = key_map.get(key, ("close", fig_line(dfv, "close", key)))
    # conversational one-liner for the modal:
    talk = luna_answer(symbol_raw, dfv if not dfv.empty else df, tf, question="general")
    return jsonify({"fig": fig.to_plotly_json(), "talk": talk, "tf": tf, "key": key})

@app.get("/api/refresh/<symbol>")
def api_refresh(symbol: str):
    df = hydrate_symbol(symbol, force=True)
    return jsonify({"ok": (not df.empty), "rows": 0 if df is None else len(df)})

@app.post("/api/luna")
def api_luna():
    data    = request.get_json(silent=True) or {}
    symbol  = (data.get("symbol") or "ETH").strip()
    tf      = data.get("tf") or "12h"
    text    = (data.get("text") or data.get("question") or "").strip()

    df = load_cached_frame(symbol)
    if df.empty:
        df = hydrate_symbol(symbol, force=False)

    reply = luna_answer(symbol, df, tf, text) if not df.empty else f"{resolve_display_meta(symbol).get('label')}: I don’t have enough fresh data yet."
    return jsonify({"symbol": resolve_display_meta(symbol).get("label"), "reply": reply})

# --- API: suggestions --------------------------------------------------------
def _list_cached_symbols(maxn=500) -> list[str]:
    out = set()
    for p in (FRAMES_DIR.glob("*.csv")):
        out.add(_disp_symbol(p.stem))
    for p in (FRAMES_DIR.glob("*.parquet")):
        out.add(_disp_symbol(p.stem))
    try:
        for k in (CG_IDS.keys() if 'CG_IDS' in globals() else []):
            out.add(k.upper())
    except Exception:
        pass
    try:
        coins = cg_fetch_coin_list()
        for c in coins[:2000]:
            sym = c.get("symbol")
            if sym:
                out.add(sym.upper())
    except Exception:
        pass
    syms = sorted(out)
    return syms[:maxn]

def _resolve_symbol(query: str) -> str | None:
    if not query:
        return None
    q = query.strip()

    # contract: canonicalize and return
    if _is_address_like(q):
        return canonicalize_query(q)

    syms = _list_cached_symbols(maxn=2000)
    if q.upper() in syms:
        return q.upper()

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
    if not sym:
        return jsonify({"ok": False, "error": "not found"}), 404
    return jsonify({"ok": True, "symbol": sym})

# --- Optional debug endpoints -----------------------------------------------
@app.get("/debug/resolve")
def debug_resolve():
    q = (request.args.get("q") or "").strip()
    return jsonify({
        "input": q,
        "is_addr_like": _is_address_like(q),
        "is_evm": _is_evm_addr(q),
        "is_solana_like": _is_solana_addr(q),
        "canonical": canonicalize_query(q),
        "build": BUILD_TAG
    })

@app.get("/debug/ds_pairs")
def debug_ds_pairs():
    q = (request.args.get("q") or "").strip()
    pairs = ds_pairs_for_token(q)
    return jsonify({"count": len(pairs), "pairs": pairs[:3]})

# ---------- run ----------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
