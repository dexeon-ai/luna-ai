# ============================================================
# server.py — Luna Cockpit (restore original grid + expand)
# Fixes:
#  - Correct candle granularity for contract addresses (DS→GT)
#  - Restore Expand modals / unchanged tile keys
#  - Cap‑first main chart for sub‑$0.10 tokens (with Fib)
#  - DexScreener meta for name/symbol; better address handling
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
BUILD_TAG = "LUNA-FIXED-DS-GT-GRANULARITY-2025-11-04"

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
FRAMES_DIR = DERIVED / "frames"           # *.parquet or *.csv cache
STATE_DIR  = DATA_DIR / "state"           # TTL stamps, etc.
SESS_DIR   = DATA_DIR / "sessions"
COINS_DIR  = DATA_DIR / "coins"           # large CSV archives (1998 coins)

for d in (FRAMES_DIR, STATE_DIR, SESS_DIR):
    d.mkdir(parents=True, exist_ok=True)

FETCH_LOG = STATE_DIR / "fetch_log.json"
COIN_LIST_PATH = STATE_DIR / "cg_coin_list.json"

# ---------- constants ----------
TTL_MINUTES = 15
TTL_SECONDS = TTL_MINUTES * 60
COIN_LIST_TTL  = 60 * 60 * 24   # 24h

# ---------- env ----------
load_dotenv(ROOT / ".env")

def _parse_keys(raw: str) -> List[str]:
    return [t.strip() for t in re.split(r"[\s,;]+", raw or "") if t.strip()]

CC_KEYS = _parse_keys(os.getenv("CRYPTOCOMPARE_KEYS") or os.getenv("CRYPTOCOMPARE_KEY") or "")
CG_KEY  = (os.getenv("COINGECKO_API_KEY") or os.getenv("CG_API_KEY") or "").strip()

# ---------- Flask JSON for Plotly ----------
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

def money(x: Optional[float]) -> str:
    try:
        return "$" + format(float(x), ",.2f")
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

# --- address detection / normalization (EVM + Solana + base58) -----
_BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_BASE58_RE = re.compile(r"^[%s]{32,64}$" % re.escape(_BASE58_ALPHABET))

def _is_evm_addr(s: str) -> bool:
    return isinstance(s, str) and re.fullmatch(r"0[xX][a-fA-F0-9]{40}", s.strip()) is not None

def _is_solana_addr(s: str) -> bool:
    return isinstance(s, str) and (_BASE58_RE.match(s.strip()) is not None)

def _is_address_like(s: str) -> bool:
    if not isinstance(s, str) or not s:
        return False
    if _is_evm_addr(s): return True
    if _is_solana_addr(s): return True
    return bool(re.fullmatch(r"[A-Za-z0-9]{26,64}", s.strip()))

def _norm_for_cache(s: str) -> str:
    s = (s or "").strip()
    if _is_evm_addr(s): return s.lower()
    if _is_address_like(s): return s
    return s.upper()

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
    if _is_address_like(symbol):  # CC can't do contracts
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
CG_IDS = {"BTC":"bitcoin","ETH":"ethereum","SOL":"solana","BNB":"binancecoin",
          "XRP":"ripple","ADA":"cardano","DOGE":"dogecoin","LINK":"chainlink",
          "AVAX":"avalanche-2","TON":"the-open-network"}

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
    if _is_address_like(q_raw):
        for c in coins:
            plats = c.get("platforms") or {}
            for _, addr in (plats or {}).items():
                if addr and str(addr).lower() == ql:
                    return c["id"]
        return None
    for c in coins:
        if (c.get("id","") or "").lower() == ql:
            return c["id"]
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

    dp["open"] = dp["close"].shift(1)
    dp["high"] = dp["close"].rolling(3, min_periods=1).max()
    dp["low"]  = dp["close"].rolling(3, min_periods=1).min()
    return dp[["timestamp","open","high","low","close","volume","market_cap"]].dropna(subset=["timestamp"]).sort_values("timestamp")

# ---------- DexScreener (resolver) + GeckoTerminal (OHLCV) ----------
DS_BASE = "https://api.dexscreener.com"
GT_BASE = "https://api.geckoterminal.com/api/v2"

PREFERRED_QUOTES = {"USDC","USDT","SOL","ETH","WETH","USD"}
EVM_CHAIN_GUESS = ["ethereum","base","arbitrum","bsc","polygon","optimism","avalanche","fantom","linea","zksync","blast"]
DS_TO_GT = {
    "solana": "solana", "ethereum": "eth", "bsc": "bsc", "base": "base", "arbitrum": "arbitrum",
    "polygon": "polygon_pos", "avalanche": "avalanche", "optimism": "optimism", "fantom": "fantom",
    "linea": "linea", "zksync": "zksync", "blast": "blast", "ton":"ton","sui":"sui"
}

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
    # map days -> GeckoTerminal timeframe
    if days <= 1:  return "5m"
    if days <= 3:  return "5m"
    if days <= 7:  return "15m"
    if days <= 30: return "1h"
    if days <= 120:return "4h"
    return "1d"

def _days_for_tf(tf: str) -> int:
    mp = {"1h":1, "4h":2, "8h":3, "12h":3, "24h":4, "7d":9, "30d":35, "1y":370, "all":370}
    return mp.get(tf, 3)

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
        return df
    except Exception as e:
        LOG.warning("[GT] parse error: %s", e)
        return pd.DataFrame()

def ds_series_via_geckoterminal(addr: str, tf: str = "12h") -> Tuple[pd.DataFrame, dict]:
    """
    Resolve addr -> best DS pair -> GT OHLCV for that pool at the right granularity.
    Returns (df, meta) where meta includes token name/symbol and supply_est if available.
    """
    best = ds_best_pair(addr)
    if not best:
        return pd.DataFrame(), {}

    chain = (best.get("chainId") or "").lower().strip()
    pair  = best.get("pairAddress") or ""
    net = DS_TO_GT.get(chain, chain)
    if not net or not pair:
        LOG.info("[DS→GT] missing mapping or pair for %s (chain=%s, pair=%s)", addr, chain, pair)
        return pd.DataFrame(), {}

    days = _days_for_tf(tf)
    tf_gt = _gt_timeframe_for_days(days)
    LOG.info("[DS→GT] GT OHLCV net=%s pair=%s tf=%s (days=%s)", net, pair, tf_gt, days)
    df = gt_ohlcv_by_pool(net, pair, timeframe=tf_gt, limit=1000)
    if df is None or df.empty:
        return pd.DataFrame(), {}

    # Try to estimate supply to derive a market‑cap series.
    price_now = to_float(best.get("priceUsd"))
    fdv_now   = to_float(best.get("fdv"))
    supply_est = None
    if price_now and fdv_now and price_now > 0:
        supply_est = fdv_now / price_now

    meta = {
        "ds_chain": chain,
        "ds_pair": pair,
        "token_symbol": ((best.get("baseToken") or {}).get("symbol") or "").upper(),
        "token_name": (best.get("baseToken") or {}).get("name") or "",
        "supply_est": supply_est
    }

    df["market_cap"] = np.nan
    if supply_est:
        try:
            df["market_cap"] = df["close"].astype(float) * float(supply_est)
        except Exception:
            pass

    # synth minor extras for indicators if any column missing
    if "open" not in df.columns:  df["open"] = df["close"].shift(1)
    if "high" not in df.columns:  df["high"] = df["close"].rolling(3, min_periods=1).max()
    if "low"  not in df.columns:  df["low"]  = df["close"].rolling(3, min_periods=1).min()

    return df[["timestamp","open","high","low","close","volume","market_cap"]].dropna(subset=["timestamp"]).sort_values("timestamp"), meta

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

    # ADX(14) — simplified Wilder
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

# ---------- hydrate (CC→CG→DS/GT with tf-aware granularity) ----------
def hydrate_symbol(symbol: str, tf: str = "12h", force: bool=False) -> Tuple[pd.DataFrame, dict]:
    raw = (symbol or "").strip()
    s_for_cache = _norm_for_cache(raw)

    # serve cache if fresh
    if (not force) and _fresh_enough(s_for_cache):
        cached = load_cached_frame(s_for_cache)
        if not cached.empty:
            return cached, {}

    LOG.info("[Hydrate] %s (tf=%s, force=%s, ttl=%ss)", s_for_cache, tf, force, TTL_SECONDS)

    # 0) local archive (optional)
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

    # 1) CryptoCompare for symbols only
    is_addr = _is_address_like(s_for_cache)
    m = cc_hist(s_for_cache, "minute", limit=360) if not is_addr else pd.DataFrame()
    h = cc_hist(s_for_cache, "hour",   limit=24*30) if not is_addr else pd.DataFrame()
    d = cc_hist(s_for_cache, "day",    limit=365)   if not is_addr else pd.DataFrame()

    meta = {}
    if (m is None or m.empty) and (h is None or h.empty) and (d is None or d.empty):
        LOG.info("[Hydrate] CC empty/unsupported → CG fallback for %s", s_for_cache)
        df = cg_series(raw, days=max(7, _days_for_tf(tf)))
        if df.empty and is_addr:
            LOG.info("[Hydrate] CG empty → DexScreener+GeckoTerminal for %s (tf=%s)", raw, tf)
            df, meta = ds_series_via_geckoterminal(raw, tf=tf)
    else:
        parts = [x for x in (d,h,m) if x is not None and not x.empty]
        df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

    # 2) add market_cap from CG if missing
    if (df is not None) and (not df.empty) and "market_cap" not in df.columns:
        caps = cg_series(raw, days=90)
        if not caps.empty and "market_cap" in caps.columns:
            df = df.merge(caps[["timestamp","market_cap"]], on="timestamp", how="left")

    if df is None or df.empty:
        cached = load_cached_frame(s_for_cache)
        if not cached.empty:
            _touch_fetch(s_for_cache)
            return cached, meta
        LOG.warning("[Hydrate] %s no data after CC/CG/DSGT", s_for_cache)
        return pd.DataFrame(), meta

    if not df_arch.empty:
        df = pd.concat([df_arch, df], ignore_index=True)

    df = df.dropna(subset=["timestamp"]).drop_duplicates(subset=["timestamp"], keep="last")\
           .sort_values("timestamp").reset_index(drop=True)

    df = compute_indicators(df)

    save_frame(s_for_cache, df)
    _touch_fetch(s_for_cache)
    return df, meta

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

# ---------- Plotly figures ----------
def _apply_time_axis(fig: go.Figure) -> None:
    fig.update_xaxes(
        tickformatstops=[
            dict(dtickrange=[None, 1000*60*60*24], value="%H:%M"),
            dict(dtickrange=[1000*60*60*24, None], value="%m-%d"),
        ]
    )

def fig_price_candles(df: pd.DataFrame, symbol: str) -> go.Figure:
    fig = make_subplots(rows=3, cols=1, shared_xaxes=True,
                        row_heights=[0.64, 0.22, 0.14], vertical_spacing=0.03)
    if not df.empty:
        fig.add_trace(go.Candlestick(
            x=df["timestamp"], open=df["open"], high=df["high"], low=df["low"], close=df["close"],
            name=f"{_disp_symbol(symbol)} OHLC",
            increasing_line_color="#36d399", decreasing_line_color="#f87272", opacity=0.95
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
        if "macd_line" in df.columns and "macd_signal" in df.columns:
            fig.add_trace(go.Scatter(x=df["timestamp"], y=df["macd_line"],   name="MACD",  line=dict(width=1.1)),
                          row=2, col=1)
            fig.add_trace(go.Scatter(x=df["timestamp"], y=df["macd_signal"], name="Signal", line=dict(width=1, dash="dot")),
                          row=2, col=1)
        if "macd_hist" in df.columns:
            fig.add_trace(go.Bar(x=df["timestamp"], y=df["macd_hist"], name="MACD Hist"), row=2, col=1)
        if "volume" in df.columns:
            diff = df["close"].diff().fillna(0)
            colors = ['#36d399' if d >= 0 else '#f87272' for d in diff.tolist()]
            fig.add_trace(go.Bar(x=df["timestamp"], y=df["volume"].fillna(0), name="Volume",
                                 marker=dict(color=colors), opacity=0.8), row=3, col=1)
    fig.update_layout(template="plotly_dark", height=420, margin=dict(l=12,r=12,t=24,b=10),
                      legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                      xaxis_rangeslider_visible=False, barmode="relative")
    _apply_time_axis(fig); return fig

def _fib_levels(series: pd.Series) -> List[Tuple[str,float]]:
    s = series.dropna()
    if s.empty: return []
    hi, lo = float(s.max()), float(s.min())
    rng = hi - lo
    levels = [("Fib 100%", hi), ("Fib 61.8%", hi - 0.618*rng), ("Fib 50%", hi - 0.5*rng),
              ("Fib 38.2%", hi - 0.382*rng), ("Fib 23.6%", hi - 0.236*rng), ("Fib 0%", lo)]
    return levels

def fig_mcap_main(df: pd.DataFrame, symbol: str, name_hint: str="") -> go.Figure:
    fig = go.Figure()
    y = "market_cap" if ("market_cap" in df.columns and not pd.isna(df["market_cap"]).all()) else "close"
    lab = "Market Cap" if y == "market_cap" else "Price"
    fig.add_trace(go.Scatter(x=df["timestamp"], y=df[y], name=f"{(name_hint or _disp_symbol(symbol))} {lab}", mode="lines"))
    # Fib lines
    levels = _fib_levels(df[y])
    for title, val in levels:
        fig.add_hline(y=val, line_width=1, line_dash="dot", annotation_text=title, opacity=0.35)
    fig.update_layout(template="plotly_dark", height=420, margin=dict(l=12,r=12,t=24,b=10),
                      legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
    _apply_time_axis(fig); return fig

def fig_line(df: pd.DataFrame, y: str, name: str, h: int = 155) -> go.Figure:
    fig = go.Figure()
    if not df.empty and y in df.columns and not pd.isna(df[y]).all():
        y_data = df[y].astype(float)
        fig.add_trace(go.Scatter(x=df["timestamp"], y=y_data, name=name, line=dict(width=1.6)))
        ymin, ymax = float(np.nanmin(y_data)), float(np.nanmax(y_data))
        if ymin == ymax:
            ymin -= 0.5; ymax += 0.5
        fig.update_yaxes(range=[ymin, ymax], fixedrange=False)
    else:
        fig.add_annotation(text="No data for this timeframe.", showarrow=False, xref="paper", yref="paper", x=0.5, y=0.5)
    fig.update_layout(template="plotly_dark", height=h, margin=dict(l=10,r=10,t=18,b=8), showlegend=False)
    _apply_time_axis(fig); return fig

# ---------- simple NLU / answers (kept; wording friendlier) ----------
INTRO_LINES = [
    "Here’s what I’m seeing:",
    "Quick read from Luna:",
    "Let’s unpack this:",
    "Alright — chart check:",
]
def intro_line() -> str: return random.choice(INTRO_LINES)

def classify_question(q: str) -> str:
    q = (q or "").lower()
    if any(k in q for k in ["why", "reason", "cause", "factor", "driver"]): return "reasons"
    if any(k in q for k in ["up", "increase", "rise", "bullish", "rally", "pump", "green"]): return "upside"
    if any(k in q for k in ["down", "decrease", "fall", "bearish", "dump", "red"]): return "downside"
    if any(k in q for k in ["trend", "momentum", "direction"]): return "trend"
    if any(k in q for k in ["volatility", "range", "volume", "liquidity", "squeeze"]): return "volatility"
    if any(k in q for k in ["buy", "dip", "entry", "accumulate", "accumulation"]): return "buy"
    if any(k in q for k in ["coin", "what is this", "which token"]): return "what"
    return "general"

def _fmt_pct(v: Optional[float]) -> str: return "n/a" if v is None else f"{v:+.2f}%"

def _safe_tail(series: pd.Series, n: int) -> pd.Series:
    try: return series.dropna().tail(n)
    except Exception: return pd.Series(dtype="float64")

def _build_snapshot(df: pd.DataFrame, tf: str) -> Dict[str, Any]:
    view = slice_df(df, tf)
    if view.empty: view = df.tail(200)
    ch, _ = compute_rollups(view)
    last = view.iloc[-1]
    close = to_float(last.get("close")); rsi   = to_float(last.get("rsi"))
    macdl = to_float(last.get("macd_line")); macds = to_float(last.get("macd_signal"))
    hist  = to_float(last.get("macd_hist")); adx   = to_float(last.get("adx14"))
    bbw   = to_float(last.get("bb_width"));  atr   = to_float(last.get("atr14"))
    altm  = to_float(last.get("alt_momentum"))
    obv_slope = 0.0
    if "obv" in view.columns:
        obv = _safe_tail(view["obv"], 5)
        if len(obv) >= 2: obv_slope = float(obv.iloc[-1] - obv.iloc[0])
    vol_now = to_float(last.get("volume"))
    vol_ma20 = float(view["volume"].rolling(20).mean().iloc[-1]) if "volume" in view.columns and len(view["volume"].dropna())>=20 else None
    vol_ratio = (vol_now / vol_ma20) if (vol_now is not None and vol_ma20 and vol_ma20>0) else None
    look = min(len(view), 60)
    try:
        recent_high = float(view["high"].tail(look).max())
        recent_low  = float(view["low"].tail(look).min())
    except Exception:
        recent_high = recent_low = None
    score = 0
    if rsi is not None:
        if rsi >= 60: score += 1
        if rsi <= 40: score -= 1
    if hist is not None:
        if hist > 0: score += 1
        if hist < 0: score -= 1
    if altm is not None:
        if altm > 0: score += 1
        if altm < 0: score -= 1
    bias = "bullish" if score > 0 else ("bearish" if score < 0 else "range")
    conf = 30.0
    if adx is not None: conf = max(20.0, min(70.0, (adx * 1.5)))
    conf = round(conf, 0)
    bb_is_tight = (bbw is not None and bbw <= 1.0)
    obv_state = "accumulation" if obv_slope > 0 else "distribution" if obv_slope < 0 else "neutral"
    return {"view":view,"ch":ch,"close":close,"price_str":money(close),"rsi":rsi,"macdl":macdl,"macds":macds,"hist":hist,
            "adx":adx,"bbw":bbw,"atr":atr,"alt_momentum":altm,"bb_is_tight":bb_is_tight,"obv_state":obv_state,
            "vol_now":vol_now,"vol_ma20":vol_ma20,"vol_ratio":vol_ratio,"recent_high":recent_high,"recent_low":recent_low,
            "bias":bias,"confidence":conf}

def _token_name_line(meta: dict, symbol: str) -> str:
    sym = (meta.get("token_symbol") or "").upper()
    name = meta.get("token_name") or ""
    if sym and name: return f"{name} ({sym})"
    if sym: return sym
    return _disp_symbol(symbol)

def _answer_basic(symbol: str, df: pd.DataFrame, snap: Dict[str, Any], tf: str, name_hint: str) -> str:
    ch = snap["ch"]; rsi, hist, adx = snap["rsi"], snap["hist"], snap["adx"]
    bbw, atr = snap["bbw"], snap["atr"]; bias, conf = snap["bias"], snap["confidence"]
    obv_state = snap["obv_state"]; vol_ratio = snap["vol_ratio"]; rh, rl = snap["recent_high"], snap["recent_low"]
    price = snap["price_str"]; perf = f"1h {_fmt_pct(ch.get('1h'))}, 4h {_fmt_pct(ch.get('4h'))}, 12h {_fmt_pct(ch.get('12h'))}, 24h {_fmt_pct(ch.get('24h'))}"
    mom_bits = []
    if rsi is not None: mom_bits.append(f"RSI {rsi:.1f}")
    if hist is not None: mom_bits.append(f"MACD hist {hist:+.2f}")
    if adx is not None: mom_bits.append(f"ADX {adx:.1f}")
    if bbw is not None: mom_bits.append(f"bands {bbw:.2f}%")
    if vol_ratio is not None: mom_bits.append(f"vol ~{vol_ratio:.1f}× 20‑bar")
    levels = []
    if rh is not None: levels.append(f"↑ {money(rh)}")
    if rl is not None: levels.append(f"↓ {money(rl)}")
    bias_txt = "bullish" if bias == "bullish" else "bearish" if bias == "bearish" else "range‑bound"
    return (f"{intro_line()} {_token_name_line({}, symbol) if not name_hint else name_hint} around {price}. {perf}. "
            f"Momentum/Trend: {', '.join(mom_bits)}. OBV suggests {obv_state}. "
            f"Bias: {bias_txt}, confidence ~{int(conf)}%. Key levels: {', '.join(levels) if levels else 'watch recent highs/lows'}. "
            f"This is educational analysis, not advice.")

def luna_answer(symbol: str, df: pd.DataFrame, tf: str, question: str = "", name_hint: str="") -> str:
    try:
        snap = _build_snapshot(df, tf)
    except Exception as e:
        LOG.warning("Snapshot build failed: %s", e)
        return f"{_disp_symbol(symbol)}: I don’t have enough fresh data yet."
    intent = classify_question(question)
    if intent in ("trend","general","what"):
        return _answer_basic(symbol, df, snap, tf, name_hint)
    if intent == "upside":
        return _answer_basic(symbol, df, snap, tf, name_hint).split("Bias:")[0] + "Upside keys: strong closes above recent highs on rising volume; RSI > 60 and MACD expanding help follow‑through."
    if intent == "downside":
        return "Downside risks: loss of recent support, RSI < 40, MACD negative with fading volume. If ADX rises while price slips, trend‑down risk increases."
    if intent == "volatility":
        bbw, atr, close = snap["bbw"], snap["atr"], snap["close"]
        atr_pct = (atr/close*100.0) if (atr and close) else None
        return f"Volatility on {tf}: bands {f1(bbw)}%; ATR ≈ {atr_pct:.2f}% of price." if atr_pct else f"Volatility on {tf}: bands {f1(bbw)}%."
    if intent == "buy":
        return "What helps dip buys: pullbacks into prior value area with supportive volume; watch for RSI holding ~45–50 and MACD curling up. (Education only.)"
    if intent == "reasons":
        return "Drivers: liquidity/volume, market trend (BTC/ETH), exchange listings, and community catalysts. Confirm moves with volume and OBV."
    return _answer_basic(symbol, df, snap, tf, name_hint)

# ---------- routes ----------
@app.get("/")
def home():
    return ('<meta http-equiv="refresh" content="0; url=/analyze?symbol=ETH&tf=12h">', 302)

@app.get("/analyze")
def analyze():
    symbol_raw = (request.args.get("symbol") or "ETH").strip()
    tf        = (request.args.get("tf") or "12h")
    df_full, meta = hydrate_symbol(symbol_raw, tf=tf, force=False)
    if df_full.empty:
        return Response("No data.", 500)
    df_view = slice_df(df_full, tf)
    perf, invest = compute_rollups(df_full)  # pills on full history

    # choose main chart: cap-first if price < $0.10 and we have cap estimate
    use_cap = False
    last_close = to_float(df_full["close"].dropna().iloc[-1]) if not df_full.empty else None
    if last_close is not None and last_close < 0.10 and ("market_cap" in df_full.columns) and not pd.isna(df_full["market_cap"]).all():
        use_cap = True

    symbol_disp = _disp_symbol(symbol_raw)
    name_hint = meta.get("token_symbol") or ""
    ui_name = meta.get("token_symbol") or symbol_disp

    tiles: Dict[str, str] = {
        "RSI":   pio.to_html(fig_line(df_view, "rsi", "RSI"), include_plotlyjs=False, full_html=False),
        "PRICE": pio.to_html(fig_mcap_main(df_view if not df_view.empty else df_full, symbol_disp, name_hint if use_cap else ""), include_plotlyjs=False, full_html=False) if use_cap
                  else pio.to_html(fig_price_candles(df_view if not df_view.empty else df_full, symbol_disp), include_plotlyjs=False, full_html=False),
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
    def pct(k): v = ch_view.get(k); return ("n/a" if v is None else f"{v:+.2f}%")
    tldr_line = (
        f"{(meta.get('token_symbol') or symbol_disp)}: 1h {pct('1h')}, 4h {pct('4h')}, 12h {pct('12h')}, 24h {pct('24h')}. "
        f"{'Cheap tokens are judged more by market cap than price-per-token.' if use_cap else 'Majors are shown with price candles.'}"
    )
    updated = (_latest_ts(df_full) or utcnow()).strftime("UTC %Y-%m-%d %H:%M")

    return render_template(
        "control_panel.html",
        symbol=symbol_disp, ui_name=ui_name, tf=tf, updated=updated,
        tiles=tiles, performance=perf, investment=invest, tldr_line=tldr_line
    )

@app.get("/expand_json")
def expand_json():
    symbol_raw = (request.args.get("symbol") or "ETH").strip()
    tf     = (request.args.get("tf") or "12h")
    key    = (request.args.get("key") or "RSI").upper()
    df = load_cached_frame(symbol_raw)
    meta = {}
    if df.empty:
        df, meta = hydrate_symbol(symbol_raw, tf=tf, force=False)
    dfv = slice_df(df, tf)

    fig = None
    if key == "PRICE":
        last_close = to_float(df["close"].dropna().iloc[-1]) if not df.empty else None
        use_cap = last_close is not None and last_close < 0.10 and ("market_cap" in df.columns) and not pd.isna(df["market_cap"]).all()
        fig = fig_mcap_main(dfv if not dfv.empty else df, _disp_symbol(symbol_raw), meta.get("token_symbol") if use_cap else "") if use_cap \
              else fig_price_candles(dfv if not dfv.empty else df, _disp_symbol(symbol_raw))
    else:
        key_map = {
            "RSI":   fig_line(dfv, "rsi", "RSI"),
            "MACD":  fig_line(dfv, "macd_line", "MACD"),
            "BANDS": fig_line(dfv, "bb_width", "Bands Width"),
            "VOL":   fig_line(dfv, "volume", "Volume Trend"),
            "LIQ":   fig_line(dfv, "volume", "Liquidity"),
            "OBV":   fig_line(dfv, "obv", "OBV"),
            "ADX":   fig_line(dfv, "adx14", "ADX 14"),
            "MCAP":  fig_line(dfv if "market_cap" in dfv.columns else df, "market_cap", "Market Cap"),
            "ATR":   fig_line(dfv, "atr14", "Volatility (ATR 14)"),
            "ALT":   fig_line(dfv, "alt_momentum", "ALT (Momentum)"),
        }
        fig = key_map.get(key, fig_line(dfv, "close", key))

    talk = luna_answer(symbol_raw, dfv if not dfv.empty else df, tf, "", meta.get("token_symbol") or "")
    return jsonify({"fig": fig.to_plotly_json(), "talk": talk, "tf": tf, "key": key})

@app.get("/api/refresh/<symbol>")
def api_refresh(symbol: str):
    df, _ = hydrate_symbol(symbol, tf="12h", force=True)
    return jsonify({"ok": (not df.empty), "rows": 0 if df is None else len(df)})

# --- suggestions / resolve (unchanged) ---------------------------------------
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
    syms = sorted(out); return syms[:maxn]

def _resolve_symbol(query: str) -> str | None:
    if not query: return None
    q = query.strip()
    if _is_address_like(q): return q  # keep address
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
    if q: syms = [s for s in syms if q in s]
    return jsonify({"symbols": syms[:20]})

@app.get("/api/resolve")
def api_resolve():
    q = (request.args.get("query") or "").strip()
    sym = _resolve_symbol(q)
    if not sym:
        return jsonify({"ok": False, "error": "not found"}), 404
    return jsonify({"ok": True, "symbol": sym})

# --- debug (kept) ------------------------------------------------------------
@app.get("/debug/resolve")
def debug_resolve():
    q = (request.args.get("q") or "").strip()
    return jsonify({
        "input": q,
        "is_addr_like": _is_address_like(q),
        "is_evm": _is_evm_addr(q),
        "is_solana_like": _is_solana_addr(q),
        "canonical": q,
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
