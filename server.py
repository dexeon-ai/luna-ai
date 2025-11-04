# ============================================================
# server.py — Luna Cockpit (stable grid + richer expand + CSV merge)
# Brain upgrade: free/offline metrics + conversational analysis
# + Universal coin/contract support via CoinGecko coin list cache
# + DexScreener resolver (for unknown contracts) + GeckoTerminal OHLCV fallback
# + Token metadata (name/symbol) resolver & cache for contract addresses
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
BUILD_TAG = "LUNA-META-2025-11-04-07"

# ====== NEW brain modules (safe fallbacks if missing) ======
try:
    from luna_agent.metrics import enrich_with_metrics
    HAVE_METRICS = True
except Exception:
    enrich_with_metrics = None
    HAVE_METRICS = False

try:
    from luna_agent.scoring import generate_analysis
    HAVE_SCORING = True
except Exception:
    generate_analysis = None
    HAVE_SCORING = False

# (Optional legacy import; we don't rely on it)
try:
    from luna_agent import analyze_indicators as _legacy_analyze_indicators
except Exception:
    _legacy_analyze_indicators = None

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
COINS_DIR  = DATA_DIR / "coins"           # your large CSV archives (1998 coins)

for d in (FRAMES_DIR, STATE_DIR, SESS_DIR):
    d.mkdir(parents=True, exist_ok=True)

FETCH_LOG = STATE_DIR / "fetch_log.json"

# ---------- constants ----------
TTL_MINUTES = 15
TTL_SECONDS = TTL_MINUTES * 60

# CoinGecko coin list cache for universal symbol/contract lookup
COIN_LIST_PATH = STATE_DIR / "cg_coin_list.json"
COIN_LIST_TTL  = 60 * 60 * 24   # 24h

# token meta cache (name/symbol for contract addresses)
TOKEN_META_CACHE = STATE_DIR / "token_meta_cache.json"

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

def _read_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _write_json(path: Path, data: dict) -> None:
    try:
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    except Exception as e:
        LOG.warning("write json failed: %s", e)

def _short_addr(addr: str) -> str:
    return f"{addr[:4]}…{addr[-4:]}" if isinstance(addr, str) and len(addr) > 12 else addr

# --- address detection / normalization (EVM + Solana + generic base58) -----
_BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_BASE58_RE = re.compile(r"^[%s]{32,64}$" % re.escape(_BASE58_ALPHABET))  # Solana-like

def _is_evm_addr(s: str) -> bool:
    return isinstance(s, str) and re.fullmatch(r"0[xX][a-fA-F0-9]{40}", s.strip()) is not None

def _is_solana_addr(s: str) -> bool:
    return isinstance(s, str) and (_BASE58_RE.match(s.strip()) is not None)

def _is_address_like(s: str) -> bool:
    if not isinstance(s, str) or not s: return False
    return _is_evm_addr(s) or _is_solana_addr(s) or bool(re.fullmatch(r"[A-Za-z0-9]{26,64}", s.strip()))

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

# ---------- CoinGecko (universal fallback) ----------
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
    """Fetch and cache CoinGecko coin list (with platforms) for 24h."""
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
    """Resolve symbol (ETH), id (ethereum), or contract (0x.. / base58) to a CG id."""
    if not q: return None
    q_raw = q.strip()
    if not _is_address_like(q_raw):
        if q_raw.upper() in CG_IDS:
            return CG_IDS[q_raw.upper()]
    coins = cg_fetch_coin_list()
    if not coins:
        return CG_IDS.get(q_raw.upper()) if not _is_address_like(q_raw) else None

    ql = q_raw.lower()

    # Contract resolution across known platforms
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

    # Symbol match (may be many; prefer CG_IDS mapping first)
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
    """Fetch price/volume/market_cap series by CG id resolved from symbol/id/contract."""
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

    # synth OHLC for indicators
    dp["open"] = dp["close"].shift(1)
    dp["high"] = dp["close"].rolling(3, min_periods=1).max()
    dp["low"]  = dp["close"].rolling(3, min_periods=1).min()
    return dp[["timestamp","open","high","low","close","volume","market_cap"]]\
        .dropna(subset=["timestamp"]).sort_values("timestamp")

# ---------- DexScreener (resolver) + GeckoTerminal (OHLCV candles) ---------
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
    if _is_solana_addr(addr) or (len(addr) >= 32 and addr[0] in "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"):
        return ["solana"]
    if _is_evm_addr(addr):
        return EVM_CHAIN_GUESS
    return ["solana"] + EVM_CHAIN_GUESS

def ds_pairs_for_token(addr: str) -> List[dict]:
    pairs: List[dict] = []
    for ch in _guess_ds_chains(addr):
        js1 = ds_get(f"/token-pairs/v1/{ch}/{addr}")
        pairs.extend(_collect_pairs_from_ds_payload(js1))
        if pairs:
            break
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
        if pairs:
            break
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
                 str((((best.get("liquidity") or {}).get("usd")) or "0")))
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
        return df
    except Exception as e:
        LOG.warning("[GT] parse error: %s", e)
        return pd.DataFrame()

def ds_series_via_geckoterminal(addr: str, days: int = 30) -> pd.DataFrame:
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
    if (df is None or df.empty):
        try:
            LOG.info("[DS→GT] empty OHLCV for pool; trying GT pools list for token")
            js = gt_get(f"/networks/{net}/tokens/{addr}/pools", params={"include":"base_token,quote_token"})
            pools = []
            if isinstance(js, dict):
                pools = [x.get("id") for x in (js.get("data") or []) if isinstance(x, dict) and x.get("id")]
            for pid in pools[:3]:
                LOG.info("[DS→GT] trying pool id %s", pid)
                df = gt_ohlcv_by_pool(net, pid, timeframe=tf, limit=500)
                if not df.empty: break
        except Exception as e:
            LOG.warning("[GT] token->pools fallback failed: %s", e)
    if df is not None and not df.empty and "market_cap" not in df.columns:
        df["market_cap"] = np.nan
    return df

# ---------- address canonicalization ----------
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
        if s.startswith("0X"):
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

# ---------------- token metadata (name/symbol) resolvers ----------------
def _meta_cache_get(addr: str) -> Optional[dict]:
    st = _read_json(TOKEN_META_CACHE)
    key = addr.lower() if _is_evm_addr(addr) else addr
    v = st.get(key)
    if isinstance(v, dict):
        return v
    return None

def _meta_cache_put(addr: str, meta: dict) -> None:
    st = _read_json(TOKEN_META_CACHE)
    key = addr.lower() if _is_evm_addr(addr) else addr
    st[key] = meta
    _write_json(TOKEN_META_CACHE, st)

def ds_token_meta(addr: str) -> Optional[dict]:
    try:
        best = ds_best_pair(addr)
        if not best:
            return None
        bt = (best.get("baseToken") or {})
        qt = (best.get("quoteToken") or {})
        tok = bt if str(bt.get("address","")).lower() == addr.lower() else \
              qt if str(qt.get("address","")).lower() == addr.lower() else bt
        name = tok.get("name") or None
        sym  = (tok.get("symbol") or None)
        if sym:
            sym = str(sym).upper()
        out = {
            "address": addr,
            "name": name,
            "symbol": sym,
            "chainId": (best.get("chainId") or None),
            "pairAddress": (best.get("pairAddress") or None),
            "source": "dexscreener",
        }
        return out
    except Exception as e:
        LOG.warning("[Meta/DS] %s", e)
        return None

def cg_token_meta(addr: str) -> Optional[dict]:
    try:
        coins = cg_fetch_coin_list()
        ql = addr.lower()
        for c in coins:
            plats = c.get("platforms") or {}
            for _, a in plats.items():
                if a and str(a).lower() == ql:
                    sym = (c.get("symbol") or "").upper() or None
                    return {
                        "address": addr,
                        "name": c.get("name") or None,
                        "symbol": sym,
                        "coingecko_id": c.get("id"),
                        "source": "coingecko",
                    }
        return None
    except Exception as e:
        LOG.warning("[Meta/CG] %s", e)
        return None

def gt_token_meta_with_pair(best_pair: dict, addr: str) -> Optional[dict]:
    try:
        chain = (best_pair.get("chainId") or "").lower()
        net = DS_TO_GT.get(chain, chain)
        if not net:
            return None
        js = gt_get(f"/networks/{net}/tokens/{addr}")
        data = (js or {}).get("data") or {}
        attrs = data.get("attributes") or {}
        name = attrs.get("name") or None
        sym  = (attrs.get("symbol") or None)
        if sym:
            sym = str(sym).upper()
        if name or sym:
            return {
                "address": addr,
                "name": name,
                "symbol": sym,
                "chainId": chain,
                "source": "geckoterminal",
            }
        return None
    except Exception as e:
        LOG.warning("[Meta/GT] %s", e)
        return None

def token_meta_for(query: str) -> dict:
    """
    Return a dict with: name, symbol, label, address, source.
    For non-address symbols, label=symbol.upper().
    For addresses, tries DS -> CG -> GT.
    Cached on disk to avoid repeated lookups.
    """
    s = (query or "").strip()
    # Simple symbol path
    if not _is_address_like(s):
        up = s.upper()
        return {"address": None, "name": up, "symbol": up, "label": up, "source": "symbol"}

    addr = s.lower() if _is_evm_addr(s) else s
    cached = _meta_cache_get(addr)
    if cached:
        return cached

    meta = ds_token_meta(addr)
    if not meta:
        meta = cg_token_meta(addr)
    if not meta:
        best = ds_best_pair(addr)
        if best:
            meta = gt_token_meta_with_pair(best, addr)

    if not meta:
        meta = {"address": addr, "name": None, "symbol": None, "source": "unknown"}

    name, sym = (meta.get("name") or ""), (meta.get("symbol") or "")
    if name and sym:
        label = f"{name} ({sym})"
    elif sym:
        label = sym
    elif name:
        label = name
    else:
        label = _short_addr(addr)
    meta["label"] = label

    _meta_cache_put(addr, meta)
    return meta

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

# ---------- hydrate (TTL + CSV merge + CC→CG→DS/GT) ----------
def hydrate_symbol(symbol: str, force: bool=False) -> pd.DataFrame:
    raw_in = (symbol or "").strip()
    # canonicalize early (repairs 0X..., recovers Solana case via DS)
    raw = canonicalize_query(raw_in)
    s_for_cache = _norm_for_cache(raw)

    # serve cache if fresh
    if (not force) and _fresh_enough(s_for_cache):
        cached = load_cached_frame(s_for_cache)
        if not cached.empty:
            LOG.info("[Hydrate] %s served from fresh cache", s_for_cache)
            return cached

    LOG.info("[Hydrate] %s (force=%s, ttl=%ss)", s_for_cache, force, TTL_SECONDS)

    # 0) merge local CSV archives first (once)
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

    # 1) live pulls (CryptoCompare) — only for symbols (skip for addresses)
    is_addr = _is_address_like(s_for_cache)
    m = cc_hist(s_for_cache, "minute", limit=360) if not is_addr else pd.DataFrame()
    h = cc_hist(s_for_cache, "hour",   limit=24*30) if not is_addr else pd.DataFrame()
    d = cc_hist(s_for_cache, "day",    limit=365)   if not is_addr else pd.DataFrame()

    if (m is None or m.empty) and (h is None or h.empty) and (d is None or d.empty):
        LOG.info("[Hydrate] CC empty/unsupported → CG fallback for %s", s_for_cache)
        df = cg_series(raw, days=365)  # try broader first
        if df.empty:
            df = cg_series(raw, days=30)
        # If still empty and looks like an address → DexScreener + GeckoTerminal
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

    # 3) stitch archives + live, compute indicators + enrich
    if df is None or df.empty:
        cached = load_cached_frame(s_for_cache)
        if not cached.empty:
            _touch_fetch(s_for_cache)
            LOG.info("[Hydrate] %s returning from older cache", s_for_cache)
            return cached
        LOG.warning("[Hydrate] %s no data after CC/CG/DSGT", s_for_cache)
        return pd.DataFrame()

    if not df_arch.empty:
        df = pd.concat([df_arch, df], ignore_index=True)

    df = df.dropna(subset=["timestamp"]).drop_duplicates(subset=["timestamp"], keep="last")\
           .sort_values("timestamp").reset_index(drop=True)

    df = compute_indicators(df)

    # extra metrics (free/offline)
    if HAVE_METRICS and enrich_with_metrics is not None:
        try:
            df = enrich_with_metrics(df)
        except Exception as e:
            LOG.warning("[Metrics] enrich failed: %s", e)

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

# ---------- improved paragraph (keeps same output location) ----------
def luna_paragraph(symbol_for_display: str, df: pd.DataFrame, tf: str, question: str) -> str:
    if df.empty:
        return f"{symbol_for_display}: I don’t have enough fresh data yet."
    view = slice_df(df, tf)
    if view.empty:
        view = df.tail(200)
    _, _ = compute_rollups(df)
    ch_view, _ = compute_rollups(view)

    if HAVE_SCORING and generate_analysis is not None:
        try:
            return generate_analysis(symbol_for_display, df, view, ch_view, question or "")
        except Exception as e:
            LOG.warning("[Luna] generate_analysis failed: %s", e)

    if _legacy_analyze_indicators is not None:
        last = view.iloc[-1]
        trend, reasoning = _legacy_analyze_indicators(last, ch_view)
        if trend == "bullish":
            plan = "Upside momentum should persist if volume expands and resistance breaks."
        elif trend == "bearish":
            plan = "Further downside risk unless buyers reclaim lost ground."
        else:
            plan = "Expect consolidation until a new catalyst drives direction."
        return f"{symbol_for_display}: {reasoning} {plan}"

    last = view.iloc[-1]
    price = money(last.get("close"))
    rsi   = to_float(last.get("rsi"))
    macdl = to_float(last.get("macd_line"))
    macds = to_float(last.get("macd_signal"))
    hist  = to_float(last.get("macd_hist"))
    adx   = to_float(last.get("adx14"))
    bbw   = to_float(last.get("bb_width"))
    perf_bits = [f"{k} {v:+.2f}%" for k,v in ch_view.items() if v is not None and k in ("1h","4h","12h","24h")]
    perf_txt  = ", ".join(perf_bits) if perf_bits else "flat"
    side = "bulls" if (macdl is not None and macds is not None and macdl > macds) else "bears" if (macdl is not None and macds is not None and macdl < macds) else "neither side"
    return (
        f"{symbol_for_display} around {price}. Over this window: {perf_txt}. "
        f"RSI {f1(rsi)}; MACD {('>' if (macdl and macds and macdl>macds) else '<' if (macdl and macds and macdl<macds) else '=')} signal "
        f"(hist {f1(hist)}). ADX {f1(adx)} (trend). Bands width {f1(bbw)}%. Short answer: "
        f"{('bulls have a small edge' if side=='bulls' else 'bears have the edge' if side=='bears' else 'range-bound')}."
    )

# ---------- modal talk ----------
def talk_indicator(key: str, df: pd.DataFrame) -> str:
    if df.empty:
        return "No data available yet. Try refreshing in a few minutes."
    last = df.iloc[-1]
    if key == "RSI":
        r = to_float(last.get("rsi"))
        if r is None:
            return "RSI is unavailable for this timeframe."
        bias = "bullish momentum building" if r > 60 else "neutral consolidation" if 40 <= r <= 60 else "bearish momentum increasing"
        return f"RSI at {r:.1f}. This suggests {bias}. Above 70 = overheated, below 30 = washed‑out."
    if key == "MACD":
        m = to_float(last.get("macd_line")); s = to_float(last.get("macd_signal")); h = to_float(last.get("macd_hist"))
        if m is None or s is None:
            return "MACD data not available yet."
        cross = "bullish crossover" if m > s else "bearish crossover" if m < s else "flat momentum"
        return f"MACD shows {cross} ({m:.2f} vs {s:.2f}); histogram {h:.2f} measures short‑term acceleration."
    if key == "ATR":
        a = to_float(last.get("atr14"))
        return f"ATR(14) = {a:.3f}. Average true range measures typical candle size; higher ATR means wider swings and more risk."
    if key == "ADX":
        a = to_float(last.get("adx14"))
        trend = "strong" if a and a >= 40 else "moderate" if a and a >= 25 else "weak or sideways"
        return f"ADX {f1(a)}. Trend strength is {trend}. Values above 25 indicate a directional move is establishing."
    if key == "OBV":
        ser = df["obv"].dropna()
        if len(ser) < 5:
            return "OBV not yet meaningful on this timeframe."
        slope = np.sign(ser.iloc[-1] - ser.iloc[-5])
        side = "accumulation (buying pressure)" if slope > 0 else "distribution (selling pressure)" if slope < 0 else "neutral flow"
        return f"On‑Balance Volume indicates {side}. Rising OBV typically confirms uptrend participation."
    if key == "BANDS":
        w = to_float(last.get("bb_width"))
        note = "tight squeeze — volatility compression" if w and w <= 1 else "normal range" if (w and w < 5) else "expanding volatility"
        return f"Bollinger width {f1(w)}. Current state: {note}."
    if key == "ALT":
        alt = to_float(last.get("alt_momentum"))
        return f"ALT momentum {f1(alt)}. Positive = recovery momentum; negative = fading drive."
    if key == "VOL":
        v = to_float(last.get("volume"))
        return f"Current volume {format(v,',.0f') if v is not None else 'n/a'}. Rising volume confirms active interest."
    if key == "LIQ":
        v = to_float(last.get("volume"))
        return f"Liquidity proxy {format(v,',.0f') if v is not None else 'n/a'}. Thin liquidity increases slippage."
    if key == "MCAP":
        mc = to_float(last.get("market_cap"))
        if mc is None:
            return "Market capitalization data not present."
        return f"Market cap {money(mc)}. Larger caps move slower; small caps amplify both gains and losses."
    return "No detailed commentary available for this indicator."

# ---------- Plotly helpers ----------
def _apply_time_axis(fig: go.Figure) -> None:
    fig.update_xaxes(
        tickformatstops=[
            dict(dtickrange=[None, 1000*60*60*24], value="%H:%M"),
            dict(dtickrange=[1000*60*60*24, None], value="%m-%d"),
        ]
    )

def fig_price(df: pd.DataFrame, name_for_legend: str) -> go.Figure:
    # Three rows inside the same tile: Price, MACD, Volume (mini)
    fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True,
        row_heights=[0.64, 0.22, 0.14], vertical_spacing=0.03
    )
    if not df.empty:
        # Row 1: OHLC + Bollinger
        fig.add_trace(go.Candlestick(
            x=df["timestamp"], open=df["open"], high=df["high"], low=df["low"], close=df["close"],
            name=f"{name_for_legend} OHLC", increasing_line_color="#36d399", decreasing_line_color="#f87272", opacity=0.95
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

        # Row 2: MACD lines + hist
        if "macd_line" in df.columns and "macd_signal" in df.columns:
            fig.add_trace(go.Scatter(x=df["timestamp"], y=df["macd_line"],   name="MACD",  line=dict(width=1.1)),
                          row=2, col=1)
            fig.add_trace(go.Scatter(x=df["timestamp"], y=df["macd_signal"], name="Signal", line=dict(width=1, dash="dot")),
                          row=2, col=1)
        if "macd_hist" in df.columns:
            fig.add_trace(go.Bar(x=df["timestamp"], y=df["macd_hist"], name="MACD Hist"), row=2, col=1)

        # Row 3: Volume (mini)
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

# ---------- routes ----------
@app.get("/")
def home():
    return ('<meta http-equiv="refresh" content="0; url=/analyze?symbol=ETH&tf=12h">', 302)

@app.get("/analyze")
def analyze():
    symbol_raw = (request.args.get("symbol") or "ETH").strip()
    tf        = (request.args.get("tf") or "12h")

    # NEW: friendly display label for contract addresses
    meta = token_meta_for(symbol_raw)
    token_label  = meta.get("label") or _disp_symbol(symbol_raw)
    token_symbol = meta.get("symbol") or _disp_symbol(symbol_raw)
    token_name   = meta.get("name")
    contract     = symbol_raw if _is_address_like(symbol_raw) else None

    df_full = hydrate_symbol(symbol_raw, force=False)
    if df_full.empty:
        return Response("No data.", 500)

    df_view = slice_df(df_full, tf)
    perf, invest = compute_rollups(df_full)

    # tiles (UNCHANGED KEYS / LAYOUT)
    tiles: Dict[str, str] = {
        "RSI":   pio.to_html(fig_line(df_view, "rsi", "RSI"), include_plotlyjs=False, full_html=False),
        "PRICE": pio.to_html(fig_price(df_view if not df_view.empty else df_full, token_label), include_plotlyjs=False, full_html=False),
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

    # TLDR line — use the friendly label
    ch_view, _ = compute_rollups(df_view if not df_view.empty else df_full)
    def pct(k):
        v = ch_view.get(k)
        return ("n/a" if v is None else f"{v:+.2f}%")
    tldr_line = (
        f"{token_label}: 1h {pct('1h')}, 4h {pct('4h')}, 12h {pct('12h')}, 24h {pct('24h')}. "
        f"Momentum/Trend vary by timeframe; bands and volume provide context."
    )

    updated = (_latest_ts(df_full) or utcnow()).strftime("UTC %Y-%m-%d %H:%M")

    return render_template(
        "control_panel.html",
        symbol_raw=symbol_raw,                # raw value for API calls
        symbol=_disp_symbol(symbol_raw),      # legacy display if needed
        token_label=token_label,              # NEW
        token_symbol=token_symbol,            # NEW
        token_name=token_name,                # NEW
        contract=contract,                    # NEW
        tf=tf, updated=updated,
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
        "PRICE": ("close", fig_price(dfv if not dfv.empty else df, token_meta_for(symbol_raw).get("label") or _disp_symbol(symbol_raw))),
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
    talk = talk_indicator(key, dfv if not dfv.empty else df)

    return jsonify({"fig": fig.to_plotly_json(), "talk": talk, "tf": tf, "key": key})

@app.get("/api/refresh/<symbol>")
def api_refresh(symbol: str):
    df = hydrate_symbol(symbol, force=True)
    return jsonify({"ok": (not df.empty), "rows": 0 if df is None else len(df)})

# ---------- Ask-Luna ----------
INTRO_LINES = [
    "Here’s what I’m seeing:",
    "Quick read from Luna:",
    "Let’s unpack this:",
    "Alright — chart check:",
]

def intro_line() -> str:
    return random.choice(INTRO_LINES)

def classify_question(q: str) -> str:
    q = (q or "").lower()
    if any(k in q for k in ["why", "reason", "cause", "factor", "driver"]):
        return "reasons"
    if any(k in q for k in ["up", "increase", "rise", "bullish", "rally", "pump", "green"]):
        return "upside"
    if any(k in q for k in ["down", "decrease", "fall", "bearish", "dump", "red"]):
        return "downside"
    if any(k in q for k in ["trend", "momentum", "direction"]):
        return "trend"
    if any(k in q for k in ["volatility", "range", "volume", "liquidity", "squeeze"]):
        return "volatility"
    if any(k in q for k in ["buy", "dip", "entry", "accumulate", "accumulation"]):
        return "buy"
    return "general"

def _fmt_pct(v: Optional[float]) -> str:
    return "n/a" if v is None else f"{v:+.2f}%"

def _safe_tail(series: pd.Series, n: int) -> pd.Series:
    try:
        return series.dropna().tail(n)
    except Exception:
        return pd.Series(dtype="float64")

def _build_snapshot(df: pd.DataFrame, tf: str) -> Dict[str, Any]:
    view = slice_df(df, tf)
    if view.empty:
        view = df.tail(200)

    ch, _ = compute_rollups(view)
    last = view.iloc[-1]

    close = to_float(last.get("close"))
    rsi   = to_float(last.get("rsi"))
    macdl = to_float(last.get("macd_line"))
    macds = to_float(last.get("macd_signal"))
    hist  = to_float(last.get("macd_hist"))
    adx   = to_float(last.get("adx14"))
    bbw   = to_float(last.get("bb_width"))
    atr   = to_float(last.get("atr14"))
    altm  = to_float(last.get("alt_momentum"))

    obv_slope = 0.0
    if "obv" in view.columns:
        obv = _safe_tail(view["obv"], 5)
        if len(obv) >= 2:
            obv_slope = float(obv.iloc[-1] - obv.iloc[0])

    vol_now = to_float(last.get("volume"))
    vol_ma20 = None
    if "volume" in view.columns and len(view["volume"].dropna()) >= 20:
        vol_ma20 = float(view["volume"].rolling(20).mean().iloc[-1])
    vol_ratio = None
    if vol_now is not None and vol_ma20 and vol_ma20 > 0:
        vol_ratio = vol_now / vol_ma20

    look = min(len(view), 60)
    recent_high = None
    recent_low  = None
    try:
        recent_high = float(view["high"].tail(look).max())
        recent_low  = float(view["low"].tail(look).min())
    except Exception:
        pass

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
    if adx is not None:
        conf = max(20.0, min(70.0, (adx * 1.5)))
    conf = round(conf, 0)

    bb_is_tight = (bbw is not None and bbw <= 1.0)
    obv_state = "accumulation" if obv_slope > 0 else "distribution" if obv_slope < 0 else "neutral"

    return {
        "view": view,
        "ch": ch,
        "close": close,
        "price_str": money(close),
        "rsi": rsi,
        "macdl": macdl,
        "macds": macds,
        "hist": hist,
        "adx": adx,
        "bbw": bbw,
        "atr": atr,
        "alt_momentum": altm,
        "bb_is_tight": bb_is_tight,
        "obv_state": obv_state,
        "vol_now": vol_now,
        "vol_ma20": vol_ma20,
        "vol_ratio": vol_ratio,
        "recent_high": recent_high,
        "recent_low": recent_low,
        "bias": bias,
        "confidence": conf
    }

def _bias_answer(symbol_for_display: str, df: pd.DataFrame, snap: Dict[str, Any], tf: str) -> str:
    ch = snap["ch"]
    rsi, hist, adx = snap["rsi"], snap["hist"], snap["adx"]
    bbw, atr = snap["bbw"], snap["atr"]
    bias, conf = snap["bias"], snap["confidence"]
    obv_state = snap["obv_state"]
    vol_ratio = snap["vol_ratio"]
    rh, rl = snap["recent_high"], snap["recent_low"]
    price = snap["price_str"]

    perf_txt = f"1h {_fmt_pct(ch.get('1h'))}, 4h {_fmt_pct(ch.get('4h'))}, 12h {_fmt_pct(ch.get('12h'))}, 24h {_fmt_pct(ch.get('24h'))}"

    mom_line = []
    if rsi is not None: mom_line.append(f"RSI {rsi:.1f}")
    if hist is not None: mom_line.append(f"MACD hist {hist:+.2f}")
    if adx is not None: mom_line.append(f"ADX {adx:.1f}")
    if bbw is not None: mom_line.append(f"bands width {bbw:.2f}%")
    if vol_ratio is not None: mom_line.append(f"vol~{vol_ratio:.1f}× 20‑bar avg")

    levels = []
    if rh is not None: levels.append(f"↑ {money(rh)}")
    if rl is not None: levels.append(f"↓ {money(rl)}")
    levels_txt = (", ".join(levels)) if levels else "key recent highs/lows"

    bias_txt = "bullish" if bias == "bullish" else "bearish" if bias == "bearish" else "range‑bound"

    return (
        f"{intro_line()} {symbol_for_display} around {price}. {perf_txt}. "
        f"Momentum/Trend: {', '.join(mom_line)}. OBV suggests {obv_state}. "
        f"Overall bias: {bias_txt}, confidence ~{int(conf)}%. "
        f"Key levels: {levels_txt}. "
        f"This is educational analysis; conditions can change quickly."
    )

def _factors_answer(symbol_for_display: str, df: pd.DataFrame, snap: Dict[str, Any], tf: str) -> str:
    rsi, hist, adx = snap["rsi"], snap["hist"], snap["adx"]
    bbw, atr = snap["bbw"], snap["atr"]
    vol_ratio = snap["vol_ratio"]
    obv_state = snap["obv_state"]
    rh, rl = snap["recent_high"], snap["recent_low"]

    ups = []
    if rh is not None: ups.append(f"Break & hold above ~{money(rh)} (recent high / resistance).")
    if rsi is not None: ups.append("RSI sustains > 60 (bullish momentum regime).")
    if hist is not None: ups.append("MACD histogram stays positive and expands (acceleration).")
    if vol_ratio is not None: ups.append("Volume > 1.2× 20‑bar average on green candles (follow‑through).")
    ups.append(f"OBV climbs (accumulation) — currently {obv_state}.")
    if adx is not None: ups.append("ADX rises toward/above 25 (trend strength confirming).")

    dns = []
    if rl is not None: dns.append(f"Loss of ~{money(rl)} support (recent swing low).")
    if rsi is not None: dns.append("RSI drops < 45 then < 40 (momentum rolls over).")
    if hist is not None: dns.append("MACD histogram flips negative / bearish cross.")
    dns.append("OBV keeps falling (distribution) or bounces on low volume.")
    if bbw is not None: dns.append("Volatility expansion from tight bands breaks lower.")
    if atr is not None and snap["close"]:
        try:
            atr_pct = (atr / float(snap["close"])) * 100.0
            dns.append(f"ATR expands (±{atr_pct:.2f}% daily swings), increasing breakdown risk.")
        except Exception:
            pass

    txt = [f"{intro_line()} {symbol_for_display} — factors that move price on {tf}:"]
    txt.append("Upside (what could lift):")
    for s in ups: txt.append(f"- {s}")
    txt.append("Downside (what could hurt):")
    for s in dns: txt.append(f"- {s}")
    return "\n".join(txt)

def _vol_answer(symbol_for_display: str, snap: Dict[str, Any], tf: str) -> str:
    bbw, atr, close = snap["bbw"], snap["atr"], snap["close"]
    state = "tight (squeeze)" if (bbw is not None and bbw <= 1.0) else "normal" if (bbw is not None and bbw < 5.0) else "expanding"
    atr_pct = None
    if atr is not None and close:
        try:
            atr_pct = (atr/close)*100.0
        except Exception:
            atr_pct = None
    bits = [f"Bollinger width {f1(bbw)}% → {state}"]
    if atr_pct is not None:
        bits.append(f"ATR ≈ {atr_pct:.2f}% of price (typical daily range).")
    return f"{intro_line()} {symbol_for_display} volatility on {tf}: " + "; ".join(bits)

def _buy_zone_line(view: pd.DataFrame) -> str:
    if view is None or view.empty: return ""
    closes = view["close"].dropna().tail(100)
    if closes.empty: return ""
    mean, std = closes.mean(), closes.std()
    lower = mean - 1.5*std
    upper = mean - 0.5*std
    return f"If price revisits between {money(lower)}–{money(upper)}, that’s been a local 'value area' in the last 100 bars (not advice)."

def luna_answer(symbol_for_display: str, df: pd.DataFrame, tf: str, question: str = "") -> str:
    try:
        snap = _build_snapshot(df, tf)
    except Exception as e:
        LOG.warning("Snapshot build failed: %s", e)
        return f"{symbol_for_display}: I don’t have enough fresh data yet."

    intent = classify_question(question)

    try:
        if intent == "upside":
            full = _factors_answer(symbol_for_display, df, snap, tf)
            parts = full.split("Downside (what could hurt):")
            return parts[0].strip()
        elif intent == "downside":
            full = _factors_answer(symbol_for_display, df, snap, tf)
            if "Downside (what could hurt):" in full:
                return "Downside (what could hurt):\n" + full.split("Downside (what could hurt):", 1)[1].strip()
            return full
        elif intent == "reasons":
            return _factors_answer(symbol_for_display, df, snap, tf)
        elif intent == "buy":
            base = _factors_answer(symbol_for_display, df, snap, tf)
            bz = _buy_zone_line(snap.get("view"))
            return (base + ("\n" + bz if bz else "")).strip()
        elif intent == "trend" or intent == "general":
            return _bias_answer(symbol_for_display, df, snap, tf)
        elif intent == "volatility":
            return _vol_answer(symbol_for_display, snap, tf)
        else:
            return _bias_answer(symbol_for_display, df, snap, tf)
    except Exception as e:
        LOG.warning("Luna agent fallback: %s", e)
        view = snap.get("view")
        if view is None or view.empty:
            return f"{symbol_for_display}: I don’t have enough fresh data yet."
        ch = snap["ch"]
        last  = view.iloc[-1]
        price = money(last.get("close"))
        rsi   = to_float(last.get("rsi"))
        macdl = to_float(last.get("macd_line"))
        macds = to_float(last.get("macd_signal"))
        hist  = to_float(last.get("macd_hist"))
        adx   = to_float(last.get("adx14"))
        bbw   = to_float(last.get("bb_width"))
        ser_obv = view["obv"] if "obv" in view.columns else None

        perf_bits = [f"{k} {v:+.2f}%" for k,v in ch.items() if v is not None and k in ("1h","4h","12h","24h")]
        perf_txt  = ", ".join(perf_bits) if perf_bits else "mostly flat"

        side = "bulls" if (macdl is not None and macds is not None and macdl > macds) \
               else "bears" if (macdl is not None and macds is not None and macdl < macds) else "neither side"

        obv_note = "accumulation" if (ser_obv is not None and len(ser_obv.dropna())>5 and (ser_obv.iloc[-1]-ser_obv.iloc[-5])>0) \
                   else "distribution" if (ser_obv is not None and len(ser_obv.dropna())>5 and (ser_obv.iloc[-1]-ser_obv.iloc[-5])<0) \
                   else "neutral flow"

        plan  = "If price breaks and closes beyond the recent band with rising volume, I favor follow‑through; "
        plan += "if the move fizzles and OBV diverges, I fade the breakout."

        return (
            f"{intro_line()} {symbol_for_display} around {price}. Over this window: {perf_txt}. "
            f"RSI {f1(rsi)}; MACD {('>' if (macdl and macds and macdl>macds) else '<' if (macdl and macds and macdl<macds) else '=')} signal "
            f"(hist {f1(hist)}). ADX {f1(adx)} (trend). Bands width {f1(bbw)}%. OBV shows {obv_note}. "
            f"Short answer: {('bulls have a small edge' if side=='bulls' else 'bears have the edge' if side=='bears' else 'range‑bound')}. "
            f"{plan}"
        )

@app.post("/api/luna")
def api_luna():
    data    = request.get_json(silent=True) or {}
    symbol  = (data.get("symbol") or "ETH").strip()
    tf      = data.get("tf") or "12h"
    text    = (data.get("text") or data.get("question") or "").strip()

    # Friendly label in replies (doesn't affect data retrieval)
    label = token_meta_for(symbol).get("label") or _disp_symbol(symbol)

    df = load_cached_frame(symbol)
    if df.empty:
        df = hydrate_symbol(symbol, force=False)

    reply = luna_answer(label, df, tf, text) if not df.empty else f"{label}: I don’t have enough fresh data yet."
    return jsonify({"symbol": label, "reply": reply})

# --- helpers: enumerate cached symbols + map queries -------------------------
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

# --- API: suggestions --------------------------------------------------------
@app.get("/api/suggest")
def api_suggest():
    q = (request.args.get("q") or "").strip().upper()
    syms = _list_cached_symbols()
    if q:
        syms = [s for s in syms if q in s]
    return jsonify({"symbols": syms[:20]})

# --- API: resolve query -> symbol -------------------------------------------
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

@app.get("/debug/tokenmeta")
def debug_tokenmeta():
    q = (request.args.get("q") or "").strip()
    return jsonify(token_meta_for(q))

# ---------- run ----------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
