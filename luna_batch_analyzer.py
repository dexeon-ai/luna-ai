# ============================================================
# luna_analyzer.py — Overnight batch analysis (OpenAI + fallback)
# Writes analysis to luna_cache/data/analysis/<coin>.json
# ============================================================

import os, json, time, math, argparse, pathlib, sys, random
from datetime import datetime, timezone, timedelta
import pandas as pd
from dotenv import load_dotenv

# find .env in project root or alongside this file
env_path = pathlib.Path(__file__).parent / ".env"
if not env_path.exists():
    env_path = pathlib.Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

ROOT = pathlib.Path(__file__).parent.resolve()
DATA_DIR = ROOT / "luna_cache" / "data"
COINS_DIR = DATA_DIR / "coins"
ANALYSIS_DIR = DATA_DIR / "analysis"
STATE_DIR = DATA_DIR / "state"
LOGS_DIR = ROOT / "logs"
ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
STATE_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

STATE_FILE = STATE_DIR / "analyze_progress.json"

# ---------- tiny utils ----------
def utcnow(): return datetime.now(timezone.utc)
def now_iso(): return utcnow().replace(microsecond=0).isoformat()

def to_float(x):
    try:
        if pd.isna(x): return None
        return float(x)
    except Exception:
        return None

def last(df, col):
    return to_float(df[col].iloc[-1]) if col in df.columns and len(df) else None

def pct(a, b):
    if a is None or b in (None, 0): return None
    try:
        return round((a / b - 1) * 100, 2)
    except Exception:
        return None

def safe_print(*args, **kwargs):
    try:
        print(*args, **kwargs, flush=True)
    except Exception:
        try: sys.stdout.buffer.write((" ".join(str(a) for a in args) + "\n").encode("utf-8"))
        except Exception: pass

# ---------- CSV loader ----------
def load_csv(coin_id: str) -> pd.DataFrame:
    p = COINS_DIR / f"{coin_id}.csv"
    if not p.exists(): return pd.DataFrame()
    df = pd.read_csv(p)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        df = df.dropna(subset=["timestamp"]).sort_values("timestamp")
    # normalize numbers
    for c in ["price","open","high","low","close","volume","volume_24h","market_cap","fdv","liquidity",
              "rsi","macd_line","macd_signal","macd_hist",
              "bb_lower","bb_middle","bb_upper","bb_width",
              "volume_trend","sentiment","fear_greed","whale_txn_count","adx14","obv","stoch_k","stoch_d"]:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")
    if "close" not in df.columns and "price" in df.columns:
        df["close"] = df["price"]
    if "volume" not in df.columns and "volume_24h" in df.columns:
        df["volume"] = df["volume_24h"]
    return df

# ---------- ROI rollups ----------
def value_at_or_before(df: pd.DataFrame, hours_back: int):
    if df.empty or "timestamp" not in df.columns or "close" not in df.columns:
        return None
    target = utcnow() - timedelta(hours=hours_back)
    older = df[df["timestamp"] <= target]
    if older.empty:
        try:
            return to_float(df["close"].iloc[-hours_back])
        except Exception:
            return None
    return to_float(older["close"].iloc[-1])

def compute_rollups(df):
    if df.empty or "close" not in df.columns: return {}, {}
    ch = {}
    latest = last(df, "close")
    for label, hrs in [("1h",1),("4h",4),("8h",8),("12h",12),("24h",24),("7d",24*7),("30d",24*30),("1y",24*365)]:
        base = value_at_or_before(df, hrs)
        ch[label] = pct(latest, base)
    ch["all"] = pct(latest, to_float(df["close"].iloc[0]) if len(df)>0 else None)
    inv = {k: (round(1000*(1+(v or 0)/100),2) if v is not None else None) for k,v in ch.items()}
    return ch, inv

# ---------- rule-based fallback ----------
def fallback_tool_texts(df):
    v = dict(
        RSI=last(df,"rsi"),
        MACD=last(df,"macd_line"),
        MS=last(df,"macd_signal"),
        MH=last(df,"macd_hist"),
        BBU=last(df,"bb_upper"),
        BBL=last(df,"bb_lower"),
        VOLT=last(df,"volume_trend"),
        LIQ=last(df,"liquidity"),
        MCAP=last(df,"market_cap"),
        SENT=last(df,"sentiment"),
        FG=last(df,"fear_greed"),
        WHALE=last(df,"whale_txn_count"),
    )
    blurbs, details = {}, {}

    rsi=v["RSI"]
    if rsi is None:
        blurbs["RSI"]="RSI unavailable on this window."; details["RSI"]="RSI not available."
    else:
        tone="overbought" if rsi>70 else "oversold" if rsi<30 else "balanced"
        blurbs["RSI"]=f"RSI {round(rsi,1)} — {tone}."
        details["RSI"]=">70 = overbought, <30 = oversold."

    ml,ms,mh=v["MACD"],v["MS"],v["MH"]
    if ml is None or ms is None:
        blurbs["MACD"]="MACD n/a."; details["MACD"]="Line/signal not present."
    else:
        cross="bullish" if ml>ms else "bearish" if ml<ms else "flat"
        blurbs["MACD"]=f"MACD {cross}; hist {round(mh or 0,2)}."
        details["MACD"]=f"Line {round(ml,2)} vs signal {round(ms,2)}."

    if v["BBU"] is not None and v["BBL"] is not None:
        blurbs["BANDS"]="Bands normal."; details["BANDS"]="Compression often precedes breakouts."
    else:
        blurbs["BANDS"]="Bollinger n/a."; details["BANDS"]="Insufficient data."

    vt=v["VOLT"]
    if vt is None:
        blurbs["VOL"]="Volume trend n/a."; details["VOL"]="Missing volume trend."
    else:
        label="rising" if vt>0 else "fading" if vt<0 else "flat"
        blurbs["VOL"]=f"Volume {label}."; details["VOL"]="Rising participation aids breakouts."

    liq=v["LIQ"]
    if liq is None:
        blurbs["LIQUIDITY"]="Liquidity n/a."; details["LIQUIDITY"]="No liquidity feed yet."
    else:
        blurbs["LIQUIDITY"]="Liquidity stable."; details["LIQUIDITY"]="Mind order size vs depth."

    m=v["MCAP"]
    if m in (None,0):
        blurbs["MCAP"]="Market cap n/a."; details["MCAP"]="Unavailable."
    else:
        blurbs["MCAP"]="Market cap steady."; details["MCAP"]="No major supply change."

    s=v["SENT"]
    if s is None:
        blurbs["SENTIMENT"]="Sentiment n/a."; details["SENTIMENT"]="24h proxy not present."
    else:
        tone="positive" if s>0 else "negative" if s<0 else "flat"
        blurbs["SENTIMENT"]=f"Sentiment {tone}."; details["SENTIMENT"]=f"24h change {round(s,2)}%."

    fg=v["FG"]
    if fg is None:
        blurbs["FEAR_GREED"]="Composite n/a."; details["FEAR_GREED"]="Not present."
    else:
        zone = "greed" if fg>60 else "fear" if fg<40 else "neutral"
        blurbs["FEAR_GREED"]=f"Composite {zone}."; details["FEAR_GREED"]=f"Score {round(fg)}."

    wh=v["WHALE"]
    if wh in (None,0):
        blurbs["WHALE_ACTIVITY"]="Whale tracking soon."; details["WHALE_ACTIVITY"]="Planned module."
    else:
        blurbs["WHALE_ACTIVITY"]=f"Whales active ({int(wh)} tx)."; details["WHALE_ACTIVITY"]="Large‑tx spikes = volatility risk."

    overview={"summary":"Mixed momentum; confirm moves with volume and bands.",
              "bullets":[b for b in [
                (f"RSI {round(rsi,1)}" if rsi is not None else None),
                ("MACD > signal" if (ml and ms and ml>ms) else "MACD < signal" if (ml and ms and ml<ms) else None),
                ("Volume rising" if (vt and vt>0) else "Volume fading" if (vt and vt<0) else None)
              ] if b], "regime":"sideways"}
    return blurbs, details, overview

# ---------- OpenAI client (optional) ----------
def openai_client():
    key = os.getenv("OPENAI_API_KEY")
    if not key: return None
    try:
        from openai import OpenAI
        return OpenAI(api_key=key)
    except Exception:
        return None

def ai_analysis(df, coin_id, symbol, model="gpt-4o-mini"):
    client = openai_client()
    if client is None or df.empty:
        return fallback_tool_texts(df)

    ch,_ = compute_rollups(df)
    snap = {
        "symbol": symbol,
        "latest": {
            "price": last(df,"close"),
            "rsi": last(df,"rsi"),
            "macd_line": last(df,"macd_line"),
            "macd_signal": last(df,"macd_signal"),
            "macd_hist": last(df,"macd_hist"),
            "bb_upper": last(df,"bb_upper"),
            "bb_lower": last(df,"bb_lower"),
            "bb_width": last(df,"bb_width"),
            "volume_trend": last(df,"volume_trend"),
            "market_cap": last(df,"market_cap"),
            "liquidity": last(df,"liquidity"),
            "sentiment_24h": last(df,"sentiment"),
            "fear_greed": last(df,"fear_greed"),
            "obv": last(df,"obv"),
            "adx14": last(df,"adx14"),
            "stoch_k": last(df,"stoch_k"),
            "stoch_d": last(df,"stoch_d"),
        },
        "roi": ch
    }

    sys = ("You are Luna, a crypto technical analyst. Precise, non‑hype, no advice. "
           "Use only the provided metrics.")
    user = (
        "Return STRICT JSON:\n"
        "{"
        ' "overview": {"summary": str, "bullets": [str], "regime": "bullish|bearish|sideways"},'
        ' "tool_blurbs": {RSI:str, MACD:str, BANDS:str, VOL:str, MCAP:str, LIQUIDITY:str, SENTIMENT:str, FEAR_GREED:str, WHALE_ACTIVITY:str},'
        ' "tool_details": {RSI:str, MACD:str, BANDS:str, VOL:str, MCAP:str, LIQUIDITY:str, SENTIMENT:str, FEAR_GREED:str, WHALE_ACTIVITY:str}'
        "}\n"
        "Tile blurbs must be 8–12 words. Details: 2–4 compact sentences.\n\n"
        f"Metrics:\n```json\n{json.dumps(snap, default=lambda x: None)}\n```"
    )

    backoff = 2.0
    for attempt in range(5):
        try:
            resp = client.chat.completions.create(
                model=model, temperature=0.2,
                response_format={"type":"json_object"},
                messages=[{"role":"system","content":sys},{"role":"user","content":user}],
            )
            data = json.loads(resp.choices[0].message.content)
            blurbs = {k:str(v) for k,v in (data.get("tool_blurbs") or {}).items()}
            details= {k:str(v) for k,v in (data.get("tool_details") or {}).items()}
            ov = data.get("overview") or {}
            overview = {
                "summary": str(ov.get("summary","")).strip() or "Analysis generated.",
                "bullets": [str(x) for x in (ov.get("bullets") or [])][:6],
                "regime": str(ov.get("regime","sideways"))
            }
            return blurbs, details, overview
        except Exception as e:
            safe_print(f"[Luna] OpenAI error ({attempt+1}/5): {e}")
            time.sleep(backoff); backoff = min(backoff*1.8, 30)
    return fallback_tool_texts(df)

# ---------- short narrative ----------
def compose_short_paragraph(symbol: str, df: pd.DataFrame, ch: dict) -> str:
    if df.empty: return f"{symbol}: not enough data."
    def val(col):
        s = df.get(col)
        s = s.dropna() if isinstance(s, pd.Series) else pd.Series(dtype=float)
        return float(s.iloc[-1]) if not s.empty else None
    rsi = val("rsi"); adx = val("adx14")
    ml, ms, mh = val("macd_line"), val("macd_signal"), val("macd_hist")
    bbw = val("bb_width")

    last_ts = pd.to_datetime(df["timestamp"].iloc[-1])
    age_min = int((utcnow() - last_ts).total_seconds()//60)
    age = "live now" if age_min<1 else f"~{age_min} min old"

    keys = ["1h","4h","12h","24h","7d","30d"]
    score = ", ".join([f"{k} {ch.get(k,0):+,.2f}%" for k in keys if k in ch])

    bits=[]
    if rsi is not None: bits.append(f"RSI {rsi:.1f}")
    if ml is not None and ms is not None:
        rel = ">" if ml>ms else "<" if ml<ms else "="
        bits.append(f"MACD {rel} signal ({(ml-ms):+.2f})")
    if adx is not None:
        label = "strong" if adx>=40 else "firm" if adx>=25 else "weak"
        bits.append(f"ADX {adx:.1f} {label}")
    if bbw is not None:
        bits.append("bands tight" if bbw <= (pd.Series(df["bb_width"]).dropna().quantile(0.2) if "bb_width" in df else bbw) else "bands normal")

    return f"Data current (UTC, {age}). {symbol} — {score}. " + "; ".join(bits) + "."

# ---------- per‑coin writer ----------
def build_payload(df, coin_id, symbol):
    price_change, inv = compute_rollups(df)
    blurbs, details, overview = ai_analysis(df, coin_id, symbol)
    luna_paragraph = compose_short_paragraph(symbol, df, price_change)
    payload = {
        "coin_id": coin_id,
        "symbol": symbol,
        "name": coin_id.capitalize(),
        "last_updated": now_iso(),
        "overview": overview,
        "metrics": {
            "price_usd": last(df,"close"),
            "market_cap_usd": last(df,"market_cap"),
            "volume_24h_usd": last(df,"volume"),
            "liquidity_usd": last(df,"liquidity"),
            "fdv_usd": last(df,"fdv"),
            "price_change": {k:(None if v is None else float(v)) for k,v in price_change.items()},
        },
        "investment_model": {"hypothetical_1000_usd": {k:(None if v is None else float(v)) for k,v in inv.items()}},
        "tool_blurbs": blurbs,
        "tool_details": details,
        "luna_paragraph": luna_paragraph
    }
    return payload

def analyze_coin(coin_id, force=False, stale_hours=None, use_openai=True):
    coin_id = coin_id.lower().strip()
    csv_path = COINS_DIR / f"{coin_id}.csv"
    if not csv_path.exists():
        safe_print(f"[Luna] ❌ No CSV found for {coin_id}")
        return False

    out = ANALYSIS_DIR / f"{coin_id}.json"
    if out.exists() and not force and stale_hours is not None:
        try:
            j = json.loads(out.read_text(encoding="utf-8"))
            ts = j.get("last_updated")
            if ts:
                age = utcnow() - datetime.fromisoformat(ts)
                if age.total_seconds() < stale_hours*3600:
                    safe_print(f"[Luna] ⏭  Skip fresh: {coin_id} ({age.total_seconds()/3600:.1f}h old)")
                    return True
        except Exception:
            pass

    df = load_csv(coin_id)
    if df.empty:
        safe_print(f"[Luna] ❌ Empty CSV for {coin_id}")
        return False

    if not use_openai:
        def no_ai(df, cid, sym): return fallback_tool_texts(df)
        globals()["ai_analysis"] = lambda *a, **k: no_ai(*a, **k)

    symbol = coin_id[:4].upper()
    payload = build_payload(df, coin_id, symbol)
    out.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=lambda o: None), encoding="utf-8")
    safe_print(f"[Luna] ✅ Saved analysis for {coin_id} → {out}")
    return True

# ---------- batch driver ----------
def save_state(idx):
    try:
        STATE_FILE.write_text(json.dumps({"index":idx, "at":now_iso()}, indent=2), encoding="utf-8")
    except Exception:
        pass

def load_state():
    try:
        j = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        return int(j.get("index", 0))
    except Exception:
        return 0

def coin_universe():
    coins = sorted(p.stem for p in COINS_DIR.glob("*.csv"))
    return coins

def run_batch(args):
    coins = coin_universe()
    if args.shuffle:
        random.shuffle(coins)

    start = args.start
    if args.resume:
        start = max(start, load_state())

    if args.max > 0:
        coins = coins[start:start+args.max]
    else:
        coins = coins[start:]

    total = len(coins)
    COST_PER_COIN = 0.00075
    LOG_INTERVAL = 900
    last_log = time.time()

    safe_print(f"[Luna] 🧠 Starting analysis for {total} coins (start={start})...")
    rpm = max(1, args.rpm)
    per_call_sleep = 60.0 / rpm

    ok = fail = 0
    last_tick = time.perf_counter()
    t0 = time.time()

    for i, cid in enumerate(coins, start=start):
        now = time.perf_counter()
        elapsed = now - last_tick
        if elapsed < per_call_sleep:
            time.sleep(per_call_sleep - elapsed)
        last_tick = time.perf_counter()

        try:
            done = analyze_coin(
                cid,
                force=args.force,
                stale_hours=args.stale if not args.force else None,
                use_openai=not args.no_openai
            )
            if done: ok += 1
            else: fail += 1
        except KeyboardInterrupt:
            safe_print("\n[Luna] ⛔ Interrupted by user.")
            break
        except Exception as e:
            safe_print(f"[Luna] ⚠️ Error on {cid}: {e}")
            fail += 1

        if args.resume:
            save_state(i+1)

        if time.time() - last_log >= LOG_INTERVAL:
            pct_done = (i+1) / total * 100
            elapsed_h = (time.time() - t0) / 3600
            est_total_h = (elapsed_h / pct_done * 100) if pct_done > 0 else 0
            remaining_h = max(0, est_total_h - elapsed_h)
            cost_so_far = (i+1) * COST_PER_COIN
            est_total_cost = total * COST_PER_COIN
            msg = (
                f"[Luna] ⏱ Progress: {i+1}/{total} coins ({pct_done:.2f}%) "
                f"| ok={ok} fail={fail}\n"
                f"[Luna] 💰 Est. cost so far: ${cost_so_far:.2f} "
                f"(of ≈${est_total_cost:.2f}) | est {remaining_h:.1f} h left\n"
            )
            safe_print(msg)
            log_path = LOGS_DIR / f"progress_{datetime.now().strftime('%Y-%m-%d')}.log"
            with open(log_path, "a", encoding="utf-8") as lf:
                lf.write(f"{datetime.now().isoformat()} {msg}\n")
            last_log = time.time()

    elapsed_total = (time.time() - t0) / 3600
    cost_total = ok * COST_PER_COIN
    safe_print(f"[Luna] ✅ Batch done. ok={ok} fail={fail} | time={elapsed_total:.1f} h | est cost=${cost_total:.2f}")

# ---------- CLI ----------
def parse_args():
    ap = argparse.ArgumentParser(description="Luna overnight analyzer")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--coin", help="single coin id (e.g., solana)")
    g.add_argument("--all", action="store_true", help="process all CSVs in coins dir (default)")
    ap.add_argument("--rpm", type=int, default=30, help="target analyses per minute (default 30)")
    ap.add_argument("--force", action="store_true", help="force re-analyze even if fresh")
    ap.add_argument("--stale", type=int, default=24*30, help="re-analyze if older than N hours (default 30 days)")
    ap.add_argument("--resume", action="store_true", help="resume from last saved index")
    ap.add_argument("--start", type=int, default=0, help="start index when not resuming")
    ap.add_argument("--max", type=int, default=0, help="limit number of coins")
    ap.add_argument("--shuffle", action="store_true", help="randomize order")
    ap.add_argument("--no-openai", action="store_true", help="disable OpenAI, use rule-based fallback only")
    return ap.parse_args()

if __name__ == "__main__":
    args = parse_args()
    if args.coin:
        analyze_coin(args.coin, force=args.force, stale_hours=args.stale, use_openai=not args.no_openai)
    else:
        run_batch(args)
