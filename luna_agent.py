# luna_agent.py
from __future__ import annotations
import re
from typing import Dict, List, Tuple
import numpy as np
import pandas as pd

# --- compatibility shim (used by server.luna_paragraph) ----------------------
def analyze_indicators(last_row: pd.Series, pct_changes: Dict[str, float]) -> Tuple[str, str]:
    """Tiny shim to keep server.luna_paragraph working if it still calls this."""
    rsi = _to_float(last_row.get("rsi"))
    macd = _to_float(last_row.get("macd_line"))
    sig  = _to_float(last_row.get("macd_signal"))
    adx  = _to_float(last_row.get("adx14"))
    bbw  = _to_float(last_row.get("bb_width"))
    obv  = _to_float(last_row.get("obv"))
    # crude bias
    if macd is not None and sig is not None:
        trend = "bullish" if macd > sig else "bearish" if macd < sig else "neutral"
    else:
        trend = "neutral"
    reasoning = f"RSI {f1(rsi)}; MACD {sym_compare(macd, sig)} signal; ADX {f1(adx)}; Bands width {f1(bbw)}%."
    return trend, reasoning

# --- public entry -------------------------------------------------------------
def answer_question(symbol: str, df: pd.DataFrame, tf: str, question: str) -> str:
    """
    Smart QA: returns a tailored paragraph.
    - If the question asks for factors/reasons/what could cause ↑/↓, produce drivers/risks with metric hooks.
    - Otherwise, produce the concise bias + context paragraph.
    """
    if df is None or df.empty:
        return f"{symbol}: I don’t have enough fresh data yet."

    view = _slice_for_tf(df, tf)
    if view.empty:
        view = df.tail(300).copy()

    # Ensure we have what we need
    view = view.copy()
    _ensure_local_emas(view)  # ema20/50/200 (local calc, fast)
    snap = _snapshot(view)    # structured metrics from the last bars

    q = (question or "").lower()
    wants_factors = bool(re.search(r"\b(factor|cause|driver|why|what.*(increase|decrease|go up|go down))\b", q))

    if wants_factors:
        return _factors_answer(symbol, view, snap, tf)

    # default concise bias paragraph (newer style)
    return _bias_answer(symbol, view, snap, tf)

# --- factors mode -------------------------------------------------------------
def _factors_answer(symbol: str, v: pd.DataFrame, s: Dict[str, float], tf: str) -> str:
    """
    Produces:
    - Upside catalysts (each tied to a metric and what to look for)
    - Downside risks (same)
    - What to watch next (level‑based & volatility cues)
    """
    price = money(s["price"])
    perf  = _perf_line(v)

    # Build drivers with metric hooks
    ups: List[str] = []
    dns: List[str] = []

    # RSI
    if s["rsi"] is not None:
        if s["rsi"] >= 55:
            ups.append(f"RSI {s['rsi']:.1f} and rising → momentum building (watch for >60 to confirm).")
        elif s["rsi"] <= 45:
            dns.append(f"RSI {s['rsi']:.1f} and falling → momentum fading (risk increases <40).")
        else:
            # neutral: give both sides guidance
            ups.append(f"RSI near {s['rsi']:.1f}; a push >55 favors upside continuation.")
            dns.append(f"RSI near {s['rsi']:.1f}; a drop <45 favors downside continuation.")

    # MACD
    if s["macd_diff"] is not None:
        if s["macd_diff"] > 0:
            ups.append(f"MACD above signal (hist {s['macd_hist']:+.2f}) → positive acceleration if histogram expands.")
        else:
            dns.append(f"MACD below signal (hist {s['macd_hist']:+.2f}) → downside bias if histogram keeps widening.")

    # ADX (trend strength)
    if s["adx"] is not None:
        if s["adx"] >= 25:
            # strong trend — side decided by direction of returns & MACD
            direction = "uptrend" if s["macd_diff"] and s["macd_diff"] > 0 else "downtrend"
            tag = "Upside" if direction == "uptrend" else "Downside"
            (ups if direction == "uptrend" else dns).append(
                f"ADX {s['adx']:.1f} → strong {direction}; {tag.lower()} continuation likely unless ADX rolls over."
            )
        else:
            ups.append(f"ADX {s['adx']:.1f} → weak trend; breakouts can flip quickly (confirm with volume).")
            dns.append(f"ADX {s['adx']:.1f} → weak trend; failed moves are common (avoid chasing without volume).")

    # OBV (flow)
    if s["obv_slope"] is not None:
        if s["obv_slope"] > 0:
            ups.append("OBV rising → accumulation; expect better follow‑through on green candles.")
        elif s["obv_slope"] < 0:
            dns.append("OBV falling → distribution; rallies likely to stall without clear volume expansion.")

    # Bollinger width & squeeze
    if s["bb_width"] is not None:
        if s["bb_width"] <= s["bb_width_p20"]:
            ups.append("Bands are tight (volatility compression) → expansion up is on the table if price closes above the upper band on volume.")
            dns.append("Bands are tight (volatility compression) → expansion down is on the table if price closes below the lower band on volume.")
        else:
            # expanding vol: respect direction
            if s["macd_diff"] and s["macd_diff"] > 0:
                ups.append("Volatility expanding with positive momentum → upside swings can extend.")
            else:
                dns.append("Volatility expanding with negative momentum → downside swings can extend.")

    # Structure vs EMAs
    if s["ema20"] and s["ema50"] and s["ema200"]:
        if s["price"] > s["ema20"] > s["ema50"]:
            ups.append(f"Price above EMA20/EMA50 → constructive posture (pullbacks to {money(s['ema20'])} likely get bought if volume supports).")
        if s["price"] < s["ema20"] < s["ema50"]:
            dns.append(f"Price below EMA20/EMA50 → supply overhead (rallies into {money(s['ema20'])}–{money(s['ema50'])} may fade).")
        # longer trend guardrail
        if s["price"] > s["ema200"]:
            ups.append(f"Above EMA200 ({money(s['ema200'])}) → longer‑term up‑bias intact unless lost.")
        else:
            dns.append(f"Below EMA200 ({money(s['ema200'])}) → longer‑term down‑bias unless reclaimed.")

    # Simple levels
    lvl_up  = s["recent_high"]
    lvl_dn  = s["recent_low"]
    watch: List[str] = []
    if lvl_up:
        watch.append(f"Upside break if candle closes above recent high ~{money(lvl_up)} with rising OBV.")
    if lvl_dn:
        watch.append(f"Downside risk if candle closes below recent low ~{money(lvl_dn)} with expanding ATR.")

    # Compose answer
    ups = _uniq(ups)[:6]
    dns = _uniq(dns)[:6]
    watch = _uniq(watch)[:3]

    return (
        f"{symbol} around {price}. {perf}\n\n"
        f"**Upside catalysts**\n" + ("\n".join([f"• {u}" for u in ups]) or "• None obvious; need a fresh trigger.") + "\n\n"
        f"**Downside risks**\n"  + ("\n".join([f"• {d}" for d in dns]) or "• None obvious; watch for sudden volume flips.") + "\n\n"
        f"**What to watch next**\n" + ("\n".join([f"• {w}" for w in watch]) or "• Await a clean breakout/breakdown with volume.") +
        "\n\nThis is educational analysis, not financial advice. Conditions can change quickly."
    )

# --- concise bias mode --------------------------------------------------------
def _bias_answer(symbol: str, v: pd.DataFrame, s: Dict[str, float], tf: str) -> str:
    price = money(s["price"])
    perf  = _perf_line(v)

    # lightweight scoring
    mom  = _score_momentum(s)      # -1..+1
    flow = _score_flow(s)          # -1..+1
    vol  = _score_volatility(s)    # -1..+1 (expansion risk)
    trend = _score_trend(s)        # -1..+1

    total = 0.35*mom + 0.25*trend + 0.2*flow + 0.2*vol
    bias = "bullish" if total > 0.15 else "bearish" if total < -0.15 else "neutral"
    conf = int(round(abs(total)*100))

    # simple directional probabilities
    up_p   = max(10, min(80, int(round(33 + 45*mom))))
    down_p = max(10, min(80, int(round(33 - 45*mom))))
    flat_p = max(10, 100 - up_p - down_p)

    flavor = _flavor_line(mom, trend, s)

    return (
        f"{symbol} around {price}. {flavor} {perf} "
        f"Overall bias: **{bias}**, confidence ~{conf}%. "
        f"Projection (near term): ↑ {up_p}%, → {flat_p}%, ↓ {down_p}%. "
        f"Bands {('tight' if s['bb_is_tight'] else 'normal')} — {('volatility expansion likely' if s['bb_is_tight'] else 'respect current ranges')}. "
        f"This is educational analysis; conditions can change quickly."
    )

# --- helpers -----------------------------------------------------------------
def _slice_for_tf(df: pd.DataFrame, tf: str) -> pd.DataFrame:
    tf = (tf or "").lower()
    hours = 12
    if tf.endswith("h"):
        try: hours = int(tf[:-1])
        except: hours = 12
    elif tf.endswith("d"):
        try: hours = int(tf[:-1])*24
        except: hours = 24
    elif tf.endswith("y"):
        try: hours = int(tf[:-1])*24*365
        except: hours = 24*365
    anchor = _latest_ts(df)
    if anchor is None:
        return df.tail(300).copy()
    cutoff = anchor - pd.Timedelta(hours=hours)
    return df[df["timestamp"] >= cutoff].copy()

def _ensure_local_emas(df: pd.DataFrame) -> None:
    def ema(s: pd.Series, span: int) -> pd.Series:
        return s.ewm(span=span, adjust=False).mean()
    if "ema20" not in df.columns:
        df["ema20"]  = ema(df["close"], 20)
    if "ema50" not in df.columns:
        df["ema50"]  = ema(df["close"], 50)
    if "ema200" not in df.columns:
        df["ema200"] = ema(df["close"], 200)

def _snapshot(v: pd.DataFrame) -> Dict[str, float]:
    last = v.iloc[-1]
    # width percentile for "tightness"
    bb = v["bb_width"].dropna()
    p20 = float(np.nanpercentile(bb, 20)) if len(bb) >= 20 else np.nan
    obv = v["obv"].dropna()
    obv_slope = float(obv.iloc[-1] - obv.iloc[-5]) if len(obv) >= 6 else np.nan
    recent_high = float(v["high"].tail(60).max()) if "high" in v.columns else np.nan
    recent_low  = float(v["low"].tail(60).min())  if "low" in v.columns else np.nan

    return {
        "price": _to_float(last.get("close")),
        "rsi": _to_float(last.get("rsi")),
        "macd_line": _to_float(last.get("macd_line")),
        "macd_signal": _to_float(last.get("macd_signal")),
        "macd_hist": _to_float(last.get("macd_hist")),
        "macd_diff": _safe_diff(last.get("macd_line"), last.get("macd_signal")),
        "adx": _to_float(last.get("adx14")),
        "bb_width": _to_float(last.get("bb_width")),
        "bb_width_p20": p20,
        "bb_is_tight": (last.get("bb_width") is not None) and (p20 == p20) and (_to_float(last.get("bb_width")) <= p20),
        "atr": _to_float(last.get("atr14")),
        "obv_slope": obv_slope if obv_slope == obv_slope else None,
        "ema20": _to_float(last.get("ema20")),
        "ema50": _to_float(last.get("ema50")),
        "ema200": _to_float(last.get("ema200")),
        "recent_high": recent_high if recent_high == recent_high else None,
        "recent_low": recent_low if recent_low == recent_low else None,
    }

def _perf_line(v: pd.DataFrame) -> str:
    # approximate performance windows
    def pct(nh: int) -> str:
        base = _value_at_or_before(v, nh)
        last = _to_float(v["close"].iloc[-1])
        if base is None or last is None or base == 0:
            return "n/a"
        return f"{(last/base-1)*100:+.2f}%"
    return f"Recent: 1h {pct(1)}, 4h {pct(4)}, 12h {pct(12)}, 24h {pct(24)}."

def _score_momentum(s) -> float:
    rsi = s["rsi"] or 50.0
    macd = s["macd_diff"] or 0.0
    rsi_term = (rsi - 50.0) / 25.0  # ~ -2..+2 compressed later
    macd_term = np.tanh(macd)
    return float(np.tanh(0.8*rsi_term + 0.7*macd_term))

def _score_trend(s) -> float:
    out = 0.0
    if s["ema20"] and s["ema50"]:
        out += 0.6 * np.tanh((s["ema20"] - s["ema50"]) / max(1e-9, s["ema50"]))
        out += 0.4 * np.tanh(((s["price"] or s["ema20"]) - s["ema20"]) / max(1e-9, s["ema20"]))
    if s["ema50"] and s["ema200"]:
        out += 0.5 * np.tanh((s["ema50"] - s["ema200"]) / max(1e-9, s["ema200"]))
    return float(np.clip(out, -1, 1))

def _score_flow(s) -> float:
    return float(np.tanh((s["obv_slope"] or 0.0) / 1e9))  # scale OBV slope

def _score_volatility(s) -> float:
    # penalize tight squeeze (uncertain, breakout risk); reward orderly expansion with positive momentum
    bbw = s["bb_width"] or 0.0
    tight = s["bb_is_tight"]
    base = -0.2 if tight else 0.1
    return float(np.tanh(base + 0.02*bbw * (1 if (s["macd_diff"] or 0) > 0 else -1)))

# --- tiny utils --------------------------------------------------------------
def _to_float(x):
    try:
        v = float(x)
        if np.isnan(v) or np.isinf(v): return None
        return v
    except Exception:
        return None

def f1(x) -> str:
    try:
        return f"{float(x):.1f}"
    except Exception:
        return "—"

def sym_compare(a, b) -> str:
    a = _to_float(a); b=_to_float(b)
    if a is None or b is None: return "="
    return ">" if a > b else "<" if a < b else "="

def _latest_ts(df: pd.DataFrame):
    s = pd.to_datetime(df["timestamp"], utc=True, errors="coerce").dropna()
    return None if s.empty else s.iloc[-1].to_pydatetime()

def _value_at_or_before(df: pd.DataFrame, hours: int):
    anchor = _latest_ts(df)
    if anchor is None: return None
    t = anchor - pd.Timedelta(hours=hours)
    older = df[df["timestamp"] <= t]
    if older.empty:
        try: return float(df["close"].iloc[0])
        except: return None
    try: return float(older["close"].iloc[-1])
    except: return None

def _safe_diff(a, b):
    a = _to_float(a); b=_to_float(b)
    if a is None or b is None: return None
    return a - b

def _uniq(items: List[str]) -> List[str]:
    seen = set(); out=[]
    for i in items:
        k = i.strip()
        if not k or k in seen: continue
        seen.add(k); out.append(k)
    return out

def money(x) -> str:
    try:
        return "$" + format(float(x), ",.2f")
    except Exception:
        return "—"

def _flavor_line(mom, trend, s) -> str:
    # short, human line
    if mom < -0.25 and trend < -0.25:
        return "Momentum is fading, trend pressure is down, structure favors continuation; liquidity is light."
    if mom > 0.25 and trend > 0.25:
        return "Momentum is improving, trend structure is supportive; dips are being bought."
    if s["bb_is_tight"]:
        return "Range is compressed; expect a volatility expansion soon."
    return "Conditions are mixed; respect levels and watch volume."
