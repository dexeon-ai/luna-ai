import math
import os
import json
from typing import Dict, Any
from providers_dexscreener import DexscreenerClient
from providers_coingecko import CoinGeckoClient
from risk_providers import fetch_deep_risk
from macro_providers import build_macro_summary
from community_providers import fetch_community
from utils import pct_str, usd, smart_price, chain_slug

# ---- Helpers ----
def safety_gauge_v3(pair: Dict[str, Any], verified: bool, whale_conc_pct: float) -> (int, list):
    liq = float(((pair.get("liquidity") or {}).get("usd")) or 0.0)
    fdv = float(pair.get("fdv") or 0.0)
    vol24 = float(((pair.get("volume") or {}).get("h24")) or 0.0)
    score, context = 5, []
    if fdv > 0 and liq > 0:
        ratio = fdv / liq
        if ratio > 1000: score += 2; context.append(f"Thin depth vs size (FDV {usd(fdv)} / Liq {usd(liq)}).")
        elif ratio > 200: score += 1; context.append("Moderate depth vs size.")
        else: score -= 1; context.append("Strong depth support for size.")
    if liq > 0 and vol24 > 0:
        churn = vol24 / liq
        if churn > 5: score += 1; context.append("High trading churn vs depth.")
        else: context.append("Normal trading activity vs depth.")
    if fdv > 500_000_000:
        score = max(1, score - 1); context.append("Large-cap adjustment applied.")
    if verified is True:
        score = max(0, score - 1); context.append("Verified contract.")
    elif verified is False:
        score = min(10, score + 1); context.append("Unverified contract.")
    if whale_conc_pct is None:
        context.append("Holder concentration unavailable.")
    return max(0, min(10, score)), context

def degeneracy_score(fdv, liq):
    try:
        fdv = float(fdv or 0); liq = float(liq or 0)
        if fdv == 0 or liq == 0: return 10
        ratio = fdv / liq
        if ratio > 5000: return 9
        if ratio > 1000: return 7
        if ratio > 200:  return 5
        if ratio > 50:   return 3
        return 1
    except Exception:
        return 5

def luna_mood(score):
    if score <= 2: return "calm seas"
    if score <= 4: return "steady breeze"
    if score <= 6: return "rolling waves"
    if score <= 8: return "stormy waters"
    return "tsunami risk"

def what_if_buyhold_fallback(price_now, pct_change, invest=1000):
    results = {}
    try:
        for horizon, pct in pct_change.items():
            if pct is None: continue
            val = invest * (1 + float(pct)/100)
            results[horizon] = round(val, 2)
    except Exception:
        pass
    return results

def what_if_lp(price_now, liq, vol24, invest=1000):
    try:
        liq = float(liq or 0); vol24 = float(vol24 or 0)
        if liq <= 0 or vol24 <= 0:
            return {"note": "Insufficient data"}
        fee_yield = (vol24/liq) * 0.0025 * invest
        il_risk = "low" if vol24/liq < 2 else "moderate"
        return {"est_daily_fees": round(fee_yield,2), "il_risk": il_risk}
    except Exception:
        return {"note":"n/a"}

def lp_slippage_analysis(price_usd, liq, vol24, invest_usd=1000):
    try:
        liq = float(liq or 0); vol24 = float(vol24 or 0)
        if liq <= 0 or price_usd is None:
            return {"note": "Insufficient data"}
        slippage_1k = (invest_usd / liq) * 100
        slippage_10k = (10 * invest_usd / liq) * 100
        apr = (vol24 / liq) * 0.0025 * 365 * 100
        return {
            "slippage_1k_pct": round(slippage_1k, 3),
            "slippage_10k_pct": round(slippage_10k, 3),
            "apr_est": round(apr, 2)
        }
    except Exception:
        return {"note": "n/a"}

# ---- Snapshot ----
def build_snapshot(chain_input: str, contract: str) -> Dict[str, Any]:
    ds = DexscreenerClient()
    cg = CoinGeckoClient()
    chain_id = chain_slug(chain_input)
    pair = ds.best_pair_snapshot(contract, wanted_chain=chain_id)
    if not pair:
        return {"ok": False, "error": "Token not found", "chain": chain_id, "contract": contract}

    price_usd = pair.get("priceUsd")
    price_change = pair.get("priceChange") or {}
    liq = (pair.get("liquidity") or {}).get("usd")
    fdv = pair.get("fdv")
    volume = (pair.get("volume") or {}).get("h24")
    base_symbol = (pair.get("baseToken") or {}).get("symbol")
    base_name   = (pair.get("baseToken") or {}).get("name")

    deep = fetch_deep_risk(chain_id, contract)
    verified = deep.get("verified_contract")

    gauge, risk_notes = safety_gauge_v3(pair, verified, None)
    deg_score = degeneracy_score(fdv, liq)
    mood = luna_mood(gauge)

    buyhold_results = what_if_buyhold_fallback(price_usd, price_change, invest=1000)
    lp_est = what_if_lp(price_usd, liq, volume, invest=1000)
    lp_slip = lp_slippage_analysis(price_usd, liq, volume, invest_usd=1000)

    macro = build_macro_summary()

    # Community signals
    cg_id = cg.resolve_id(base_symbol)
    community = fetch_community(cg_id=cg_id)

    # TL;DR
    tldr = (
        f"{base_symbol} on {chain_id}: FDV {usd(fdv)} | Price {smart_price(price_usd, fdv)} | "
        f"1h {pct_str(price_change.get('h1'))} | 24h {pct_str(price_change.get('h24'))} | "
        f"Liquidity {usd(liq)}, Vol {usd(volume)} | Safety {gauge}/10 | Mood: {mood}. "
    )
    if buyhold_results.get("h24"):
        tldr += f"If you invested $1000 24h ago → ${buyhold_results['h24']}. "
    if lp_est.get("est_daily_fees"):
        tldr += f"LP est daily fees ${lp_est['est_daily_fees']} (IL {lp_est['il_risk']}). "
    if community.get("twitter_followers"):
        tldr += f" Twitter followers: {community['twitter_followers']}. "
    if community.get("telegram_users"):
        tldr += f" Telegram members: {community['telegram_users']}. "
    if macro and macro.get("ok"):
        tldr += f" Market: {macro['macro']['summary']}"

    # ✅ Save mood JSON for OBS overlay
    try:
        os.makedirs("voice", exist_ok=True)
        mood_file = os.path.join("voice", f"{base_symbol}_{chain_id}_mood.json")
        with open(mood_file, "w") as f:
            json.dump({"summary": mood}, f)
    except Exception as e:
        print("[Mood Save] Error:", e)

    return {
        "ok": True,
        "chain": chain_id,
        "contract": contract,
        "token": {"symbol": base_symbol, "name": base_name},
        "market": {
            "price_usd": float(price_usd) if price_usd else None,
            "fdv_usd": float(fdv) if fdv else None,
            "liquidity_usd": float(liq) if liq else None,
            "volume_24h_usd": float(volume) if volume else None,
            "pct_change": price_change
        },
        "risk": {
            "safety_gauge": gauge,
            "context": risk_notes,
            "verified_contract": verified,
            "degeneracy_score": deg_score,
            "mood": mood
        },
        "what_if": {"buy_hold": buyhold_results, "lp": lp_est},
        "lp_analysis": lp_slip,
        "macro": macro,
        "community": community,
        "tldr": tldr
    }
