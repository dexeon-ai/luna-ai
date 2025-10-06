# qa_handler.py — handles both structured and natural-language questions
import re
from snapshot import build_snapshot

# -------------------------------------------------------------
# Compare two tokens (your original logic, unchanged)
# -------------------------------------------------------------
def compare_tokens(chain1, contract1, chain2, contract2):
    snap1 = build_snapshot(chain1, contract1)
    snap2 = build_snapshot(chain2, contract2)

    if not snap1.get("ok") or not snap2.get("ok"):
        return {"ok": False, "error": "One or both tokens not found."}

    t1 = snap1["token"]["symbol"]
    t2 = snap2["token"]["symbol"]

    summary = (
        f"Comparison {t1} vs {t2}:\n"
        f"- {t1}: FDV {snap1['market']['fdv_usd']}, "
        f"Price {snap1['market']['price_usd']}, "
        f"Liquidity {snap1['market']['liquidity_usd']}\n"
        f"- {t2}: FDV {snap2['market']['fdv_usd']}, "
        f"Price {snap2['market']['price_usd']}, "
        f"Liquidity {snap2['market']['liquidity_usd']}\n"
    )

    return {"ok": True, "summary": summary, "snapshots": [snap1, snap2]}


# -------------------------------------------------------------
# Natural-language handler for “analyze …” style questions
# -------------------------------------------------------------
def analyze_single(chain, contract):
    """Fetch a snapshot and return readable metrics."""
    snap = build_snapshot(chain, contract)
    if not snap.get("ok"):
        return {"ok": False, "error": snap.get("error", "Snapshot failed.")}

    token = snap.get("token", {}).get("symbol", "Unknown")
    mkt = snap.get("market", {})
    price = mkt.get("price_usd") or snap.get("price", "N/A")
    volume = mkt.get("volume_24h_usd") or snap.get("volume_24h", "N/A")
    fdv = mkt.get("fdv_usd", "N/A")
    liquidity = mkt.get("liquidity_usd", "N/A")
    change = mkt.get("change_24h", "N/A")

    # Rough risk tag
    risk = "Medium"
    try:
        chg_val = float(str(change).replace("%", "").strip())
        if chg_val <= -10:
            risk = "High"
        elif abs(chg_val) < 5:
            risk = "Low"
    except Exception:
        pass

    summary = (
        f"{token} on {chain.title()} is trading near ${price}. "
        f"24 h change {change}, volume ${volume}, FDV ${fdv}, liquidity ${liquidity}. "
        f"Risk level {risk}."
    )

    return {
        "ok": True,
        "symbol": token,
        "metrics": {
            "price": price,
            "change_24h": change,
            "volume_24h": volume,
            "market_cap": fdv,
            "risk": risk,
        },
        "summary": summary,
    }


# -------------------------------------------------------------
# Main router
# -------------------------------------------------------------
def handle_question(data):
    """
    Accepts either structured JSON (with 'action') or plain-language questions.
    """

    # ---------- Case 1: explicit action ----------
    action = data.get("action")
    if action == "compare":
        tokens = data.get("tokens", [])
        if len(tokens) == 2:
            return compare_tokens(
                tokens[0]["chain"], tokens[0]["contract"],
                tokens[1]["chain"], tokens[1]["contract"]
            )
        return {"ok": False, "error": "Need two tokens to compare."}

    # ---------- Case 2: natural-language question ----------
    question = (data.get("question") or "").strip()
    if not question:
        return {"ok": False, "error": "No question text provided."}

    # Detect chain name
    chain = None
    if "solana" in question.lower():
        chain = "solana"
    elif "ethereum" in question.lower():
        chain = "ethereum"
    elif "bsc" in question.lower() or "binance" in question.lower():
        chain = "bsc"

    # Extract first probable contract address (32–44 chars = Solana; 0x…40 hex = EVM)
    match = re.search(r"(0x[a-fA-F0-9]{40}|[A-Za-z0-9]{32,44})", question)
    contract = match.group(0) if match else None

    if chain and contract:
        return analyze_single(chain, contract)

    # Could not understand the request
    return {"ok": False, "error": "Unrecognized question — specify chain and contract."}
