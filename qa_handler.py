# qa_handler.py — natural language + compare
import re
from utils import chain_slug            # NEW import
from snapshot import build_snapshot

# -----------------------------
# Compare two tokens
# -----------------------------
def compare_tokens(chain1, contract1, chain2, contract2):
    snap1 = build_snapshot(chain_slug(chain1), contract1)
    snap2 = build_snapshot(chain_slug(chain2), contract2)

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


# -----------------------------
# Analyze a single token
# -----------------------------
def analyze_single(chain, contract):
    """Aligns with snapshot.build_snapshot logic."""
    slug = chain_slug(chain)  # normalize chain input
    snap = build_snapshot(slug, contract)
    if not snap.get("ok"):
        return {"ok": False, "error": snap.get("error", "Snapshot failed.")}

    token = snap["token"].get("symbol") or "Unknown"
    m = snap.get("market", {})
    price = m.get("price_usd", "N/A")
    change24 = (m.get("pct_change") or {}).get("h24", "N/A")
    vol = m.get("volume_24h_usd", "N/A")
    fdv = m.get("fdv_usd", "N/A")
    liq = m.get("liquidity_usd", "N/A")

    risk = snap.get("risk", {}).get("mood", "N/A")
    summary = snap.get("tldr")

    return {
        "ok": True,
        "symbol": token,
        "metrics": {
            "price": price,
            "change_24h": change24,
            "volume_24h": vol,
            "market_cap": fdv,
            "risk": risk
        },
        "summary": summary
    }


# -----------------------------
# Router
# -----------------------------
def handle_question(data):
    """
    Handles both structured {"action": ...} and natural text {"question": ...}
    """
    action = data.get("action")
    if action == "compare":
        tokens = data.get("tokens", [])
        if len(tokens) == 2:
            return compare_tokens(
                tokens[0]["chain"], tokens[0]["contract"],
                tokens[1]["chain"], tokens[1]["contract"]
            )
        return {"ok": False, "error": "Need two tokens to compare."}

    # Natural-language
    q = (data.get("question") or "").strip()
    if not q:
        return {"ok": False, "error": "No question text provided."}

    # detect chain
    chain = None
    if "solana" in q.lower():
        chain = "solana"
    elif "ethereum" in q.lower():
        chain = "ethereum"
    elif "bsc" in q.lower() or "binance" in q.lower():
        chain = "bsc"

    # extract contract address
    match = re.search(r"(0x[a-fA-F0-9]{40}|[A-Za-z0-9]{32,44})", q)
    contract = match.group(0) if match else None

    if chain and contract:
        return analyze_single(chain, contract)

    return {"ok": False, "error": "Unrecognized question — include chain and contract."}
