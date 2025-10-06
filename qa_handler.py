# qa_handler.py — directs questions to Dexscreener snapshot
from snapshot import build_snapshot

def handle_question(data):
    q = (data.get("question") or "").lower()
    # hard-wire MOMO contract for testing
    contract = "G4zwEA9NSd3nMBbEj31MMPq2853Brx2oGsKzex3ebonk"
    chain = "solana"

    snap = build_snapshot(chain, contract)
    if not snap.get("ok"):
        return {"ok": False, "error": snap.get("error", "Snapshot failed.")}

    token = snap["token"].get("symbol", "Unknown")
    market = snap["market"]
    summary = snap.get("tldr", f"{token} snapshot loaded.")
    return {
        "ok": True,
        "symbol": token,
        "chain": chain,
        "contract": contract,
        "market": market,
        "summary": summary,
    }
