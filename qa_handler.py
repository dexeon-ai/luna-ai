from snapshot import build_snapshot

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

def handle_question(data):
    """
    Simple Q&A router.
    Example supported input:
      {
        "action":"compare",
        "tokens":[
          {"chain":"solana","contract":"DezX...BONK"},
          {"chain":"ethereum","contract":"0x6982...PEPE"}
        ]
      }
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

    return {"ok": False, "error": "Unknown action."}
