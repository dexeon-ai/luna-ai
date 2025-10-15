# merge_contracts.py — unify crypto + meme + new into all_contracts.json
import os, json, re

CACHE_DIR = "luna_cache"
IN_TOP500  = os.path.join(CACHE_DIR, "contracts.json")
IN_MEME    = os.path.join(CACHE_DIR, "meme_contracts.json")
IN_NEW     = os.path.join(CACHE_DIR, "new_contracts.json")
OUT_ALL    = os.path.join(CACHE_DIR, "all_contracts.json")

def norm_key(s):
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s)
    return s.strip("-")

def as_tokens(path):
    if not os.path.exists(path): return {}
    with open(path, "r", encoding="utf-8") as f:
        j = json.load(f)
    # allow {tokens:{...}} or raw dict mapping
    if isinstance(j, dict) and "tokens" in j and isinstance(j["tokens"], dict):
        return j["tokens"]
    # contracts.json generated earlier may embed more structure: normalize
    if isinstance(j, dict) and all(k in ("rank","name","symbol","id","chain","contract","verified") for k in j.keys()):
        # unexpected shape; wrap
        return {"auto": j}
    # or a list of token dicts
    if isinstance(j, list):
        out = {}
        for t in j:
            k = norm_key(t.get("name") or t.get("id") or t.get("symbol"))
            out[k] = t
        return out
    return j if isinstance(j, dict) else {}

def better(a, b):
    """Return best of two entries."""
    if not a: return b
    if not b: return a
    # prefer verified
    if bool(a.get("verified")) != bool(b.get("verified")):
        return a if a.get("verified") else b
    # prefer one with contract set
    if (a.get("contract") and not b.get("contract")): return a
    if (b.get("contract") and not a.get("contract")): return b
    # prefer one with chain set
    if (a.get("chain") and not b.get("chain")): return a
    if (b.get("chain") and not a.get("chain")): return b
    # default: keep a
    return a

def unify(t):
    return {
        "name": t.get("name"),
        "symbol": (t.get("symbol") or "").upper(),
        "id": t.get("id"),
        "chain": t.get("chain"),
        "contract": t.get("contract"),
        "verified": bool(t.get("verified")),
    }

def main():
    merged = {}

    for path in (IN_TOP500, IN_MEME, IN_NEW):
        for k, t in as_tokens(path).items():
            key = norm_key(t.get("name") or t.get("id") or t.get("symbol") or k)
            t = unify(t)
            merged[key] = better(merged.get(key), t)

    # Build fast indexes
    idx_name, idx_symbol, idx_contract = {}, {}, {}
    for k, t in merged.items():
        n = (t.get("name") or "").strip().lower()
        s = (t.get("symbol") or "").strip().lower()
        c = (t.get("contract") or "").strip().lower()
        if n and n not in idx_name: idx_name[n] = k
        if s and s not in idx_symbol: idx_symbol[s] = k
        if c and c not in idx_contract: idx_contract[c] = k

    out = {
        "total": len(merged),
        "index_by_name": idx_name,
        "index_by_symbol": idx_symbol,
        "index_by_contract": idx_contract,
        "tokens": merged
    }
    with open(OUT_ALL, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    print(f"✅ wrote {OUT_ALL} with {len(merged)} tokens")

if __name__ == "__main__":
    main()
