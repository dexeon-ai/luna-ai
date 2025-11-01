# ============================================================
# Luna AI - Coin Map Cleaner (alphabetical + valid symbols)
# ============================================================
import json, re
from pathlib import Path

ROOT = Path(r"C:\Users\jmpat\Desktop\Luna AI\luna_cache\data")
SRC = ROOT / "coin_map.json"
DST = ROOT / "coin_map_clean.json"

def is_valid_symbol(sym):
    """Only keep real tickers (A–Z and up to 10 chars)."""
    return bool(re.fullmatch(r"[A-Z]{2,10}", sym or ""))

def is_valid_name(name):
    """Reject anything starting with numbers or symbols."""
    return bool(re.match(r"^[A-Za-z]", name or ""))

with open(SRC, "r", encoding="utf-8") as f:
    data = json.load(f)

cleaned = {}
for cid, meta in data.items():
    sym = (meta.get("symbol") or "").upper().strip()
    name = (meta.get("name") or "").strip()

    # Skip junk symbols/names
    if not is_valid_symbol(sym):
        continue
    if not is_valid_name(name):
        continue
    cleaned[cid] = {"symbol": sym, "name": name}

# Sort alphabetically
cleaned = dict(sorted(cleaned.items(), key=lambda x: x[1]["symbol"]))

with open(DST, "w", encoding="utf-8") as f:
    json.dump(cleaned, f, indent=2)

print(f"✅ Saved clean map with {len(cleaned)} valid coins → {DST}")
