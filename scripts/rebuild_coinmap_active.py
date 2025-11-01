import json, re
from pathlib import Path

ROOT = Path(r"C:\Users\jmpat\Desktop\Luna AI\luna_cache\data")
SRC = ROOT / "coin_map_active.json"
DST = ROOT / "coin_map_active_clean.json"

data = json.load(open(SRC, "r", encoding="utf-8"))
cleaned = {}

for cid, meta in data.items():
    sym = str(meta.get("symbol", "")).strip().upper()
    if not sym or not re.match(r"^[A-Z0-9]{2,10}$", sym):
        continue
    cleaned[cid] = {"symbol": sym, "name": meta.get("name", cid)}

print(f"✅ {len(cleaned):,} valid symbols after cleaning.")
json.dump(cleaned, open(DST, "w", encoding="utf-8"), indent=2)
print(f"💾 Saved → {DST}")
