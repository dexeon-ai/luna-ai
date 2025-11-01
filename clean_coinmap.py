import json, re, pathlib

IN  = pathlib.Path(r"C:\Users\jmpat\Desktop\Luna AI\luna_cache\data\coin_map.json")
OUT = pathlib.Path(r"C:\Users\jmpat\Desktop\Luna AI\luna_cache\data\coin_map_clean.json")

print(f"[Luna] Cleaning {IN}...")
data = json.load(open(IN, "r", encoding="utf-8"))

cleaned = {}
for cid, meta in data.items():
    sym = (meta.get("symbol") or "").upper()
    # drop if symbol invalid or too long
    if not re.fullmatch(r"[A-Z0-9]{2,10}", sym):
        continue
    cleaned[cid] = {"symbol": sym, "name": meta.get("name", cid)}

print(f"[Luna] {len(cleaned):,} valid entries (from {len(data):,})")
json.dump(cleaned, open(OUT, "w", encoding="utf-8"), indent=2)
print(f"[Luna] Saved → {OUT}")
