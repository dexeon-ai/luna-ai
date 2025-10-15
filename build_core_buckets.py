# build_core_buckets.py — rebuilds CORE_1..8 buckets from your Top 500 Crypto CSV
import csv, json, math

INPUT  = "crypto_500.csv"
OUTPUT = "symbol_buckets_core.py"

def chunk_list(lst, n):
    size = max(1, math.ceil(len(lst) / n))
    return [lst[i:i + size] for i in range(0, len(lst), size)]

tokens = []
with open(INPUT, "r", encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    for row in reader:
        name = (row.get("Name") or row.get("name") or "").strip()
        symbol = (row.get("Symbol") or row.get("symbol") or "").strip()
        if not name or not symbol:
            continue
        key = symbol.lower()
        tokens.append({"key": key, "name": name, "symbol": symbol})

print(f"Loaded {len(tokens)} core tokens from CSV")

chunks = chunk_list(tokens, 8)
buckets = {}
for i, chunk in enumerate(chunks, 1):
    buckets[f"CORE_{i}"] = chunk

with open(OUTPUT, "w", encoding="utf-8") as f:
    f.write("# symbol_buckets_core.py — auto-generated from Top 500 Crypto\n")
    f.write("BUCKETS = ")
    json.dump(buckets, f, indent=2)

print(f"✅ Created {OUTPUT} with {len(tokens)} assets.")
