# build_meme_buckets_full.py — rebuilds all MEME_1..8 buckets from the Top 1000 MemeCoin list
import csv, json, math, re

INPUT  = "meme_1000.csv"
OUTPUT = "symbol_buckets_memes.py"

def chunk_list(lst, n):
    size = max(1, math.ceil(len(lst) / n))
    return [lst[i:i + size] for i in range(0, len(lst), size)]

def clean_symbol(value):
    """Extract ticker like DOGE from '151.28B DOGE' or similar."""
    match = re.search(r"\b[A-Z]{2,}\b", value)
    return match.group(0) if match else value.strip().upper()

tokens = []
with open(INPUT, "r", encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    for row in reader:
        name = (row.get("Name") or "").strip()
        circ = (row.get("Circulating Supply") or "").strip()
        symbol = clean_symbol(circ)
        if not name or not symbol:
            continue
        key = symbol.lower()
        tokens.append({"key": key, "name": name, "symbol": symbol})

print(f"Loaded {len(tokens)} meme coins from CSV")

chunks = chunk_list(tokens, 8)
buckets = {}
for i, chunk in enumerate(chunks, 1):
    buckets[f"MEME_{i}"] = chunk

with open(OUTPUT, "w", encoding="utf-8") as f:
    f.write("# symbol_buckets_memes.py — auto-generated from Top 1000 MemeCoins\n")
    f.write("BUCKETS = ")
    json.dump(buckets, f, indent=2)

print(f"✅ Created {OUTPUT} with {len(tokens)} meme coins distributed across 8 buckets.")
