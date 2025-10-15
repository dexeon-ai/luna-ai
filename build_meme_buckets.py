# build_meme_buckets.py — converts your meme CSV into MEME_1..8 buckets
import csv, json, math, re

INPUT = "TOP 1000 MEMECOINS 11pm PM OCT 9 2025 - Sheet1.csv"
OUTPUT = "symbol_buckets_memes.py"

def chunk_list(lst, n):
    size = max(1, math.ceil(len(lst) / n))
    return [lst[i:i + size] for i in range(0, len(lst), size)]

def clean_symbol(value):
    """Extract symbol like DOGE from '151.28B DOGE' or '62.86B PENGU'"""
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

# Split into 8 buckets
chunks = chunk_list(tokens, 8)

buckets = {}
for i, chunk in enumerate(chunks, 1):
    buckets[f"MEME_{i}"] = chunk

with open(OUTPUT, "w", encoding="utf-8") as f:
    f.write("# symbol_buckets_memes.py — auto-generated from meme list\n")
    f.write("BUCKETS = ")
    json.dump(buckets, f, indent=2)

print(f"✅ Created {OUTPUT} with {len(tokens)} meme coins.")
