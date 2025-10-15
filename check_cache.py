import os, csv, glob

path = "luna_cache/data/coins"
files = glob.glob(f"{path}/*.csv")
ready = []

for f in files:
    try:
        with open(f, encoding="utf-8") as fh:
            rows = sum(1 for _ in fh) - 1  # minus header
        if rows > 0:
            ready.append((os.path.basename(f), rows))
    except:
        pass

print(f"{len(ready)} coins have data.")
for name, rows in sorted(ready)[:20]:
    print(f"{name:30}  {rows} rows")
