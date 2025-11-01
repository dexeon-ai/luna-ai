# verify_coverage.py
import json, pathlib, datetime

ROOT = pathlib.Path(__file__).parent.resolve()
COINS = ROOT/"luna_cache"/"data"/"coins"
AN = ROOT/"luna_cache"/"data"/"analysis"

coins = sorted(p.stem for p in COINS.glob("*.csv"))
have = []
stale = []
missing = []

now = datetime.datetime.now(datetime.timezone.utc)
for c in coins:
    fp = AN / f"{c}.json"
    if not fp.exists():
        missing.append(c); continue
    try:
        j = json.loads(fp.read_text(encoding="utf-8"))
        ts = j.get("last_updated")
        if ts:
            age_h = (now - datetime.datetime.fromisoformat(ts)).total_seconds()/3600
        else:
            age_h = 1e9
        have.append((c, age_h))
        if age_h > 24*30: stale.append((c, age_h))
    except Exception:
        stale.append((c, 1e9))

print(f"Total CSVs: {len(coins)}")
print(f"Have analysis: {len(have)}")
print(f"Missing: {len(missing)}")
print(f"Stale (>30d): {len(stale)}")
