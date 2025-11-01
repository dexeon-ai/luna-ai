import pandas as pd, json
from pathlib import Path

src = Path(r"C:\Users\jmpat\Desktop\Luna AI\luna_cache\data\historical")
dst = src.parent / "derived" / "frames"
dst.mkdir(parents=True, exist_ok=True)

for f in src.glob("*.json"):
    try:
        j = json.load(open(f))
        df = pd.DataFrame(j["data"])
        if not df.empty:
            df.to_parquet(dst / f"{f.stem}.parquet", index=False)
            print(f"✅ {f.stem} → Parquet")
    except Exception as e:
        print(f"Skip {f.stem}:", e)
