"""
cleanup_bad_parquet.py
Scans all Parquet files in frames/, removes zero rows and 1960s timestamps.
"""
from pathlib import Path
import pandas as pd
from datetime import datetime

FRAMES = Path(r"C:\Users\jmpat\Desktop\Luna AI\luna_cache\data\derived\frames")
BAD_AFTER = datetime(1970, 1, 1)

for f in FRAMES.glob("*.parquet"):
    try:
        df = pd.read_parquet(f)
        before = len(df)
        # remove nonsense timestamps or zero candles
        if "time" in df.columns:
            df["timestamp"] = pd.to_datetime(df["time"], unit="s", errors="coerce")
        elif "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df[df["timestamp"] > BAD_AFTER]
        if "close" in df.columns:
            df = df[df["close"] > 0]
        if len(df) == 0:
            print(f"⚠️  {f.name}: all junk ({before} rows).")
            continue
        df.to_parquet(f, index=False)
        print(f"✅  {f.name}: cleaned {before - len(df)} rows; kept {len(df)}")
    except Exception as e:
        print(f"❌  {f.name}: {e}")
