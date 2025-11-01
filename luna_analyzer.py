# luna_analyzer.py — optional standalone CMC-style analysis used by /api/luna (fallback)
# It expects a CSV/Parquet in luna_cache/data/derived/frames/<symbol>.*
import json, pathlib, datetime as dt
import pandas as pd
from typing import Dict, Any
from zoneinfo import ZoneInfo

ROOT = pathlib.Path(__file__).parent
FRAMES = ROOT / "luna_cache" / "data" / "derived" / "frames"
TZ_NY = ZoneInfo("America/New_York")

def load(symbol: str) -> pd.DataFrame:
    p = FRAMES / f"{symbol.upper()}.parquet"
    if p.exists():
        return pd.read_parquet(p)
    p2 = p.with_suffix(".csv")
    if p2.exists():
        return pd.read_csv(p2, parse_dates=["timestamp"])
    return pd.DataFrame()

# simple re-using fields server computed
def analyze(symbol: str, tf: str="4h") -> Dict[str,Any]:
    df = load(symbol)
    if "timestamp" not in df.columns:
        return {"error":"no data"}
    from server import slice_df, compute_indicators, cmc_like_analysis  # reuse
    df = compute_indicators(df)
    return cmc_like_analysis(symbol, df, tf)

if __name__ == "__main__":
    import sys
    sym = (sys.argv[1] if len(sys.argv)>1 else "BTC").upper()
    tf  = (sys.argv[2] if len(sys.argv)>2 else "4h")
    print(json.dumps(analyze(sym, tf), indent=2))
