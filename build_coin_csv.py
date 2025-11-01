# build_coin_csv.py — convert frames/*.parquet -> coins/<id>.csv with indicators
from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).parent.resolve()
DATA = ROOT / "luna_cache" / "data"
FRAMES = DATA / "derived" / "frames"
COINS = DATA / "coins"
COINS.mkdir(parents=True, exist_ok=True)

def load_frame(symbol: str) -> pd.DataFrame:
    p = FRAMES / f"{symbol.upper()}.parquet"
    if not p.exists():
        return pd.DataFrame()
    df = pd.read_parquet(p)
    # normalize time
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    elif "time" in df.columns:
        df["timestamp"] = pd.to_datetime(df["time"], unit="s", utc=True, errors="coerce")
    else:
        return pd.DataFrame()
    # numeric
    for c in ["open","high","low","close","volumefrom","volumeto","volume","market_cap","fdv"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "volume" not in df.columns:
        if "volumeto" in df.columns:
            df["volume"] = df["volumeto"]
        elif "volumefrom" in df.columns:
            df["volume"] = df["volumefrom"]
    df = df.dropna(subset=["timestamp","close"]).sort_values("timestamp").reset_index(drop=True)
    return df

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["price"] = out["close"].astype(float)

    # RSI(14)
    d = out["price"].diff()
    up = d.clip(lower=0.0)
    dn = -d.clip(upper=0.0).replace(0.0, 1e-9)
    rsi = 100 - 100 / (1 + up.ewm(span=14, adjust=False).mean() / dn.ewm(span=14, adjust=False).mean())
    out["rsi"] = rsi

    # MACD 12/26/9
    ema12 = out["price"].ewm(span=12, adjust=False).mean()
    ema26 = out["price"].ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    out["macd_line"] = macd
    out["macd_signal"] = macd.ewm(span=9, adjust=False).mean()
    out["macd_hist"] = out["macd_line"] - out["macd_signal"]

    # Bollinger(20,2)
    mid = out["price"].rolling(20, min_periods=5).mean()
    std = out["price"].rolling(20, min_periods=5).std()
    out["bb_lower"] = mid - 2*std
    out["bb_middle"] = mid
    out["bb_upper"] = mid + 2*std

    # Volume trend & sentiment
    out["volume_trend"] = out["volume"].rolling(20, min_periods=5).mean()
    out["sentiment"] = out["price"].pct_change(1) * 100.0

    # Liquidity proxy
    if "market_cap" in out.columns and out["market_cap"].notna().any():
        with pd.option_context("mode.use_inf_as_na", True):
            out["liquidity"] = (out["volume"] / out["market_cap"]).replace([pd.NA], 0)
    else:
        out["liquidity"] = out["volume"]

    return out

def save_coin(symbol: str):
    df = load_frame(symbol)
    if df.empty:
        print(f"❌ No frame for {symbol}")
        return
    df = add_indicators(df)
    keep = ["timestamp","price","volume","market_cap","fdv","liquidity",
            "rsi","macd_line","macd_signal","macd_hist",
            "bb_lower","bb_middle","bb_upper",
            "volume_trend","sentiment"]
    for c in keep:
        if c not in df.columns:
            df[c] = pd.NA
    out = df[keep].copy()
    out.to_csv(COINS / f"{symbol.lower()}.csv", index=False)
    print(f"✅ {symbol}: wrote {len(out):,} rows → {COINS / (symbol.lower() + '.csv')}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", "-s", help="e.g. BTC, SOL")
    ap.add_argument("--all", action="store_true", help="convert all frames/*.parquet")
    args = ap.parse_args()

    if args.all:
        for p in sorted(FRAMES.glob("*.parquet")):
            save_coin(p.stem)
    elif args.symbol:
        save_coin(args.symbol)
    else:
        print("Usage: python build_coin_csv.py --symbol BTC  (or --all)")

if __name__ == "__main__":
    main()
