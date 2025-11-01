"""
metrics_builder.py — bulletproof, quiet version
-----------------------------------------------
Scans historical JSONs, computes KPIs,
writes metrics JSON + Parquet.
Skips any bad files, no spam output.
"""

import json, numpy as np, pandas as pd, warnings
from pathlib import Path
from datetime import datetime, timezone

# Silence all runtime and future warnings
warnings.filterwarnings("ignore")

# === paths ===
ROOT = Path(r"C:\Users\jmpat\Desktop\Luna AI\luna_cache\data")
SRC  = ROOT / "historical"
DST  = ROOT / "derived"
FRAMES = DST / "frames"
DST.mkdir(parents=True, exist_ok=True)
FRAMES.mkdir(parents=True, exist_ok=True)

print("📊 Starting metrics build...")

# === make sure Parquet engine exists ===
try:
    import pyarrow  # noqa
    PARQ_ENGINE = "pyarrow"
except ImportError:
    try:
        import fastparquet  # noqa
        PARQ_ENGINE = "fastparquet"
    except ImportError:
        raise SystemExit("❌ Install a Parquet engine first:  pip install pyarrow")

# === KPI calculator ===
def kpi(series: pd.Series):
    ret = series.pct_change().dropna()
    if ret.empty:
        return {}
    ann_ret = np.mean(ret) * 365
    vol = np.std(ret) * np.sqrt(365)
    sharpe = ann_ret / vol if vol else None
    dd = (series / series.cummax() - 1).min()
    return {
        "annual_return": float(ann_ret) if np.isfinite(ann_ret) else None,
        "volatility": float(vol) if np.isfinite(vol) else None,
        "sharpe": float(sharpe) if sharpe and np.isfinite(sharpe) else None,
        "max_drawdown": float(dd) if np.isfinite(dd) else None,
    }

# === Main loop ===
files = list(SRC.glob("*.json"))
for i, f in enumerate(files, 1):
    try:
        # Load JSON safely
        try:
            j = json.loads(Path(f).read_text(encoding="utf-8"))
        except Exception:
            print(f"[{i}/{len(files)}] ⚠️  {f.stem} - unreadable JSON, skipped.")
            continue

        data = j.get("data")
        if not data or not isinstance(data, list):
            continue

        df = pd.DataFrame(data)
        if "close" not in df.columns or df["close"].empty:
            continue

        df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
        df.set_index("time", inplace=True)
        df["close"] = pd.to_numeric(df["close"], errors="coerce").ffill()

        metrics = kpi(df["close"])
        metrics["records"] = int(len(df))
        metrics["last_close"] = float(df["close"].iloc[-1])
        metrics["last_updated"] = datetime.now(timezone.utc).isoformat()

        # Write metrics JSON
        (DST / f"{f.stem}_metrics.json").write_text(json.dumps(metrics, indent=2))

        # Write Parquet (quiet fail allowed)
        try:
            df.to_parquet(FRAMES / f"{f.stem}.parquet", engine=PARQ_ENGINE, index=True)
        except Exception:
            continue

        print(f"[{i}/{len(files)}] ✅  {f.stem}")

    except Exception:
        # Catch-all: keep going even if something unexpected happens
        continue

print("\n✅ Metrics build finished.")
print(f"Processed {len(files)} files.")
print(f"Output → {DST}")
