# batch_chart_builder.py
import logging
from pathlib import Path
from plot_engine import build_tech_panel, OVERLAYS_DIR

logging.basicConfig(level=logging.INFO)

SYMBOLS = [
    "bitcoin",
    "ethereum",
    "solana",
    "bonk",
    "pepe",
    "dogwifhat"
]

def main():
    OVERLAYS_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[PATH] Overlays saving to: {OVERLAYS_DIR.resolve()}")
    for s in SYMBOLS:
        try:
            out = build_tech_panel(s)
            print(f"✅ Chart built for {s}: {Path(out).resolve()}")
        except Exception as e:
            print(f"❌ {s}: {e}")

if __name__ == "__main__":
    main()
