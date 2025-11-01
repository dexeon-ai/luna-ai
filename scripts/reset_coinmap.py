from pathlib import Path
import shutil

root = Path(r"C:\Users\jmpat\Desktop\Luna AI\luna_cache\data")
src = root / "coin_map_clean.json"
dst = root / "coin_map.json"

if not src.exists():
    print(f"❌ Source not found: {src}")
else:
    shutil.copy2(src, dst)
    print(f"✅ coin_map.json has been replaced with coin_map_clean.json")
