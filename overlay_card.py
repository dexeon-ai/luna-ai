# overlay_card.py — debug-safe fallback
from PIL import Image, ImageDraw, ImageFont
import os, time

def make_overlay_card(snap, out_dir="overlays"):
    os.makedirs(out_dir, exist_ok=True)
    token = (snap.get("token") or {}).get("symbol", "LUNA")
    text  = snap.get("summary") or "Overlay generator test successful."
    img = Image.new("RGB", (1280, 720), (20, 25, 40))
    draw = ImageDraw.Draw(img)
    font = ImageFont.load_default()
    draw.text((40, 40), f"Token: {token}", fill=(255,255,255), font=font)
    draw.text((40, 80), text[:300], fill=(180,210,255), font=font)
    filename = f"{token}_{int(time.time())}.png"
    path = os.path.join(out_dir, filename)
    img.save(path)
    print(f"[Overlay Saved] {path}")
    return path
