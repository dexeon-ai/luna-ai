# overlay_card.py — write-safe version for Render
# This simplified generator ALWAYS saves a PNG to /tmp/overlays
# so Luna's widget will have something to display.

from PIL import Image, ImageDraw, ImageFont
import os, time


def make_overlay_card(snapshot: dict, out_dir="/tmp/overlays"):
    """
    Creates a simple overlay card PNG and saves it in /tmp/overlays.
    Works even if snapshot is missing data.
    Returns the absolute path of the saved file.
    """

    # --- ensure directory exists ---
    os.makedirs(out_dir, exist_ok=True)

    # --- extract whatever info is available ---
    token = (snapshot.get("token") or {}).get("symbol", "LUNA")
    text = snapshot.get("summary") or snapshot.get("tldr") or "Overlay generator test successful."

    # --- canvas setup ---
    W, H = 1280, 720
    img = Image.new("RGB", (W, H), (20, 25, 40))
    draw = ImageDraw.Draw(img)
    font = ImageFont.load_default()

    # --- draw token title ---
    draw.text((40, 40), f"Token: {token}", fill=(255, 255, 255), font=font)

    # --- draw text summary ---
    y = 100
    for line in split_text(text, 90):
        draw.text((40, y), line, fill=(180, 210, 255), font=font)
        y += 22
        if y > H - 60:
            break

    # --- save image ---
    filename = f"{token}_{int(time.time())}.png"
    path = os.path.join(out_dir, filename)
    img.save(path)

    print(f"[Overlay Saved] {path}")
    return path


def split_text(text, width=80):
    """
    Splits text into lines of approximately `width` characters.
    """
    words = text.split()
    lines, current = [], []
    for w in words:
        current.append(w)
        if len(" ".join(current)) >= width:
            lines.append(" ".join(current))
            current = []
    if current:
        lines.append(" ".join(current))
    return lines
