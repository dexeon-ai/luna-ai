# overlay_card.py — upgraded Luna overlay card (v2)
import os, time
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

def make_overlay_card(snapshot: dict, out_dir="/tmp/overlays"):
    """
    Creates a visually enhanced overlay card for Luna's on-screen display.
    """
    os.makedirs(out_dir, exist_ok=True)

    token = (snapshot.get("token") or {}).get("symbol", "—")
    market = snapshot.get("market") or {}
    summary = snapshot.get("summary") or "No summary available."

    price = market.get("price") or market.get("price_usd", "—")
    change = market.get("change_24h") or market.get("pct_change", {}).get("h24", "—")
    volume = market.get("volume_24h") or market.get("volume_24h_usd", "—")
    fdv = market.get("market_cap") or market.get("fdv_usd", "—")
    risk = market.get("risk", "—")

    # ---- canvas setup ----
    W, H = 1280, 720
    img = Image.new("RGB", (W, H), (14, 18, 32))
    draw = ImageDraw.Draw(img)
    font_title = _load_font(size=48)
    font_sub = _load_font(size=28)
    font_body = _load_font(size=24)

    # ---- header bar ----
    header_color = (30, 40, 70)
    draw.rectangle([0, 0, W, 100], fill=header_color)
    draw.text((40, 25), f"{token} Snapshot", font=font_title, fill=(255, 255, 255))

    # ---- price & metrics ----
    y0 = 140
    draw.text((60, y0), f"Price: ${price}", font=font_sub, fill=(230, 230, 250))
    draw.text((60, y0 + 40), f"24h Change: {change}", font=font_sub, fill=_color_change(change))
    draw.text((60, y0 + 80), f"Volume (24h): {volume}", font=font_sub, fill=(200, 220, 255))
    draw.text((60, y0 + 120), f"FDV: {fdv}", font=font_sub, fill=(200, 220, 255))
    draw.text((60, y0 + 160), f"Risk: {risk}", font=font_sub, fill=_color_risk(risk))

    # ---- safety gauge ----
    draw.text((60, y0 + 220), "Safety Gauge:", font=font_sub, fill=(255, 255, 255))
    gauge_x, gauge_y = 260, y0 + 230
    gauge_w, gauge_h = 300, 20
    draw.rectangle([gauge_x, gauge_y, gauge_x + gauge_w, gauge_y + gauge_h], fill=(40, 40, 60))
    level = _risk_level_numeric(risk)
    draw.rectangle([gauge_x, gauge_y, gauge_x + int(gauge_w * level / 10), gauge_y + gauge_h],
                   fill=_color_risk(risk))

    # ---- summary box ----
    box_y = y0 + 280
    draw.text((60, box_y), "Luna's Summary:", font=font_sub, fill=(255, 255, 255))
    wrapped = _wrap_text(summary, width=90)
    y = box_y + 40
    for line in wrapped:
        draw.text((80, y), line, font=font_body, fill=(210, 215, 240))
        y += 28

    # ---- save ----
    filename = f"{token}_{int(time.time())}.png"
    path = os.path.join(out_dir, filename)
    img.save(path)
    print(f"[Overlay Saved] {path}")
    return path


# ---- Helper functions ----

def _wrap_text(text, width=80):
    words, lines, current = text.split(), [], []
    for w in words:
        current.append(w)
        if len(" ".join(current)) >= width:
            lines.append(" ".join(current))
            current = []
    if current:
        lines.append(" ".join(current))
    return lines

def _load_font(size=28):
    try:
        return ImageFont.truetype("arial.ttf", size)
    except:
        return ImageFont.load_default()

def _color_change(change):
    try:
        val = float(str(change).replace("%", ""))
        return (100, 255, 100) if val >= 0 else (255, 100, 100)
    except:
        return (200, 200, 200)

def _color_risk(risk):
    if isinstance(risk, (int, float)):
        if risk <= 3: return (100, 255, 100)
        if risk <= 6: return (255, 210, 100)
        return (255, 120, 120)
    s = str(risk).lower()
    if "low" in s or "calm" in s: return (100, 255, 100)
    if "medium" in s or "rolling" in s: return (255, 210, 100)
    if "high" in s or "storm" in s: return (255, 120, 120)
    return (200, 200, 200)

def _risk_level_numeric(risk):
    if isinstance(risk, (int, float)): return float(risk)
    s = str(risk).lower()
    if "low" in s or "calm" in s: return 3
    if "medium" in s or "rolling" in s: return 6
    if "high" in s or "storm" in s: return 9
    return 5
