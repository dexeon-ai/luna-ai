# overlay_card.py — Luna AI Overlay v3
# Sexy, data-driven HD overlay with Dexscreener sparkline + full metrics

import os, io, math, time, requests
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

DEX_API = "https://api.dexscreener.com/latest/dex/pairs"

def make_overlay_card(snapshot: dict, out_dir="/tmp/overlays"):
    """
    Builds an HD overlay card (1280x720) showing token stats, mood, and chart.
    Works on Render, pulls real chart data from Dexscreener.
    """
    os.makedirs(out_dir, exist_ok=True)

    # --- Extract token info ---
    token = (snapshot.get("token") or {}).get("symbol", "—")
    chain = snapshot.get("chain", "solana").capitalize()
    market = snapshot.get("market") or {}
    summary = snapshot.get("summary") or snapshot.get("tldr") or "No summary."

    price = _fmt(market.get("price") or market.get("price_usd"))
    change = str(market.get("change_24h") or (market.get("pct_change") or {}).get("h24") or "0")
    volume = _fmt(market.get("volume_24h") or market.get("volume_24h_usd"))
    fdv = _fmt(market.get("market_cap") or market.get("fdv_usd"))
    liq = _fmt(market.get("liquidity_usd") or "--")
    mood = str((snapshot.get("risk") or {}).get("mood", "—")).title()
    risk = (snapshot.get("risk") or {}).get("safety_gauge", 5)

    # --- Base canvas ---
    W, H = 1280, 720
    img = Image.new("RGB", (W, H), (8, 10, 25))
    draw = ImageDraw.Draw(img)
    _draw_gradient(draw, W, H, (10, 14, 40), (30, 40, 80))

    # --- Title bar ---
    header_h = 90
    draw.rectangle([0, 0, W, header_h], fill=(20, 25, 45))
    title_font = _font(52)
    draw.text((40, 18), f"{token}  ({chain})", fill=(255, 255, 255), font=title_font)

    # --- Live sparkline from Dexscreener ---
    _draw_sparkline(draw, token, chain.lower(), W-420, 60, 340, 120)

    # --- Metric grid ---
    label_font = _font(28)
    value_font = _font(30)
    start_x, start_y = 60, 140
    spacing = 50

    def metric(label, val, color=(220, 230, 255)):
        nonlocal start_y
        draw.text((start_x, start_y), f"{label}:", font=label_font, fill=(180,190,210))
        draw.text((start_x + 240, start_y), str(val), font=value_font, fill=color)
        start_y += spacing

    metric("Price", f"${price}")
    metric("24h Change", f"{change}%", _color_change(change))
    metric("24h Volume", f"${volume}")
    metric("Liquidity", f"${liq}")
    metric("FDV", f"${fdv}")
    metric("Safety", f"{risk}/10")
    metric("Mood", mood, _color_mood(mood))

    # --- Safety gauge ring ---
    cx, cy, r = 1060, 230, 90
    _draw_gauge(draw, cx, cy, r, risk)

    # --- Luna Mood Emoji ---
    emoji = _mood_emoji(mood)
    draw.text((cx-25, cy-35), emoji, font=_font(72), fill=(255, 255, 255))

    # --- Summary box ---
    box_y = 420
    draw.rectangle([40, box_y-20, W-40, H-40], fill=(15, 20, 40), outline=(60, 70, 110), width=2)
    draw.text((60, box_y), "Luna's Analysis:", font=_font(28), fill=(255,255,255))
    wrapped = _wrap(summary, 90)
    y = box_y + 40
    for line in wrapped[:9]:
        draw.text((80, y), line, font=_font(24), fill=(210,215,240))
        y += 28

    # --- Save image ---
    filename = f"{token}_{int(time.time())}.png"
    path = os.path.join(out_dir, filename)
    img.save(path)
    print(f"[Overlay Saved] {path}")
    return path


# ===========================================================
# Helper functions
# ===========================================================

def _draw_gradient(draw, W, H, c1, c2):
    for i in range(H):
        r = int(c1[0] + (c2[0]-c1[0])*i/H)
        g = int(c1[1] + (c2[1]-c1[1])*i/H)
        b = int(c1[2] + (c2[2]-c1[2])*i/H)
        draw.line([(0,i),(W,i)], fill=(r,g,b))

def _draw_sparkline(draw, token, chain, x, y, w, h):
    try:
        url = f"{DEX_API}/{chain}/{token}"
        r = requests.get(url, timeout=6)
        data = r.json()
        pair = (data.get("pairs") or [None])[0]
        if not pair: return
        points = (pair.get("sparkline") or [])[-50:]
        if not points: return
        nums = [float(p) for p in points]
        mx, mn = max(nums), min(nums)
        scale_x = w/len(nums)
        scale_y = h/(mx-mn+1e-6)
        px = [x + i*scale_x for i in range(len(nums))]
        py = [y + h - (n-mn)*scale_y for n in nums]
        pts = list(zip(px, py))
        draw.line(pts, fill=(80,180,255), width=3)
    except Exception as e:
        print("[Sparkline error]", e)

def _draw_gauge(draw, cx, cy, r, score):
    try:
        val = max(0, min(10, float(score)))
    except: val = 5
    for i in range(0, 180, 3):
        col = (80,80,90)
        if i/18 < val: col = (int(25*i/3), 255-int(20*i/3), 80)
        ang = math.radians(180 + i)
        x1 = cx + int(r*math.cos(ang))
        y1 = cy + int(r*math.sin(ang))
        draw.line([(cx, cy), (x1, y1)], fill=col, width=4)

def _color_change(change):
    try:
        v = float(str(change).replace("%",""))
        return (100,255,100) if v>=0 else (255,100,100)
    except: return (230,230,230)

def _color_mood(m):
    s = str(m).lower()
    if "calm" in s: return (100,255,120)
    if "rolling" in s: return (255,210,100)
    if "storm" in s or "high" in s: return (255,120,120)
    return (220,220,255)

def _mood_emoji(m):
    s = str(m).lower()
    if "calm" in s: return "😌"
    if "rolling" in s or "medium" in s: return "🌊"
    if "storm" in s: return "🌪️"
    if "tsunami" in s: return "🌋"
    return "✨"

def _wrap(text, width=85):
    words = text.split()
    lines, cur = [], []
    for w in words:
        cur.append(w)
        if len(" ".join(cur)) >= width:
            lines.append(" ".join(cur)); cur=[]
    if cur: lines.append(" ".join(cur))
    return lines

def _fmt(x):
    try:
        f = float(x)
        if f >= 1_000_000_000: return f"{f/1_000_000_000:.2f}B"
        if f >= 1_000_000: return f"{f/1_000_000:.2f}M"
        if f >= 1000: return f"{f/1000:.2f}K"
        return f"{f:.4f}"
    except: return str(x)

def _font(size=24):
    try:
        return ImageFont.truetype("arial.ttf", size)
    except:
        return ImageFont.load_default()
