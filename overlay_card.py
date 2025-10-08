# overlay_card.py — Luna Overlay Renderer v6 (fixed for build_tech_panel v2)
# Generates dual-panel charts from CoinGecko/CoinPaprika data
# Compatible with updated plot_engine.py (no cg_id argument)
# Updated: 2025-10-08

import os, time
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

from plot_engine import build_tech_panel

DEFAULT_OUTDIR = "/tmp/overlays"
Path(DEFAULT_OUTDIR).mkdir(parents=True, exist_ok=True)

def make_overlay_card(snapshot: dict, out_dir: str = DEFAULT_OUTDIR) -> str:
    os.makedirs(out_dir, exist_ok=True)

    token = (snapshot.get("token") or {}).get("symbol", "BTC").upper()
    chain = (snapshot.get("chain") or "bitcoin").title()

    print(f"[Overlay] Rendering chart for {token} ({chain})")

    try:
        # ✅ Only symbol and out_path now — cg_id removed
        tech = build_tech_panel(symbol=token, out=os.path.join(out_dir, "tech_panel.png"))
        chart_path = tech["chart_path"]
        M = tech["metrics"]
    except Exception as e:
        print("[Overlay Error] build_tech_panel failed:", e)
        chart_path = None
        M = {}

    # --- Canvas setup ---
    W, H = 1280, 720
    img = Image.new("RGB", (W, H), (10, 12, 35))
    draw = ImageDraw.Draw(img)
    _gradient(draw, W, H, (38, 18, 80), (10, 34, 100))

    # --- Header ---
    header_h = 90
    draw.rectangle([0, 0, W, header_h], fill=(28, 22, 60))
    draw.text((40, 20), f"{token} — Technical Overview", fill=(255, 255, 255), font=_font(48))
    draw.text((40, 60), f"Network: {chain}", fill=(190, 200, 230), font=_font(24))

    # --- Paste chart ---
    if chart_path and os.path.exists(chart_path):
        try:
            chart = Image.open(chart_path).convert("RGBA")
            chart = chart.resize((600, 400))
            panel_x, panel_y = W - 660, 160
            draw.rounded_rectangle([panel_x - 16, panel_y - 16, panel_x + 620, panel_y + 420],
                                   radius=12, fill=(15, 18, 40), outline=(70, 80, 120), width=2)
            img.paste(chart, (panel_x, panel_y), chart)
        except Exception as e:
            print("[Overlay] Failed to paste chart:", e)

    # --- Metrics ---
    y = 160
    metrics = [
        ("Price", _usd(M.get("price"))),
        ("24h Change", "+0.00%"),
        ("Market Cap", _usd(M.get("market_cap", 0))),
        ("24h Volume", _usd(M.get("vol_24h", 0))),
        ("From ATH", f"{M.get('from_ath_pct', 0):.2f}%"),
    ]
    for label, val in metrics:
        draw.text((60, y), f"{label}:", font=_font(28), fill=(180, 190, 210))
        draw.text((280, y), val, font=_font(30), fill=(255, 255, 255))
        y += 48

    # --- Summary box ---
    summary = snapshot.get("summary") or snapshot.get("tldr") or "Automated technical chart overview."
    box_y = 580
    draw.rounded_rectangle([40, box_y - 16, W - 40, H - 40], radius=10, fill=(15, 20, 45))
    draw.text((60, box_y), "Luna’s Analysis", font=_font(26), fill=(255, 255, 255))
    for i, line in enumerate(_wrap(summary, 100)[:5]):
        draw.text((60, box_y + 32 + i * 26), line, font=_font(22), fill=(210, 215, 240))

    # --- Save ---
    out_file = os.path.join(out_dir, f"{token}_{int(time.time())}.png")
    img.save(out_file, quality=90)
    print(f"[Overlay Saved] {out_file}")
    return out_file

# -----------------------
# Helpers
# -----------------------
def _gradient(draw, W, H, c1, c2):
    for i in range(H):
        r = int(c1[0] + (c2[0] - c1[0]) * i / H)
        g = int(c1[1] + (c2[1] - c1[1]) * i / H)
        b = int(c1[2] + (c2[2] - c1[2]) * i / H)
        draw.line([(0, i), (W, i)], fill=(r, g, b))

def _font(s):
    try:
        return ImageFont.truetype("arial.ttf", s)
    except:
        return ImageFont.load_default()

def _wrap(text, width=90):
    words = str(text).split()
    lines, cur = [], []
    for w in words:
        cur.append(w)
        if len(" ".join(cur)) >= width:
            lines.append(" ".join(cur)); cur = []
    if cur:
        lines.append(" ".join(cur))
    return lines

def _usd(x):
    try:
        f = float(x or 0)
        if f >= 1_000_000_000: return f"${f/1_000_000_000:.2f}B"
        if f >= 1_000_000: return f"${f/1_000_000:.2f}M"
        if f >= 1_000: return f"${f/1_000:.2f}K"
        return f"${f:.2f}"
    except:
        return str(x)
