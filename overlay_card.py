# overlay_card.py — Luna Broadcast Overlay v6 (Tech Panel + Metrics + Projection)
# Output: /tmp/overlays/<SYMBOL>_<ts>.png

import time
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

from plot_engine import build_tech_panel

DEFAULT_OUTDIR = "/tmp/overlays"
Path(DEFAULT_OUTDIR).mkdir(parents=True, exist_ok=True)

def make_overlay_card(snapshot: dict, out_dir: str = DEFAULT_OUTDIR) -> str:
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    token = ((snapshot or {}).get("token") or {}).get("symbol") or "BTC"
    token = str(token).upper()
    chain = (snapshot or {}).get("chain") or "Bitcoin"
    chain = str(chain).title()

    # Render chart + gather metrics (cg_id is accepted but optional)
    tech = build_tech_panel(
        symbol=token,
        cg_id=None,
        short_days=7,
        out_path="/tmp/tech_panel.png",
        theme="purple",
    )
    chart_path = tech["chart_path"]
    M = tech["metrics"]

    # Canvas
    W, H = 1280, 720
    img = Image.new("RGB", (W, H), (10, 12, 35))
    draw = ImageDraw.Draw(img)
    _gradient(draw, W, H, (28, 20, 70), (12, 32, 92))

    # Header
    header_h = 86
    draw.rectangle([0, 0, W, header_h], fill=(24, 22, 58))
    draw.text((40, 18), f"{token} — Technical Overview", fill=(255, 255, 255), font=_font(52, bold=True))
    draw.text((40, 58), f"Network: {chain}", fill=(194, 205, 236), font=_font(22))

    # Chart panel on right
    try:
        chart = Image.open(chart_path).convert("RGBA")
        chart_w, chart_h = 620, 430
        chart = chart.resize((chart_w, chart_h))
        px, py = W - chart_w - 60, 140
        _panel(draw, px-18, py-18, chart_w+36, chart_h+36)
        img.paste(chart, (px, py), chart)
    except Exception as e:
        print("[Overlay] chart paste error:", e)

    # Metrics column (left)
    y = 150
    metrics = [
        ("Price",        _usd(M["price"]), (240, 245, 255)),
        ("24h Change",   _pct(M["pct_24h"]), _chg_col(M["pct_24h"])),
        ("7d Change",    _pct(M["pct_7d"]),  _chg_col(M["pct_7d"])),
        ("Market Cap",   _usd(M["market_cap"]), (230, 235, 255)),
        ("24h Volume",   _usd(M["vol_24h"]), (230, 235, 255)),
        ("From ATH",     _pct(M["from_ath_pct"]), _chg_col(-abs(M["from_ath_pct"]))),
        ("Nearest Support",   _usd(M.get("nearest_support")) if M.get("nearest_support") else "—", (180, 210, 255)),
        ("Nearest Resistance", _usd(M.get("nearest_resistance")) if M.get("nearest_resistance") else "—", (255, 210, 170)),
    ]

    for label, val, col in metrics:
        draw.text((60, y), f"{label}:", font=_font(28), fill=(185,195,215))
        draw.text((300, y), val, font=_font(30, bold=True), fill=col)
        y += 44

    # Analysis box
    summary = _build_summary(token, M)
    box_y = 520
    _panel(draw, 40, box_y-20, W-80, H-80)
    draw.text((60, box_y-2), "Luna’s Analysis", font=_font(26, bold=True), fill=(255,255,255))
    for i, line in enumerate(_wrap(summary, 98)[:6]):
        draw.text((60, box_y + 30 + i*26), line, font=_font(22), fill=(212,218,242))

    out_path = str(Path(out_dir) / f"{token}_{int(time.time())}.png")
    img.save(out_path, quality=92)
    print(f"[Overlay Saved] {out_path}")
    return out_path

# ---------------- Helpers ----------------
def _build_summary(sym, M):
    dir_24 = "up" if float(M.get("pct_24h", 0)) >= 0 else "down"
    dir_7d = "up" if float(M.get("pct_7d", 0)) >= 0 else "down"

    sup = M.get("nearest_support")
    res = M.get("nearest_resistance")

    sup_s = _usd(sup) if sup else "—"
    res_s = _usd(res) if res else "—"

    proj_note = (
        "Projection is a simple linear forecast with ±1σ band over the recent trend; "
        "this is *not* financial advice."
    )

    return (
        f"{sym} shows a {dir_24} move over 24h ({_pct(M.get('pct_24h', 0))}) and "
        f"{dir_7d} over 7d ({_pct(M.get('pct_7d', 0))}). "
        f"Nearest support: {sup_s}; nearest resistance: {res_s}. "
        f"Market cap {_usd(M.get('market_cap', 0))}, 24h volume {_usd(M.get('vol_24h', 0))}. "
        f"From ATH: {_pct(M.get('from_ath_pct', 0))}. "
        f"{proj_note}"
    )

def _panel(draw, x, y, w, h):
    # rounded rectangle panel
    draw.rounded_rectangle([x, y, x + w, y + h], radius=12,
                           fill=(16, 20, 46), outline=(84, 96, 160), width=2)

def _gradient(draw, W, H, c1, c2):
    for i in range(H):
        r = int(c1[0] + (c2[0]-c1[0]) * i / H)
        g = int(c1[1] + (c2[1]-c1[1]) * i / H)
        b = int(c1[2] + (c2[2]-c1[2]) * i / H)
        draw.line([(0, i), (W, i)], fill=(r, g, b))

def _usd(x):
    try:
        f = float(x)
        if f >= 1_000_000_000: return f"${f/1_000_000_000:.2f}B"
        if f >= 1_000_000:     return f"${f/1_000_000:.2f}M"
        if f >= 1_000:         return f"${f/1_000:.2f}K"
        if f >= 1:             return f"${f:,.2f}"
        return f"${f:.6f}"
    except:
        return str(x)

def _pct(x):
    try:
        return f"{float(x):+.2f}%"
    except:
        return str(x)

def _chg_col(v):
    try:
        return (120, 255, 120) if float(v) >= 0 else (255, 120, 120)
    except:
        return (230, 230, 230)

def _font(size=24, bold=False):
    # Use default PIL font if system arial not present
    try:
        if bold:
            return ImageFont.truetype("arialbd.ttf", size)
        return ImageFont.truetype("arial.ttf", size)
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
