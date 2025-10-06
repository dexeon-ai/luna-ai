# overlay_card.py — Luna Broadcast Overlay vFinal (MOMO on Solana)
import os, io, math, time, requests
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

DEX_API = "https://api.dexscreener.com/latest/dex/pairs"

def make_overlay_card(snapshot: dict, out_dir="/tmp/overlays"):
    os.makedirs(out_dir, exist_ok=True)

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

    # --- canvas ---
    W, H = 1280, 720
    img = Image.new("RGB", (W, H), (12, 10, 25))
    draw = ImageDraw.Draw(img)
    _gradient(draw, W, H, (40, 10, 80), (10, 30, 90))

    # --- title bar ---
    header_h = 90
    draw.rectangle([0, 0, W, header_h], fill=(25, 15, 55))
    draw.text((40, 20), f"{token} ({chain})", fill=(255, 255, 255), font=_font(52))

    # --- sparkline ---
    _sparkline(draw, chain.lower(), snapshot.get("contract"), W-420, 60, 340, 120)

    # --- metric grid ---
    label_font = _font(28)
    value_font = _font(30)
    start_x, start_y = 60, 140
    spacing = 50
    def metric(lbl, val, color=(220,230,255)):
        nonlocal start_y
        draw.text((start_x, start_y), f"{lbl}:", font=label_font, fill=(180,190,210))
        draw.text((start_x+240, start_y), str(val), font=value_font, fill=color)
        start_y += spacing

    metric("Price", f"${price}")
    metric("24h Change", f"{change}%", _chg_col(change))
    metric("24h Volume", f"${volume}")
    metric("Liquidity", f"${liq}")
    metric("FDV", f"${fdv}")
    metric("Safety", f"{risk}/10")
    metric("Mood", mood, _mood_col(mood))

    # --- gauge + emoji ---
    cx, cy, r = 1060, 230, 90
    _gauge(draw, cx, cy, r, risk)
    draw.text((cx-25, cy-35), _emoji(mood), font=_font(72), fill=(255,255,255))

    # --- summary ---
    box_y = 420
    draw.rectangle([40, box_y-20, W-40, H-40], fill=(15,20,45), outline=(80,90,150), width=2)
    draw.text((60, box_y), "Luna’s Analysis:", font=_font(28), fill=(255,255,255))
    y = box_y+40
    for line in _wrap(summary, 95)[:9]:
        draw.text((80, y), line, font=_font(24), fill=(215,220,245))
        y += 28

    fn = f"{token}_{int(time.time())}.png"
    path = os.path.join(out_dir, fn)
    img.save(path)
    print(f"[Overlay Saved] {path}")
    return path


# ---------- helpers ----------
def _sparkline(draw, chain, contract, x, y, w, h):
    if not contract: return
    try:
        url = f"{DEX_API}/{chain}/{contract}"
        r = requests.get(url, timeout=6)
        data = r.json()
        pair = (data.get("pairs") or [None])[0]
        if not pair: return
        pts = [float(p) for p in (pair.get("sparkline") or [])[-50:]]
        if not pts: return
        mx, mn = max(pts), min(pts)
        sx, sy = w/len(pts), h/(mx-mn+1e-6)
        px = [x+i*sx for i in range(len(pts))]
        py = [y+h-(n-mn)*sy for n in pts]
        draw.line(list(zip(px,py)), fill=(140,200,255), width=3)
    except Exception as e:
        print("[Sparkline]", e)

def _gradient(draw, W,H,c1,c2):
    for i in range(H):
        r = int(c1[0]+(c2[0]-c1[0])*i/H)
        g = int(c1[1]+(c2[1]-c1[1])*i/H)
        b = int(c1[2]+(c2[2]-c1[2])*i/H)
        draw.line([(0,i),(W,i)], fill=(r,g,b))

def _gauge(draw,cx,cy,r,val):
    try: val=float(val)
    except: val=5
    for i in range(0,180,3):
        c=(60,60,70)
        if i/18<val: c=(int(25*i/3),255-int(20*i/3),100)
        a=math.radians(180+i)
        x1=cx+int(r*math.cos(a)); y1=cy+int(r*math.sin(a))
        draw.line([(cx,cy),(x1,y1)], fill=c,width=4)

def _chg_col(c):
    try:
        v=float(str(c).replace("%",""))
        return (120,255,120) if v>=0 else (255,100,100)
    except: return (230,230,230)

def _mood_col(m):
    s=str(m).lower()
    if "calm" in s: return (100,255,120)
    if "rolling" in s: return (255,210,100)
    if "storm" in s: return (255,120,120)
    return (220,220,255)

def _emoji(m):
    s=str(m).lower()
    if "calm" in s: return "😌"
    if "rolling" in s: return "🌊"
    if "storm" in s: return "🌪️"
    return "✨"

def _wrap(t,w=85):
    wd,ln,cur=t.split(),[],[]
    for x in wd:
        cur.append(x)
        if len(" ".join(cur))>=w: ln.append(" ".join(cur));cur=[]
    if cur: ln.append(" ".join(cur))
    return ln

def _fmt(x):
    try:
        f=float(x)
        if f>=1_000_000_000: return f"{f/1_000_000_000:.2f}B"
        if f>=1_000_000: return f"{f/1_000_000:.2f}M"
        if f>=1000: return f"{f/1000:.2f}K"
        return f"{f:.4f}"
    except: return str(x)

def _font(s=24):
    try: return ImageFont.truetype("arial.ttf",s)
    except: return ImageFont.load_default()
