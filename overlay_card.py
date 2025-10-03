import os
from PIL import Image, ImageDraw, ImageFont

def make_overlay_card(snapshot: dict, out_dir="overlays"):
    if not snapshot.get("ok"):
        return None

    os.makedirs(out_dir, exist_ok=True)
    token = snapshot["token"].get("symbol", "UNKNOWN")
    chain = snapshot.get("chain", "")
    tldr = snapshot.get("tldr", "")

    # Card size
    W, H = 1000, 600
    img = Image.new("RGB", (W, H), color=(20, 20, 30))
    draw = ImageDraw.Draw(img)

    # Fonts
    try:
        font_title = ImageFont.truetype("arialbd.ttf", 48)
        font_text = ImageFont.truetype("arial.ttf", 28)
    except:
        font_title = ImageFont.load_default()
        font_text = ImageFont.load_default()

    # Title
    title = f"{token} on {chain}".upper()
    draw.text((40, 30), title, font=font_title, fill=(255, 255, 255))

    # Extract fields
    market = snapshot.get("market", {})
    risk = snapshot.get("risk", {})
    what_if = snapshot.get("what_if", {})
    lp = snapshot.get("lp_analysis", {})
    community = snapshot.get("community", {})
    sentiment = snapshot.get("sentiment", "")

    lines = []
    if market.get("fdv_usd"):
        lines.append(f"FDV: {usd_fmt(market.get('fdv_usd'))}")
    if market.get("price_usd"):
        lines.append(f"Price: {market.get('price_usd')}")
    if market.get("pct_change"):
        pct = market["pct_change"]
        lines.append(f"1h: {pct.get('h1')}% | 24h: {pct.get('h24')}%")
    lines.append(f"Liquidity: {usd_fmt(market.get('liquidity_usd'))} | Vol 24h: {usd_fmt(market.get('volume_24h_usd'))}")
    lines.append(f"Safety: {risk.get('safety_gauge')}/10 | Mood: {risk.get('mood')}")
    if what_if.get("buy_hold", {}).get("h24"):
        lines.append(f"$1000 → {what_if['buy_hold']['h24']} (24h)")
    if lp.get("apr_est"):
        lines.append(f"LP APR: {lp['apr_est']}% | Slippage $1K: {lp.get('slippage_1k_pct')}%")
    if community:
        lines.append(f"Twitter: {community.get('twitter_followers')} | Telegram: {community.get('telegram_users')}")
    if sentiment:
        lines.append(f"Sentiment: {sentiment}")

    # Draw lines
    y = 120
    for line in lines:
        draw.text((40, y), str(line), font=font_text, fill=(200, 200, 220))
        y += 40

    # Save
    out_path = os.path.join(out_dir, f"{token}_{chain}.png")
    img.save(out_path)
    return out_path

def usd_fmt(x):
    try:
        if x is None: return "n/a"
        val = float(x)
        if val >= 1_000_000_000: return f"${val/1_000_000_000:.2f}B"
        if val >= 1_000_000: return f"${val/1_000_000:.2f}M"
        if val >= 1_000: return f"${val/1_000:.2f}K"
        return f"${val:.2f}"
    except:
        return str(x)
