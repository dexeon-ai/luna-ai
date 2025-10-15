from flask import Flask, render_template, abort
from config import Settings as S

app = Flask(__name__)

COIN_NAMES = {
    "bitcoin": "Bitcoin", "btc": "Bitcoin",
    "eth": "Ethereum", "ethereum": "Ethereum",
    "sol": "Solana", "solana": "Solana",
    "bonk": "Bonk", "pepe": "PEPE", "dogwifhat": "dogwifhat"
}

DEFAULT_BLURBS = {
    "price": "Shows recent price movement. Watch for support or breakout levels.",
    "rsi": "RSI(14) – Momentum oscillator above 70 = overbought, below 30 = oversold.",
    "bb": "Bollinger Bands(20, 2) – Volatility envelope that tightens before large moves.",
    "macd": "MACD(12, 26, 9) – Crossovers between MACD & signal lines indicate momentum shifts.",
    "trend": "OLS trendline – Shows directional bias over recent data.",
    "volume": "24 h trading volume; spikes = trend confirmation.",
    "marketcap": "Total network value; tracks money flow in/out of the asset.",
    "liquidity": "Depth of orderbooks; low liquidity can amplify price moves."
}

def build_img_map(symbol: str):
    base = S.OVERLAYS_BASE_URL.rstrip("/") + "/"
    s = symbol.lower()
    return {
        "center": f"{base}{s}_matrix.png",
        "price": f"{base}{s}_price.png",
        "rsi": f"{base}{s}_rsi.png",
        "bb": f"{base}{s}_bb.png",
        "macd": f"{base}{s}_macd.png",
        "trend": f"{base}{s}_trend.png",
        "volume": f"{base}{s}_volume.png",
        "marketcap": f"{base}{s}_marketcap.png",
        "liquidity": f"{base}{s}_liquidity.png",
    }

@app.route("/")
def home():
    return render_template("base.html", brand=S.BRAND_NAME)

@app.route("/response/<symbol>")
def response(symbol):
    s = symbol.lower()
    if not s:
        abort(404)
    name = COIN_NAMES.get(s, s.upper())
    imgs = build_img_map(s)
    blurbs = DEFAULT_BLURBS
    return render_template(
        "response_page.html",
        brand=S.BRAND_NAME,
        coin_name=name,
        coin_symbol=s.upper(),
        imgs=imgs,
        blurbs=blurbs,
    )

if __name__ == "__main__":
    app.run(debug=True, port=8080)
