import os, json, time, requests, random
from pathlib import Path
from datetime import datetime, timedelta
from openai import OpenAI

# Load your API key
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

BASE = Path("C:/Users/jmpat/Desktop/Luna AI/luna_cache/data/snapshots")
BASE.mkdir(parents=True, exist_ok=True)

# Load coin list (make sure assets_master.csv or similar exists)
COINS_FILE = Path("C:/Users/jmpat/Desktop/Luna AI/luna_cache/data/coins_master.csv")
if not COINS_FILE.exists():
    raise FileNotFoundError("Missing coins_master.csv — please ensure the coin list is built.")

with open(COINS_FILE, "r", encoding="utf-8") as f:
    coins = [line.strip().split(",")[0] for line in f.readlines() if line.strip()]

def fetch_data(coin_id):
    """Try CoinGecko first, then Dexscreener if unavailable."""
    try:
        cg_url = f"https://api.coingecko.com/api/v3/coins/{coin_id.lower()}"
        r = requests.get(cg_url, timeout=10)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    # fallback for Solana-style tickers
    try:
        dx_url = f"https://api.dexscreener.io/latest/dex/search?q={coin_id}"
        r = requests.get(dx_url, timeout=10)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None

def analyze_with_openai(coin_id, data):
    """Use GPT-4o-mini to interpret market data into a concise analysis."""
    try:
        summary = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are Luna, an AI crypto analyst."},
                {"role": "user", "content": f"Summarize key insights for {coin_id} from this JSON:\n{json.dumps(data)[:7000]}"}
            ],
            temperature=0.4,
            max_tokens=250
        )
        return summary.choices[0].message.content
    except Exception as e:
        return f"[Error during OpenAI analysis: {e}]"

def save_snapshot(coin_id, data, analysis):
    out = BASE / f"{coin_id}.json"
    payload = {
        "coin_id": coin_id,
        "source": "coingecko_or_dex",
        "fetched_at": datetime.utcnow().isoformat(),
        "market_data": data or {},
        "analysis": analysis
    }
    with open(out, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

def refresh_all(batch_limit=None):
    total = len(coins)
    done, last_report = 0, time.time()

    for coin_id in coins[:batch_limit or total]:
        data = fetch_data(coin_id)
        if not data:
            print(f"[Skip] {coin_id} — no valid data found")
            continue
        analysis = analyze_with_openai(coin_id, data)
        save_snapshot(coin_id, data, analysis)
        done += 1

        # log progress every 15 min
        if time.time() - last_report > 900:
            pct = (done / total) * 100
            print(f"[Progress] {done}/{total} coins ({pct:.2f}%) complete at {datetime.now().strftime('%H:%M:%S')}")
            last_report = time.time()

        time.sleep(random.uniform(0.3, 0.7))  # throttle requests

    print(f"[Done ✅] Processed {done}/{total} coins at {datetime.utcnow().isoformat()}")

if __name__ == "__main__":
    print(f"[Luna] 🔁 Refreshing {len(coins)} coins...")
    refresh_all()
