# luna_host.py
import json, os, time

TREND_FILE = "luna_trends.json"

def get_trend(asset_name):
    """Return simple summary of the latest trend for a given asset."""
    if not os.path.exists(TREND_FILE):
        return "I don't have any recent data yet. Please try again soon."

    with open(TREND_FILE, "r") as f:
        trends = json.load(f)

    asset_name = asset_name.lower()
    if asset_name not in trends:
        return f"I haven’t tracked {asset_name.upper()} recently."

    t = trends[asset_name]
    return (
        f"As of {t['timestamp']}, {asset_name.upper()} is {t['direction']} "
        f"{abs(t['change_percent'])}% since the previous hour, "
        f"now priced around ${t['current_price']}."
    )

if __name__ == "__main__":
    while True:
        asset = input("Ask Luna about a coin (e.g. BONK, PEPE, SOL): ").strip()
        if asset.lower() in ["exit", "quit"]:
            break
        print(get_trend(asset))
        print()
