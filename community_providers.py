import os
import requests
from dotenv import load_dotenv

load_dotenv()

COINGECKO_API = "https://api.coingecko.com/api/v3"
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
X_API_BEARER = os.getenv("X_API_BEARER", "").strip()

# -----------------------------
# CoinGecko community fallback
# -----------------------------
def fetch_from_coingecko(cg_id: str):
    try:
        url = f"{COINGECKO_API}/coins/{cg_id}"
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()
        comm = data.get("community_data", {})
        return {
            "twitter_followers": comm.get("twitter_followers"),
            "reddit_subs": comm.get("reddit_subscribers"),
            "telegram_users": comm.get("telegram_channel_user_count"),
            "source": "coingecko"
        }
    except Exception:
        return None

# -----------------------------
# Telegram group member count
# -----------------------------
def fetch_telegram_members(group_username: str):
    """
    group_username = Telegram @handle without @
    Requires TG_BOT_TOKEN and bot added to group as admin.
    """
    if not TG_BOT_TOKEN:
        return None
    try:
        url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/getChatMembersCount"
        resp = requests.get(url, params={"chat_id": f"@{group_username}"}, timeout=20)
        data = resp.json()
        if data.get("ok"):
            return {"telegram_users": data.get("result"), "source": "telegram"}
        return None
    except Exception:
        return None

# -----------------------------
# X / Twitter API (stubbed)
# -----------------------------
def fetch_x_account(handle: str):
    """
    handle = twitter username without @
    Requires X_API_BEARER (Basic tier). Stubbed until you subscribe.
    """
    if not X_API_BEARER:
        return None
    try:
        url = "https://api.twitter.com/2/users/by"
        params = {
            "usernames": handle,
            "user.fields": "public_metrics,verified,description,created_at,profile_image_url"
        }
        headers = {"Authorization": f"Bearer {X_API_BEARER}"}
        r = requests.get(url, params=params, headers=headers, timeout=20)
        r.raise_for_status()
        data = r.json()
        if "data" in data:
            u = data["data"][0]
            return {
                "twitter_followers": u.get("public_metrics", {}).get("followers_count"),
                "twitter_verified": u.get("verified"),
                "twitter_bio": u.get("description"),
                "source": "x"
            }
        return None
    except Exception:
        return None

# -----------------------------
# Public wrapper
# -----------------------------
def fetch_community(cg_id: str = None, tg_group: str = None, x_handle: str = None):
    """
    Attempts sources in order:
      - CoinGecko (if cg_id provided)
      - Telegram (if tg_group provided and TG_BOT_TOKEN set)
      - X (if x_handle provided and X_API_BEARER set)
    Returns merged dict.
    """
    result = {}

    if cg_id:
        cg_data = fetch_from_coingecko(cg_id)
        if cg_data: result.update({k: v for k, v in cg_data.items() if v})

    if tg_group:
        tg_data = fetch_telegram_members(tg_group)
        if tg_data: result.update({k: v for k, v in tg_data.items() if v})

    if x_handle:
        x_data = fetch_x_account(x_handle)
        if x_data: result.update({k: v for k, v in x_data.items() if v})

    if not result:
        result = {"note": "No community data available from configured sources."}

    return result
