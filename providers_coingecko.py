import json, os, requests
from typing import Optional, Dict, Any

COINGECKO_BASE = "https://api.coingecko.com/api/v3"
CG_MAP_FILE = "cg_tokens.json"

class CoinGeckoClient:
    def __init__(self):
        self.base = COINGECKO_BASE
        self.map = {}
        if os.path.exists(CG_MAP_FILE):
            try:
                with open(CG_MAP_FILE, "r") as f:
                    self.map = json.load(f)
            except Exception:
                self.map = {}

    def save_map(self):
        try:
            with open(CG_MAP_FILE, "w") as f:
                json.dump(self.map, f, indent=2)
        except Exception:
            pass

    def resolve_id(self, symbol_or_name: str) -> Optional[str]:
        """Look up CoinGecko ID from local map, else API search."""
        s = (symbol_or_name or "").lower()
        if not s:
            return None
        if s in self.map:
            return self.map[s]

        # Hit CG search
        try:
            url = f"{self.base}/search?query={s}"
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            data = r.json() or {}
            coins = data.get("coins", [])
            if coins:
                cg_id = coins[0]["id"]
                self.map[s] = cg_id
                self.save_map()
                return cg_id
        except Exception:
            return None
        return None

    def token_info(self, cg_id: str) -> Optional[Dict[str, Any]]:
        try:
            url = f"{self.base}/coins/{cg_id}"
            r = requests.get(url, timeout=15)
            r.raise_for_status()
            return r.json()
        except Exception:
            return None
