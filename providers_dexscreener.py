import time
import requests
from typing import Optional, Dict, Any, List, Tuple
from config import DEXSCREENER_TOKEN_API, DEXSCREENER_BARS_API
from utils import pick_best_pair

class DexscreenerClient:
    def __init__(self):
        self.base = DEXSCREENER_TOKEN_API

    # ---------- Snapshots ----------
    def fetch_pairs_for_token(self, contract: str) -> List[Dict[str, Any]]:
        url = self.base.format(contract=contract)
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json() or {}
        return data.get("pairs") or []

    def best_pair_snapshot(self, contract: str, wanted_chain: Optional[str] = None) -> Optional[Dict[str, Any]]:
        pairs = self.fetch_pairs_for_token(contract)
        if not pairs:
            return None
        best = pick_best_pair(pairs, wanted_chain=wanted_chain)
        return best

    # ---------- Candles (bars) ----------
    def _bars_url(self, chain_id: str, pair_address: str, tf: str, from_ts: int, to_ts: int) -> str:
        return DEXSCREENER_BARS_API.format(chain_id=chain_id, pair_address=pair_address, tf=tf, from_ts=from_ts, to_ts=to_ts)

    def fetch_bars(self, chain_id: str, pair_address: str, tf: str, lookback_seconds: int) -> List[Dict[str, Any]]:
        """
        Returns a list of bars with keys: t, o, h, l, c, v (if available).
        If the API changes or is unavailable, returns [] gracefully.
        """
        try:
            now = int(time.time())
            frm = now - lookback_seconds
            url = self._bars_url(chain_id, pair_address, tf, frm * 1000, now * 1000)  # API expects ms
            r = requests.get(url, timeout=15)
            r.raise_for_status()
            data = r.json() or {}

            # Response may be {"bars":[...]} or {"series":[...]}
            bars = data.get("bars")
            if bars is None:
                bars = data.get("series")
            if not bars:
                return []

            # Normalize keys to t,o,h,l,c,v
            norm = []
            for b in bars:
                norm.append({
                    "t": b.get("t") or b.get("time"),
                    "o": b.get("o") or b.get("open"),
                    "h": b.get("h") or b.get("high"),
                    "l": b.get("l") or b.get("low"),
                    "c": b.get("c") or b.get("close"),
                    "v": b.get("v") or b.get("volume"),
                })
            return norm
        except Exception:
            return []
