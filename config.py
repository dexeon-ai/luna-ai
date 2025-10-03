import os
from dotenv import load_dotenv

load_dotenv()

# Token & pair snapshot
DEXSCREENER_TOKEN_API = "https://api.dexscreener.com/latest/dex/tokens/{contract}"

# Candles (bars). Unofficial but widely used; we guard with try/except.
# tf can be: 5m, 15m, 1h, 4h, 1d (we'll use 5m + 1h + 1d as needed)
DEXSCREENER_BARS_API = "https://api.dexscreener.com/charts/bars/{chain_id}/{pair_address}?tf={tf}&from={from_ts}&to={to_ts}"

# Optional keys for future blocks (not used here)
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "")
BSCSCAN_API_KEY   = os.getenv("BSCSCAN_API_KEY", "")
HELIUS_API_KEY    = os.getenv("HELIUS_API_KEY", "")
COVALENT_API_KEY  = os.getenv("COVALENT_API_KEY", "")
BITQUERY_API_KEY  = os.getenv("BITQUERY_API_KEY", "")
