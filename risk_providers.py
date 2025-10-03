import os
import time
import requests
from typing import Dict, Any, Optional, List, Tuple
from dotenv import load_dotenv

load_dotenv()

ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "").strip()
BSCSCAN_API_KEY   = os.getenv("BSCSCAN_API_KEY", "").strip()
HELIUS_API_KEY    = os.getenv("HELIUS_API_KEY", "").strip()

ETHERSCAN_BASE = "https://api.etherscan.io/api"
BSCSCAN_BASE   = "https://api.bscscan.com/api"
HELIUS_BASE    = "https://api.helius.xyz"

# ---------- Utilities ----------

def _get(url: str, params: Dict[str, Any], timeout: int = 20) -> Optional[Dict[str, Any]]:
    try:
        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None

def _safe_float(x) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

# ---------- ETH / BSC: Contract verification & ownership ----------

def etherscan_contract_verification(address: str) -> Tuple[Optional[bool], Optional[str]]:
    """
    Returns (verified, contract_name) for an ETH contract, or (None, None) on failure.
    """
    if not ETHERSCAN_API_KEY:
        return None, None
    params = {
        "module": "contract",
        "action": "getsourcecode",
        "address": address,
        "apikey": ETHERSCAN_API_KEY
    }
    data = _get(ETHERSCAN_BASE, params)
    try:
        result = data.get("result", [])
        if not result:
            return None, None
        row = result[0]
        verified = bool(row.get("SourceCode"))
        name = row.get("ContractName") or None
        return verified, name
    except Exception:
        return None, None

def bscscan_contract_verification(address: str) -> Tuple[Optional[bool], Optional[str]]:
    if not BSCSCAN_API_KEY:
        return None, None
    params = {
        "module": "contract",
        "action": "getsourcecode",
        "address": address,
        "apikey": BSCSCAN_API_KEY
    }
    data = _get(BSCSCAN_BASE, params)
    try:
        result = data.get("result", [])
        if not result:
            return None, None
        row = result[0]
        verified = bool(row.get("SourceCode"))
        name = row.get("ContractName") or None
        return verified, name
    except Exception:
        return None, None

# ---------- ETH / BSC: Top holders (best-effort) ----------

def etherscan_top_holders(token_address: str, top_n: int = 10) -> Optional[List[Dict[str, Any]]]:
    """
    Etherscan 'tokenholderlist' is a Pro endpoint; we try it and degrade gracefully if unavailable.
    Returns list of {address, balance} or None.
    """
    if not ETHERSCAN_API_KEY:
        return None
    params = {
        "module": "token",
        "action": "tokenholderlist",
        "contractaddress": token_address,
        "page": 1,
        "offset": top_n,
        "apikey": ETHERSCAN_API_KEY
    }
    data = _get(ETHERSCAN_BASE, params)
    try:
        if not data or data.get("status") != "1":
            return None
        holders = data.get("result", [])
        out = []
        for h in holders:
            out.append({
                "address": h.get("TokenHolderAddress"),
                "balance": h.get("TokenHolderQuantity")
            })
        return out or None
    except Exception:
        return None

def bscscan_top_holders(token_address: str, top_n: int = 10) -> Optional[List[Dict[str, Any]]]:
    if not BSCSCAN_API_KEY:
        return None
    params = {
        "module": "token",
        "action": "tokenholderlist",
        "contractaddress": token_address,
        "page": 1,
        "offset": top_n,
        "apikey": BSCSCAN_API_KEY
    }
    data = _get(BSCSCAN_BASE, params)
    try:
        if not data or data.get("status") != "1":
            return None
        holders = data.get("result", [])
        out = []
        for h in holders:
            out.append({
                "address": h.get("TokenHolderAddress"),
                "balance": h.get("TokenHolderQuantity")
            })
        return out or None
    except Exception:
        return None

def aggregate_percent_from_holders(holders: Optional[List[Dict[str, Any]]]) -> Optional[float]:
    """
    If the API returns raw balances without total supply, we can't compute a true percentage.
    Many public endpoints omit denominator; return None if unknown.
    (We still can use holder count as a rough dispersal signal later.)
    """
    return None

# ---------- Solana (Helius): Top holders best-effort ----------

def helius_top_holders(mint: str, top_n: int = 10) -> Optional[List[Dict[str, Any]]]:
    """
    Helius has token holder / balances endpoints in various API versions.
    We'll call a balances style endpoint if available; degrade on failure.
    """
    if not HELIUS_API_KEY:
        return None
    url = f"{HELIUS_BASE}/v0/token-accounts"
    params = {
        "api-key": HELIUS_API_KEY,
        "mint": mint,
        "page": 1,
        "page-size": top_n,
        "displayOptions": "none"
    }
    data = _get(url, params)
    try:
        accounts = data.get("token_accounts", [])
        out = []
        for acc in accounts:
            out.append({
                "address": acc.get("owner"),
                "balance": acc.get("amount")
            })
        return out or None
    except Exception:
        return None

# ---------- Public wrapper ----------

def fetch_deep_risk(chain: str, contract_or_mint: str) -> Dict[str, Any]:
    """
    Returns deep risk info:
      - verified (bool/None)
      - contract_name (str/None)
      - top_holders (list or None)
      - top_holders_notes (str/None)
    Degrades gracefully if provider/endpoint is unavailable.
    """
    chain = (chain or "").lower()
    verified = None
    contract_name = None
    top_holders = None
    holders_notes = None

    if chain in ("ethereum", "base", "arbitrum", "optimism", "polygon", "avalanche", "fantom", "linea", "blast"):
        v, nm = etherscan_contract_verification(contract_or_mint)
        verified = v if v is not None else verified
        contract_name = nm or contract_name
        th = etherscan_top_holders(contract_or_mint, top_n=10)
        top_holders = th or top_holders
        if th is None:
            holders_notes = "Top-holder API unavailable (Etherscan Pro or rate-limited)."
    elif chain in ("bsc",):
        v, nm = bscscan_contract_verification(contract_or_mint)
        verified = v if v is not None else verified
        contract_name = nm or contract_name
        th = bscscan_top_holders(contract_or_mint, top_n=10)
        top_holders = th or top_holders
        if th is None:
            holders_notes = "Top-holder API unavailable (BscScan Pro or rate-limited)."
    elif chain in ("sol", "solana"):
        th = helius_top_holders(contract_or_mint, top_n=10)
        top_holders = th or top_holders
        if th is None:
            holders_notes = "Top-holder data unavailable (Helius key required or endpoint unsupported)."
    else:
        holders_notes = "Unsupported chain for deep risk providers."

    return {
        "verified_contract": verified,
        "contract_name": contract_name,
        "top_holders": top_holders,
        "top_holders_notes": holders_notes
    }
