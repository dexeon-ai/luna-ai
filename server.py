# ===========================================================
# server.py — Luna AI Backend v9 (text + chart from cached CSVs)
# ===========================================================
import os, time, json, csv, logging
from pathlib import Path
from flask import Flask, request, jsonify, send_from_directory, abort, send_file
from flask_cors import CORS
import requests  # For TTS fallback

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === API Key Configuration ===
CG_API_KEY = os.getenv("COINGECKO_API_KEY", "CG-mVmG196hWJwz9tjqLZvmmTSQ").strip()
if not CG_API_KEY or CG_API_KEY == "CG-mVmG196hWJwz9tjqLZvmmTSQ":
    logger.warning("Using fallback API key—set COINGECKO_API_KEY in Render env or system variables with a valid Pro key")
CG_BASE = "https://api.coingecko.com/api/v3" if CG_API_KEY else "https://api.coingecko.com/api/v3"  # Force free endpoint for Demo key
CG_HEADERS = {"accept": "application/json", "x-cg-pro-api-key": CG_API_KEY.strip()} if CG_API_KEY else {}
USE_CG_API = True  # Default to enabled, adjust below

# Validate API key on startup
if CG_API_KEY:
    try:
        # Test with a clean key
        clean_key = CG_API_KEY.replace(" ", "")
        test_headers = {"accept": "application/json", "x-cg-pro-api-key": clean_key}
        response = requests.get(f"{CG_BASE}/ping", headers=test_headers, timeout=5)
        if response.status_code == 401 or response.status_code == 403:  # Unauthorized or forbidden
            logger.warning(f"API key rejected: {response.text}. Using free tier with Demo key.")
        elif response.status_code != 200:
            logger.error(f"Unexpected response from CoinGecko ping: {response.status_code} - {response.text}")
            USE_CG_API = False
        else:
            logger.info(f"CoinGecko API key validated successfully with key: {clean_key[:8]}...")  # Mask key
        CG_HEADERS["x-cg-pro-api-key"] = clean_key  # Update with cleaned key
    except requests.exceptions.RequestException as e:
        logger.error(f"CoinGecko API key validation failed: {e}. Request: {e.response.text if hasattr(e, 'response') else 'No response'}. Falling back to local data.")
        USE_CG_API = False
else:
    logger.warning("COINGECKO_API_KEY not set or invalid, falling back to local data only")
    USE_CG_API = False

logger.info(f"Using CoinGecko API base: {CG_BASE}, Key set: {bool(CG_API_KEY)}, API enabled: {USE_CG_API}")

ELEVENLABS_API_KEY = os.getenv("ELEVENLABS_API_KEY", "")  # Optional TTS fallback

# ---- Internal Modules ----
from session_manager import manager
from snapshot import build_snapshot
from overlay_card import make_overlay_card
from voice_adapter import tts_generate  # Existing TTS, will fallback
from risk_providers import fetch_deep_risk
from macro_providers import build_macro_summary
from qa_handler import handle_question
from plot_engine import build_tech_panel  # Matrix chart engine

# ---- Quick-Resolver Modules ----
from quick_resolver import quick_resolve
from quick_snapshot import quick_snapshot
from merge_contracts import main as merge_all

# ===========================================================
# Directories — Render-safe (voice on /voice, overlays in /tmp)
# ===========================================================
ROOT = Path(__file__).resolve().parent
VOICE_DIR = ROOT / "voice"
OVERLAY_DIR = Path("/tmp/overlays")
ASSETS_DIR = ROOT

VOICE_DIR.mkdir(exist_ok=True)
OVERLAY_DIR.mkdir(parents=True, exist_ok=True)

print(">>> Luna AI backend starting …")
print(">>> Voice directory:", VOICE_DIR)
print(">>> Overlay directory:", OVERLAY_DIR)

# ===========================================================
# Flask Setup
# ===========================================================
app = Flask(__name__)
CORS(app, supports_credentials=True, origins="*")

print(">>> Routes loaded: /ping, /qa, /voice/*, /overlays/*, /cache/status, /analyze, /chart/data, /overlay/analysis")

# ---------------- Shared Paths for Chart Data ----------------
DATA_ROOT = ROOT / "luna_cache" / "data"
COIN_DIR = DATA_ROOT / "coins"
MEME_DIR = DATA_ROOT / "memes"
SESSION_LAST = {}  # sid -> {"symbol": "BTC"}

# ----------------------------------------------------------- 
# Health
# ----------------------------------------------------------- 
@app.get("/ping")
def ping():
    return jsonify({"ok": True, "message": "pong", "ts": int(time.time())})

# ----------------------------------------------------------- 
# Session Management
# ----------------------------------------------------------- 
@app.post("/session/start")
def session_start():
    ttl = int(os.getenv("SESSION_TTL_SECONDS", "1800"))
    sid = manager.start(ttl_seconds=ttl)
    token_balance = 100  # Mock initial Luna Token balance (replace with blockchain call)
    SESSION_LAST[sid] = {"symbol": "BTC", "token_balance": token_balance}
    return jsonify({
        "ok": True,
        "session_id": sid,
        "questions_left": 21,
        "ttl_seconds": ttl,
        "token_balance": token_balance
    })

@app.post("/session/status")
@app.get("/session/status")
def session_status():
    data = request.get_json(silent=True) or {}
    sid = data.get("session_id") or request.args.get("session_id")
    if not sid:
        logger.warning("Missing session_id in /session/status request")
        return jsonify({"ok": False, "error": "missing session_id", "remaining_questions": 0, "token_balance": 0}), 200
    if not manager.touch(sid):
        logger.warning(f"Session {sid} not found or expired")
        return jsonify({"ok": False, "error": "invalid or expired session", "remaining_questions": 0, "token_balance": 0}), 200
    session = SESSION_LAST.get(sid, {})
    return jsonify({
        "ok": True,
        "remaining_questions": manager.remaining(sid),
        "token_balance": session.get("token_balance", 0)
    })

@app.post("/session/end")
def session_end():
    data = request.get_json(silent=True) or {}
    sid = data.get("session_id")
    if not sid:
        return jsonify({"ok": False, "error": "missing session_id"}), 400
    history, text = manager.end(sid)
    if sid in SESSION_LAST:
        del SESSION_LAST[sid]
    return jsonify({
        "ok": True,
        "message": "session closed",
        "history": history,
        "history_text": text
    })

# ----------------------------------------------------------- 
# /analyze (Original Structured-Chain Endpoint)
# ----------------------------------------------------------- 
@app.post("/analyze")
def analyze_chain():
    data = request.get_json(silent=True) or {}
    sid = data.get("session_id")
    chain = data.get("chain")
    contract = data.get("contract")
    overlay = bool(data.get("overlay", False))

    if not sid or not chain or not contract:
        return jsonify({"ok": False, "error": "missing session_id/chain/contract"}), 400
    if not manager.touch(sid):
        return jsonify({"ok": False, "error": "invalid or expired session"}), 403

    snap = build_snapshot(chain, contract)
    if not snap.get("ok"):
        return jsonify({"ok": False, "error": snap.get("error", "snapshot failed")}), 500

    card_url = None
    if overlay and USE_CG_API:
        path = make_overlay_card(snap, out_dir=str(OVERLAY_DIR))
        if path:
            card_url = f"/overlays/{Path(path).name}"

    manager.add_history(sid, "system", f"[analyze] {chain}:{contract}")
    return jsonify({"ok": True, "snapshot": snap, "overlay_card_url": card_url})

# ----------------------------------------------------------- 
# /risk & /macro
# ----------------------------------------------------------- 
@app.post("/risk")
def risk():
    data = request.get_json(silent=True) or {}
    chain = data.get("chain")
    contract = data.get("contract")
    if not chain or not contract:
        return jsonify({"ok": False, "error": "missing chain/contract"}), 400
    return jsonify(fetch_deep_risk(chain, contract))

@app.post("/macro")
def macro():
    return jsonify(build_macro_summary())

# ----------------------------------------------------------- 
# /qa — Main Interactive Route (21 Questions Mode)
# ----------------------------------------------------------- 
@app.post("/qa")
def qa():
    data = request.get_json(silent=True) or {}
    sid = data.get("session_id")
    if not sid:
        return jsonify({"ok": False, "error": "missing session_id"}), 400
    if not manager.touch(sid):
        return jsonify({"ok": False, "error": "invalid or expired session"}), 403

    session = SESSION_LAST.get(sid, {})
    remaining = manager.decrement(sid)
    if remaining < 0 or (session.get("token_balance", 0) < 1 and remaining == 21):
        return jsonify({"ok": False, "error": "Insufficient Luna Tokens or questions exceeded", "remaining_questions": 0}), 403

    result = handle_question(data) or {}
    text_for_tts = (
        result.get("summary")
        or result.get("tldr")
        or result.get("message")
        or result.get("error")
        or "Answer ready."
    )

    try:
        if result.get("symbol") and USE_CG_API:
            SESSION_LAST[sid] = {"symbol": result["symbol"], "token_balance": session.get("token_balance", 0) - 1}
            overlay_path = build_tech_panel(result["symbol"])
            overlay_url = f"/overlays/{Path(overlay_path).name}"
        else:
            overlay_url = None
    except Exception as e:
        logger.error(f"[Overlay Error] {e}")
        overlay_url = None

    manager.add_history(sid, "user", json.dumps({k: v for k, v in data.items() if k != "session_id"})[:500])
    manager.add_history(sid, "assistant", text_for_tts)

    # TTS with ElevenLabs fallback
    voice_url, lipsync_url = None, None
    try:
        audio_path = tts_generate(text_for_tts, base_name=f"reply_{int(time.time())}", out_dir=str(VOICE_DIR))
        if audio_path:
            name = Path(audio_path).name
            voice_url = f"/voice/{name}"
            json_candidate = Path(audio_path).with_suffix(".json")
            if json_candidate.exists():
                lipsync_url = f"/voice/{json_candidate.name}"
    except Exception as e:
        logger.warning(f"[TTS Error] Local TTS failed: {e}")
        if ELEVENLABS_API_KEY:
            try:
                response = requests.post(
                    "https://api.elevenlabs.io/v1/text-to-speech/21m00Tcm4TlvDq8ikWAM",
                    headers={"xi-api-key": ELEVENLABS_API_KEY, "Content-Type": "application/json"},
                    json={"text": text_for_tts, "voice_settings": {"stability": 0.5, "similarity_boost": 0.5}}
                )
                response.raise_for_status()
                audio_path = VOICE_DIR / f"eleven_{int(time.time())}.wav"
                with open(audio_path, "wb") as f:
                    f.write(response.content)
                voice_url = f"/voice/{audio_path.name}"
                # Lipsync generation would require additional API
            except Exception as e:
                logger.error(f"[ElevenLabs Error] {e}")
                voice_url = "/voice/placeholder.wav"

    payload = {
        "ok": bool(result.get("ok", True)),
        "answer": text_for_tts,
        "overlay_url": overlay_url,
        "voice_path": voice_url,
        "lipsync_path": lipsync_url,
        "remaining_questions": remaining,
        "symbol": result.get("symbol"),
        "metrics": result.get("metrics", {}),
        "token_balance": session.get("token_balance", 0) - 1
    }
    if remaining <= 0:
        payload["notice"] = "Session limit reached. Use /session/end to copy the transcript."
    return jsonify(payload)

# ----------------------------------------------------------- 
# /cache/status — Inspect Luna’s Cached Market Data
# ----------------------------------------------------------- 
@app.get("/cache/status")
def cache_status():
    try:
        files = sorted(CACHE_DIR.glob("*.json"), key=lambda f: f.stat().st_mtime, reverse=True)
        entries = []
        for f in files:
            try:
                j = json.load(open(f))
                entries.append({
                    "symbol": j.get("symbol"),
                    "last_updated": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(j.get("timestamp", 0))),
                    "points": len(j.get("points", [])),
                    "age_minutes": round((time.time() - j.get("timestamp", 0)) / 60, 1)
                })
            except Exception as e:
                entries.append({"file": f.name, "error": str(e)})
        return jsonify({"count": len(entries), "entries": entries})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# ----------------------------------------------------------- 
# Static Routes: Voice / Overlays / Assets
# ----------------------------------------------------------- 
@app.get("/voice/latest.json")
def latest_voice_json():
    files = sorted(VOICE_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        return jsonify({"error": "no voice files yet"}), 404
    latest = files[0]
    base = latest.stem
    audio_path = f"/voice/{base}.wav"
    lipsync_path = f"/voice/{latest.name}"
    return jsonify({"audio_url": audio_path, "lipsync_url": lipsync_path})

@app.get("/voice/<path:filename>")
def voice_files(filename):
    return send_from_directory(str(VOICE_DIR), filename)

@app.get("/overlays/<path:filename>")
def serve_overlay(filename):
    fpath = os.path.join("/tmp/overlays", filename)
    if not os.path.exists(fpath):
        logger.error(f"Overlay not found: {fpath}")
        return jsonify({"ok": False, "error": "File not found"}), 404
    return send_file(fpath, mimetype="image/png")

@app.get("/avatar_overlay.html")
def overlay_html():
    return send_from_directory(str(ASSETS_DIR), "avatar_overlay.html")

@app.get("/<path:asset>")
def base_assets(asset):
    path = ASSETS_DIR / asset
    if path.exists():
        return send_from_directory(str(ASSETS_DIR), asset)
    abort(404)

# ----------------------------------------------------------- 
# Quick-Resolver + Chart Routes (Real-Time Unknown-Coin Support)
# ----------------------------------------------------------- 
@app.get("/analyze")
def analyze_quick():
    """
    Handle unknown coins instantly:
    1. Resolve contract info on the fly.
    2. Grab a one-time market snapshot.
    3. Merge new data into all_contracts.json.
    4. Return JSON for widget (chart + metrics).
    """
    symbol = request.args.get("symbol", "").strip()
    if not symbol:
        return jsonify({"ok": False, "error": "missing symbol"}), 400

    info = quick_resolve(symbol)
    snap = quick_snapshot(symbol)
    try:
        merge_all()
    except Exception as e:
        logger.error(f"[Merge Error] {e}")

    if USE_CG_API:
        overlay_path = build_tech_panel(symbol)
    else:
        overlay_path = build_tech_panel(symbol)  # Force CSV fallback if API disabled
    return jsonify({
        "ok": True,
        "symbol": snap.get("symbol") if snap else (info.get("symbol") if info else symbol),
        "metrics": (snap or {}).get("metrics"),
        "answer": f"Here’s the latest setup for {symbol.upper()} — chart and stats are based on the most recent snapshot.",
        "overlay_url": f"/overlays/{Path(overlay_path).name}"
    })

# ----------------------- Chart Helpers & Route ----------------------- 
def _csv_for_symbol(sym: str):
    s = (sym or "").lower().strip()
    p1 = COIN_DIR / f"{s}.csv"
    p2 = MEME_DIR / f"{s}.csv"
    if p1.exists(): return p1
    if p2.exists(): return p2
    return None

def _rows_for_window(rows, win: str):
    keep = {"1d": 24, "3d": 72, "7d": 168, "30d": 720}.get((win or "1d").lower(), 24)
    return rows[-keep:] if keep < len(rows) else rows

@app.get("/chart/data")
def chart_data():
    """Serve Chart.js-ready JSON built purely from local CSVs (no external API)."""
    symbol = (request.args.get("symbol") or "").strip().upper()
    sid = request.args.get("session_id")
    metric = request.args.get("metric", "price")
    window = request.args.get("time", "1d")

    if not symbol and sid:
        symbol = ((SESSION_LAST.get(sid) or {}).get("symbol") or "").upper()
    if not symbol:
        return jsonify({"ok": False, "error": "symbol missing"}), 400

    path = _csv_for_symbol(symbol)
    if not path:
        return jsonify({"labels": [], "datasets": [{"label": f"{symbol} {metric}", "data": []}]})

    with open(path, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return jsonify({"labels": [], "datasets": [{"label": f"{symbol} {metric}", "data": []}]})

    rows = _rows_for_window(rows, window)
    metric_map = {
        "price": "price", "price_dex": "price_dex",
        "volume": "volume_24h", "volume_24h": "volume_24h",
        "market_cap": "market_cap", "mcap": "market_cap",
        "liquidity_usd": "liquidity_usd", "liq": "liquidity_usd",
        "fdv": "fdv", "volume_dex_24h": "volume_dex_24h"
    }
    col = metric_map.get(metric, "price")

    labels, values = [], []
    for r in rows:
        labels.append(r.get("timestamp"))
        v = r.get(col)
        try:
            values.append(float(v) if v not in (None, "", "None") else None)
        except:
            values.append(None)

    return jsonify({"labels": labels, "datasets": [{"label": f"{symbol} {col}", "data": values}]})

# ----------------------------------------------------------- 
# /overlay/analysis — Generate Overlay Hints for a Symbol
# ----------------------------------------------------------- 
from chart_analyst import analyze_chart

@app.get("/overlay/analysis")
def overlay_analysis():
    symbol = request.args.get("symbol", "").strip()
    if not symbol:
        return jsonify({"ok": False, "error": "missing symbol"}), 400
    result = analyze_chart(symbol)
    return jsonify(result)

# ----------------------------------------------------------- 
# Temporary Sentiment Stub
# ----------------------------------------------------------- 
@app.get("/sentiment")
def sentiment_stub():
    sym = (request.args.get("symbol") or "").upper()
    return jsonify({"symbol": sym, "sentiment_score": 0.0})

# ----------------------------------------------------------- 
# Run (Local Dev); Render Uses Gunicorn
# ----------------------------------------------------------- 
if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    print(f"⚙️  Starting Luna server on port {port}…")
    app.run(host="0.0.0.0", port=port, debug=False)