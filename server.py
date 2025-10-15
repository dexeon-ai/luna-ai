# ===========================================================
# server.py — Luna AI Backend v10 (compatible with latest plot_engine)
# ===========================================================
import os, time, json, csv, logging
from pathlib import Path
from flask import Flask, request, jsonify, send_from_directory, abort, send_file
from flask_cors import CORS
import requests

# ---------------- Logging ----------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------- CoinGecko Setup ----------------
CG_API_KEY = os.getenv("COINGECKO_API_KEY", "CG-mVmG196hWJwz9tjqLZvmmTSQ").strip()
if not CG_API_KEY or CG_API_KEY == "CG-mVmG196hWJwz9tjqLZvmmTSQ":
    logger.warning("⚠️ Using fallback CoinGecko API key (Demo tier)")
CG_BASE = "https://api.coingecko.com/api/v3"
CG_HEADERS = {"accept": "application/json", "x-cg-pro-api-key": CG_API_KEY}
USE_CG_API = True

try:
    r = requests.get(f"{CG_BASE}/ping", headers=CG_HEADERS, timeout=5)
    if r.status_code != 200:
        logger.warning(f"⚠️ CoinGecko ping returned {r.status_code}: {r.text}")
        USE_CG_API = False
    else:
        logger.info(f"✅ CoinGecko API connected ({CG_BASE})")
except Exception as e:
    logger.error(f"CoinGecko validation failed: {e}")
    USE_CG_API = False

# ---------------- Directories ----------------
ROOT = Path(__file__).resolve().parent
VOICE_DIR = ROOT / "voice"
OVERLAY_DIR = Path("/tmp/overlays")
DATA_ROOT = ROOT / "luna_cache" / "data"
COIN_DIR = DATA_ROOT / "coins"
MEME_DIR = DATA_ROOT / "memes"
ASSETS_DIR = ROOT
VOICE_DIR.mkdir(exist_ok=True)
OVERLAY_DIR.mkdir(parents=True, exist_ok=True)

print(">>> Luna backend starting ...")

# ---------------- Flask Setup ----------------
app = Flask(__name__)
CORS(app, supports_credentials=True, origins="*")

# ---------------- Internal Modules ----------------
from session_manager import manager
from snapshot import build_snapshot
from overlay_card import make_overlay_card
from voice_adapter import tts_generate
from risk_providers import fetch_deep_risk
from macro_providers import build_macro_summary
from qa_handler import handle_question
from plot_engine import build_tech_panel
from quick_resolver import quick_resolve
from quick_snapshot import quick_snapshot
from merge_contracts import main as merge_all
from chart_analyst import analyze_chart

ELEVENLABS_API_KEY = os.getenv("ELEVENLABS_API_KEY", "")

SESSION_LAST = {}

# ---------------- Health ----------------
@app.get("/ping")
def ping():
    return jsonify({"ok": True, "message": "pong", "ts": int(time.time())})

# ---------------- Session Handling ----------------
@app.post("/session/start")
def session_start():
    ttl = int(os.getenv("SESSION_TTL_SECONDS", "1800"))
    sid = manager.start(ttl_seconds=ttl)
    token_balance = 100
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
        return jsonify({"ok": False, "error": "missing session_id"}), 400
    if not manager.touch(sid):
        return jsonify({"ok": False, "error": "expired session"}), 403
    s = SESSION_LAST.get(sid, {})
    return jsonify({
        "ok": True,
        "remaining_questions": manager.remaining(sid),
        "token_balance": s.get("token_balance", 0)
    })

@app.post("/session/end")
def session_end():
    data = request.get_json(silent=True) or {}
    sid = data.get("session_id")
    if not sid:
        return jsonify({"ok": False, "error": "missing session_id"}), 400
    hist, txt = manager.end(sid)
    if sid in SESSION_LAST:
        del SESSION_LAST[sid]
    return jsonify({"ok": True, "message": "session closed", "history": hist, "history_text": txt})

# ---------------- Main Q&A Route ----------------
@app.post("/qa")
def qa():
    data = request.get_json(silent=True) or {}
    sid = data.get("session_id")
    if not sid:
        return jsonify({"ok": False, "error": "missing session_id"}), 400
    if not manager.touch(sid):
        return jsonify({"ok": False, "error": "expired session"}), 403

    session = SESSION_LAST.get(sid, {})
    remaining = manager.decrement(sid)
    if remaining < 0:
        return jsonify({"ok": False, "error": "limit reached"}), 403

    result = handle_question(data) or {}
    summary = result.get("summary") or result.get("tldr") or "Answer ready."

    overlay_url = None
    try:
        if result.get("symbol"):
            overlay_path = build_tech_panel(result["symbol"])
            overlay_url = f"/overlays/{Path(overlay_path).name}"
    except Exception as e:
        logger.error(f"[Overlay Error] {e}")

    manager.add_history(sid, "user", json.dumps(data)[:500])
    manager.add_history(sid, "assistant", summary)

    payload = {
        "ok": True,
        "answer": summary,
        "overlay_url": overlay_url,
        "remaining_questions": remaining,
        "symbol": result.get("symbol"),
        "token_balance": session.get("token_balance", 0) - 1
    }
    return jsonify(payload)

# ---------------- Chart Endpoints ----------------
@app.get("/analyze")
def analyze_quick():
    symbol = request.args.get("symbol", "").strip()
    if not symbol:
        return jsonify({"ok": False, "error": "missing symbol"}), 400
    try:
        merge_all()
    except Exception as e:
        logger.error(f"[Merge Error] {e}")

    overlay_path = build_tech_panel(symbol)
    return jsonify({
        "ok": True,
        "symbol": symbol.upper(),
        "answer": f"Here’s the latest technical overview for {symbol.upper()}",
        "overlay_url": f"/overlays/{Path(overlay_path).name}"
    })

@app.get("/chart/data")
def chart_data():
    symbol = (request.args.get("symbol") or "").upper()
    if not symbol:
        return jsonify({"ok": False, "error": "missing symbol"}), 400
    path = COIN_DIR / f"{symbol.lower()}.csv"
    if not path.exists():
        return jsonify({"ok": False, "error": "no data"}), 404
    with open(path, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    labels = [r["timestamp"] for r in rows[-100:]]
    values = [float(r["price"]) for r in rows[-100:] if r.get("price")]
    return jsonify({"labels": labels, "datasets": [{"label": f"{symbol} Price", "data": values}]})

@app.get("/overlay/analysis")
def overlay_analysis():
    symbol = request.args.get("symbol", "").strip()
    if not symbol:
        return jsonify({"ok": False, "error": "missing symbol"}), 400
    result = analyze_chart(symbol)
    return jsonify(result)

# ---------------- Assets ----------------
@app.get("/voice/<path:filename>")
def voice_files(filename):
    return send_from_directory(str(VOICE_DIR), filename)

@app.get("/overlays/<path:filename>")
def serve_overlay(filename):
    f = os.path.join("/tmp/overlays", filename)
    if not os.path.exists(f):
        return jsonify({"ok": False, "error": "file not found"}), 404
    return send_file(f, mimetype="image/png")

@app.get("/<path:asset>")
def base_assets(asset):
    p = ASSETS_DIR / asset
    if p.exists():
        return send_from_directory(str(ASSETS_DIR), asset)
    abort(404)

# ---------------- Run ----------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    print(f"⚙️ Starting Luna server on port {port}...")
    app.run(host="0.0.0.0", port=port, debug=False)
