# server.py — Render-ready Flask API
import os, glob, time, json
from pathlib import Path
from flask import Flask, request, jsonify, send_from_directory, abort
from flask_cors import CORS

from session_manager import manager
from snapshot import build_snapshot
from overlay_card import make_overlay_card
from voice_adapter import tts_generate
from risk_providers import fetch_deep_risk
from macro_providers import build_macro_summary
from qa_handler import handle_question

ROOT        = Path(__file__).resolve().parent
VOICE_DIR   = ROOT / "voice"
OVERLAY_DIR = ROOT / "overlays"
ASSETS_DIR  = ROOT   # avatar_base.png, mouth_*.png, avatar_overlay.html

VOICE_DIR.mkdir(exist_ok=True)
OVERLAY_DIR.mkdir(exist_ok=True)

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": os.getenv("CORS_ORIGINS", "*").split(",")}})

print(">>> Luna AI server live with /ping, /session/*, /analyze, /risk, /macro, /qa, /voice/*")

# ------------- Health -------------
@app.get("/ping")
def ping():
    return jsonify({"ok": True, "message": "pong", "ts": int(time.time())})

# ------------- Sessions -------------
@app.post("/session/start")
def session_start():
    ttl = int(os.getenv("SESSION_TTL_SECONDS", "1800"))
    sid = manager.start(ttl_seconds=ttl)
    return jsonify({"ok": True, "session_id": sid, "questions_left": 21, "ttl_seconds": ttl})

@app.post("/session/status")
def session_status():
    data = request.get_json(silent=True) or {}
    sid = data.get("session_id")
    if not manager.touch(sid):
        return jsonify({"ok": False, "error": "invalid or expired session"}), 403
    return jsonify({"ok": True, "questions_left": manager.remaining(sid)})

@app.post("/session/end")
def session_end():
    data = request.get_json(silent=True) or {}
    sid = data.get("session_id")
    if not sid:
        return jsonify({"ok": False, "error": "missing session_id"}), 400
    history, text = manager.end(sid)
    # Ephemeral storage policy: clear voice files for privacy if you want:
    # for f in VOICE_DIR.glob("*"): f.unlink(missing_ok=True)
    return jsonify({"ok": True, "message": "session closed", "history": history, "history_text": text})

# ------------- Analysis -------------
@app.post("/analyze")
def analyze():
    data = request.get_json(silent=True) or {}
    sid      = data.get("session_id")
    chain    = data.get("chain")
    contract = data.get("contract")
    overlay  = bool(data.get("overlay", False))

    if not sid or not chain or not contract:
        return jsonify({"ok": False, "error": "missing session_id/chain/contract"}), 400
    if not manager.touch(sid):
        return jsonify({"ok": False, "error": "invalid or expired session"}), 403

    cached = manager.get_cached(sid, chain, contract)
    if cached:
        snap = cached
    else:
        snap = build_snapshot(chain, contract)
        if snap.get("ok"):
            manager.set_cached(sid, chain, contract, snap)

    if not snap.get("ok"):
        return jsonify({"ok": False, "error": snap.get("error", "snapshot failed")}), 500

    card_url = None
    if overlay:
        path = make_overlay_card(snap, out_dir=str(OVERLAY_DIR))
        if path:
            card_url = f"/overlays/{Path(path).name}"

    manager.add_history(sid, "system", f"[analyze] {chain}:{contract}")
    return jsonify({"ok": True, "snapshot": snap, "overlay_card_url": card_url})

# ------------- Risk -------------
@app.post("/risk")
def risk():
    data = request.get_json(silent=True) or {}
    chain    = data.get("chain")
    contract = data.get("contract")
    if not chain or not contract:
        return jsonify({"ok": False, "error": "missing chain/contract"}), 400
    return jsonify(fetch_deep_risk(chain, contract))

# ------------- Macro -------------
@app.post("/macro")
def macro():
    return jsonify(build_macro_summary())

# ------------- Q&A (21 questions) -------------
@app.post("/qa")
def qa():
    data = request.get_json(silent=True) or {}
    sid = data.get("session_id")
    if not sid:
        return jsonify({"ok": False, "error": "missing session_id"}), 400
    if not manager.touch(sid):
        return jsonify({"ok": False, "error": "invalid or expired session"}), 403

    # Decrement question counter
    remaining = manager.decrement(sid)

    # Route to your existing handler (compare, etc.)
    result = handle_question(data)  # expects dict response
    # Make a simple text for history + TTS
    text_for_tts = result.get("summary") or result.get("tldr") or result.get("message") \
                   or result.get("error") or "Answer ready."

    manager.add_history(sid, "user", json.dumps({k:v for k,v in data.items() if k!='session_id'})[:500])
    manager.add_history(sid, "assistant", text_for_tts)

    # Generate voice (optional if ELEVENLABS_API_KEY missing)
    voice_url = None
    lipsync_url = None
    audio_path = tts_generate(text_for_tts, base_name=f"reply_{int(time.time())}", out_dir=str(VOICE_DIR))
    if audio_path:
        name = Path(audio_path).name
        voice_url = f"/voice/{name}"
        json_candidate = Path(audio_path).with_suffix(".json")
        if json_candidate.exists():
            lipsync_url = f"/voice/{json_candidate.name}"

    payload = {
        "ok": bool(result.get("ok", True)),
        "result": result,
        "remaining_questions": remaining,
        "voice_url": voice_url,
        "lipsync_url": lipsync_url
    }

    # If out of questions, include transcript prompt text
    if remaining <= 0:
        payload["notice"] = "Session limit reached. Use /session/end to copy the transcript."
    return jsonify(payload)

# ------------- Static for voice + overlays + assets -------------
@app.get("/voice/latest.json")
def latest_voice_json():
    files = sorted(VOICE_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        abort(404)
    return send_from_directory(str(VOICE_DIR), files[0].name)

@app.get("/voice/<path:filename>")
def voice_files(filename):
    return send_from_directory(str(VOICE_DIR), filename)

@app.get("/overlays/<path:filename>")
def overlay_files(filename):
    return send_from_directory(str(OVERLAY_DIR), filename)

@app.get("/avatar_overlay.html")
def overlay_html():
    return send_from_directory(str(ASSETS_DIR), "avatar_overlay.html")

@app.get("/<path:asset>")
def base_assets(asset):
    # allows avatar_base.png, mouth_*.png to be served
    path = (ASSETS_DIR / asset)
    if path.exists():
        return send_from_directory(str(ASSETS_DIR), asset)
    abort(404)

# ------------- Run (local dev); Render uses gunicorn -------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=False)
