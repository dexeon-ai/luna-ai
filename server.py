# server.py — Luna AI backend (Render-ready, overlay-safe)
import os, time, json
from pathlib import Path
from flask import Flask, request, jsonify, send_from_directory, abort
from flask_cors import CORS

# ---- internal modules ----
from session_manager import manager
from snapshot import build_snapshot
from overlay_card import make_overlay_card
from voice_adapter import tts_generate
from risk_providers import fetch_deep_risk
from macro_providers import build_macro_summary
from qa_handler import handle_question


# ===========================================================
# Directories — Render-safe (voice on /voice, overlays in /tmp)
# ===========================================================
ROOT = Path(__file__).resolve().parent
VOICE_DIR = ROOT / "voice"
OVERLAY_DIR = Path("/tmp/overlays")     # ✅ Render write-safe location
ASSETS_DIR = ROOT                       # avatar_base.png, etc.

VOICE_DIR.mkdir(exist_ok=True)
OVERLAY_DIR.mkdir(parents=True, exist_ok=True)

print(">>> Luna AI backend starting …")
print(">>> Voice directory:", VOICE_DIR)
print(">>> Overlay directory:", OVERLAY_DIR)

# ===========================================================
# Flask setup
# ===========================================================
app = Flask(__name__)
CORS(app, supports_credentials=True, origins="*")

print(">>> Routes loaded: /ping, /qa, /voice/*, /overlays/*")


# -----------------------------------------------------------
# Health
# -----------------------------------------------------------
@app.get("/ping")
def ping():
    return jsonify({"ok": True, "message": "pong", "ts": int(time.time())})


# -----------------------------------------------------------
# Session management
# -----------------------------------------------------------
@app.post("/session/start")
def session_start():
    ttl = int(os.getenv("SESSION_TTL_SECONDS", "1800"))
    sid = manager.start(ttl_seconds=ttl)
    return jsonify({"ok": True, "session_id": sid, "questions_left": 21, "ttl_seconds": ttl})


@app.post("/session/status")
@app.get("/session/status")
def session_status():
    data = request.get_json(silent=True) or {}
    sid = data.get("session_id") or request.args.get("session_id")
    if not sid:
        return jsonify({"ok": False, "error": "missing session_id"}), 400
    if not manager.touch(sid):
        return jsonify({"ok": False, "error": "invalid or expired session"}), 403
    return jsonify({"ok": True, "remaining_questions": manager.remaining(sid)})


@app.post("/session/end")
def session_end():
    data = request.get_json(silent=True) or {}
    sid = data.get("session_id")
    if not sid:
        return jsonify({"ok": False, "error": "missing session_id"}), 400
    history, text = manager.end(sid)
    return jsonify({"ok": True, "message": "session closed", "history": history, "history_text": text})


# -----------------------------------------------------------
# /analyze — optional direct analysis endpoint
# -----------------------------------------------------------
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

    snap = build_snapshot(chain, contract)
    if not snap.get("ok"):
        return jsonify({"ok": False, "error": snap.get("error", "snapshot failed")}), 500

    card_url = None
    if overlay:
        path = make_overlay_card(snap, out_dir=str(OVERLAY_DIR))
        if path:
            card_url = f"/overlays/{Path(path).name}"

    manager.add_history(sid, "system", f"[analyze] {chain}:{contract}")
    return jsonify({"ok": True, "snapshot": snap, "overlay_card_url": card_url})


# -----------------------------------------------------------
# /risk  &  /macro
# -----------------------------------------------------------
@app.post("/risk")
def risk():
    data = request.get_json(silent=True) or {}
    chain    = data.get("chain")
    contract = data.get("contract")
    if not chain or not contract:
        return jsonify({"ok": False, "error": "missing chain/contract"}), 400
    return jsonify(fetch_deep_risk(chain, contract))


@app.post("/macro")
def macro():
    return jsonify(build_macro_summary())


# -----------------------------------------------------------
# /qa — main interactive route (21 Questions mode)
# -----------------------------------------------------------
@app.post("/qa")
def qa():
    data = request.get_json(silent=True) or {}
    sid = data.get("session_id")
    if not sid:
        return jsonify({"ok": False, "error": "missing session_id"}), 400
    if not manager.touch(sid):
        return jsonify({"ok": False, "error": "invalid or expired session"}), 403

    # decrement counter
    remaining = manager.decrement(sid)

    # get core answer
    result = handle_question(data) or {}
    text_for_tts = (
        result.get("summary")
        or result.get("tldr")
        or result.get("message")
        or result.get("error")
        or "Answer ready."
    )

    manager.add_history(sid, "user", json.dumps({k: v for k, v in data.items() if k != "session_id"})[:500])
    manager.add_history(sid, "assistant", text_for_tts)

    # ---------- voice ----------
    voice_url = None
    lipsync_url = None
    audio_path = tts_generate(text_for_tts, base_name=f"reply_{int(time.time())}", out_dir=str(VOICE_DIR))
    if audio_path:
        name = Path(audio_path).name
        voice_url = f"/voice/{name}"
        json_candidate = Path(audio_path).with_suffix(".json")
        if json_candidate.exists():
            lipsync_url = f"/voice/{json_candidate.name}"

    # ---------- overlay ----------
    overlay_path = None
    try:
        snap_like = {
            "token": {"symbol": result.get("symbol")},
            "market": result.get("metrics", {}),
            "summary": text_for_tts,
        }
        overlay_path = make_overlay_card(snap_like, out_dir=str(OVERLAY_DIR))
        print("[QA] Overlay created:", overlay_path)
    except Exception as e:
        print("[Overlay Error]", e)

    payload = {
        "ok": bool(result.get("ok", True)),
        "answer": text_for_tts,
        "overlay_url": f"/overlays/{Path(overlay_path).name}" if overlay_path else None,
        "voice_path": voice_url,
        "lipsync_path": lipsync_url,
        "remaining_questions": remaining,
        "symbol": result.get("symbol"),
        "metrics": result.get("metrics", {}),
    }

    if remaining <= 0:
        payload["notice"] = "Session limit reached. Use /session/end to copy the transcript."
    return jsonify(payload)


# -----------------------------------------------------------
# Static routes: voice / overlays / assets
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


@app.get("/overlays/latest.png")
def latest_overlay():
    files = sorted(Path("/tmp/overlays").glob("*.png"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        abort(404)
    latest = files[0]
    print("[GET] Serving overlay:", latest)
    return send_from_directory("/tmp/overlays", latest.name)


@app.get("/overlays/<path:filename>")
def overlay_files(filename):
    return send_from_directory("/tmp/overlays", filename)


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
# Run (local dev); Render uses gunicorn
# -----------------------------------------------------------
if __name__ == "__main__":
    # Render dynamically sets PORT, so always use that value
    port = int(os.environ.get("PORT", "10000"))
    print(f"⚙️  Starting Luna server on port {port}...")
    app.run(host="0.0.0.0", port=port, debug=False)
