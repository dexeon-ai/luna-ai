# voice_bridge.py
# Provider-agnostic text-to-speech bridge for Luna.
# Supports: ElevenLabs, OpenAI TTS, or a custom HTTP endpoint returning audio bytes.

from __future__ import annotations
import os, json, requests

VOICE_PROVIDER = (os.getenv("VOICE_PROVIDER") or "disabled").lower()

# ---- ElevenLabs (recommended for your current setup) ----
ELEVEN_API_KEY   = os.getenv("ELEVENLABS_API_KEY")
ELEVEN_VOICE_ID  = os.getenv("ELEVENLABS_VOICE_ID") or "EXAVITQu4vr4xnSDxMaL"  # 'Jessica'
ELEVEN_MODEL_ID  = os.getenv("ELEVENLABS_MODEL_ID") or "eleven_monolingual_v1"

# ---- OpenAI TTS (optional) ----
OPENAI_API_KEY   = os.getenv("OPENAI_API_KEY")
OPENAI_TTS_MODEL = os.getenv("OPENAI_TTS_MODEL", "gpt-4o-mini-tts")
OPENAI_TTS_VOICE = os.getenv("OPENAI_TTS_VOICE", "alloy")

# ---- Custom clone service (optional) ----
# Expected to accept: POST JSON {"text": "..."} → returns audio/mpeg bytes.
CUSTOM_TTS_URL   = os.getenv("CUSTOM_TTS_URL")  # e.g. https://tts.myclone.dev/speak

def voice_status() -> dict:
    ok = VOICE_PROVIDER in ("elevenlabs", "openai", "custom")
    reason = "configured" if ok else "disabled"
    return {"enabled": ok, "provider": VOICE_PROVIDER, "reason": reason}

def tts_bytes(text: str) -> bytes | None:
    text = (text or "").strip()
    if not text or VOICE_PROVIDER == "disabled":
        return None

    # 1) ElevenLabs
    if VOICE_PROVIDER == "elevenlabs" and ELEVEN_API_KEY:
        url = f"https://api.elevenlabs.io/v1/text-to-speech/{ELEVEN_VOICE_ID}"
        headers = {
            "xi-api-key": ELEVEN_API_KEY,
            "accept": "audio/mpeg",
            "content-type": "application/json",
        }
        payload = {
            "text": text,
            "model_id": ELEVEN_MODEL_ID,
            "voice_settings": {"stability": 0.45, "similarity_boost": 0.8},
        }
        r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=60)
        r.raise_for_status()
        return r.content

    # 2) Custom clone
    if VOICE_PROVIDER == "custom" and CUSTOM_TTS_URL:
        r = requests.post(CUSTOM_TTS_URL, json={"text": text}, timeout=60)
        r.raise_for_status()
        return r.content

    # 3) OpenAI TTS light HTTP call (no extra SDK dep)
    if VOICE_PROVIDER == "openai" and OPENAI_API_KEY:
        url = "https://api.openai.com/v1/audio/speech"
        headers = {
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": OPENAI_TTS_MODEL,
            "voice": OPENAI_TTS_VOICE,
            "input": text,
            "format": "mp3",
        }
        r = requests.post(url, headers=headers, json=payload, timeout=60)
        r.raise_for_status()
        return r.content

    return None
