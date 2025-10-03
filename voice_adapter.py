# voice_adapter.py (Render-ready, WAV output)
import os, time, re
from typing import Optional
from dotenv import load_dotenv
from elevenlabs import ElevenLabs, save
from lipsync import generate_lipsync

load_dotenv()

ELEVENLABS_API_KEY  = os.getenv("ELEVENLABS_API_KEY", "").strip()
VOICE_NAME          = os.getenv("ELEVENLABS_VOICE_NAME", "").strip()
VOICE_ID            = os.getenv("ELEVENLABS_VOICE_ID", "").strip()  # optional if VOICE_NAME provided

_client = ElevenLabs(api_key=ELEVENLABS_API_KEY) if ELEVENLABS_API_KEY else None

def _pick_voice_id() -> Optional[str]:
    if not _client:
        return None
    # Prefer explicit VOICE_ID
    if VOICE_ID:
        return VOICE_ID
    # Else attempt VOICE_NAME lookup
    if VOICE_NAME:
        try:
            voices = _client.voices.get_all()
            for v in (voices.voices or []):
                if v.name.strip().lower() == VOICE_NAME.strip().lower():
                    return v.voice_id
        except Exception:
            pass
    # Fallback to a known default if tenant has it; otherwise None -> TTS disabled
    return None

def _safe_slug(text: str, limit: int = 40) -> str:
    base = re.sub(r"[^a-zA-Z0-9_-]+", "-", text.strip()) or "luna"
    return base[:limit].strip("-").lower()

def tts_generate(text: str, base_name: Optional[str] = None, out_dir: str = "voice") -> Optional[str]:
    """
    Returns path to WAV file if TTS succeeds, else None.
    Also writes a matching .json lipsync file via generate_lipsync().
    """
    os.makedirs(out_dir, exist_ok=True)
    if not _client:
        return None

    vid = _pick_voice_id()
    if not vid:
        return None

    # Filenames: voice/<base>.wav / .json
    base = base_name or f"luna_{int(time.time())}"
    base = _safe_slug(base)
    wav_path = os.path.join(out_dir, f"{base}.wav")

    try:
        # Prefer WAV so we avoid ffmpeg on Render
        audio = _client.text_to_speech.convert(
            voice_id=vid,
            model_id="eleven_multilingual_v2",
            text=text,
            output_format="wav_44100_16"
        )
        save(audio, wav_path)

        # Generate lipsync JSON next to WAV
        generate_lipsync(wav_path, out_dir=out_dir)
        return wav_path
    except Exception:
        # As a fallback, try MP3 (still works; no lipsync if ffmpeg unavailable)
        try:
            mp3_path = os.path.join(out_dir, f"{base}.mp3")
            audio = _client.text_to_speech.convert(
                voice_id=vid,
                model_id="eleven_multilingual_v2",
                text=text,
                output_format="mp3_44100_128"
            )
            save(audio, mp3_path)
            # No lipsync for mp3 unless ffmpeg installed; return mp3 anyway
            return mp3_path
        except Exception:
            return None
