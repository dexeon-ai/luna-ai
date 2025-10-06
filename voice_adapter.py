# voice_adapter.py — Render-ready, WAV output (new ElevenLabs SDK style)
import os, time, re
from typing import Optional
from dotenv import load_dotenv
from elevenlabs import generate, save   # ✅ this is correct, NO "ElevenLabs"
from lipsync import generate_lipsync

load_dotenv()

ELEVENLABS_API_KEY  = os.getenv("ELEVENLABS_API_KEY", "").strip()
VOICE_NAME          = os.getenv("ELEVENLABS_VOICE_NAME", "").strip()
VOICE_ID            = os.getenv("ELEVENLABS_VOICE_ID", "").strip()

def _safe_slug(text: str, limit: int = 40) -> str:
    base = re.sub(r"[^a-zA-Z0-9_-]+", "-", text.strip()) or "luna"
    return base[:limit].strip("-").lower()

def tts_generate(text: str, base_name: Optional[str] = None, out_dir: str = "voice") -> Optional[str]:
    """
    Generate TTS audio with ElevenLabs.
    Produces a WAV file + matching lipsync JSON.
    Returns path to WAV file or None on failure.
    """
    os.makedirs(out_dir, exist_ok=True)
    if not ELEVENLABS_API_KEY:
        print("❌ Missing ELEVENLABS_API_KEY")
        return None

    # Filenames
    base = base_name or f"luna_{int(time.time())}"
    base = _safe_slug(base)
    wav_path = os.path.join(out_dir, f"{base}.wav")

    try:
        # Generate audio with ElevenLabs
        audio = generate(
            api_key=ELEVENLABS_API_KEY,
            text=text,
            voice=VOICE_ID or VOICE_NAME,
            model="eleven_multilingual_v2"
        )

        # Save audio to WAV file
        save(audio, wav_path)

        # Generate lipsync JSON
        generate_lipsync(wav_path, out_dir=out_dir)

        print(f"✅ TTS generated: {wav_path}")
        return wav_path

    except Exception as e:
        print("❌ TTS error:", e)
        return None
