# voice_adapter.py — Offline TTS for Luna AI (no ElevenLabs, no API calls)
import os
import pyttsx3
from pathlib import Path

def tts_generate(text, base_name="reply", out_dir="voice"):
    """
    Generates a WAV file using pyttsx3 (offline, free).
    Returns the full file path if successful.
    """

    os.makedirs(out_dir, exist_ok=True)
    path = Path(out_dir) / f"{base_name}.wav"

    try:
        engine = pyttsx3.init()
        engine.setProperty("rate", 180)     # speaking speed
        engine.setProperty("volume", 0.9)   # volume 0.0–1.0

        # Try to select a female voice if available
        voices = engine.getProperty("voices")
        for v in voices:
            if "female" in v.name.lower():
                engine.setProperty("voice", v.id)
                break

        # Save speech directly to file (no playback needed)
        engine.save_to_file(text, str(path))
        engine.runAndWait()

        print(f"[TTS] Saved offline voice to {path}")
        return str(path)

    except Exception as e:
        print("[TTS Error]", e)
        return None
