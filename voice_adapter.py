# voice_adapter.py — Offline TTS with Render-safe fallback
import os
import pyttsx3
from pathlib import Path

def tts_generate(text, base_name="reply", out_dir="voice"):
    """
    Generates a WAV file using pyttsx3 (offline). 
    On Linux/Render, if audio backend is missing, 
    writes a silent placeholder instead.
    """
    os.makedirs(out_dir, exist_ok=True)
    path = Path(out_dir) / f"{base_name}.wav"

    try:
        engine = pyttsx3.init()
        engine.setProperty("rate", 180)
        engine.setProperty("volume", 0.9)
        voices = engine.getProperty("voices")
        for v in voices:
            if "female" in v.name.lower():
                engine.setProperty("voice", v.id)
                break
        engine.save_to_file(text, str(path))
        engine.runAndWait()
        print(f"[TTS] Saved offline voice to {path}")
        return str(path)

    except Exception as e:
        # Render or Linux fallback
        print(f"[TTS Warning] pyttsx3 failed ({e}); writing placeholder WAV.")
        try:
            with open(path, "wb") as f:
                # minimal 1-second silent WAV header
                f.write(b"RIFF$\x00\x00\x00WAVEfmt \x10\x00\x00\x00\x01\x00\x01\x00D\xac\x00\x00"
                        b"\x88X\x01\x00\x02\x00\x10\x00data\x00\x00\x00\x00")
            return str(path)
        except Exception as e2:
            print("[TTS Error]", e2)
            return None
