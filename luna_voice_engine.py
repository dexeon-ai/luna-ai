# luna_voice_engine.py
import base64, io, os, json, time
from pathlib import Path

VOICE_ROOT = Path(__file__).resolve().parent / "voices" / "luna"
PROFILE    = VOICE_ROOT / "profile" / "luna_manifest.txt"

# Lazy-import Coqui so the main app still runs if TTS isn't installed
_engine = None
_speaker_ref = None

def _load_engine():
    global _engine, _speaker_ref
    if _engine is not None:
        return _engine
    try:
        from TTS.api import TTS
        # Pick a robust multi-speaker English model
        model_name = os.getenv("LUNA_TTS_MODEL", "tts_models/en/vctk/vits")
        _engine = TTS(model_name=model_name, progress_bar=False, gpu=False)
        if PROFILE.exists():
            with PROFILE.open("r", encoding="utf-8") as f:
                # In Coqui, multi-speaker models accept speaker_wav (a path) or a list of paths
                _speaker_ref = [line.strip() for line in f if line.strip()]
        return _engine
    except Exception as e:
        print("[Luna Voice] TTS load failed:", e)
        _engine = None
        return None

def synth_to_wav_base64(text: str) -> dict:
    """
    Returns dict { 'format':'wav', 'base64':..., 'duration_ms': int }
    If TTS is unavailable, returns { 'format':'none', 'base64': None, 'duration_ms': 0 }.
    """
    eng = _load_engine()
    if eng is None or not text:
        return {"format": "none", "base64": None, "duration_ms": 0}

    try:
        # synthesize
        wav_bytes = io.BytesIO()
        if _speaker_ref:
            audio = eng.tts(text=text, speaker_wav=_speaker_ref[0])
        else:
            audio = eng.tts(text=text)

        # Write to buffer
        from scipy.io.wavfile import write as wavwrite
        import numpy as np
        sr = 22050
        wav_np = (audio if isinstance(audio, list) else audio)
        wav_np = (wav_np if hasattr(wav_np, "__len__") else [wav_np])
        wav_np = np.array(wav_np, dtype=np.float32)
        wavwrite(wav_bytes, sr, wav_np)
        b64 = base64.b64encode(wav_bytes.getvalue()).decode("ascii")
        dur_ms = int(len(wav_np) / sr * 1000)
        return {"format":"wav", "base64": b64, "duration_ms": dur_ms}

    except Exception as e:
        print("[Luna Voice] synth failed:", e)
        return {"format": "none", "base64": None, "duration_ms": 0}
