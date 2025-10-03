# lipsync.py (cloud-safe, no ffmpeg)
import os
import json
import wave
import audioop
from typing import List, Dict

def _rms_levels_wav(wav_path: str, frame_ms: int = 50) -> List[float]:
    with wave.open(wav_path, 'rb') as wf:
        n_channels = wf.getnchannels()
        sampwidth  = wf.getsampwidth()
        framerate  = wf.getframerate()
        n_frames   = wf.getnframes()

        # size in samples per frame, then bytes
        samples_per_chunk = int(framerate * (frame_ms / 1000.0))
        bytes_per_sample  = sampwidth
        chunk_size_bytes  = samples_per_chunk * n_channels * bytes_per_sample

        frames = []
        while True:
            chunk = wf.readframes(samples_per_chunk)
            if not chunk:
                break
            # audioop.rms expects mono; average channels if needed
            if n_channels > 1:
                chunk = audioop.tomono(chunk, sampwidth, 0.5, 0.5)
            rms = audioop.rms(chunk, sampwidth)
            frames.append(float(rms))
        return frames

def generate_lipsync(audio_path: str, out_dir: str = "voice") -> str:
    """
    Generate lipsync timeline JSON from a WAV file.
    Returns path to JSON file, same basename as input.
    """
    if not os.path.exists(audio_path):
        return None

    os.makedirs(out_dir, exist_ok=True)
    # Compute simple visemes based on loudness thresholds
    levels = _rms_levels_wav(audio_path, frame_ms=50)
    if not levels:
        frames = [{"time": 0.0, "viseme": "rest"}]
    else:
        mx = max(levels) or 1.0
        frames: List[Dict] = []
        t = 0.0
        for v in levels:
            ratio = v / mx
            if ratio > 0.66:
                viseme = "open"
            elif ratio > 0.33:
                viseme = "mid"
            else:
                viseme = "rest"
            frames.append({"time": round(t, 2), "viseme": viseme})
            t += 0.05  # 50ms increments

    json_path = os.path.splitext(audio_path)[0] + ".json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(frames, f)
    return json_path
