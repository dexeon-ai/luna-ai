# lipsync.py (cloud-safe, no ffmpeg, no audioop)
import os
import json
import wave
import struct
import math
from typing import List, Dict


def _rms_levels_wav(wav_path: str, frame_ms: int = 50) -> List[float]:
    """
    Compute RMS loudness values for each frame of a WAV file without audioop.
    """
    with wave.open(wav_path, 'rb') as wf:
        n_channels = wf.getnchannels()
        sampwidth = wf.getsampwidth()
        framerate = wf.getframerate()
        n_frames = wf.getnframes()

        samples_per_chunk = int(framerate * (frame_ms / 1000.0))
        fmt = {1: 'b', 2: 'h', 4: 'i'}[sampwidth]
        max_val = float(1 << (8 * sampwidth - 1))
        levels = []

        while True:
            chunk = wf.readframes(samples_per_chunk)
            if not chunk:
                break

            # Unpack the samples
            try:
                samples = struct.unpack(fmt * (len(chunk) // sampwidth), chunk)
            except struct.error:
                break

            # For multi-channel audio, average the channels
            if n_channels > 1:
                samples = [sum(samples[i::n_channels]) / n_channels for i in range(n_channels)]

            # Compute RMS manually
            rms = math.sqrt(sum(s * s for s in samples) / len(samples))
            levels.append(rms / max_val)

        return levels


def generate_lipsync(audio_path: str, out_dir: str = "voice") -> str:
    """
    Generate lipsync timeline JSON from a WAV file.
    Returns path to JSON file, same basename as input.
    """
    if not os.path.exists(audio_path):
        return None

    os.makedirs(out_dir, exist_ok=True)

    # Compute loudness-based visemes
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
