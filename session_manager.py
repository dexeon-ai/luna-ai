# session_manager.py — in-memory 21 Questions sessions + transcript
import uuid, time
from typing import Dict, Any, List, Tuple, Optional

class SessionManager:
    def __init__(self):
        # sid -> record
        self.sessions: Dict[str, Dict[str, Any]] = {}

    def start(self, ttl_seconds: int = 1800) -> str:
        sid = str(uuid.uuid4())
        self.sessions[sid] = {
            "questions_left": 21,
            "history": [],                 # list of {"role": "user|assistant|system", "text": str}
            "ttl": ttl_seconds,
            "last_touch": time.time(),
            "cache": {}                    # (chain, contract) -> snapshot
        }
        return sid

    def _expired(self, sid: str) -> bool:
        s = self.sessions.get(sid)
        if not s: return True
        return (time.time() - s["last_touch"]) > s["ttl"]

    def touch(self, sid: str) -> bool:
        if sid not in self.sessions: return False
        if self._expired(sid):
            self.sessions.pop(sid, None)
            return False
        self.sessions[sid]["last_touch"] = time.time()
        return True

    def decrement(self, sid: str) -> int:
        s = self.sessions.get(sid)
        if not s: return 0
        if s["questions_left"] > 0:
            s["questions_left"] -= 1
        return s["questions_left"]

    def remaining(self, sid: str) -> int:
        s = self.sessions.get(sid)
        return int(s["questions_left"]) if s else 0

    def add_history(self, sid: str, role: str, text: str):
        s = self.sessions.get(sid)
        if not s: return
        s["history"].append({"role": role, "text": (text or "").strip()[:5000]})
        s["last_touch"] = time.time()

    def dump_history_text(self, sid: str) -> str:
        s = self.sessions.get(sid)
        if not s: return ""
        lines: List[str] = []
        qn = 0
        for item in s["history"]:
            role = item.get("role")
            txt  = item.get("text", "")
            if role == "user":
                qn += 1
                lines.append(f"Q{qn}: {txt}")
            elif role == "assistant":
                lines.append(f"A{qn}: {txt}")
            else:
                lines.append(txt)
        return "\n".join(lines)

    def end(self, sid: str) -> Tuple[List[Dict[str, str]], str]:
        s = self.sessions.pop(sid, None)
        if not s: return [], ""
        return s["history"], self._format_end_text(s)

    def _format_end_text(self, s: Dict[str, Any]) -> str:
        # Format transcript for “Copy All”
        lines: List[str] = []
        qn = 0
        for item in s["history"]:
            role = item.get("role")
            txt  = item.get("text", "")
            if role == "user":
                qn += 1
                lines.append(f"Q{qn}: {txt}")
            elif role == "assistant":
                lines.append(f"A{qn}: {txt}")
        return "\n".join(lines)

    # Optional caching for token snapshots
    def get_cached(self, sid: str, chain: str, contract: str):
        s = self.sessions.get(sid)
        if not s: return None
        return s["cache"].get((chain, contract))

    def set_cached(self, sid: str, chain: str, contract: str, snap):
        s = self.sessions.get(sid)
        if not s: return
        s["cache"][(chain, contract)] = snap

# Singleton
manager = SessionManager()
