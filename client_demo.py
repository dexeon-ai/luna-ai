import requests, json

BASE = "http://127.0.0.1:5000"

def pretty(x):
    print(json.dumps(x, indent=2))

def run_analysis(session_id, chain, contract, name=""):
    payload = {
        "session_id": session_id,
        "chain": chain,
        "contract": contract,
        "overlay": True
    }
    r = requests.post(f"{BASE}/analyze", json=payload).json()
    print(f"\n=== {name or contract} ({chain}) ===")
    if r.get("ok") and r.get("snapshot", {}).get("ok"):
        snap = r["snapshot"]
        print("TL;DR:", snap["tldr"])
        if r.get("overlay_path"):
            print("Overlay saved:", r["overlay_path"])
        if r.get("voice_path"):
            print("Voice saved:", r["voice_path"])
    else:
        pretty(r)

def run_risk(chain, contract, name=""):
    payload = {"chain": chain, "contract": contract}
    try:
        resp = requests.post(f"{BASE}/risk", json=payload)
        if resp.status_code != 200:
            print(f"\n=== RISK ONLY: {name or contract} ({chain}) ===")
            print("Error:", resp.status_code, resp.text)
            return
        r = resp.json()
    except Exception as e:
        print(f"\n=== RISK ONLY: {name or contract} ({chain}) ===")
        print("Error parsing JSON:", str(e))
        print("Raw response:", resp.text if 'resp' in locals() else None)
        return

    print(f"\n=== RISK ONLY: {name or contract} ({chain}) ===")
    pretty(r)

def run_macro():
    try:
        resp = requests.get(f"{BASE}/macro")
        if resp.status_code != 200:
            print("\n=== MACRO MARKET CONTEXT ===")
            print("Error:", resp.status_code, resp.text)
            return
        r = resp.json()
    except Exception as e:
        print("\n=== MACRO MARKET CONTEXT ===")
        print("Error parsing JSON:", str(e))
        print("Raw response:", resp.text if 'resp' in locals() else None)
        return

    print("\n=== MACRO MARKET CONTEXT ===")
    pretty(r)

def run_qa(session_id, question_payload):
    try:
        resp = requests.post(f"{BASE}/qa", json=question_payload)
        if resp.status_code != 200:
            print("\n=== QA EXAMPLE ===")
            print("Error:", resp.status_code, resp.text)
            return
        r = resp.json()
    except Exception as e:
        print("\n=== QA EXAMPLE ===")
        print("Error parsing JSON:", str(e))
        print("Raw response:", resp.text if 'resp' in locals() else None)
        return

    print("\n=== QA EXAMPLE ===")
    pretty(r)
    if "remaining_questions" in r:
        print("Remaining questions:", r["remaining_questions"])

def main():
    # 1. Start session
    r = requests.post(f"{BASE}/session/start").json()
    session_id = r["session_id"]
    print("SESSION:", session_id)
    print("Questions available:", r.get("questions"))

    # 2. Run standard tests
    run_analysis(session_id, "solana", "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263", "BONK")
    run_analysis(session_id, "ethereum", "0x6982508145454Ce325dDbE47a25d4ec3d2311933", "PEPE")
    run_risk("ethereum", "0x6982508145454Ce325dDbE47a25d4ec3d2311933", "PEPE")
    run_macro()

    # 3. Ask QA using session-aware payload
    payload = {
        "session_id": session_id,
        "question": "Compare BONK vs PEPE across Solana and Ethereum",
        "action": "compare",
        "tokens": [
            {"chain": "solana", "contract": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"},  # BONK
            {"chain": "ethereum", "contract": "0x6982508145454Ce325dDbE47a25d4ec3d2311933"}   # PEPE
        ]
    }
    run_qa(session_id, payload)

    # 4. End session and fetch transcript
    r = requests.post(f"{BASE}/session/end", json={"session_id": session_id}).json()
    print("\nSession ended:", r.get("message"))
    print("\nFull transcript:\n")
    print(r.get("history_text", ""))

if __name__ == "__main__":
    main()
