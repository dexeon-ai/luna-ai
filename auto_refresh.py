# ===========================================================
# auto_refresh.py — background cache updater for Luna AI
# ===========================================================
import threading, time
from plot_engine import build_tech_panel

# List of high-priority coins to refresh automatically
DEFAULT_SYMBOLS = ["BTC", "ETH", "SOL", "BNB", "DOGE", "MATIC", "AVAX"]

def _refresh_worker():
    """Background loop that keeps cache warm."""
    print("🌙 [AutoRefresh] Background cache refresh thread started.")
    while True:
        for sym in DEFAULT_SYMBOLS:
            try:
                print(f"[AutoRefresh] Updating {sym}...")
                build_tech_panel(symbol=sym)
                print(f"[AutoRefresh] {sym} cached successfully.")
            except Exception as e:
                print(f"[AutoRefresh] Error updating {sym}: {e}")
        # Sleep 15 minutes between full cycles
        time.sleep(15 * 60)

# Start thread automatically on import
def start():
    t = threading.Thread(target=_refresh_worker, daemon=True)
    t.start()

# Launch immediately when module is imported
start()
