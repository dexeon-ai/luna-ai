# init_quick_dataset.py — run a few buckets now for testing
import subprocess, time, sys

buckets = ["CORE_1", "CORE_2"]  # covers the top 120–130 coins
for b in buckets:
    print(f"\n🧠 Bootstrapping bucket {b} …\n")
    subprocess.run([sys.executable, "data_agent_v3.py", "--bucket", b])
    time.sleep(10)  # short pause between buckets
print("\n✅ quick dataset initialized.\n")
