import argparse, json, sys
from snapshot import build_snapshot
from overlay_card import make_overlay_card

def main():
    parser = argparse.ArgumentParser(description="Luna AI Snapshot")
    parser.add_argument("--chain", required=True)
    parser.add_argument("--contract", required=True)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()

    snap = build_snapshot(args.chain, args.contract)

    if args.pretty:
        print(json.dumps(snap, indent=2))
    else:
        print(json.dumps(snap))

    if snap.get("ok"):
        print("\nTL;DR:")
        print(snap["tldr"])
        print("\nCharts:")
        charts = snap.get("charts", {})
        if charts:
            print("  Pair:", charts.get("pair_chart"))
            print("  Token:", charts.get("token_page"))

        # NEW: generate overlay card
        path = make_overlay_card(snap)
        if path:
            print(f"\nOverlay card saved: {path}")
    else:
        print("\nERROR:", snap.get("error"), file=sys.stderr)

if __name__ == "__main__":
    main()
