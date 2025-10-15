# chart_fetcher.py — FINAL VERSION
# Captures a Dexscreener chart screenshot reliably (Windows + Render compatible)

import asyncio
import os
from pathlib import Path
from playwright.async_api import async_playwright

DEX_BASE = "https://dexscreener.com"

async def get_chart(chain: str, contract: str, out_file: str = "/tmp/chart.png"):
    """
    Launch a headless Chromium browser with Playwright, open the Dexscreener page,
    wait for the TradingView chart to render, then take a screenshot.
    Works locally and on Render.
    """

    # Make sure the output directory exists (even if relative path)
    out_dir = os.path.dirname(out_file) or "."
    os.makedirs(out_dir, exist_ok=True)

    url = f"{DEX_BASE}/{chain}/{contract}"
    print(f"[ChartFetcher] Capturing chart from {url}")

    try:
        async with async_playwright() as p:
            # Launch Chromium headless
            browser = await p.chromium.launch(args=["--no-sandbox"])
            page = await browser.new_page(viewport={"width": 1600, "height": 900})

            # Load the page quickly, without waiting for every network call
            await page.goto(url, wait_until="domcontentloaded")

            # Give the TradingView chart time to render
            await page.wait_for_timeout(8000)

            # Try several possible chart element selectors
            chart = None
            for selector in [
                "div.chart-container",
                "div.tradingview-widget-container",
                "canvas",
            ]:
                try:
                    chart = await page.query_selector(selector)
                    if chart:
                        break
                except Exception:
                    continue

            # Screenshot the found chart or fallback to the full viewport
            if chart:
                await chart.screenshot(path=out_file)
                print(f"[ChartFetcher] Chart element captured → {out_file}")
            else:
                await page.screenshot(path=out_file)
                print(f"[ChartFetcher] Fallback: full page captured → {out_file}")

            await browser.close()
            return os.path.abspath(out_file)

    except Exception as e:
        print(f"[ChartFetcher] Error: {e}")
        return None


# -------------------------------------------------------------
# Standalone test harness (for local testing only)
# -------------------------------------------------------------
if __name__ == "__main__":
    import sys

    chain = "solana"
    # MOMO contract example
    contract = "G4zwEA9NSd3nMBbEj31MMPq2853Brx2oGsKzex3ebonk"
    out_path = Path("chart_test.png")

    try:
        asyncio.run(get_chart(chain, contract, str(out_path)))
        print(f"[ChartFetcher] Chart saved to {out_path.resolve()}")
    except KeyboardInterrupt:
        print("[ChartFetcher] Interrupted by user.")
