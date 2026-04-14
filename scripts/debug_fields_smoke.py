from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

from playwright.async_api import async_playwright


REPO_ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = REPO_ROOT / "output" / "playwright" / f"fields-debug-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
BASE_URL = "http://127.0.0.1:8125/"


async def _dump_state(page):
    return await page.evaluate(
        """
        async () => {
          const stateMod = await import('/static/src/state.js');
          const view = document.getElementById('sidebar-fields-view');
          const status = document.getElementById('fields-status');
          const list = document.getElementById('fields-list');
          const map = stateMod.store.map;
          const center = map?.getCenter ? map.getCenter() : null;
          return {
            sidebarView: stateMod.store.sidebarView,
            farmOptionsLoaded: !!stateMod.store.farmOptions,
            farmFieldsCount: (stateMod.store.farmFields || []).length,
            selectedFieldId: stateMod.store.selectedFieldId || null,
            selectedPaddockId: stateMod.store.selectedPaddockId || null,
            paddockCount: Object.keys(stateMod.store.farmPaddocksLookup || {}).length,
            viewHidden: view?.classList.contains('hidden'),
            status: status?.textContent || null,
            listHtml: list?.innerHTML || null,
            zoom: map?.getZoom ? map.getZoom() : null,
            center: center ? { lat: center.lat, lng: center.lng } : null,
          };
        }
        """
    )


async def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(viewport={"width": 1440, "height": 960})
        await page.goto(BASE_URL, wait_until="domcontentloaded", timeout=120_000)
        await page.wait_for_selector("#map", timeout=120_000)
        await page.wait_for_selector("#sidebar-fields-tab", timeout=120_000)
        await page.wait_for_timeout(2_000)
        await page.screenshot(path=str(OUT_DIR / "00-home.png"), full_page=True)

        await page.click("#sidebar-fields-tab")
        await page.wait_for_timeout(5_000)
        await page.screenshot(path=str(OUT_DIR / "01-fields-tab.png"), full_page=True)

        state = await _dump_state(page)
        (OUT_DIR / "state.json").write_text(json.dumps(state, indent=2), encoding="utf-8")
        print(json.dumps({"out_dir": str(OUT_DIR), "state": state}, ensure_ascii=False))
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
