"""
Stage-2 Smoke Harness (Playwright)
---------------------------------
Goal: reproducible, screenshot-driven smoke for national view:
  - new browser session (fresh context)
  - open app (national)
  - enable RGB + Alerta layers
  - zoom in
  - timeline playback at 1x then 8x
  - save screenshots + a minimal run log

This script is intentionally self-contained and writes outputs under:
  output/playwright/scripts/<run_id>/
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from playwright.async_api import Browser, BrowserContext, Page, async_playwright


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT_ROOT = REPO_ROOT / "output" / "playwright" / "scripts"


@dataclass(frozen=True)
class RunConfig:
    base_url: str
    out_dir: Path
    headful: bool
    interactive_login: bool
    storage_state_path: Path | None
    bearer_token: str | None
    timeout_ms: int
    zoom_clicks: int
    playback_wait_ms_1x: int
    playback_wait_ms_8x: int


def _utc_run_id() -> str:
    return datetime.now(timezone.utc).strftime("stage2-%Y%m%dT%H%M%SZ")


def _env_str(name: str) -> str | None:
    value = os.environ.get(name)
    if value is None:
        return None
    value = value.strip()
    return value or None


def _safe_base_url(value: str) -> str:
    value = value.strip()
    if not value:
        raise ValueError("base_url is empty")
    if not (value.startswith("http://") or value.startswith("https://")):
        raise ValueError("base_url must start with http:// or https://")
    return value.rstrip("/") + "/"


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _write_json(path: Path, payload: Any) -> None:
    _write_text(path, json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True))


async def _attach_loggers(page: Page, *, out_dir: Path) -> dict[str, Any]:
    run_log: dict[str, Any] = {
        "console": [],
        "page_errors": [],
        "requests_failed": [],
    }

    def on_console(msg) -> None:
        try:
            run_log["console"].append(
                {
                    "type": msg.type,
                    "text": msg.text,
                    "location": msg.location,
                }
            )
        except Exception:
            # best effort only
            pass

    def on_page_error(exc: BaseException) -> None:
        try:
            run_log["page_errors"].append(str(exc))
        except Exception:
            pass

    def on_request_failed(req) -> None:
        try:
            run_log["requests_failed"].append(
                {
                    "url": req.url,
                    "method": req.method,
                    "resource_type": req.resource_type,
                    "failure": req.failure,
                }
            )
        except Exception:
            pass

    page.on("console", on_console)
    page.on("pageerror", on_page_error)
    page.on("requestfailed", on_request_failed)

    # Also dump a quick environment snapshot.
    _write_json(
        out_dir / "run_env.json",
        {
            "python": sys.version,
            "cwd": str(Path.cwd()),
            "base_url": page.url,
        },
    )
    return run_log


async def _screenshot(page: Page, *, out_dir: Path, name: str) -> Path:
    safe_name = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in name).strip("._")
    if not safe_name.lower().endswith(".png"):
        safe_name += ".png"
    path = out_dir / safe_name
    await page.screenshot(path=str(path), full_page=True)
    return path


async def _ensure_app_ready(page: Page, *, cfg: RunConfig, out_dir: Path) -> None:
    # Wait for the main map container.
    await page.wait_for_selector("#map", timeout=cfg.timeout_ms)
    await page.wait_for_selector("#map-layer-menu-toggle", timeout=cfg.timeout_ms)

    # If auth-gate shows up, we can't proceed unless interactive mode is enabled.
    gate = page.locator("#auth-gate")
    if await gate.is_visible():
        await _screenshot(page, out_dir=out_dir, name="00-auth-gate-visible.png")
        if not cfg.interactive_login:
            raise RuntimeError(
                "Auth gate is visible. Run with --interactive-login (and --headful), "
                "or provide --storage-state / AGROCLIMAX_SMOKE_STORAGE_STATE, "
                "or set AGROCLIMAX_SMOKE_BEARER_TOKEN if the backend accepts it."
            )
        # Best-effort: click the Google login button and wait for the user to finish.
        await page.locator("#auth-gate-login-btn").click(timeout=cfg.timeout_ms)
        # Give the user time to complete OAuth in the same browser window.
        # We detect readiness by the auth gate hiding OR by timeline controls becoming usable.
        await page.wait_for_timeout(1000)
        await page.wait_for_function(
            "(() => { const g = document.getElementById('auth-gate'); return !g || g.classList.contains('hidden') || !g.offsetParent; })()",
            timeout=180_000,
        )

    # Leaflet panes may not all be visible immediately, but they should exist.
    await page.wait_for_function(
        "() => document.querySelectorAll('.leaflet-pane').length > 0",
        timeout=cfg.timeout_ms,
    )


async def _open_layer_menu_and_enable(page: Page, *, out_dir: Path, layer_ids: list[str], cfg: RunConfig) -> None:
    # Open the layer menu (it is a toggle, and the panel is [hidden] when closed).
    await page.locator("#map-layer-menu-toggle").click(timeout=cfg.timeout_ms)
    await page.wait_for_selector("#map-layer-menu-panel:not([hidden])", timeout=cfg.timeout_ms)
    for layer_id in layer_ids:
        selector = f'input.map-layer-checkbox[data-layer-id="{layer_id}"]'
        await page.wait_for_selector(selector, timeout=cfg.timeout_ms)
        checkbox = page.locator(selector)
        if not await checkbox.is_checked():
            await checkbox.check(timeout=cfg.timeout_ms)
        await page.wait_for_timeout(250)
    await _screenshot(page, out_dir=out_dir, name="01-layer-menu-enabled.png")

    # Close the menu to avoid obscuring the map screenshots.
    await page.locator("#map-layer-menu-toggle").click(timeout=cfg.timeout_ms)
    await page.wait_for_function(
        "() => { const panel = document.getElementById('map-layer-menu-panel'); return !!panel && panel.hasAttribute('hidden'); }",
        timeout=cfg.timeout_ms,
    )


async def _wait_timeline_enabled(page: Page, *, cfg: RunConfig) -> None:
    # Timeline is enabled once at least one analytic layer is active and frames are fetched.
    await page.wait_for_selector("#map-timeline-play:not([disabled])", timeout=cfg.timeout_ms)
    await page.wait_for_selector("#map-timeline-speed:not([disabled])", timeout=cfg.timeout_ms)
    await page.wait_for_selector("#map-timeline-slider:not([disabled])", timeout=cfg.timeout_ms)


async def _wait_visible_analytic_overlay(
    page: Page,
    *,
    cfg: RunConfig,
    min_loaded_tiles: int = 4,
    settle_ms: int = 700,
) -> None:
    await page.wait_for_function(
        """
        (minLoaded) => {
          const containers = Array.from(document.querySelectorAll('.analytic-layer')).filter((element) => {
            const style = window.getComputedStyle(element);
            return style.display !== 'none' && Number(style.opacity || '0') > 0.05;
          });
          if (!containers.length) return false;
          const loaded = containers.flatMap((container) => Array.from(container.querySelectorAll('img'))).filter((img) => {
            const src = String(img.currentSrc || img.src || '');
            return (
              img.complete
              && Number(img.naturalWidth || 0) > 1
              && Number(img.naturalHeight || 0) > 1
              && !src.startsWith('data:image')
            );
          });
          return loaded.length >= minLoaded;
        }
        """,
        min_loaded_tiles,
        timeout=cfg.timeout_ms,
    )
    try:
        await page.wait_for_function(
            """
            () => {
              const loading = document.getElementById('map-tile-loading');
              if (!loading) return true;
              return window.getComputedStyle(loading).display === 'none';
            }
            """,
            timeout=min(cfg.timeout_ms, 4000),
        )
    except Exception:
        pass
    await page.wait_for_timeout(max(0, int(settle_ms)))


async def _zoom_in(page: Page, *, clicks: int, cfg: RunConfig) -> None:
    zoom_in_btn = page.locator(".leaflet-control-zoom-in")
    await zoom_in_btn.wait_for(timeout=cfg.timeout_ms)
    for _ in range(max(0, int(clicks))):
        await zoom_in_btn.click(timeout=cfg.timeout_ms)
        await page.wait_for_timeout(350)


async def _timeline_playback(page: Page, *, out_dir: Path, speed_value: str, wait_ms: int, cfg: RunConfig, label: str) -> None:
    await _wait_timeline_enabled(page, cfg=cfg)
    await page.select_option("#map-timeline-speed", speed_value)
    await page.wait_for_timeout(150)

    # Ensure we are playing.
    play_btn = page.locator("#map-timeline-play")
    previous_date = (await page.locator("#map-timeline-date").text_content()) or ""
    await play_btn.click(timeout=cfg.timeout_ms)
    await page.wait_for_function(
        """
        (previousDate) => {
          const el = document.getElementById('map-timeline-date');
          return !!el && String(el.textContent || '').trim() !== String(previousDate || '').trim();
        }
        """,
        previous_date,
        timeout=cfg.timeout_ms,
    )
    await _wait_visible_analytic_overlay(page, cfg=cfg)
    await page.wait_for_timeout(max(0, int(wait_ms)))
    await _screenshot(page, out_dir=out_dir, name=f"{label}.png")

    # Stop playback to keep deterministic captures.
    # If it's already paused, this is harmless.
    try:
        if (await play_btn.text_content()) and "pause" in str(await play_btn.text_content()).strip().lower():
            await play_btn.click(timeout=cfg.timeout_ms)
    except Exception:
        pass


async def _run(cfg: RunConfig) -> int:
    cfg.out_dir.mkdir(parents=True, exist_ok=True)

    async with async_playwright() as pw:
        browser: Browser = await pw.chromium.launch(
            headless=(not cfg.headful),
            args=[
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ],
        )

        context_kwargs: dict[str, Any] = {
            "viewport": {"width": 1920, "height": 1080},
            "ignore_https_errors": True,
        }
        if cfg.storage_state_path:
            context_kwargs["storage_state"] = str(cfg.storage_state_path)
        if cfg.bearer_token:
            context_kwargs["extra_http_headers"] = {"Authorization": f"Bearer {cfg.bearer_token}"}

        context: BrowserContext = await browser.new_context(**context_kwargs)
        page: Page = await context.new_page()
        page.set_default_timeout(cfg.timeout_ms)
        run_log = await _attach_loggers(page, out_dir=cfg.out_dir)

        try:
            await page.goto(cfg.base_url, wait_until="domcontentloaded")
            await page.reload(wait_until="domcontentloaded")
            await _ensure_app_ready(page, cfg=cfg, out_dir=cfg.out_dir)
            await _screenshot(page, out_dir=cfg.out_dir, name="00-initial.png")

            await _open_layer_menu_and_enable(page, out_dir=cfg.out_dir, layer_ids=["rgb", "alerta"], cfg=cfg)
            await _wait_timeline_enabled(page, cfg=cfg)
            await _wait_visible_analytic_overlay(page, cfg=cfg)
            await _screenshot(page, out_dir=cfg.out_dir, name="02-national-rgb-alerta.png")

            await _zoom_in(page, clicks=cfg.zoom_clicks, cfg=cfg)
            await _wait_visible_analytic_overlay(page, cfg=cfg)
            await _screenshot(page, out_dir=cfg.out_dir, name="03-zoom-in.png")

            # Playback at 1x then 8x.
            await _timeline_playback(
                page,
                out_dir=cfg.out_dir,
                speed_value="1",
                wait_ms=cfg.playback_wait_ms_1x,
                cfg=cfg,
                label="04-playback-1x",
            )
            await _timeline_playback(
                page,
                out_dir=cfg.out_dir,
                speed_value="8",
                wait_ms=cfg.playback_wait_ms_8x,
                cfg=cfg,
                label="05-playback-8x",
            )

            # Snapshot final timeline date/mode/status to support debugging without opening screenshots.
            timeline_snapshot = {
                "timeline_date": (await page.locator("#map-timeline-date").text_content()) or "",
                "timeline_mode": (await page.locator("#map-timeline-mode").text_content()) or "",
                "timeline_status": (await page.locator("#map-timeline-status").text_content()) or "",
                "timeline_source": (await page.locator("#map-timeline-source").text_content()) or "",
                "url": page.url,
            }
            _write_json(cfg.out_dir / "timeline_snapshot.json", timeline_snapshot)
            return 0
        except Exception as exc:
            run_log["fatal_error"] = f"{type(exc).__name__}: {exc}"
            try:
                await _screenshot(page, out_dir=cfg.out_dir, name="zz-fatal.png")
            except Exception:
                pass
            return 2
        finally:
            _write_json(cfg.out_dir / "run_log.json", run_log)
            await context.close()
            await browser.close()


def _parse_args(argv: list[str]) -> RunConfig:
    parser = argparse.ArgumentParser(description="AgroClimaX stage-2 smoke harness (Playwright)")
    parser.add_argument(
        "--base-url",
        default=_env_str("AGROCLIMAX_SMOKE_BASE_URL") or "http://127.0.0.1:8115/",
        help="Base URL for the AgroClimaX frontend (default: env AGROCLIMAX_SMOKE_BASE_URL or http://127.0.0.1:8115/).",
    )
    parser.add_argument(
        "--out-dir",
        default=None,
        help="Output directory. Default: output/playwright/scripts/<run_id>/",
    )
    parser.add_argument("--headful", action="store_true", help="Run with a visible browser window.")
    parser.add_argument(
        "--interactive-login",
        action="store_true",
        help="If the auth gate is visible, click Google login and wait up to 180s for manual completion.",
    )
    parser.add_argument(
        "--storage-state",
        default=_env_str("AGROCLIMAX_SMOKE_STORAGE_STATE"),
        help="Optional Playwright storage state JSON file (env: AGROCLIMAX_SMOKE_STORAGE_STATE).",
    )
    parser.add_argument(
        "--bearer-token",
        default=_env_str("AGROCLIMAX_SMOKE_BEARER_TOKEN"),
        help="Optional bearer token injected as Authorization header for all requests (env: AGROCLIMAX_SMOKE_BEARER_TOKEN).",
    )
    parser.add_argument("--timeout-ms", type=int, default=45_000, help="Default Playwright timeout per action.")
    parser.add_argument("--zoom-clicks", type=int, default=5, help="How many times to click Leaflet zoom-in.")
    parser.add_argument("--wait-1x-ms", type=int, default=3000, help="How long to wait while playing at 1x before screenshot.")
    parser.add_argument("--wait-8x-ms", type=int, default=2500, help="How long to wait while playing at 8x before screenshot.")

    ns = parser.parse_args(argv)
    base_url = _safe_base_url(str(ns.base_url))

    if ns.interactive_login and not ns.headful:
        raise SystemExit("--interactive-login requires --headful")

    out_dir = Path(str(ns.out_dir)) if ns.out_dir else (DEFAULT_OUT_ROOT / _utc_run_id())
    storage_state_path = Path(str(ns.storage_state)) if ns.storage_state else None
    if storage_state_path and not storage_state_path.exists():
        raise SystemExit(f"--storage-state path does not exist: {storage_state_path}")

    return RunConfig(
        base_url=base_url,
        out_dir=out_dir,
        headful=bool(ns.headful),
        interactive_login=bool(ns.interactive_login),
        storage_state_path=storage_state_path,
        bearer_token=str(ns.bearer_token).strip() if ns.bearer_token else None,
        timeout_ms=int(ns.timeout_ms),
        zoom_clicks=int(ns.zoom_clicks),
        playback_wait_ms_1x=int(ns.wait_1x_ms),
        playback_wait_ms_8x=int(ns.wait_8x_ms),
    )


def main(argv: list[str]) -> int:
    cfg = _parse_args(argv)
    cfg.out_dir.mkdir(parents=True, exist_ok=True)
    _write_json(
        cfg.out_dir / "run_config.json",
        {
            "base_url": cfg.base_url,
            "headful": cfg.headful,
            "interactive_login": cfg.interactive_login,
            "storage_state_path": str(cfg.storage_state_path) if cfg.storage_state_path else None,
            "bearer_token_configured": bool(cfg.bearer_token),
            "timeout_ms": cfg.timeout_ms,
            "zoom_clicks": cfg.zoom_clicks,
            "playback_wait_ms_1x": cfg.playback_wait_ms_1x,
            "playback_wait_ms_8x": cfg.playback_wait_ms_8x,
        },
    )
    return asyncio.run(_run(cfg))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
