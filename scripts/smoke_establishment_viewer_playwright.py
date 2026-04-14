"""
Establishment Viewer Smoke Harness (Playwright)
----------------------------------------------
Goal: reproducible, screenshot-driven smoke for the Establishment Viewer tab:
  - new browser session (fresh context)
  - open app
  - open Establishment Viewer
  - ensure establishments are present
  - ensure at least 1 field is listed
  - click a field and verify the frontend store reflects the selection
  - save screenshots + a minimal run log + store snapshot

Outputs are written under:
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
    settle_ms: int


def _utc_run_id() -> str:
    return datetime.now(timezone.utc).strftime("viewer-%Y%m%dT%H%M%SZ")


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
    run_log: dict[str, Any] = {"console": [], "page_errors": [], "requests_failed": []}

    def on_console(msg) -> None:
        try:
            run_log["console"].append({"type": msg.type, "text": msg.text, "location": msg.location})
        except Exception:
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

    _write_json(out_dir / "run_env.json", {"python": sys.version, "cwd": str(Path.cwd())})
    return run_log


async def _screenshot(page: Page, *, out_dir: Path, name: str) -> Path:
    safe_name = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in name).strip("._")
    if not safe_name.lower().endswith(".png"):
        safe_name += ".png"
    path = out_dir / safe_name
    await page.screenshot(path=str(path), full_page=True)
    return path


async def _ensure_app_ready(page: Page, *, cfg: RunConfig, out_dir: Path) -> None:
    await page.wait_for_selector("#map", timeout=cfg.timeout_ms)
    await page.wait_for_selector("#sidebar-establishment-viewer-tab", timeout=cfg.timeout_ms)

    gate = page.locator("#auth-gate")
    if await gate.is_visible():
        await _screenshot(page, out_dir=out_dir, name="00-auth-gate-visible.png")
        if not cfg.interactive_login:
            raise RuntimeError(
                "Auth gate is visible. Run with --interactive-login (and --headful), "
                "or provide --storage-state / AGROCLIMAX_SMOKE_STORAGE_STATE, "
                "or set AGROCLIMAX_SMOKE_BEARER_TOKEN if the backend accepts it."
            )
        await page.locator("#auth-gate-login-btn").click(timeout=cfg.timeout_ms)
        await page.wait_for_timeout(1000)
        await page.wait_for_function(
            "(() => { const g = document.getElementById('auth-gate'); return !g || g.classList.contains('hidden') || !g.offsetParent; })()",
            timeout=180_000,
        )

    await page.wait_for_function("() => document.querySelectorAll('.leaflet-pane').length > 0", timeout=cfg.timeout_ms)


async def _dump_store_snapshot(page: Page) -> dict[str, Any]:
    return await page.evaluate(
        """
        async () => {
          const stateMod = await import('/static/src/state.js');
          const map = stateMod.store.map;
          const center = map?.getCenter ? map.getCenter() : null;
          const selected = {
            establishmentId: stateMod.store.estViewerSelectedEstablishmentId || null,
            fieldId: stateMod.store.estViewerSelectedFieldId || null,
            paddockId: stateMod.store.selectedPaddockId || null,
          };
          return {
            sidebarView: stateMod.store.sidebarView,
            viewerMode: Boolean(stateMod.store.viewerMode),
            farmEstablishmentsCount: (stateMod.store.farmEstablishments || []).length,
            estViewerFieldsCount: (stateMod.store.estViewerFields || []).length,
            selected,
            map: {
              zoom: map?.getZoom ? map.getZoom() : null,
              center: center ? { lat: center.lat, lng: center.lng } : null,
            },
          };
        }
        """
    )


async def _open_establishment_viewer(page: Page, *, cfg: RunConfig) -> None:
    await page.locator("#sidebar-establishment-viewer-tab").click(timeout=cfg.timeout_ms)
    await page.wait_for_function(
        "() => { const v = document.getElementById('sidebar-establishment-viewer-view'); return !!v && !v.classList.contains('hidden'); }",
        timeout=cfg.timeout_ms,
    )


async def _wait_for_establishments(page: Page, *, cfg: RunConfig) -> None:
    await page.wait_for_function(
        """
        () => {
          const select = document.getElementById('establishment-viewer-select');
          if (!select) return false;
          // 1 placeholder + at least 1 establishment
          return (select.options?.length || 0) >= 2;
        }
        """,
        timeout=cfg.timeout_ms,
    )


async def _wait_for_fields(page: Page, *, cfg: RunConfig) -> None:
    # Either we have at least one field, or the panel is explicitly empty.
    await page.wait_for_function(
        """
        () => {
          const list = document.getElementById('establishment-viewer-fields-list');
          if (!list) return false;
          if (list.querySelector('[data-est-viewer-field-id]')) return true;
          const empty = list.querySelector('.fields-empty');
          return !!empty;
        }
        """,
        timeout=cfg.timeout_ms,
    )


async def _ensure_establishment_with_fields(page: Page, *, cfg: RunConfig) -> None:
    if await page.locator("[data-est-viewer-field-id]").count() > 0:
        return
    options: list[dict[str, str]] = await page.evaluate(
        """
        () => {
          const select = document.getElementById('establishment-viewer-select');
          return Array.from(select?.options || [])
            .map((option) => ({ value: String(option.value || ''), label: String(option.textContent || '') }))
            .filter((option) => option.value);
        }
        """
    )
    for option in options:
        await page.select_option("#establishment-viewer-select", option["value"])
        try:
            await page.wait_for_function(
                """
                async (establishmentId) => {
                  const stateMod = await import('/static/src/state.js');
                  return String(stateMod.store.estViewerSelectedEstablishmentId || '') === String(establishmentId || '')
                    && !stateMod.store.estViewerLoading;
                }
                """,
                arg=option["value"],
                timeout=cfg.timeout_ms,
            )
        except Exception:
            continue
        field_count = await page.evaluate(
            """
            async () => {
              const stateMod = await import('/static/src/state.js');
              return Number((stateMod.store.estViewerFields || []).length || 0);
            }
            """
        )
        if field_count > 0:
            return
    raise RuntimeError("Establishment Viewer has no fields in any establishment option.")


async def _select_first_field(page: Page, *, cfg: RunConfig) -> str:
    btn = page.locator("[data-est-viewer-field-id]").first
    if await btn.count() == 0:
        raise RuntimeError("No fields available in Establishment Viewer (fields list is empty).")
    field_id = await btn.get_attribute("data-est-viewer-field-id")
    if not field_id:
        raise RuntimeError("First field button missing data-est-viewer-field-id.")
    await btn.click(timeout=cfg.timeout_ms)
    await page.wait_for_function(
        """
        async (fieldId) => {
          const stateMod = await import('/static/src/state.js');
          return String(stateMod.store.estViewerSelectedFieldId || '') === String(fieldId || '');
        }
        """,
        arg=field_id,
        timeout=cfg.timeout_ms,
    )
    return str(field_id)


async def _run(cfg: RunConfig) -> int:
    cfg.out_dir.mkdir(parents=True, exist_ok=True)
    async with async_playwright() as pw:
        browser: Browser = await pw.chromium.launch(
            headless=(not cfg.headful),
            args=["--disable-dev-shm-usage", "--no-sandbox"],
        )

        context_kwargs: dict[str, Any] = {"viewport": {"width": 1920, "height": 1080}, "ignore_https_errors": True}
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
            # Force a refresh to avoid confusing cache artifacts.
            await page.reload(wait_until="domcontentloaded")
            await _ensure_app_ready(page, cfg=cfg, out_dir=cfg.out_dir)
            await _screenshot(page, out_dir=cfg.out_dir, name="00-initial.png")

            await _open_establishment_viewer(page, cfg=cfg)
            await _screenshot(page, out_dir=cfg.out_dir, name="01-viewer-open.png")

            await _wait_for_establishments(page, cfg=cfg)
            await _screenshot(page, out_dir=cfg.out_dir, name="02-establishments-loaded.png")

            await _wait_for_fields(page, cfg=cfg)
            await _ensure_establishment_with_fields(page, cfg=cfg)
            await _screenshot(page, out_dir=cfg.out_dir, name="03-fields-list.png")

            # If empty, fail with a clear snapshot.
            if await page.locator("[data-est-viewer-field-id]").count() == 0:
                snapshot = await _dump_store_snapshot(page)
                _write_json(cfg.out_dir / "store_snapshot.json", snapshot)
                raise RuntimeError("Establishment Viewer has no fields (cannot validate map selection flow).")

            field_id = await _select_first_field(page, cfg=cfg)
            await page.wait_for_timeout(max(0, int(cfg.settle_ms)))
            await _screenshot(page, out_dir=cfg.out_dir, name=f"04-field-selected-{field_id}.png")

            snapshot = await _dump_store_snapshot(page)
            snapshot["selected_field_id"] = field_id
            snapshot["url"] = page.url
            _write_json(cfg.out_dir / "store_snapshot.json", snapshot)
            return 0
        except Exception as exc:
            run_log["fatal_error"] = f"{type(exc).__name__}: {exc}"
            try:
                await _screenshot(page, out_dir=cfg.out_dir, name="zz-fatal.png")
                snapshot = await _dump_store_snapshot(page)
                snapshot["fatal_error"] = run_log["fatal_error"]
                _write_json(cfg.out_dir / "store_snapshot.json", snapshot)
            except Exception:
                pass
            return 2
        finally:
            _write_json(cfg.out_dir / "run_log.json", run_log)
            await context.close()
            await browser.close()


def _parse_args(argv: list[str]) -> RunConfig:
    parser = argparse.ArgumentParser(description="AgroClimaX Establishment Viewer smoke harness (Playwright)")
    parser.add_argument(
        "--base-url",
        default=_env_str("AGROCLIMAX_SMOKE_BASE_URL") or "http://127.0.0.1:8115/",
        help="Base URL for the AgroClimaX frontend.",
    )
    parser.add_argument("--out-dir", default=None, help="Output directory. Default: output/playwright/scripts/<run_id>/")
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
    parser.add_argument("--settle-ms", type=int, default=900, help="How long to wait after selecting a field before screenshot.")

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
        settle_ms=int(ns.settle_ms),
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
            "settle_ms": cfg.settle_ms,
        },
    )
    return asyncio.run(_run(cfg))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
