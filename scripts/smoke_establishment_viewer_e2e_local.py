"""
E2E Local Smoke: Establishment Viewer + Screenshots
---------------------------------------------------
This harness makes the Establishment Viewer smoke reproducible locally by:
  1) seeding a fresh sqlite DB with 1 establishment + 1 field + 1 paddock
  2) starting the backend (serves the frontend too)
  3) running the Playwright viewer smoke to produce screenshots + logs

Usage:
  python scripts/smoke_establishment_viewer_e2e_local.py --port 8125
"""

from __future__ import annotations

import argparse
import asyncio
import os
import socket
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx


REPO_ROOT = Path(__file__).resolve().parents[1]
BACKEND_DIR = REPO_ROOT / "apps" / "backend"
DEFAULT_OUT_ROOT = REPO_ROOT / "output" / "playwright" / "scripts"


def _utc_run_id() -> str:
    return datetime.now(timezone.utc).strftime("viewer-e2e-%Y%m%dT%H%M%SZ")


async def _seed_sqlite_db(db_path: Path) -> None:
    os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{db_path.as_posix()}"
    os.environ["DATABASE_SYNC_URL"] = f"sqlite:///{db_path.as_posix()}"
    os.environ["APP_ENV"] = "testing"
    os.environ["AUTH_BYPASS_FOR_TESTS"] = "true"
    os.environ["DATABASE_USE_POSTGIS"] = "false"
    os.environ["PIPELINE_STARTUP_WARMUP_ENABLED"] = "false"
    os.environ["PIPELINE_SCHEDULER_ENABLED"] = "false"
    os.environ["PRELOAD_ENABLED"] = "false"
    os.environ["CONEAT_PREWARM_ENABLED"] = "false"
    os.environ["TEMPORAL_PREWARM_ENABLED"] = "false"

    sys.path.insert(0, str(BACKEND_DIR))
    from app.db.session import AsyncSessionLocal, Base, engine
    from app.models.auth import AppUser
    from app.models.farm import FarmEstablishment, FarmField, FarmPaddock
    from app.models.humedad import AOIUnit
    from app.services.catalog import department_payloads

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # IMPORTANT: when AUTH_BYPASS_FOR_TESTS is enabled, the backend synthesizes an auth context
    # with a fixed user_id="test-user" (see app.services.auth._testing_auth_context). For E2E
    # determinism, we seed rows under that same user id.
    async with AsyncSessionLocal() as session:
        # Pre-seed department AOIs to avoid lazy catalog seeding races on first page load.
        # We use fallback-box geometries (no network required).
        for payload in department_payloads(refresh_geometries=False):
            session.add(
                AOIUnit(
                    id=payload["id"],
                    slug=payload["slug"],
                    unit_type="department",
                    scope="departamento",
                    name=payload["name"],
                    department=payload["department"],
                    geometry_geojson=payload.get("geometry_geojson"),
                    centroid_lat=payload.get("centroid_lat"),
                    centroid_lon=payload.get("centroid_lon"),
                    coverage_class=payload.get("coverage_class") or "pastura_cultivo",
                    source=payload.get("geometry_source") or "fallback_boxes",
                    data_mode="catalog",
                    metadata_extra=payload.get("metadata_extra") or {},
                    active=True,
                )
            )

        user = AppUser(
            id="test-user",
            google_sub="test-google-sub",
            email="test@agroclimax.local",
            email_verified=True,
            full_name="Test User",
            is_active=True,
        )
        session.add(user)

        establishment = FarmEstablishment(
            id="smoke-est-1",
            user_id=user.id,
            name="Establecimiento Smoke",
            description="Seeded by local smoke harness",
            active=True,
        )
        session.add(establishment)

        field_id = "smoke-field-1"
        field_geom = {
            "type": "Polygon",
            "coordinates": [[[-56.25, -31.55], [-56.05, -31.55], [-56.05, -31.35], [-56.25, -31.35], [-56.25, -31.55]]],
        }
        field_unit_id = f"user-field-{field_id}"
        session.add(
            AOIUnit(
                id=field_unit_id,
                slug=field_unit_id,
                unit_type="productive_unit",
                scope="unidad",
                name="Campo Smoke",
                department="Rivera",
                geometry_geojson=field_geom,
                centroid_lat=-31.45,
                centroid_lon=-56.15,
                source="user_field",
                data_mode="derived_department",
                metadata_extra={"unit_category": "campo"},
                active=True,
            )
        )
        session.add(
            FarmField(
                id=field_id,
                establishment_id=establishment.id,
                user_id=user.id,
                name="Campo Smoke",
                department="Rivera",
                padron_value="12345",
                field_geometry_geojson=field_geom,
                centroid_lat=-31.45,
                centroid_lon=-56.15,
                area_ha=120.0,
                aoi_unit_id=field_unit_id,
                active=True,
            )
        )

        paddock_id = "smoke-paddock-1"
        paddock_geom = {
            "type": "Polygon",
            "coordinates": [[[-56.22, -31.52], [-56.12, -31.52], [-56.12, -31.42], [-56.22, -31.42], [-56.22, -31.52]]],
        }
        paddock_unit_id = f"user-paddock-{paddock_id}"
        session.add(
            AOIUnit(
                id=paddock_unit_id,
                slug=paddock_unit_id,
                unit_type="productive_unit",
                scope="unidad",
                name="Potrero Smoke",
                department="Rivera",
                geometry_geojson=paddock_geom,
                centroid_lat=-31.47,
                centroid_lon=-56.17,
                source="user_field",
                data_mode="derived_department",
                metadata_extra={"unit_category": "potrero"},
                active=True,
            )
        )
        session.add(
            FarmPaddock(
                id=paddock_id,
                field_id=field_id,
                user_id=user.id,
                name="Potrero Smoke",
                geometry_geojson=paddock_geom,
                area_ha=8.2,
                aoi_unit_id=paddock_unit_id,
                display_order=1,
                active=True,
            )
        )

        await session.commit()

    await engine.dispose()


def _wait_health(base_url: str, *, timeout_seconds: float = 35.0) -> None:
    deadline = time.time() + max(1.0, float(timeout_seconds))
    last_error: str | None = None
    with httpx.Client(timeout=3.0) as client:
        while time.time() < deadline:
            try:
                resp = client.get(f"{base_url.rstrip('/')}/api/health")
                if resp.status_code == 200:
                    return
                last_error = f"status={resp.status_code}"
            except Exception as exc:
                last_error = str(exc)
            time.sleep(0.5)
    raise RuntimeError(f"Backend did not become healthy within {timeout_seconds}s (last_error={last_error})")


def _find_free_port() -> int:
    # Bind to port 0 to ask the OS for a free port, then release it.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _run_subprocess(argv: list[str], *, cwd: Path, env: dict[str, str], log_path: Path | None = None) -> subprocess.Popen:
    log_file = None
    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_file = open(log_path, "wb")
    return subprocess.Popen(
        argv,
        cwd=str(cwd),
        env=env,
        stdout=log_file or subprocess.PIPE,
        stderr=log_file or subprocess.STDOUT,
    )


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Local E2E smoke for Establishment Viewer (seeds DB + screenshots).")
    parser.add_argument("--port", type=int, default=0, help="Port to bind the backend to. Use 0 to auto-pick a free port.")
    parser.add_argument("--timeout-seconds", type=float, default=45.0)
    parser.add_argument("--headful", action="store_true", help="Run Playwright headful (visible).")
    parser.add_argument("--interactive-login", action="store_true", help="Allow manual login if auth-gate appears.")
    ns = parser.parse_args(argv)

    run_id = _utc_run_id()
    out_dir = DEFAULT_OUT_ROOT / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    db_path = out_dir / f"viewer_smoke_{uuid.uuid4().hex}.db"
    asyncio.run(_seed_sqlite_db(db_path))

    port = int(ns.port) if int(ns.port) > 0 else _find_free_port()
    base_url = f"http://127.0.0.1:{port}/"

    env = dict(os.environ)
    env.update(
        {
            "PYTHONPATH": ".",
            "DATABASE_URL": f"sqlite+aiosqlite:///{db_path.as_posix()}",
            "DATABASE_SYNC_URL": f"sqlite:///{db_path.as_posix()}",
            "APP_ENV": "testing",
            "AUTH_BYPASS_FOR_TESTS": "true",
            "DATABASE_USE_POSTGIS": "false",
            "PIPELINE_STARTUP_WARMUP_ENABLED": "false",
            "PIPELINE_SCHEDULER_ENABLED": "false",
            "PRELOAD_ENABLED": "false",
            "CONEAT_PREWARM_ENABLED": "false",
            "TEMPORAL_PREWARM_ENABLED": "false",
            # Keep smokes deterministic: disable live Copernicus/Sentinel Hub access even if the developer
            # has credentials in apps/backend/.env.
            "COPERNICUS_CLIENT_ID": "",
            "COPERNICUS_CLIENT_SECRET": "",
            "SENTINELHUB_CLIENT_ID": "",
            "SENTINELHUB_CLIENT_SECRET": "",
            "SENTINELHUB_INSTANCE_ID": "",
        }
    )

    backend_log = out_dir / "backend.log"
    backend = _run_subprocess(
        [sys.executable, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", str(port)],
        cwd=BACKEND_DIR,
        env=env,
        log_path=backend_log,
    )

    try:
        # If the process crashed immediately (for example, port already in use), fail fast instead of
        # accidentally running smokes against an unrelated backend already bound to the same port.
        time.sleep(0.4)
        if backend.poll() is not None:
            try:
                tail = backend_log.read_text(encoding="utf-8")[-2000:]
            except Exception:
                tail = ""
            raise RuntimeError(f"Backend exited early (code={backend.returncode}). Log tail:\n{tail}")
        _wait_health(base_url, timeout_seconds=float(ns.timeout_seconds))
        smoke_argv = [
            sys.executable,
            str(REPO_ROOT / "scripts" / "smoke_establishment_viewer_playwright.py"),
            "--base-url",
            base_url,
            "--out-dir",
            str(out_dir),
        ]
        if ns.headful:
            smoke_argv.append("--headful")
        if ns.interactive_login:
            smoke_argv.append("--interactive-login")
        smoke = subprocess.run(smoke_argv, cwd=str(REPO_ROOT), env=env)
        return int(smoke.returncode)
    finally:
        try:
            backend.terminate()
        except Exception:
            pass
        try:
            backend.wait(timeout=8)
        except Exception:
            try:
                backend.kill()
            except Exception:
                pass
        # Best-effort cleanup of the sqlite DB file.
        try:
            db_path.unlink(missing_ok=True)
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
