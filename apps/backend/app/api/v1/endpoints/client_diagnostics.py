"""
Endpoint liviano para recibir snapshots de diagnóstico del frontend.

El frontend (apps/frontend/src/diagnostics.js) acumula console logs,
errores JS, requests HTTP y acciones de usuario. Este endpoint los
loguea como una sola entrada estructurada para que el equipo pueda
reproducir bugs sin que el usuario tenga que copiar/pegar manualmente.

No persiste en DB — solo logging. Si en el futuro queremos auditoría,
agregamos una tabla `client_diagnostics` + retención TTL.
"""
from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Request

logger = logging.getLogger("agroclimax.client_diag")
router = APIRouter(tags=["diagnostics"])


@router.post("/client-diagnostics")
async def ingest_client_diagnostics(payload: dict[str, Any], request: Request) -> dict[str, Any]:
    counts = payload.get("counts") or {}
    store_snapshot = payload.get("storeSnapshot") or {}
    entries = payload.get("entries") or []
    user_agent = payload.get("userAgent") or request.headers.get("user-agent", "")
    started_at = payload.get("startedAt")
    location = payload.get("location")

    logger.info(
        "[CLIENT_DIAG] entries=%d errors=%d fetches=%d actions=%d ua=%r location=%r started_at=%s store=%r",
        len(entries),
        int(counts.get("errors", 0)),
        int(counts.get("fetches", 0)),
        int(counts.get("actions", 0)),
        (user_agent or "")[:160],
        (location or "")[:200],
        started_at,
        {k: v for k, v in store_snapshot.items() if k in {"selectedScope", "selectedDepartment", "selectedFieldId", "activeLayers", "timelineDate"}},
    )

    # Loguear también los 10 errores más recientes para triage rápido.
    errors = [e for e in entries if isinstance(e, dict) and e.get("level") == "error"][-10:]
    for err in errors:
        logger.warning(
            "[CLIENT_DIAG][ERROR] t=%s type=%s message=%s meta=%s",
            err.get("t"),
            err.get("type"),
            (err.get("message") or "")[:400],
            (err.get("meta") or "")[:400],
        )

    return {
        "received": True,
        "stored_in_db": False,
        "entries_logged": len(entries),
        "errors_logged": len(errors),
    }
