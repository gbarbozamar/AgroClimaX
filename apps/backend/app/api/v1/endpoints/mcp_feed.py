"""
Fase 5 — MCP feed router.

Endpoints protegidos por `require_service_token` (X-Service-Token header).
Aceptan opcionalmente X-User-Id para scopear queries al owner.

Estos endpoints alimentan al MCP server en apps/mcp/server.py (que invoca
Claude Desktop u otros clientes MCP). El service token se configura vía
env MCP_SERVICE_TOKEN.

Endpoints:
  GET  /api/v1/mcp/fields/{field_id}/snapshot?layer=...&date=...
  GET  /api/v1/mcp/fields/{field_id}/timeline?layer=...&days=...
  POST /api/v1/mcp/fields/{field_id}/video
  GET  /api/v1/mcp/fields/by-alert?min_level=N
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.models.farm import FarmField
from app.services.auth import require_service_token

router = APIRouter(prefix="/mcp", tags=["mcp"])


class VideoRequestBody(BaseModel):
    layer_key: str = "ndvi"
    duration_days: int = 30


def _field_ownership_filter(user_id: str | None):
    """Si X-User-Id viene, filtramos por él; sino devolvemos todo (admin service)."""
    if user_id:
        return FarmField.user_id == user_id
    return None


@router.get("/fields/{field_id}/snapshot")
async def mcp_get_snapshot(
    field_id: str,
    layer: str = Query("ndvi"),
    date_str: str | None = Query(None, alias="date"),
    db: AsyncSession = Depends(get_db),
    auth: dict = Depends(require_service_token),
) -> dict:
    try:
        from app.models.field_snapshot import FieldImageSnapshot
    except Exception:
        raise HTTPException(status_code=503, detail="FieldImageSnapshot model not available")

    # Ownership filter si X-User-Id fue provisto.
    user_id = auth.get("user_id")
    if user_id:
        field_row = (await db.execute(
            select(FarmField).where(FarmField.id == field_id, FarmField.user_id == user_id)
        )).scalar_one_or_none()
        if field_row is None:
            raise HTTPException(status_code=404, detail="Field not found or not owned by user")

    stmt = select(FieldImageSnapshot).where(
        FieldImageSnapshot.field_id == field_id,
        FieldImageSnapshot.layer_key == layer,
    )
    if date_str:
        try:
            target = date.fromisoformat(date_str)
            stmt = stmt.where(FieldImageSnapshot.observed_at == target)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format (expect YYYY-MM-DD)")
    else:
        stmt = stmt.order_by(desc(FieldImageSnapshot.observed_at)).limit(1)

    row = (await db.execute(stmt)).scalar_one_or_none()
    if row is None:
        raise HTTPException(status_code=404, detail="Snapshot not found")
    return {
        "field_id": field_id,
        "layer_key": layer,
        "observed_at": row.observed_at.isoformat(),
        "image_url": f"/api/v1/campos/{field_id}/snapshots/{row.storage_key}",
        "metadata": {
            "risk_score": row.risk_score,
            "confidence_score": row.confidence_score,
            "s1_humidity_mean_pct": row.s1_humidity_mean_pct,
            "s2_ndmi_mean": row.s2_ndmi_mean,
            "spi_30d": row.spi_30d,
            "area_ha": row.area_ha,
            "bbox": row.bbox_json,
        },
    }


@router.get("/fields/{field_id}/timeline")
async def mcp_get_timeline(
    field_id: str,
    layer: str = Query("ndvi"),
    days: int = Query(30, ge=1, le=365),
    db: AsyncSession = Depends(get_db),
    auth: dict = Depends(require_service_token),
) -> dict:
    try:
        from app.models.field_snapshot import FieldImageSnapshot
    except Exception:
        raise HTTPException(status_code=503, detail="FieldImageSnapshot model not available")

    cutoff = date.today() - timedelta(days=days)
    stmt = (
        select(FieldImageSnapshot)
        .where(FieldImageSnapshot.field_id == field_id)
        .where(FieldImageSnapshot.layer_key == layer)
        .where(FieldImageSnapshot.observed_at >= cutoff)
        .order_by(FieldImageSnapshot.observed_at.asc())
    )
    rows = (await db.execute(stmt)).scalars().all()
    return {
        "field_id": field_id,
        "layer_key": layer,
        "total": len(rows),
        "days": [
            {
                "observed_at": r.observed_at.isoformat(),
                "image_url": f"/api/v1/campos/{field_id}/snapshots/{r.storage_key}",
                "risk_score": r.risk_score,
                "ndmi_mean": r.s2_ndmi_mean,
            }
            for r in rows
        ],
    }


@router.post("/fields/{field_id}/video")
async def mcp_request_video(
    field_id: str,
    body: VideoRequestBody,
    db: AsyncSession = Depends(get_db),
    auth: dict = Depends(require_service_token),
) -> dict:
    try:
        from app.models.field_video import FieldVideoJob
    except Exception:
        raise HTTPException(status_code=503, detail="FieldVideoJob model not available")

    from sqlalchemy import and_

    # Validar que el field existe antes de crear el job — evita 500 por FK
    # violation en Postgres cuando el field_id es invalido, y devuelve un 404
    # con detalle claro para MCP clients (Onyx, Claude Desktop). Si el caller
    # se identifica con X-User-Id, enforce ownership del field.
    field = (await db.execute(
        select(FarmField).where(FarmField.id == field_id)
    )).scalar_one_or_none()
    if field is None:
        raise HTTPException(status_code=404, detail=f"Field {field_id} not found")
    user_scope = auth.get("user_id")
    if user_scope and field.user_id != user_scope:
        raise HTTPException(status_code=404, detail=f"Field {field_id} not found")

    # Idempotencia: si hay un job ready o queued reciente, devolvemos el existente.
    recent = (await db.execute(
        select(FieldVideoJob)
        .where(and_(
            FieldVideoJob.field_id == field_id,
            FieldVideoJob.layer_key == body.layer_key,
            FieldVideoJob.duration_days == body.duration_days,
        ))
        .order_by(desc(FieldVideoJob.created_at))
        .limit(1)
    )).scalar_one_or_none()
    if recent is not None and recent.status in ("queued", "rendering", "ready"):
        return {
            "job_id": recent.id,
            "status": recent.status,
            "reused": True,
        }

    # Crear job nuevo (worker lo procesará).
    user_id = auth.get("user_id") or "mcp-service"
    job = FieldVideoJob(
        id=str(uuid4()),
        field_id=field_id,
        user_id=user_id,
        layer_key=body.layer_key,
        duration_days=body.duration_days,
        status="queued",
    )
    db.add(job)
    await db.commit()
    return {"job_id": job.id, "status": job.status, "reused": False}


@router.get("/fields/by-alert")
async def mcp_list_fields_by_alert(
    min_level: int = Query(2, ge=0, le=4),
    db: AsyncSession = Depends(get_db),
    auth: dict = Depends(require_service_token),
) -> list[dict[str, Any]]:
    try:
        from app.models.materialized import UnitIndexSnapshot
    except Exception:
        return []

    user_id = auth.get("user_id")

    # Query last snapshot per field_id en UnitIndexSnapshot (si existe columna
    # field_id); sino fallback a AlertState. Hacemos el filter en Python por
    # simplicidad (cantidad esperada pequeña).
    fields_stmt = select(FarmField)
    if user_id:
        fields_stmt = fields_stmt.where(FarmField.user_id == user_id)
    fields = (await db.execute(fields_stmt)).scalars().all()

    results: list[dict[str, Any]] = []
    for field in fields:
        # Intentar buscar snapshot por field.unit_id (si los fields están ligados
        # a AOIUnit via user-field unit_id). Si no, skip.
        unit_id = getattr(field, "unit_id", None) or f"user-field-{field.id}"
        snap = (await db.execute(
            select(UnitIndexSnapshot)
            .where(UnitIndexSnapshot.unit_id == unit_id)
            .order_by(desc(UnitIndexSnapshot.observed_at))
            .limit(1)
        )).scalar_one_or_none()
        level = getattr(snap, "state_level", None) if snap else None
        if level is not None and level >= min_level:
            results.append({
                "field_id": field.id,
                "field_name": field.name,
                "department": field.department,
                "state_level": level,
                "risk_score": getattr(snap, "risk_score", None),
                "observed_at": snap.observed_at.isoformat() if snap.observed_at else None,
            })
    return results


@router.get("/paddocks/{paddock_id}/metrics")
async def mcp_paddock_metrics(
    paddock_id: str,
    date_range_days: int = Query(30, ge=1, le=365),
    db: AsyncSession = Depends(get_db),
    auth: dict = Depends(require_service_token),
):
    try:
        from app.services.paddock_metrics import get_paddock_metrics
    except Exception:
        raise HTTPException(status_code=503, detail="paddock_metrics service not available")
    try:
        return await get_paddock_metrics(db, paddock_id, date_range_days)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/establishments/{establishment_id}/summary")
async def mcp_establishment_summary(
    establishment_id: str,
    db: AsyncSession = Depends(get_db),
    auth: dict = Depends(require_service_token),
):
    try:
        from app.services.establishment_summary import get_establishment_summary
    except Exception:
        raise HTTPException(status_code=503, detail="establishment_summary service not available")
    try:
        return await get_establishment_summary(db, establishment_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/fields/{field_id}/crop-prediction")
async def mcp_crop_prediction(
    field_id: str,
    horizon_days: int = Query(30, ge=1, le=365),
    db: AsyncSession = Depends(get_db),
    auth: dict = Depends(require_service_token),
):
    try:
        from app.services.crop_prediction import predict_crop_outlook
    except Exception:
        raise HTTPException(status_code=503, detail="crop_prediction service not available")
    try:
        return await predict_crop_outlook(db, field_id, horizon_days)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/fields/{field_id}/video/{job_id}")
async def mcp_get_video_status(
    field_id: str,
    job_id: str,
    db: AsyncSession = Depends(get_db),
    auth: dict = Depends(require_service_token),
) -> dict:
    """Estado actual de un video job: queued/rendering/ready/failed + progress + video_url + frame_count + size_bytes."""
    try:
        from app.models.field_video import FieldVideoJob
    except Exception:
        raise HTTPException(status_code=503, detail="FieldVideoJob model not available")
    from pathlib import Path
    job = await db.get(FieldVideoJob, job_id)
    if job is None or job.field_id != field_id:
        raise HTTPException(status_code=404, detail="Video job not found")
    size_bytes = None
    if job.video_path:
        p = Path(job.video_path)
        if p.exists():
            size_bytes = p.stat().st_size
    return {
        "job_id": job.id,
        "field_id": job.field_id,
        "layer_key": job.layer_key,
        "duration_days": job.duration_days,
        "status": job.status,
        "progress_pct": round(float(job.progress_pct or 0.0), 2),
        "frame_count": getattr(job, "frame_count", None),
        "size_bytes": size_bytes,
        "video_url": (f"/api/v1/campos/{job.field_id}/videos/{job.id}/file" if job.status == "ready" else None),
        "error_message": job.error_message,
        "created_at": job.created_at.isoformat() if job.created_at else None,
        "finished_at": job.finished_at.isoformat() if job.finished_at else None,
    }


@router.get("/fields/{field_id}/videos")
async def mcp_list_video_jobs(
    field_id: str,
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
    auth: dict = Depends(require_service_token),
) -> dict:
    """Lista los N video jobs más recientes de un campo."""
    try:
        from app.models.field_video import FieldVideoJob
    except Exception:
        raise HTTPException(status_code=503, detail="FieldVideoJob model not available")
    stmt = (
        select(FieldVideoJob)
        .where(FieldVideoJob.field_id == field_id)
        .order_by(desc(FieldVideoJob.created_at))
        .limit(limit)
    )
    rows = (await db.execute(stmt)).scalars().all()
    return {
        "field_id": field_id,
        "total": len(rows),
        "jobs": [
            {
                "job_id": j.id,
                "layer_key": j.layer_key,
                "duration_days": j.duration_days,
                "status": j.status,
                "frame_count": getattr(j, "frame_count", None),
                "created_at": j.created_at.isoformat() if j.created_at else None,
            }
            for j in rows
        ],
    }


@router.get("/users/{user_id}/fields")
async def mcp_list_user_fields(
    user_id: str,
    db: AsyncSession = Depends(get_db),
    auth: dict = Depends(require_service_token),
) -> dict:
    """Lista todos los campos de un usuario con datos básicos."""
    stmt = select(FarmField).where(FarmField.user_id == user_id, FarmField.active == True)  # noqa: E712
    rows = (await db.execute(stmt)).scalars().all()
    return {
        "user_id": user_id,
        "total": len(rows),
        "fields": [
            {
                "field_id": f.id,
                "field_name": f.name,
                "establishment_id": f.establishment_id,
                "department": f.department,
                "padron_value": f.padron_value,
                "area_ha": getattr(f, "area_ha", None),
            }
            for f in rows
        ],
    }


@router.get("/fields/{field_id}/details")
async def mcp_get_field_details(
    field_id: str,
    db: AsyncSession = Depends(get_db),
    auth: dict = Depends(require_service_token),
) -> dict:
    """Detalles completos de un campo: metadata + paddocks + analytics if available."""
    field = (await db.execute(select(FarmField).where(FarmField.id == field_id).limit(1))).scalar_one_or_none()
    if field is None:
        raise HTTPException(status_code=404, detail="Field not found")
    # Paddocks
    try:
        from app.models.farm import FarmPaddock
        pad_rows = (await db.execute(
            select(FarmPaddock).where(FarmPaddock.field_id == field_id, FarmPaddock.active == True)  # noqa: E712
        )).scalars().all()
        paddocks = [
            {"paddock_id": p.id, "paddock_name": p.name, "area_ha": getattr(p, "area_ha", None)}
            for p in pad_rows
        ]
    except Exception:
        paddocks = []
    return {
        "field_id": field.id,
        "field_name": field.name,
        "establishment_id": field.establishment_id,
        "department": field.department,
        "padron_value": field.padron_value,
        "area_ha": getattr(field, "area_ha", None),
        "user_id": field.user_id,
        "paddocks": paddocks,
        "n_paddocks": len(paddocks),
    }


@router.get("/fields/{field_id}/paddocks")
async def mcp_list_paddocks(
    field_id: str,
    db: AsyncSession = Depends(get_db),
    auth: dict = Depends(require_service_token),
) -> dict:
    """Lista los paddocks de un campo."""
    try:
        from app.models.farm import FarmPaddock
    except Exception:
        return {"field_id": field_id, "paddocks": [], "total": 0}
    rows = (await db.execute(
        select(FarmPaddock).where(FarmPaddock.field_id == field_id, FarmPaddock.active == True)  # noqa: E712
    )).scalars().all()
    return {
        "field_id": field_id,
        "total": len(rows),
        "paddocks": [
            {"paddock_id": p.id, "paddock_name": p.name, "area_ha": getattr(p, "area_ha", None)}
            for p in rows
        ],
    }


@router.get("/users/{user_id}/establishments")
async def mcp_list_establishments(
    user_id: str,
    db: AsyncSession = Depends(get_db),
    auth: dict = Depends(require_service_token),
) -> dict:
    """Lista los establecimientos de un usuario."""
    try:
        from app.models.farm import FarmEstablishment
    except Exception:
        return {"user_id": user_id, "establishments": [], "total": 0}
    rows = (await db.execute(
        select(FarmEstablishment).where(FarmEstablishment.user_id == user_id, FarmEstablishment.active == True)  # noqa: E712
    )).scalars().all()
    return {
        "user_id": user_id,
        "total": len(rows),
        "establishments": [
            {"establishment_id": e.id, "name": e.name, "description": getattr(e, "description", None)}
            for e in rows
        ],
    }


@router.get("/alerts/current")
async def mcp_get_alert_current(
    scope: str = Query("nacional"),
    ref: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
    auth: dict = Depends(require_service_token),
) -> dict:
    """Estado actual de alertas. scope: nacional|departamento|unidad|field. ref: department name o unit_id o field_id."""
    try:
        from app.services.analysis import _format_state_payload
        from app.models.materialized import LatestStateCache, UnitIndexSnapshot
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"model/service unavailable: {e}")

    # Mapear scope a unit_id esperado por la caché/materialized.
    if scope == "nacional":
        unit_id = "Uruguay"
    elif scope == "departamento":
        if not ref:
            raise HTTPException(status_code=400, detail="ref (department name) required for scope=departamento")
        unit_id = ref
    elif scope == "unidad":
        unit_id = ref
    elif scope == "field":
        if not ref:
            raise HTTPException(status_code=400, detail="ref (field_id) required for scope=field")
        unit_id = f"user-field-{ref}"
    else:
        raise HTTPException(status_code=400, detail=f"invalid scope: {scope}")

    # Leer último UnitIndexSnapshot.
    snap = (await db.execute(
        select(UnitIndexSnapshot)
        .where(UnitIndexSnapshot.unit_id == unit_id)
        .order_by(desc(UnitIndexSnapshot.observed_at))
        .limit(1)
    )).scalar_one_or_none()
    if snap is None:
        raise HTTPException(status_code=404, detail=f"No alert state for {scope}/{ref}")
    return {
        "scope": scope,
        "ref": ref,
        "unit_id": unit_id,
        "observed_at": snap.observed_at.isoformat() if snap.observed_at else None,
        "state": getattr(snap, "state", None),
        "state_level": getattr(snap, "state_level", None),
        "risk_score": getattr(snap, "risk_score", None),
        "confidence_score": getattr(snap, "confidence_score", None),
        "s1_humidity_mean_pct": getattr(snap, "s1_humidity_mean_pct", None),
        "s2_ndmi_mean": getattr(snap, "s2_ndmi_mean", None),
        "spi_30d": getattr(snap, "spi_30d", None),
        "primary_driver": getattr(snap, "primary_driver", None),
    }


@router.get("/alerts/history")
async def mcp_get_alert_history(
    scope: str = Query("nacional"),
    ref: str | None = Query(None),
    limit: int = Query(30, ge=1, le=365),
    db: AsyncSession = Depends(get_db),
    auth: dict = Depends(require_service_token),
) -> dict:
    """Histórico de alert states para un scope, ordenado por fecha desc, limit N."""
    try:
        from app.models.materialized import UnitIndexSnapshot
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"model unavailable: {e}")

    if scope == "nacional":
        unit_id = "Uruguay"
    elif scope == "departamento":
        unit_id = ref
    elif scope == "unidad":
        unit_id = ref
    elif scope == "field":
        unit_id = f"user-field-{ref}" if ref else None
    else:
        raise HTTPException(status_code=400, detail=f"invalid scope: {scope}")
    if not unit_id:
        raise HTTPException(status_code=400, detail="ref required")

    rows = (await db.execute(
        select(UnitIndexSnapshot)
        .where(UnitIndexSnapshot.unit_id == unit_id)
        .order_by(desc(UnitIndexSnapshot.observed_at))
        .limit(limit)
    )).scalars().all()
    return {
        "scope": scope, "ref": ref, "unit_id": unit_id,
        "total": len(rows),
        "history": [
            {
                "observed_at": r.observed_at.isoformat() if r.observed_at else None,
                "state": getattr(r, "state", None),
                "state_level": getattr(r, "state_level", None),
                "risk_score": getattr(r, "risk_score", None),
                "s2_ndmi_mean": getattr(r, "s2_ndmi_mean", None),
            }
            for r in rows
        ],
    }


@router.get("/alerts/forecast")
async def mcp_get_alert_forecast(
    scope: str = Query("nacional"),
    ref: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
    auth: dict = Depends(require_service_token),
) -> dict:
    """Forecast de alertas (si hay ForecastSignal disponible). Devuelve lista de días futuros con proyección."""
    try:
        from app.models.humedad import ForecastSignal
    except Exception:
        return {"scope": scope, "ref": ref, "forecast": [], "available": False}

    if scope == "nacional":
        unit_id = "Uruguay"
    elif scope in ("departamento", "unidad"):
        unit_id = ref
    elif scope == "field":
        unit_id = f"user-field-{ref}" if ref else None
    else:
        unit_id = None
    if not unit_id:
        return {"scope": scope, "ref": ref, "forecast": [], "available": False}

    rows = (await db.execute(
        select(ForecastSignal)
        .where(ForecastSignal.unit_id == unit_id)
        .order_by(ForecastSignal.forecast_date.asc())
        .limit(30)
    )).scalars().all()
    return {
        "scope": scope, "ref": ref, "unit_id": unit_id,
        "available": len(rows) > 0,
        "forecast": [
            {
                "forecast_date": r.forecast_date.isoformat() if r.forecast_date else None,
                "peak_risk": getattr(r, "peak_risk", None),
                "confidence": getattr(r, "confidence", None),
            }
            for r in rows
        ],
    }


class MCPBackfillBody(BaseModel):
    days: int = 30
    layers: list[str] = ["ndvi", "ndmi", "alerta_fusion"]


@router.get("/fields/{field_id}/layers-available")
async def mcp_layers_available(
    field_id: str,
    db: AsyncSession = Depends(get_db),
    auth: dict = Depends(require_service_token),
) -> dict:
    """Capas con snapshots rendereados para un campo (layer_key, count, rango fechas)."""
    try:
        from app.models.field_snapshot import FieldImageSnapshot
    except Exception:
        return {"field_id": field_id, "layers": []}
    from sqlalchemy import func
    stmt = (
        select(
            FieldImageSnapshot.layer_key,
            func.count(FieldImageSnapshot.id).label("count"),
            func.min(FieldImageSnapshot.observed_at).label("first_observed"),
            func.max(FieldImageSnapshot.observed_at).label("last_observed"),
        )
        .where(FieldImageSnapshot.field_id == field_id)
        .group_by(FieldImageSnapshot.layer_key)
        .order_by(FieldImageSnapshot.layer_key.asc())
    )
    rows = (await db.execute(stmt)).all()
    return {
        "field_id": field_id,
        "layers": [
            {
                "layer_key": r.layer_key,
                "count": int(r.count or 0),
                "first_observed": r.first_observed.isoformat() if r.first_observed else None,
                "last_observed": r.last_observed.isoformat() if r.last_observed else None,
            }
            for r in rows
        ],
    }


@router.post("/fields/{field_id}/backfill", status_code=202)
async def mcp_trigger_backfill(
    field_id: str,
    body: MCPBackfillBody,
    db: AsyncSession = Depends(get_db),
    auth: dict = Depends(require_service_token),
) -> dict:
    """Dispara backfill async de snapshots para N días y M capas."""
    if body.days < 1 or body.days > 365:
        raise HTTPException(status_code=400, detail="days must be in [1, 365]")
    import asyncio
    from datetime import date, timedelta
    from app.db.session import AsyncSessionLocal
    from app.services.field_snapshots import render_field_snapshot
    user_id = auth.get("user_id")
    async def _run():
        today = date.today()
        for i in range(body.days):
            target = today - timedelta(days=i)
            for layer in body.layers:
                async with AsyncSessionLocal() as session:
                    try:
                        await render_field_snapshot(session, field_id, layer, target, user_id=user_id)
                        await session.commit()
                    except Exception:
                        await session.rollback()
    asyncio.create_task(_run())
    return {
        "field_id": field_id,
        "status": "scheduled",
        "days": body.days,
        "layers": body.layers,
        "estimated_minutes": round(body.days * len(body.layers) * 6 / 60, 1),
    }


@router.get("/fields/{field_id}/snapshots/{storage_key:path}")
async def mcp_download_snapshot_png(
    field_id: str,
    storage_key: str,
    db: AsyncSession = Depends(get_db),
    auth: dict = Depends(require_service_token),
):
    """Sirve el PNG raw de un snapshot. storage_key esperado con prefix fields/{field_id}/."""
    from pathlib import Path
    from fastapi.responses import FileResponse
    safe_prefix = f"fields/{field_id}/"
    if not storage_key.startswith(safe_prefix):
        raise HTTPException(status_code=400, detail="storage_key does not match field scope")
    path = Path(".tile_cache") / storage_key
    if not path.exists():
        raise HTTPException(status_code=404, detail="Snapshot file not found")
    return FileResponse(path, media_type="image/png")
