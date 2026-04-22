"""
Fase 3 — Timeline propio del campo.

GET /api/v1/campos/{field_id}/timeline-frames?layer=ndvi&days=30
  -> lista de FieldImageSnapshot recientes (última ventana N días) para
     la capa pedida, con URL para el PNG rendereado + metadata embebida.

GET /api/v1/campos/{field_id}/snapshots/{storage_key:path}
  -> sirve el PNG raw leyendo del filesystem de tile cache.

Ambos endpoints requieren auth y verifican ownership del field.
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.models.farm import FarmField
from app.models.field_snapshot import FieldImageSnapshot
from app.services.auth import AuthContext, require_auth_context

router = APIRouter(tags=["field-timeline"])

# Labels amigables por layer_key — se reusan en UI y en el MCP.
LAYER_LABELS: dict[str, str] = {
    "ndvi": "NDVI (Vegetación)",
    "ndmi": "NDMI (Humedad Foliar)",
    "ndwi": "NDWI (Agua)",
    "savi": "SAVI (Suelo Ajustado)",
    "sar_vv": "SAR-VV (Radar)",
    "lst": "LST (Temperatura Superficie)",
    "rgb": "RGB (Color Natural)",
    "alerta_fusion": "Alerta Agroclimática",
}

# Ruta donde field_snapshots service escribe los PNGs.
_TILE_CACHE_ROOT = Path(".tile_cache")


async def _require_field_ownership(db: AsyncSession, field_id: str, user_id: str) -> FarmField:
    stmt = select(FarmField).where(FarmField.id == field_id)
    result = await db.execute(stmt)
    field = result.scalar_one_or_none()
    if field is None:
        raise HTTPException(status_code=404, detail="Field not found")
    if field.user_id != user_id:
        raise HTTPException(status_code=403, detail="Field not owned by user")
    return field


@router.get("/campos/{field_id}/timeline-frames")
async def get_field_timeline_frames(
    field_id: str,
    layer: str = Query("ndvi"),
    days: int = Query(30, ge=1, le=365),
    db: AsyncSession = Depends(get_db),
    auth: AuthContext = Depends(require_auth_context),
) -> dict:
    await _require_field_ownership(db, field_id, auth.user.id)
    cutoff = date.today() - timedelta(days=days)
    stmt = (
        select(FieldImageSnapshot)
        .where(FieldImageSnapshot.field_id == field_id)
        .where(FieldImageSnapshot.layer_key == layer)
        .where(FieldImageSnapshot.observed_at >= cutoff)
        .order_by(FieldImageSnapshot.observed_at.asc())
    )
    rows = (await db.execute(stmt)).scalars().all()
    frames = [
        {
            "observed_at": row.observed_at.isoformat(),
            "image_url": f"/api/v1/campos/{field_id}/snapshots/{row.storage_key}",
            "thumbnail_url": None,
            "metadata": {
                "risk_score": row.risk_score,
                "confidence_score": row.confidence_score,
                "s1_humidity_mean_pct": row.s1_humidity_mean_pct,
                "s2_ndmi_mean": row.s2_ndmi_mean,
                "spi_30d": row.spi_30d,
                "area_ha": row.area_ha,
                "bbox": row.bbox_json,
                "width_px": row.width_px,
                "height_px": row.height_px,
            },
        }
        for row in rows
    ]
    return {
        "field_id": field_id,
        "layer_key": layer,
        "total": len(frames),
        "days": frames,
    }


@router.get("/campos/{field_id}/snapshots/{storage_key:path}")
async def get_field_snapshot_image(
    field_id: str,
    storage_key: str,
    db: AsyncSession = Depends(get_db),
    auth: AuthContext = Depends(require_auth_context),
) -> Response:
    await _require_field_ownership(db, field_id, auth.user.id)
    # Sanity: storage_key debe empezar con fields/{field_id}/ para evitar path traversal.
    safe_prefix = f"fields/{field_id}/"
    if not storage_key.startswith(safe_prefix):
        raise HTTPException(status_code=400, detail="storage_key does not match field scope")
    path = _TILE_CACHE_ROOT / storage_key
    if not path.exists():
        raise HTTPException(status_code=404, detail="Snapshot file not found")
    return FileResponse(path, media_type="image/png")


class BackfillRequest(BaseModel):
    days: int = 30  # máx 365
    layers: list[str] = ["ndvi", "ndmi", "alerta_fusion"]


@router.post("/campos/{field_id}/backfill-snapshots", status_code=202)
async def trigger_backfill(
    field_id: str,
    body: BackfillRequest,
    db: AsyncSession = Depends(get_db),
    auth: AuthContext = Depends(require_auth_context),
) -> dict:
    await _require_field_ownership(db, field_id, auth.user.id)
    if body.days < 1 or body.days > 365:
        raise HTTPException(status_code=400, detail="days must be in [1, 365]")
    # Lanzar backfill async en background task (no bloquea la respuesta).
    import asyncio
    from datetime import date, timedelta
    from app.db.session import AsyncSessionLocal
    from app.services.field_snapshots import render_field_snapshot

    user_id = auth.user.id

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
