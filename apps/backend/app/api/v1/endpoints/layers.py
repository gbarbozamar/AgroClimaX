from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.services.analysis import backfill_department_spatial_cache, ensure_latest_daily_analysis
from app.services.hexagons import materialize_h3_cache
from app.services.productive_units import materialize_productive_unit_cache
from app.services.sections import materialize_police_section_cache
from app.services.warehouse import build_feature_collection, get_cached_layer_features

router = APIRouter(prefix="/capas", tags=["capas"])


@router.get("/catalogo")
async def catalogo_capas():
    from app.services.warehouse import LAYER_DEFINITIONS

    return {
        "total": len(LAYER_DEFINITIONS),
        "datos": [
            {"layer_key": key, **value, "tile_path_template": f"/api/tiles/{key}/{{z}}/{{x}}/{{y}}.png"}
            for key, value in LAYER_DEFINITIONS.items()
        ],
    }


@router.get("/departamentos")
async def capas_departamentos(
    department: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    rows = await get_cached_layer_features(db, layer_scope="departamento", department=department)
    if not rows:
        await ensure_latest_daily_analysis(db)
        await backfill_department_spatial_cache(db, department=department)
        await db.commit()
        rows = await get_cached_layer_features(db, layer_scope="departamento", department=department)
    return build_feature_collection(rows, layer_scope="departamentos", department=department)


@router.get("/secciones")
async def capas_secciones(
    department: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    rows = await get_cached_layer_features(db, layer_scope="seccion", department=department)
    if not rows:
        await materialize_police_section_cache(db, department=department)
        await db.commit()
        rows = await get_cached_layer_features(db, layer_scope="seccion", department=department)
    return build_feature_collection(rows, layer_scope="secciones_policiales", department=department)


@router.get("/hexagonos")
async def capas_hexagonos(
    department: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    rows = await get_cached_layer_features(db, layer_scope="hexagono", department=department)
    if not rows:
        await ensure_latest_daily_analysis(db)
        await materialize_h3_cache(db, department=department, ensure_base_analysis=False)
        await db.commit()
        rows = await get_cached_layer_features(db, layer_scope="hexagono", department=department)
    collection = build_feature_collection(rows, layer_scope="hexagonos_h3", department=department)
    if rows:
        resolution = next(
            (
                getattr(item, "properties", {}).get("h3_resolution")
                for item in rows
                if getattr(item, "properties", {}).get("h3_resolution") is not None
            ),
            None,
        )
        collection["metadata"]["resolution"] = resolution
    return collection


@router.get("/productivas")
async def capas_productivas(
    department: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    rows = await get_cached_layer_features(db, layer_scope="productiva", department=department)
    if not rows:
        await materialize_productive_unit_cache(db, department=department)
        rows = await get_cached_layer_features(db, layer_scope="productiva", department=department)
    return build_feature_collection(rows, layer_scope="unidades_productivas", department=department)
