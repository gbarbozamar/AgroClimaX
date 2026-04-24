from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.services.hexagons import h3_geojson, list_h3_units

router = APIRouter(prefix="/hexagonos", tags=["hexagonos"])


@router.get("")
async def hexagonos(
    department: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    datos = await list_h3_units(db, department=department)
    return {"total": len(datos), "datos": datos}


@router.get("/geojson")
async def hexagonos_geojson(
    department: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    return await h3_geojson(db, department=department)
