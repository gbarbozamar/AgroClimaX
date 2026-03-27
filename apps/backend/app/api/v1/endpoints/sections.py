from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.services.sections import list_police_sections, police_sections_geojson

router = APIRouter(prefix="/secciones", tags=["secciones"])


@router.get("")
async def secciones(
    department: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    datos = await list_police_sections(db, department=department)
    return {"total": len(datos), "datos": datos}


@router.get("/geojson")
async def secciones_geojson(
    department: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    return await police_sections_geojson(db, department=department)
