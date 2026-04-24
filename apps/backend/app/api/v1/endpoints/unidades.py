from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.services.analysis import get_unit_traceability, list_units

router = APIRouter(prefix="/unidades", tags=["unidades"])


@router.get("")
async def unidades(
    include_custom: bool = Query(False),
    include_hex: bool = Query(False),
    include_productive: bool = Query(False),
    db: AsyncSession = Depends(get_db),
):
    datos = await list_units(
        db,
        include_custom=include_custom,
        include_hex=include_hex,
        include_productive=include_productive,
    )
    return {"total": len(datos), "datos": datos}


@router.get("/{unit_id}/trazabilidad")
async def trazabilidad(unit_id: str, db: AsyncSession = Depends(get_db)):
    return await get_unit_traceability(db, unit_id)
