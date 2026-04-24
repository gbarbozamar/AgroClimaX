from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.services.auth import AuthContext, require_auth_context
from app.services.farms import (
    delete_establishment,
    delete_field,
    delete_paddock,
    get_farm_options,
    get_field,
    list_establishments,
    list_fields,
    list_fields_geojson,
    list_paddocks,
    paddocks_geojson,
    save_establishment,
    save_field,
    save_paddock,
    search_padron,
)


router = APIRouter(tags=["campos"])


class EstablishmentWriteRequest(BaseModel):
    name: str
    description: str | None = None


class FieldWriteRequest(BaseModel):
    establishment_id: str
    name: str
    department: str
    padron_value: str
    padron_source: str = "snig_padronario_rural"
    padron_lookup_payload: dict[str, Any] = Field(default_factory=dict)
    padron_geometry_geojson: dict[str, Any] | None = None
    field_geometry_geojson: dict[str, Any]
    area_ha: float | None = None


class PaddockWriteRequest(BaseModel):
    name: str
    geometry_geojson: dict[str, Any]
    display_order: int | None = None


@router.get("/padrones/search")
async def padron_search(
    department: str = Query(..., min_length=2),
    padron: str = Query(..., min_length=1),
    auth: AuthContext = Depends(require_auth_context),
    db: AsyncSession = Depends(get_db),
):
    del auth
    try:
        return await search_padron(db, department=department, padron_value=padron)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("/campos/options")
async def campos_options(
    auth: AuthContext = Depends(require_auth_context),
    db: AsyncSession = Depends(get_db),
):
    return await get_farm_options(db, user=auth.user)


@router.get("/establecimientos")
async def establecimientos(
    auth: AuthContext = Depends(require_auth_context),
    db: AsyncSession = Depends(get_db),
):
    items = await list_establishments(db, user=auth.user)
    return {"total": len(items), "items": items}


@router.post("/establecimientos")
async def create_establecimiento(
    payload: EstablishmentWriteRequest,
    auth: AuthContext = Depends(require_auth_context),
    db: AsyncSession = Depends(get_db),
):
    try:
        return await save_establishment(db, user=auth.user, payload=payload.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.put("/establecimientos/{establishment_id}")
async def update_establecimiento(
    establishment_id: str,
    payload: EstablishmentWriteRequest,
    auth: AuthContext = Depends(require_auth_context),
    db: AsyncSession = Depends(get_db),
):
    try:
        return await save_establishment(db, user=auth.user, payload=payload.model_dump(), establishment_id=establishment_id)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.delete("/establecimientos/{establishment_id}")
async def remove_establecimiento(
    establishment_id: str,
    auth: AuthContext = Depends(require_auth_context),
    db: AsyncSession = Depends(get_db),
):
    try:
        return await delete_establishment(db, user=auth.user, establishment_id=establishment_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/campos")
async def campos(
    establishment_id: str | None = Query(None),
    auth: AuthContext = Depends(require_auth_context),
    db: AsyncSession = Depends(get_db),
):
    items = await list_fields(db, user=auth.user, establishment_id=establishment_id)
    return {"total": len(items), "items": items}


@router.get("/campos/geojson")
async def campos_geojson(
    establishment_id: str | None = Query(None),
    auth: AuthContext = Depends(require_auth_context),
    db: AsyncSession = Depends(get_db),
):
    return await list_fields_geojson(db, user=auth.user, establishment_id=establishment_id)


@router.get("/campos/{field_id}")
async def campo_detail(
    field_id: str,
    auth: AuthContext = Depends(require_auth_context),
    db: AsyncSession = Depends(get_db),
):
    try:
        return await get_field(db, user=auth.user, field_id=field_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/campos")
async def create_campo(
    payload: FieldWriteRequest,
    auth: AuthContext = Depends(require_auth_context),
    db: AsyncSession = Depends(get_db),
):
    try:
        return await save_field(db, user=auth.user, payload=payload.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.put("/campos/{field_id}")
async def update_campo(
    field_id: str,
    payload: FieldWriteRequest,
    auth: AuthContext = Depends(require_auth_context),
    db: AsyncSession = Depends(get_db),
):
    try:
        return await save_field(db, user=auth.user, payload=payload.model_dump(), field_id=field_id)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.delete("/campos/{field_id}")
async def remove_campo(
    field_id: str,
    auth: AuthContext = Depends(require_auth_context),
    db: AsyncSession = Depends(get_db),
):
    try:
        return await delete_field(db, user=auth.user, field_id=field_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/campos/{field_id}/potreros")
async def campo_potreros(
    field_id: str,
    auth: AuthContext = Depends(require_auth_context),
    db: AsyncSession = Depends(get_db),
):
    items = await list_paddocks(db, user=auth.user, field_id=field_id)
    return {"total": len(items), "items": items}


@router.get("/campos/{field_id}/potreros/geojson")
async def campo_potreros_geojson(
    field_id: str,
    auth: AuthContext = Depends(require_auth_context),
    db: AsyncSession = Depends(get_db),
):
    return await paddocks_geojson(db, user=auth.user, field_id=field_id)


@router.post("/campos/{field_id}/potreros")
async def create_potrero(
    field_id: str,
    payload: PaddockWriteRequest,
    auth: AuthContext = Depends(require_auth_context),
    db: AsyncSession = Depends(get_db),
):
    try:
        return await save_paddock(db, user=auth.user, field_id=field_id, payload=payload.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.put("/campos/{field_id}/potreros/{paddock_id}")
async def update_potrero(
    field_id: str,
    paddock_id: str,
    payload: PaddockWriteRequest,
    auth: AuthContext = Depends(require_auth_context),
    db: AsyncSession = Depends(get_db),
):
    try:
        return await save_paddock(db, user=auth.user, field_id=field_id, payload=payload.model_dump(), paddock_id=paddock_id)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.delete("/campos/{field_id}/potreros/{paddock_id}")
async def remove_potrero(
    field_id: str,
    paddock_id: str,
    auth: AuthContext = Depends(require_auth_context),
    db: AsyncSession = Depends(get_db),
):
    try:
        return await delete_paddock(db, user=auth.user, field_id=field_id, paddock_id=paddock_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

