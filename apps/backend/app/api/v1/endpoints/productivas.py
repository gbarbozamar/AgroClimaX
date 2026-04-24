from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, File, Form, Query, UploadFile
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.services.productive_units import (
    import_productive_units,
    import_productive_units_file,
    list_productive_units,
    productive_units_geojson,
)

router = APIRouter(prefix="/productivas", tags=["productivas"])


class ProductiveImportRequest(BaseModel):
    feature_collection: dict[str, Any] = Field(..., description="GeoJSON FeatureCollection de predios/potreros")
    category: str = Field(default="predio")
    source_name: str = Field(default="user_import")
    name_field: str | None = Field(default=None)
    external_id_field: str | None = Field(default=None)


@router.get("")
async def productivas(
    department: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    datos = await list_productive_units(db, department=department)
    return {"total": len(datos), "datos": datos}


@router.get("/geojson")
async def productivas_geojson(
    department: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    return await productive_units_geojson(db, department=department)


@router.get("/plantilla")
async def plantilla_productivas():
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[-56.0, -31.0], [-55.96, -31.0], [-55.96, -31.04], [-56.0, -31.04], [-56.0, -31.0]]],
                },
                "properties": {
                    "name": "Predio Demo",
                    "external_id": "predio-demo-001",
                    "department_hint": "Rivera",
                },
            }
        ],
        "metadata": {
            "description": "Plantilla minima para importar predios/potreros en AgroClimaX.",
            "accepted_formats": [".geojson", ".json", ".zip"],
            "zip_note": "Para ZIP se espera shapefile con .shp, .shx y .dbf.",
        },
    }


@router.post("/import")
async def importar_productivas(
    payload: ProductiveImportRequest,
    db: AsyncSession = Depends(get_db),
):
    return await import_productive_units(
        db,
        payload.feature_collection,
        category=payload.category,
        source_name=payload.source_name,
        name_field=payload.name_field,
        external_id_field=payload.external_id_field,
    )


@router.post("/import-archivo")
async def importar_productivas_archivo(
    file: UploadFile = File(...),
    category: str = Form("predio"),
    source_name: str = Form("user_import"),
    name_field: str | None = Form(None),
    external_id_field: str | None = Form(None),
    db: AsyncSession = Depends(get_db),
):
    content = await file.read()
    return await import_productive_units_file(
        db,
        file.filename or "upload.geojson",
        content,
        category=category,
        source_name=source_name,
        name_field=name_field,
        external_id_field=external_id_field,
    )
