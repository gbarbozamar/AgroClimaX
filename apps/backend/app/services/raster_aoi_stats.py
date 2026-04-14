from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.farm import FarmField, FarmPaddock
from app.models.materialized import RasterCacheEntry, RasterProduct
from app.services.raster_cache import raster_cache_key, upsert_raster_cache_entry


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


async def backfill_aoi_raster_stats(
    session: AsyncSession,
    *,
    start_date: date,
    end_date: date,
    layers: list[str] | None = None,
    commit_every: int | None = None,
) -> dict[str, Any]:
    start_dt = datetime.combine(start_date, time.min, tzinfo=timezone.utc)
    end_dt = datetime.combine(end_date, time.min, tzinfo=timezone.utc) + timedelta(days=1)
    layer_filter = [str(item).strip().lower() for item in (layers or []) if str(item).strip()]

    product_query = select(RasterProduct).where(
        RasterProduct.product_kind == "department_daily_cog",
        RasterProduct.display_date >= start_dt,
        RasterProduct.display_date < end_dt,
        RasterProduct.status.in_(["ready", "empty"]),
    )
    if layer_filter:
        product_query = product_query.where(RasterProduct.layer_id.in_(layer_filter))
    product_result = await session.execute(product_query)
    products = product_result.scalars().all()
    if not products:
        return {
            "status": "empty",
            "rows": 0,
            "date_from": start_date.isoformat(),
            "date_to": end_date.isoformat(),
        }

    field_result = await session.execute(
        select(FarmField.id, FarmField.department, FarmField.aoi_unit_id).where(
            FarmField.active.is_(True),
            FarmField.aoi_unit_id.is_not(None),
        )
    )
    paddock_result = await session.execute(
        select(FarmPaddock.id, FarmPaddock.aoi_unit_id, FarmField.department)
        .join(FarmField, FarmField.id == FarmPaddock.field_id)
        .where(
            FarmPaddock.active.is_(True),
            FarmPaddock.aoi_unit_id.is_not(None),
            FarmField.active.is_(True),
        )
    )

    fields_by_department: dict[str, list[tuple[str, str | None]]] = {}
    for field_id, department, aoi_unit_id in field_result.all():
        fields_by_department.setdefault(str(department), []).append((str(field_id), aoi_unit_id))

    paddocks_by_department: dict[str, list[tuple[str, str | None]]] = {}
    for paddock_id, aoi_unit_id, department in paddock_result.all():
        paddocks_by_department.setdefault(str(department), []).append((str(paddock_id), aoi_unit_id))

    written_rows = 0
    commit_every = max(int(commit_every or 0), 0) or None
    for product in products:
        if not product.display_date:
            continue
        department = str(product.scope_ref or "")
        product_metadata = dict(product.metadata_extra or {})
        for field_id, aoi_unit_id in fields_by_department.get(department, []):
            await upsert_raster_cache_entry(
                session,
                cache_key=raster_cache_key(
                    cache_kind="aoi_raster_stats",
                    layer_id=str(product.layer_id),
                    display_date=product.display_date.date(),
                    scope_type="field",
                    scope_ref=f"field:{field_id}",
                    bbox_bucket=f"aoi:{aoi_unit_id or field_id}",
                ),
                layer_id=str(product.layer_id),
                cache_kind="aoi_raster_stats",
                scope_type="field",
                scope_ref=f"field:{field_id}",
                display_date=product.display_date.date(),
                source_date=product.source_date.date() if product.source_date else None,
                bbox_bucket=f"aoi:{aoi_unit_id or field_id}",
                storage_backend=product.storage_backend,
                storage_key=product.storage_key,
                status=product.status,
                metadata_extra={
                    **product_metadata,
                    "department": department,
                    "aoi_unit_id": aoi_unit_id,
                    "aoi_scope": "field",
                    "aoi_scope_ref": f"field:{field_id}",
                },
                last_warmed_at=_now_utc(),
                last_hit_at=_now_utc(),
            )
            written_rows += 1
            if commit_every and written_rows % commit_every == 0:
                await session.commit()

        for paddock_id, aoi_unit_id in paddocks_by_department.get(department, []):
            await upsert_raster_cache_entry(
                session,
                cache_key=raster_cache_key(
                    cache_kind="aoi_raster_stats",
                    layer_id=str(product.layer_id),
                    display_date=product.display_date.date(),
                    scope_type="paddock",
                    scope_ref=f"paddock:{paddock_id}",
                    bbox_bucket=f"aoi:{aoi_unit_id or paddock_id}",
                ),
                layer_id=str(product.layer_id),
                cache_kind="aoi_raster_stats",
                scope_type="paddock",
                scope_ref=f"paddock:{paddock_id}",
                display_date=product.display_date.date(),
                source_date=product.source_date.date() if product.source_date else None,
                bbox_bucket=f"aoi:{aoi_unit_id or paddock_id}",
                storage_backend=product.storage_backend,
                storage_key=product.storage_key,
                status=product.status,
                metadata_extra={
                    **product_metadata,
                    "department": department,
                    "aoi_unit_id": aoi_unit_id,
                    "aoi_scope": "paddock",
                    "aoi_scope_ref": f"paddock:{paddock_id}",
                },
                last_warmed_at=_now_utc(),
                last_hit_at=_now_utc(),
            )
            written_rows += 1
            if commit_every and written_rows % commit_every == 0:
                await session.commit()

    await session.flush()
    return {
        "status": "success",
        "rows": written_rows,
        "date_from": start_date.isoformat(),
        "date_to": end_date.isoformat(),
    }
