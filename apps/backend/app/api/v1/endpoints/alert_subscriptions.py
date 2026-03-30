from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.services.auth import AuthContext, require_auth_context
from app.services.notification_media import get_notification_media_asset
from app.services.notifications import notification_service


router = APIRouter(prefix="/alert-subscriptions", tags=["alert-subscriptions"])
public_router = APIRouter(prefix="/alert-subscriptions", tags=["alert-subscriptions-public"])


class AlertSubscriptionWriteRequest(BaseModel):
    id: str | None = None
    scope_type: str
    scope_id: str | None = None
    channels_json: list[str] = Field(default_factory=list)
    min_alert_state: str = "Alerta"
    active: bool = True

    def to_service_payload(self) -> dict[str, Any]:
        return self.model_dump()


@router.get("")
async def list_alert_subscriptions(
    auth: AuthContext = Depends(require_auth_context),
    db: AsyncSession = Depends(get_db),
):
    items = await notification_service.list_alert_subscriptions(db, user=auth.user)
    return {"total": len(items), "items": items}


@router.get("/options")
async def get_alert_subscription_options(
    auth: AuthContext = Depends(require_auth_context),
    db: AsyncSession = Depends(get_db),
):
    return await notification_service.get_alert_subscription_options(db, user=auth.user)


@router.post("")
async def save_alert_subscription(
    payload: AlertSubscriptionWriteRequest,
    auth: AuthContext = Depends(require_auth_context),
    db: AsyncSession = Depends(get_db),
):
    try:
        return await notification_service.save_alert_subscription(db, user=auth.user, payload=payload.to_service_payload())
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.put("/{subscription_id}")
async def update_alert_subscription(
    subscription_id: str,
    payload: AlertSubscriptionWriteRequest,
    auth: AuthContext = Depends(require_auth_context),
    db: AsyncSession = Depends(get_db),
):
    try:
        return await notification_service.save_alert_subscription(
            db,
            user=auth.user,
            payload={**payload.to_service_payload(), "id": subscription_id},
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.delete("/{subscription_id}")
async def delete_alert_subscription(
    subscription_id: str,
    auth: AuthContext = Depends(require_auth_context),
    db: AsyncSession = Depends(get_db),
):
    try:
        return await notification_service.delete_alert_subscription(db, user=auth.user, subscription_id=subscription_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/{subscription_id}/test-send")
async def test_alert_subscription(
    subscription_id: str,
    auth: AuthContext = Depends(require_auth_context),
    db: AsyncSession = Depends(get_db),
):
    try:
        return await notification_service.send_alert_subscription_test(db, user=auth.user, subscription_id=subscription_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@public_router.get("/assets/{asset_id}")
async def get_alert_subscription_asset(
    asset_id: str,
    token: str = Query(..., min_length=8),
    db: AsyncSession = Depends(get_db),
):
    try:
        asset, content = await get_notification_media_asset(db, asset_id=asset_id, token=token)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return Response(content=content, media_type=asset.mime_type)
