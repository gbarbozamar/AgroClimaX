from datetime import datetime

from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.db.session import get_db
from app.services.analysis import ingest_ground_truth_measurement

router = APIRouter(prefix="/ground-truth", tags=["ground-truth"])


def _validate_api_key(api_key: str | None) -> None:
    if settings.ground_truth_api_keys:
        if api_key not in settings.ground_truth_api_keys:
            raise HTTPException(status_code=401, detail="API key invalida para ground truth")
        return
    if settings.app_env == "production":
        raise HTTPException(status_code=401, detail="Ground truth API key requerida")


@router.post("/measurements")
async def create_measurement(
    payload: dict,
    db: AsyncSession = Depends(get_db),
    x_field_api_key: str | None = Header(None),
):
    _validate_api_key(x_field_api_key)
    payload.setdefault("observed_at", datetime.utcnow().isoformat())
    return await ingest_ground_truth_measurement(db, payload)
