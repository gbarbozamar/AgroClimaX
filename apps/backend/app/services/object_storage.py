from __future__ import annotations

import asyncio
from functools import lru_cache
from typing import Any

try:
    import boto3
except Exception:  # pragma: no cover
    boto3 = None

from app.core.config import settings


@lru_cache(maxsize=1)
def _s3_client():
    if boto3 is None or not settings.storage_bucket_enabled:
        return None
    return boto3.client(
        "s3",
        endpoint_url=settings.storage_s3_endpoint_url,
        region_name=settings.storage_s3_region,
        aws_access_key_id=settings.storage_s3_access_key_id,
        aws_secret_access_key=settings.storage_s3_secret_access_key,
    )


def _object_key(key: str) -> str:
    prefix = settings.storage_s3_prefix.strip().strip("/")
    normalized_key = key.strip().lstrip("/")
    if not prefix:
        return normalized_key
    return f"{prefix}/{normalized_key}"


async def storage_get_bytes(key: str) -> tuple[bytes, str | None, dict[str, Any]] | None:
    client = _s3_client()
    if client is None:
        return None

    def _read():
        response = client.get_object(Bucket=settings.storage_s3_bucket, Key=_object_key(key))
        return (
            response["Body"].read(),
            response.get("ContentType"),
            response.get("Metadata") or {},
        )

    try:
        return await asyncio.to_thread(_read)
    except Exception:
        return None


async def storage_put_bytes(
    key: str,
    content: bytes,
    *,
    content_type: str = "application/octet-stream",
    metadata: dict[str, str] | None = None,
) -> bool:
    client = _s3_client()
    if client is None:
        return False

    def _write():
        client.put_object(
            Bucket=settings.storage_s3_bucket,
            Key=_object_key(key),
            Body=content,
            ContentType=content_type,
            Metadata=metadata or {},
        )

    try:
        await asyncio.to_thread(_write)
        return True
    except Exception:
        return False
