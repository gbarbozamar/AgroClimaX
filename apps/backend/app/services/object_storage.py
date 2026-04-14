from __future__ import annotations

import asyncio
from functools import lru_cache
from typing import Any
from urllib.parse import urlparse

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


def _presign_uses_https(url: str) -> bool:
    try:
        parsed = urlparse(url)
        return parsed.scheme.lower() == "https"
    except Exception:
        return False


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


async def storage_head_object(key: str) -> dict[str, Any] | None:
    """Returns lightweight object metadata, or None if missing/disabled."""
    client = _s3_client()
    if client is None:
        return None

    def _head():
        response = client.head_object(Bucket=settings.storage_s3_bucket, Key=_object_key(key))
        return {
            "content_length": int(response.get("ContentLength") or 0),
            "content_type": response.get("ContentType"),
            "etag": response.get("ETag"),
            "last_modified": response.get("LastModified"),
            "metadata": response.get("Metadata") or {},
        }

    try:
        return await asyncio.to_thread(_head)
    except Exception:
        return None


async def storage_object_exists(key: str) -> bool:
    return (await storage_head_object(key)) is not None


async def storage_get_presigned_url(key: str, *, expires_seconds: int = 900) -> str | None:
    """Creates a presigned GET url for a private object storage key.

    This is the safest way for rasterio/rio-tiler to read Cloud Optimized GeoTIFFs
    using HTTP range requests without pulling full objects into memory.
    """
    client = _s3_client()
    if client is None:
        return None

    expires = int(max(60, min(expires_seconds, 60 * 60 * 24)))

    def _presign() -> str:
        return client.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": settings.storage_s3_bucket, "Key": _object_key(key)},
            ExpiresIn=expires,
        )

    try:
        url = await asyncio.to_thread(_presign)
    except Exception:
        return None

    # Some deployments enforce HTTPS-only egress.
    if url and settings.storage_s3_endpoint_url and _presign_uses_https(settings.storage_s3_endpoint_url):
        return url.replace("http://", "https://", 1)
    return url


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
