"""
Regression tests for fetch_tile_png tile-cache validation.

Before the fix, any 200 OK response with content-type image/* was cached,
including Copernicus's 334-byte transparent-placeholder PNGs served on
rate-limit / auth-partial failure. Those corrupt tiles would then be served
from disk cache for hours, showing blank overlays in the UI.

Now:
  - `_is_valid_tile_png(data)` checks PNG signature + >= 1024 bytes
  - `fetch_tile_png` refuses to cache tiny responses
  - Stale disk cache entries are discarded on read and re-fetched
"""
from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from app.services import public_api


# ── _is_valid_tile_png unit tests ──────────────────────────────────────

def test_is_valid_tile_png_accepts_real_png():
    """A real PNG signature + plenty of bytes passes."""
    data = public_api._PNG_SIGNATURE + b"\x00" * 2048
    assert public_api._is_valid_tile_png(data) is True


def test_is_valid_tile_png_rejects_tiny():
    """Under the 1024-byte threshold is rejected, even with valid signature."""
    data = public_api._PNG_SIGNATURE + b"\x00" * 300  # 308 bytes total
    assert public_api._is_valid_tile_png(data) is False


def test_is_valid_tile_png_rejects_wrong_signature():
    """Right size, wrong magic bytes → reject."""
    data = b"\xff\xd8\xff\xe0" + b"\x00" * 2048  # JPEG signature
    assert public_api._is_valid_tile_png(data) is False


def test_is_valid_tile_png_rejects_empty():
    assert public_api._is_valid_tile_png(None) is False
    assert public_api._is_valid_tile_png(b"") is False


def test_is_valid_tile_png_rejects_transparent_placeholder():
    """The hardcoded TRANSPARENT_PNG is itself under threshold by design."""
    assert public_api._is_valid_tile_png(public_api.TRANSPARENT_PNG) is False


# ── fetch_tile_png integration tests (mocked) ──────────────────────────

@pytest.mark.asyncio
async def test_fetch_tile_png_rejects_tiny_response(monkeypatch, tmp_path):
    """Copernicus returns 334-byte placeholder → MUST NOT cache to disk or bucket."""
    monkeypatch.setattr(public_api, "TILE_CACHE_DIR", tmp_path)
    monkeypatch.setattr(public_api.settings, "copernicus_client_id", "fake", raising=False)
    monkeypatch.setattr(public_api.settings, "copernicus_client_secret", "fake", raising=False)
    monkeypatch.setattr(public_api, "legacy_get_token", lambda: "fake-token")

    put_calls = []

    async def _fake_get_bytes(_key):
        return None

    async def _fake_put_bytes(key, content, **_kwargs):
        put_calls.append((key, len(content)))
        return None

    monkeypatch.setattr(public_api, "storage_get_bytes", _fake_get_bytes)
    monkeypatch.setattr(public_api, "storage_put_bytes", _fake_put_bytes)

    async def _fake_resolve(_layer, d):
        return {"primary_source_date": d.isoformat(), "secondary_source_date": None}

    monkeypatch.setattr(public_api, "_resolve_timeline_source_metadata", _fake_resolve)

    # 334-byte transparent-placeholder PNG — valid signature, under threshold
    tiny = public_api._PNG_SIGNATURE + b"\x00" * 326
    assert len(tiny) == 334

    fake_response = MagicMock()
    fake_response.status_code = 200
    fake_response.headers = {"content-type": "image/png"}
    fake_response.content = tiny

    monkeypatch.setattr(public_api.requests, "post", lambda *a, **k: fake_response)

    layer = next(iter(public_api.EVALSCRIPTS))
    result = await public_api.fetch_tile_png(
        layer, z=10, x=512, y=384, target_date=date(2026, 4, 19)
    )

    assert result == public_api.TRANSPARENT_PNG, "must return the placeholder fallback"
    cached_files = list(Path(tmp_path).glob("*.png"))
    assert cached_files == [], f"no disk cache should be written; got {cached_files}"
    assert put_calls == [], f"no bucket put should happen; got {put_calls}"


@pytest.mark.asyncio
async def test_fetch_tile_png_caches_valid_response(monkeypatch, tmp_path):
    """Happy path: real >=1KB PNG is cached to disk + bucket."""
    monkeypatch.setattr(public_api, "TILE_CACHE_DIR", tmp_path)
    monkeypatch.setattr(public_api.settings, "copernicus_client_id", "fake", raising=False)
    monkeypatch.setattr(public_api.settings, "copernicus_client_secret", "fake", raising=False)
    monkeypatch.setattr(public_api, "legacy_get_token", lambda: "fake-token")

    put_calls = []

    async def _fake_get_bytes(_key):
        return None

    async def _fake_put_bytes(key, content, **_kwargs):
        put_calls.append((key, len(content)))
        return None

    monkeypatch.setattr(public_api, "storage_get_bytes", _fake_get_bytes)
    monkeypatch.setattr(public_api, "storage_put_bytes", _fake_put_bytes)

    async def _fake_resolve(_layer, d):
        return {"primary_source_date": d.isoformat(), "secondary_source_date": None}

    monkeypatch.setattr(public_api, "_resolve_timeline_source_metadata", _fake_resolve)

    # Valid-size PNG stub
    valid = public_api._PNG_SIGNATURE + b"\xAA" * 4096
    fake_response = MagicMock()
    fake_response.status_code = 200
    fake_response.headers = {"content-type": "image/png"}
    fake_response.content = valid

    monkeypatch.setattr(public_api.requests, "post", lambda *a, **k: fake_response)

    layer = next(iter(public_api.EVALSCRIPTS))
    result = await public_api.fetch_tile_png(
        layer, z=10, x=512, y=384, target_date=date(2026, 4, 19)
    )

    assert result == valid
    cached_files = list(Path(tmp_path).glob("*.png"))
    assert len(cached_files) == 1
    assert cached_files[0].read_bytes() == valid
    assert len(put_calls) == 1


@pytest.mark.asyncio
async def test_fetch_tile_png_discards_stale_cache(monkeypatch, tmp_path):
    """If the disk cache has a tiny file from a previous bug era, refuse to serve it."""
    monkeypatch.setattr(public_api, "TILE_CACHE_DIR", tmp_path)
    monkeypatch.setattr(public_api.settings, "copernicus_client_id", "fake", raising=False)
    monkeypatch.setattr(public_api.settings, "copernicus_client_secret", "fake", raising=False)
    monkeypatch.setattr(public_api, "legacy_get_token", lambda: "fake-token")

    async def _fake_get_bytes(_key):
        return None

    async def _fake_put_bytes(*_a, **_k):
        return None

    monkeypatch.setattr(public_api, "storage_get_bytes", _fake_get_bytes)
    monkeypatch.setattr(public_api, "storage_put_bytes", _fake_put_bytes)

    async def _fake_resolve(_layer, d):
        return {"primary_source_date": d.isoformat(), "secondary_source_date": None}

    monkeypatch.setattr(public_api, "_resolve_timeline_source_metadata", _fake_resolve)

    layer = next(iter(public_api.EVALSCRIPTS))
    source_date = date(2026, 4, 19)
    cache_path = tmp_path / f"{public_api.resolve_temporal_layer_id(layer)}_{source_date.isoformat()}_7_44_76.png"
    cache_path.write_bytes(public_api._PNG_SIGNATURE + b"\x00" * 300)  # 308 bytes, invalid

    # Also stub the Copernicus call to return something valid so we can tell the difference
    valid = public_api._PNG_SIGNATURE + b"\xAA" * 4096
    fake_response = MagicMock()
    fake_response.status_code = 200
    fake_response.headers = {"content-type": "image/png"}
    fake_response.content = valid
    monkeypatch.setattr(public_api.requests, "post", lambda *a, **k: fake_response)

    result = await public_api.fetch_tile_png(
        layer, z=7, x=44, y=76, target_date=source_date
    )
    assert result == valid, "stale 308-byte cache must be discarded and refetched"
