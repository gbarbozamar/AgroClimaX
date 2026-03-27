import os
import unittest
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx

TEST_DB = Path(__file__).resolve().parent / "test_suite.db"
os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{TEST_DB.as_posix()}"
os.environ["DATABASE_SYNC_URL"] = f"sqlite:///{TEST_DB.as_posix()}"

from app.services.public_api import TRANSPARENT_PNG, _coneat_cache_entry_paths, _coneat_cache_key, proxy_coneat_request


class ConeatProxyTests(unittest.IsolatedAsyncioTestCase):
    def test_bbox_normalization_generates_same_cache_key(self):
        params_precise = {
            "SERVICE": "WMS",
            "REQUEST": "GetMap",
            "FORMAT": "image/png",
            "BBOX": "-56.25000000000001,-31.952162238024975,-53.43750000000001,-29.535229562948455",
        }
        params_rounded = {
            "SERVICE": "WMS",
            "REQUEST": "GetMap",
            "FORMAT": "image/png",
            "BBOX": "-56.250000,-31.952162,-53.437500,-29.535230",
        }
        self.assertEqual(_coneat_cache_key(params_precise), _coneat_cache_key(params_rounded))

    async def test_getmap_timeout_returns_transparent_png(self):
        params = {
            "SERVICE": "WMS",
            "REQUEST": "GetMap",
            "FORMAT": "image/png",
            "BBOX": f"-56,-31,-55,-30,{uuid.uuid4()}",
        }
        cache_path, meta_path = _coneat_cache_entry_paths(params)
        if cache_path.exists():
            cache_path.unlink()
        if meta_path.exists():
            meta_path.unlink()

        with patch(
            "app.services.public_api._fetch_coneat_remote",
            new=AsyncMock(side_effect=httpx.ConnectTimeout("timeout")),
        ):
            content, content_type = await proxy_coneat_request(params)

        self.assertEqual(content, TRANSPARENT_PNG)
        self.assertEqual(content_type, "image/png")

    async def test_getmap_timeout_uses_cached_tile_when_available(self):
        params = {
            "SERVICE": "WMS",
            "REQUEST": "GetMap",
            "FORMAT": "image/png",
            "BBOX": f"-57,-32,-56,-31,{uuid.uuid4()}",
        }
        cache_path, meta_path = _coneat_cache_entry_paths(params)
        cache_path.write_bytes(b"cached-image")
        meta_path.write_text('{"content_type":"image/png"}', encoding="utf-8")

        try:
            with patch(
                "app.services.public_api._fetch_coneat_remote",
                new=AsyncMock(side_effect=httpx.ConnectTimeout("timeout")),
            ):
                content, content_type = await proxy_coneat_request(params)
        finally:
            if cache_path.exists():
                cache_path.unlink()
            if meta_path.exists():
                meta_path.unlink()

        self.assertEqual(content, b"cached-image")
        self.assertEqual(content_type, "image/png")


if __name__ == "__main__":
    unittest.main()
