import importlib.util
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

TEST_DB = Path(__file__).resolve().parent / "test_suite.db"
os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{TEST_DB.as_posix()}"
os.environ["DATABASE_SYNC_URL"] = f"sqlite:///{TEST_DB.as_posix()}"
os.environ["APP_ENV"] = "testing"
os.environ["AUTH_BYPASS_FOR_TESTS"] = "true"

REPO_ROOT = Path(__file__).resolve().parents[3]
TILESERVER_PATH = REPO_ROOT / "agroclimax-tiles" / "server_app.py"

_SPEC = importlib.util.spec_from_file_location("agroclimax_tiles_server_app", TILESERVER_PATH)
assert _SPEC is not None and _SPEC.loader is not None
tileserver_module = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = tileserver_module
_SPEC.loader.exec_module(tileserver_module)


class TileServerContractTests(unittest.TestCase):
    def test_national_tile_with_only_transparent_assets_returns_empty_png(self):
        client = TestClient(tileserver_module.app)
        payload = {
            "assets": [
                {
                    "storage_key": "raster-products/cogs/raster-v1/rgb/2026-04-04/asset-a.tif",
                    "bbox": "-58,-35,-53,-30",
                }
            ]
        }

        with patch.object(tileserver_module, "_load_mosaic_payload", new=AsyncMock(return_value=payload)):
            with patch.object(tileserver_module, "_asset_intersects_tile", return_value=True):
                with patch.object(
                    tileserver_module,
                    "_read_cog_tile",
                    new=AsyncMock(
                        return_value=(
                            tileserver_module.TRANSPARENT_TILE_PNG,
                            {"renderable_pixel_pct": 0.0},
                        )
                    ),
                ):
                    response = client.get(
                        "/tiles/rgb/7/43/77.png",
                        params={
                            "display_date": "2026-04-04",
                            "scope_type": "nacional",
                            "scope_ref": "Uruguay",
                        },
                    )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get("content-type"), "image/png")
        self.assertEqual(response.headers.get("x-agroclimax-visual-empty"), "1")
        self.assertEqual(response.headers.get("x-agroclimax-visual-state"), "empty")
        self.assertEqual(response.headers.get("x-agroclimax-empty-reason"), "national_mosaic_tile_empty")
        self.assertEqual(response.content, tileserver_module.TRANSPARENT_TILE_PNG)


if __name__ == "__main__":
    unittest.main()
