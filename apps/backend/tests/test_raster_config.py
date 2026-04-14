import os
import unittest
from pathlib import Path

TEST_DB = Path(__file__).resolve().parent / "test_suite.db"
os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{TEST_DB.as_posix()}"
os.environ["DATABASE_SYNC_URL"] = f"sqlite:///{TEST_DB.as_posix()}"
os.environ["APP_ENV"] = "testing"
os.environ["AUTH_BYPASS_FOR_TESTS"] = "true"

from app.core.config import Settings
from app.core.config import settings
from app.services.raster_products import department_raster_storage_key, national_mosaic_storage_key


class RasterConfigTests(unittest.TestCase):
    def test_parses_internal_cutover_lists_and_raster_defaults(self):
        settings = Settings(
            _env_file=None,
            internal_only_layers="rgb,alerta,sar",
            internal_only_scopes="nacional,departamento",
            raster_backfill_priority_layers="alerta_fusion,rgb,ndmi,sar,ndvi,ndwi,savi,lst",
            raster_catalog_default_collections="sentinel-2-l2a,sentinel-1-grd,sentinel-3-slstr",
        )

        self.assertEqual(settings.internal_only_layers, ["rgb", "alerta", "sar"])
        self.assertEqual(settings.internal_only_scopes, ["nacional", "departamento"])
        self.assertEqual(
            settings.raster_backfill_priority_layers,
            ["alerta_fusion", "rgb", "ndmi", "sar", "ndvi", "ndwi", "savi", "lst"],
        )
        self.assertEqual(
            settings.raster_catalog_default_collections,
            ["sentinel-2-l2a", "sentinel-1-grd", "sentinel-3-slstr"],
        )

    def test_storage_bucket_enabled_requires_complete_s3_config(self):
        settings = Settings(
            _env_file=None,
            storage_backend="s3",
            storage_s3_endpoint_url="https://example-r2.invalid",
            storage_s3_bucket="agroclimax",
            storage_s3_access_key_id="abc",
            storage_s3_secret_access_key="def",
        )

        self.assertTrue(settings.storage_bucket_enabled)

    def test_raster_storage_keys_include_build_version(self):
        expected_version = str(settings.raster_product_build_version or "raster-v1")

        department_key = department_raster_storage_key(
            layer_id="rgb",
            display_date="2026-04-04",
            department="Rivera",
        )
        national_key = national_mosaic_storage_key(
            layer_id="rgb",
            display_date="2026-04-04",
        )

        self.assertIn(f"/{expected_version}/", department_key)
        self.assertIn(f"/{expected_version}/", national_key)


if __name__ == "__main__":
    unittest.main()
