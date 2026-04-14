import os
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, patch

from sqlalchemy import delete, select

TEST_DB = Path(__file__).resolve().parent / "test_suite.db"
os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{TEST_DB.as_posix()}"
os.environ["DATABASE_SYNC_URL"] = f"sqlite:///{TEST_DB.as_posix()}"
os.environ["APP_ENV"] = "testing"
os.environ["AUTH_BYPASS_FOR_TESTS"] = "true"

from app.db.session import AsyncSessionLocal, Base, engine
from app.models import RasterMosaic, RasterProduct, SceneCoverage, SatelliteScene
from app.services import raster_catalog
from app.services.raster_catalog import sync_scene_catalog
from app.services.analysis import DEPARTMENTS


class RasterCatalogTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def asyncTearDown(self):
        async with AsyncSessionLocal() as session:
            for model in (SceneCoverage, SatelliteScene, RasterProduct, RasterMosaic):
                await session.execute(delete(model))
            await session.commit()
        await engine.dispose()

    async def test_sync_scene_catalog_persists_national_coverage_and_stac_assets(self):
        departments = [record.name for record in DEPARTMENTS]
        national_geometry = {
            "type": "Polygon",
            "coordinates": [[(-58.0, -35.0), (-53.0, -35.0), (-53.0, -30.0), (-58.0, -30.0), (-58.0, -35.0)]],
        }
        department_lookup = {
            name: {"department": name, "geometry_geojson": national_geometry}
            for name in departments
        }
        feature = {
            "id": "scene-001",
            "bbox": [-57.8, -34.8, -53.2, -30.2],
            "geometry": {
                "type": "Polygon",
                "coordinates": [[(-57.8, -34.8), (-53.2, -34.8), (-53.2, -30.2), (-57.8, -30.2), (-57.8, -34.8)]],
            },
            "properties": {
                "datetime": "2026-04-04T13:10:00Z",
                "platform": "Sentinel-2",
                "eo:cloud_cover": 12.5,
                "sat:orbit_state": "descending",
                "s2:mgrs_tile": "21HWC",
            },
            "assets": {
                "thumbnail": {"href": "https://example.invalid/thumb.png"},
                "visual": {"href": "https://example.invalid/visual.tif"},
            },
        }

        with patch.object(raster_catalog, "legacy_get_token", return_value="token"):
            with patch.object(raster_catalog, "_department_geometries", return_value=department_lookup):
                with patch.object(raster_catalog, "_department_units", new=AsyncMock(return_value={})):
                    with patch.object(raster_catalog, "_national_geometry_payload", return_value=national_geometry):
                        with patch.object(raster_catalog, "_search_catalog_features", new=AsyncMock(return_value=[feature])):
                            async with AsyncSessionLocal() as session:
                                result = await sync_scene_catalog(
                                    session,
                                    start_date=date(2026, 4, 4),
                                    end_date=date(2026, 4, 4),
                                    departments=departments,
                                    collections=["sentinel-2-l2a"],
                                )
                                await session.commit()

        self.assertEqual(result["status"], "success")
        self.assertTrue(result["national_coverage"])

        async with AsyncSessionLocal() as session:
            scene_row = (
                await session.execute(select(SatelliteScene).where(SatelliteScene.scene_id == "scene-001"))
            ).scalar_one()
            national_row = (
                await session.execute(
                    select(SceneCoverage).where(
                        SceneCoverage.scene_id == "scene-001",
                        SceneCoverage.scope_type == "nacional",
                        SceneCoverage.scope_ref == "Uruguay",
                    )
                )
            ).scalar_one()

        self.assertEqual(scene_row.quicklook_url, "https://example.invalid/thumb.png")
        self.assertEqual(scene_row.orbit, "descending")
        self.assertGreater(float(national_row.covered_area_pct or 0.0), 0.0)
        self.assertGreater(float(national_row.renderable_pixel_pct or 0.0), 0.0)
        self.assertEqual(national_row.metadata_extra.get("coverage_geometry_source"), "footprint")
        self.assertEqual(national_row.metadata_extra.get("tile_id"), "21HWC")


if __name__ == "__main__":
    unittest.main()
