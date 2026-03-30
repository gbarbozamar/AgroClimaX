import asyncio
import os
from pathlib import Path
import unittest
from unittest.mock import AsyncMock, patch

from sqlalchemy import delete, select

TEST_DB = Path(__file__).resolve().parent / "test_suite.db"
os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{TEST_DB.as_posix()}"
os.environ["DATABASE_SYNC_URL"] = f"sqlite:///{TEST_DB.as_posix()}"
os.environ["APP_ENV"] = "testing"
os.environ["AUTH_BYPASS_FOR_TESTS"] = "true"

from app.db.session import AsyncSessionLocal, Base, engine
from app.models.humedad import AOIUnit
from app.services.sections import seed_police_section_units


class PoliceSectionSeedTests(unittest.TestCase):
    def setUp(self):
        asyncio.run(self._reset_state())

    async def _reset_state(self):
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        async with AsyncSessionLocal() as session:
            await session.execute(delete(AOIUnit).where(AOIUnit.unit_type == "police_section"))
            await session.commit()

    def test_seed_police_sections_skips_when_source_is_unavailable(self):
        async def _run():
            async with AsyncSessionLocal() as session:
                with patch(
                    "app.services.sections.load_police_sections_geojson",
                    new=AsyncMock(side_effect=RuntimeError("snig unavailable")),
                ):
                    changed = await seed_police_section_units(session)
                await session.commit()
                return changed

        changed = asyncio.run(_run())
        self.assertEqual(changed, 0)

    def test_seed_police_sections_preserves_existing_records_when_refresh_fails(self):
        async def _run():
            async with AsyncSessionLocal() as session:
                session.add(
                    AOIUnit(
                        id="section-police-0101",
                        slug="seccion-policial-0101",
                        unit_type="police_section",
                        scope="seccion",
                        name="Seccion Policial SP 01 - Artigas",
                        department="Artigas",
                        geometry_geojson={
                            "type": "Polygon",
                            "coordinates": [[[-56.6, -30.4], [-56.55, -30.4], [-56.55, -30.45], [-56.6, -30.45], [-56.6, -30.4]]],
                        },
                        centroid_lat=-30.425,
                        centroid_lon=-56.575,
                        source="seeded-test",
                        metadata_extra={"section_code": "0101"},
                    )
                )
                await session.commit()
                with patch(
                    "app.services.sections.load_police_sections_geojson",
                    new=AsyncMock(side_effect=RuntimeError("snig unavailable")),
                ):
                    changed = await seed_police_section_units(session)
                remaining = await session.execute(
                    select(AOIUnit).where(AOIUnit.unit_type == "police_section", AOIUnit.id == "section-police-0101")
                )
                return changed, remaining.scalar_one_or_none()

        changed, existing = asyncio.run(_run())
        self.assertEqual(changed, 0)
        self.assertIsNotNone(existing)
        self.assertEqual(existing.source, "seeded-test")
