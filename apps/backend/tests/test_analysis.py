import os
from pathlib import Path
import unittest
from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, patch

TEST_DB = Path(__file__).resolve().parent / "test_suite.db"
os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{TEST_DB.as_posix()}"
os.environ["DATABASE_SYNC_URL"] = f"sqlite:///{TEST_DB.as_posix()}"
os.environ["APP_ENV"] = "testing"
os.environ["AUTH_BYPASS_FOR_TESTS"] = "true"

from app.models.alerta import AlertState, AlertaEvento
from app.models.humedad import AOIUnit, SatelliteObservation
from app.models.materialized import UnitIndexSnapshot
from app.services.business_settings import DEFAULT_ALERT_RULESET
from app.services.analysis import (
    _apply_hysteresis,
    _build_live_observation_from_payload,
    _carry_forward_live_observation,
    _derive_productive_observation,
    _estimate_ndmi_from_calibration,
    _fixed_calibration_quantiles,
    _summarize_spatial_risk,
    get_alert_history,
)


class AnalysisLogicTests(unittest.TestCase):
    def test_calibration_reference_columns_allow_long_transient_keys(self):
        self.assertEqual(UnitIndexSnapshot.__table__.columns["calibration_ref"].type.length, 255)
        self.assertEqual(AlertState.__table__.columns["calibration_ref"].type.length, 255)
        self.assertEqual(AlertaEvento.__table__.columns["calibration_ref"].type.length, 255)

    def test_fixed_calibration_is_monotonic(self):
        calibration = {
            "quantiles": _fixed_calibration_quantiles(),
            "quality_score": 62.0,
        }
        very_dry = _estimate_ndmi_from_calibration(-16.9, calibration)
        medium = _estimate_ndmi_from_calibration(-12.4, calibration)
        wet = _estimate_ndmi_from_calibration(-9.0, calibration)

        self.assertLess(very_dry, medium)
        self.assertLess(medium, wet)

    def test_hysteresis_requires_confirmation_to_raise(self):
        previous = AlertState(unit_id="u-1", current_state="Normal", state_level=0, days_in_state=1)
        recent = [AlertaEvento(unit_id="u-1", nivel=0, nivel_nombre="Normal", risk_score=18)]

        state_name, days = _apply_hysteresis(
            risk_score=62,
            confidence_score=80,
            previous_state=previous,
            recent_events=recent,
            forecast_improvement=False,
        )

        self.assertEqual(state_name, "Normal")
        self.assertEqual(days, 2)

    def test_hysteresis_drops_after_three_low_observations(self):
        previous = AlertState(unit_id="u-1", current_state="Alerta", state_level=2, days_in_state=5)
        recent = [
            AlertaEvento(unit_id="u-1", nivel=2, nivel_nombre="Alerta", risk_score=38),
            AlertaEvento(unit_id="u-1", nivel=2, nivel_nombre="Alerta", risk_score=40),
        ]

        state_name, days = _apply_hysteresis(
            risk_score=32,
            confidence_score=75,
            previous_state=previous,
            recent_events=recent,
            forecast_improvement=True,
        )

        self.assertEqual(state_name, "Vigilancia")
        self.assertEqual(days, 1)

    def test_spatial_summary_becomes_actionable_with_high_risk(self):
        geojson = {
            "type": "Polygon",
            "coordinates": [[
                [-55.51, -31.41],
                [-55.50, -31.41],
                [-55.50, -31.42],
                [-55.51, -31.42],
                [-55.51, -31.41],
            ]],
        }

        summary = _summarize_spatial_risk(82, 7, "custom-r9-test", geojson)

        self.assertGreaterEqual(summary["affected_pct"], 35.0)
        self.assertTrue(summary["actionable"])

    def test_live_payload_maps_core_metrics_and_qc(self):
        unit = AOIUnit(
            id="department-rivera",
            slug="departamento-rivera",
            unit_type="department",
            scope="departamento",
            name="Rivera",
            department="Rivera",
            coverage_class="pastura_cultivo",
            source="geoboundaries_cache",
        )
        payload = {
            "sentinel_1": {
                "vv_suelo_db_media": -12.4,
                "humedad_media": 46.8,
                "pct_area_bajo_estres": 14.2,
                "observed_at": "2026-03-23T09:00:00+00:00",
            },
            "sentinel_2": {
                "ndmi_media": 0.08,
                "cobertura_pct": 82.0,
                "observed_at": "2026-03-23T12:00:00+00:00",
            },
            "era5": {
                "spi_30d": -1.12,
                "spi_categoria": "moderadamente_seco",
            },
        }

        observation = _build_live_observation_from_payload(unit, payload, geometry_source="geoboundaries_cache")

        self.assertEqual(observation["source_mode"], "live_copernicus")
        self.assertEqual(observation["s1_humidity_mean_pct"], 46.8)
        self.assertEqual(observation["s2_ndmi_mean"], 0.08)
        self.assertEqual(observation["spi_30d"], -1.12)
        self.assertEqual(observation["quality_control"]["geometry_source"], "geoboundaries_cache")

    def test_recent_live_observation_can_carry_forward(self):
        unit = AOIUnit(
            id="department-rivera",
            slug="departamento-rivera",
            unit_type="department",
            scope="departamento",
            name="Rivera",
            department="Rivera",
            coverage_class="pastura_cultivo",
        )
        recent = SatelliteObservation(
            unit_id=unit.id,
            department=unit.department,
            observed_at=datetime(2026, 3, 21, 12, 0, tzinfo=timezone.utc),
            coverage_class="pastura_cultivo",
            vegetation_mask="vegetacion_media",
            source_mode="live_copernicus",
            s1_vv_db_mean=-12.1,
            s1_humidity_mean_pct=48.5,
            s1_pct_area_stressed=10.0,
            s2_ndmi_mean=0.09,
            s2_valid_pct=78.0,
            cloud_cover_pct=22.0,
            lag_hours=9.0,
            spi_30d=-0.8,
            spi_categoria="normal",
            quality_score=79.0,
            quality_control={"provider": "copernicus+openmeteo", "geometry_source": "geoboundaries_cache"},
        )

        carried = _carry_forward_live_observation(unit, date(2026, 3, 23), [recent], fallback_reason="batch_failed")

        self.assertIsNotNone(carried)
        self.assertEqual(carried["source_mode"], "carry_forward_live")
        self.assertEqual(carried["quality_control"]["fallback_reason"], "batch_failed")
        self.assertEqual(carried["s1_humidity_mean_pct"], 48.5)

    def test_productive_observation_can_derive_from_department_signal(self):
        department_unit = AOIUnit(
            id="department-rivera",
            slug="departamento-rivera",
            unit_type="department",
            scope="departamento",
            name="Rivera",
            department="Rivera",
            centroid_lat=-31.50,
            centroid_lon=-55.50,
            coverage_class="pastura_cultivo",
            source="geoboundaries_cache",
        )
        productive_unit = AOIUnit(
            id="productive-potrero-demo",
            slug="productive-potrero-demo",
            unit_type="productive_unit",
            scope="unidad",
            name="Potrero Norte",
            department="Rivera",
            centroid_lat=-31.55,
            centroid_lon=-55.42,
            coverage_class="pastura_cultivo",
            source="ui_upload",
            metadata_extra={"unit_category": "potrero"},
            geometry_geojson={
                "type": "Polygon",
                "coordinates": [[
                    [-55.45, -31.53],
                    [-55.39, -31.53],
                    [-55.39, -31.57],
                    [-55.45, -31.57],
                    [-55.45, -31.53],
                ]],
            },
        )
        department_state = AlertState(
            unit_id=department_unit.id,
            scope="departamento",
            department="Rivera",
            observed_at=datetime(2026, 3, 24, 12, 0, tzinfo=timezone.utc),
            current_state="Alerta",
            state_level=2,
            risk_score=68.0,
            confidence_score=74.0,
            affected_pct=39.0,
            days_in_state=5,
            raw_metrics={
                "s1_humidity_mean_pct": 44.2,
                "s2_ndmi_mean": 0.02,
                "s1_vv_db_mean": -12.8,
                "spi_30d": -1.05,
            },
        )
        department_observation = SatelliteObservation(
            unit_id=department_unit.id,
            department="Rivera",
            observed_at=datetime(2026, 3, 24, 10, 0, tzinfo=timezone.utc),
            coverage_class="pastura_cultivo",
            vegetation_mask="vegetacion_media",
            source_mode="live_copernicus",
            s1_vv_db_mean=-12.8,
            s1_humidity_mean_pct=44.2,
            s1_pct_area_stressed=21.0,
            s2_ndmi_mean=0.02,
            s2_valid_pct=79.0,
            cloud_cover_pct=21.0,
            lag_hours=8.0,
            spi_30d=-1.05,
            spi_categoria="moderadamente_seco",
            quality_score=76.0,
            quality_control={"provider": "copernicus+openmeteo", "freshness_days": 0.5},
        )

        derived = _derive_productive_observation(
            productive_unit,
            department_unit,
            department_state,
            department_observation,
            target_date=date(2026, 3, 24),
            geojson=productive_unit.geometry_geojson,
        )

        self.assertEqual(derived["source_mode"], "derived_department")
        self.assertEqual(derived["quality_control"]["fallback_reason"], "productive_unit_derived_from_department")
        self.assertEqual(derived["quality_control"]["source_department"], "Rivera")
        self.assertEqual(derived["raw_payload"]["unit_category"], "potrero")
        self.assertIsNotNone(derived["s1_humidity_mean_pct"])
        self.assertIsNotNone(derived["s2_ndmi_mean"])


class _ScalarResult:
    def __init__(self, items):
        self._items = items

    def scalars(self):
        return self

    def all(self):
        return self._items


class AlertHistoryLogicTests(unittest.IsolatedAsyncioTestCase):
    async def test_national_history_backfills_when_recent_window_is_incomplete(self):
        event = AlertaEvento(
            unit_id="department-rivera",
            scope="departamento",
            departamento="Rivera",
            fecha=datetime(2026, 3, 27, 12, 0, tzinfo=timezone.utc),
            nivel=0,
            nivel_nombre="Normal",
            risk_score=15.4,
            confidence_score=62.9,
            pct_area_afectada=0.0,
        )
        session = AsyncMock()
        session.execute = AsyncMock(side_effect=[_ScalarResult([event]), _ScalarResult([event])])

        with patch(
            "app.services.analysis._ensure_recent_department_event_history",
            new=AsyncMock(return_value={"synthetic_events": [], "synthetic_events_by_unit": {}}),
        ) as ensure_recent:
            with patch(
                "app.services.analysis.get_effective_alert_rules",
                new=AsyncMock(return_value={"rules": DEFAULT_ALERT_RULESET, "rules_version": "global-v1"}),
            ):
                payload = await get_alert_history(session, scope="nacional", limit=30)

        ensure_recent.assert_awaited_once_with(session, limit=30)
        self.assertEqual(payload["scope"], "nacional")
        self.assertEqual(payload["total"], 1)

    async def test_department_history_backfills_when_recent_window_is_incomplete(self):
        unit = AOIUnit(
            id="department-rivera",
            slug="departamento-rivera",
            unit_type="department",
            scope="departamento",
            name="Rivera",
            department="Rivera",
            coverage_class="pastura_cultivo",
        )
        event = AlertaEvento(
            unit_id=unit.id,
            scope="departamento",
            departamento="Rivera",
            fecha=datetime(2026, 3, 27, 12, 0, tzinfo=timezone.utc),
            nivel=0,
            nivel_nombre="Normal",
            risk_score=18.0,
            confidence_score=61.0,
            pct_area_afectada=2.0,
        )
        session = AsyncMock()
        session.execute = AsyncMock(side_effect=[_ScalarResult([event]), _ScalarResult([event])])

        with patch("app.services.analysis._get_unit", new=AsyncMock(return_value=unit)):
            with patch(
                "app.services.analysis._ensure_recent_department_event_history",
                new=AsyncMock(return_value={"synthetic_events": [], "synthetic_events_by_unit": {}}),
            ) as ensure_recent:
                payload = await get_alert_history(session, scope="departamento", department="Rivera", limit=30)

        ensure_recent.assert_awaited_once_with(session, limit=30, department="Rivera")
        self.assertEqual(payload["unit_id"], unit.id)
        self.assertEqual(payload["total"], 1)


if __name__ == "__main__":
    unittest.main()
