import unittest
from datetime import date, datetime, timezone

from app.models.alerta import AlertState, AlertaEvento
from app.models.humedad import AOIUnit, SatelliteObservation
from app.services.analysis import (
    _apply_hysteresis,
    _build_live_observation_from_payload,
    _carry_forward_live_observation,
    _estimate_ndmi_from_calibration,
    _fixed_calibration_quantiles,
    _summarize_spatial_risk,
)


class AnalysisLogicTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
