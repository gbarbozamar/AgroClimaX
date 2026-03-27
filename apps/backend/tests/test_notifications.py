import os
import unittest
from datetime import datetime, timezone
from pathlib import Path

TEST_DB = Path(__file__).resolve().parent / "test_suite.db"
os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{TEST_DB.as_posix()}"
os.environ["DATABASE_SYNC_URL"] = f"sqlite:///{TEST_DB.as_posix()}"

from app.models.alerta import AlertState
from app.services.notifications import _notification_reasons


class NotificationLogicTests(unittest.TestCase):
    def test_notification_reasons_detect_state_change(self):
        current = AlertState(
            unit_id="productive-predio-demo",
            current_state="Alerta",
            state_level=2,
            confidence_score=72.0,
            forecast=[{"expected_risk": 61.0}],
            observed_at=datetime(2026, 3, 25, 12, 0, tzinfo=timezone.utc),
        )
        previous = {
            "current_state": "Vigilancia",
            "state_level": 1,
            "confidence_score": 74.0,
            "forecast": [{"expected_risk": 54.0}],
        }

        reasons = _notification_reasons(current, previous)

        self.assertIn("state_change", reasons)

    def test_notification_reasons_detect_confidence_shift(self):
        current = AlertState(
            unit_id="productive-predio-demo",
            current_state="Vigilancia",
            state_level=1,
            confidence_score=48.0,
            forecast=[{"expected_risk": 40.0}],
            observed_at=datetime(2026, 3, 25, 12, 0, tzinfo=timezone.utc),
        )
        previous = {
            "current_state": "Vigilancia",
            "state_level": 1,
            "confidence_score": 66.0,
            "forecast": [{"expected_risk": 42.0}],
        }

        reasons = _notification_reasons(current, previous)

        self.assertIn("confidence_shift", reasons)

    def test_notification_reasons_detect_forecast_deterioration(self):
        current = AlertState(
            unit_id="productive-predio-demo",
            current_state="Vigilancia",
            state_level=1,
            confidence_score=68.0,
            forecast=[{"expected_risk": 67.0}],
            observed_at=datetime(2026, 3, 25, 12, 0, tzinfo=timezone.utc),
        )
        previous = {
            "current_state": "Vigilancia",
            "state_level": 1,
            "confidence_score": 67.0,
            "forecast": [{"expected_risk": 49.0}],
        }

        reasons = _notification_reasons(current, previous)

        self.assertIn("forecast_deterioration", reasons)


if __name__ == "__main__":
    unittest.main()
