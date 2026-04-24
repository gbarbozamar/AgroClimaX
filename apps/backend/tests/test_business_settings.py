import unittest
from copy import deepcopy

from app.services.business_settings import (
    DEFAULT_ALERT_RULESET,
    _deep_diff,
    _deep_merge,
    _validate_rules,
)


class BusinessSettingsLogicTests(unittest.TestCase):
    def test_deep_merge_overrides_nested_values(self):
        base = {"risk_weights": {"magnitude": 35.0, "weather": 15.0}, "spatial": {"affected_pct_threshold": 35.0}}
        override = {"risk_weights": {"weather": 20.0}}

        merged = _deep_merge(base, override)

        self.assertEqual(merged["risk_weights"]["magnitude"], 35.0)
        self.assertEqual(merged["risk_weights"]["weather"], 20.0)
        self.assertEqual(merged["spatial"]["affected_pct_threshold"], 35.0)

    def test_deep_diff_returns_only_changed_branch(self):
        base = {"risk_weights": {"magnitude": 35.0, "weather": 15.0}}
        candidate = {"risk_weights": {"magnitude": 35.0, "weather": 20.0}}

        diff = _deep_diff(base, candidate)

        self.assertEqual(diff, {"risk_weights": {"weather": 20.0}})

    def test_validate_rules_rejects_invalid_weight_sum(self):
        rules = deepcopy(DEFAULT_ALERT_RULESET)
        rules["risk_weights"]["magnitude"] = 40.0

        with self.assertRaises(ValueError):
            _validate_rules(rules)

    def test_validate_rules_accepts_defaults(self):
        validated = _validate_rules(deepcopy(DEFAULT_ALERT_RULESET))

        self.assertEqual(validated["states"]["Normal"]["max_risk"], 24.0)
        self.assertEqual(validated["hysteresis"]["raise_consecutive_observations"], 2)


if __name__ == "__main__":
    unittest.main()
