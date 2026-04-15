"""Tests for src.pipeline.rules."""

# pyright: reportPrivateUsage=false

import unittest

import pandas as pd

from tesorotools.pipeline.engine import apply_transformations
from tesorotools.pipeline.rules import (
    ratio_rule,
    rolling_sum_rule,
    scale_rule,
    sum_rule,
    yoy_rule,
)

from src.pipeline.rules import (
    aggregation_rules,
    all_rules,
    composition_rules,
    growth_rate_rules,
    normalize_rules,
    rolling_rules,
)


def _monthly_df(cols: dict[str, list[float]]) -> pd.DataFrame:
    """Build a monthly DataFrame for testing."""
    n = len(next(iter(cols.values())))
    idx = pd.date_range("2024-01-01", periods=n, freq="MS")
    idx.name = "date"
    return pd.DataFrame(cols, index=idx)


def _quarterly_df(cols: dict[str, list[float]]) -> pd.DataFrame:
    """Build a quarterly DataFrame for testing."""
    n = len(next(iter(cols.values())))
    idx = pd.date_range("2024-01-01", periods=n, freq="QS")
    idx.name = "date"
    return pd.DataFrame(cols, index=idx)


# -- Helper rule tests ---------------------------------------


class TestScaleRule(unittest.TestCase):
    def test_divides(self) -> None:
        df = _monthly_df({"X": [1000.0, 2000.0]})
        rule = scale_rule("X_BN", "X", 1e3)
        result = apply_transformations(df, [rule])
        self.assertAlmostEqual(result["X_BN"].iloc[0], 1.0)
        self.assertAlmostEqual(result["X_BN"].iloc[1], 2.0)


class TestSumRule(unittest.TestCase):
    def test_sums(self) -> None:
        df = _monthly_df({"A": [1.0, 2.0], "B": [10.0, 20.0]})
        rule = sum_rule("T", ["A", "B"])
        result = apply_transformations(df, [rule])
        self.assertAlmostEqual(result["T"].iloc[0], 11.0)


class TestRatioRule(unittest.TestCase):
    def test_ratio(self) -> None:
        df = _monthly_df({"N": [50.0], "D": [200.0]})
        rule = ratio_rule("R", "N", "D")
        result = apply_transformations(df, [rule])
        self.assertAlmostEqual(result["R"].iloc[0], 0.25)


class TestYoyRule(unittest.TestCase):
    def test_yoy_monthly(self) -> None:
        vals = [100.0] * 12 + [110.0]
        df = _monthly_df({"X": vals})
        rule = yoy_rule("X_YOY", "X", 12)
        result = apply_transformations(df, [rule])
        self.assertAlmostEqual(result["X_YOY"].iloc[-1], 0.10)
        # First 12 should be NaN
        self.assertTrue(pd.isna(result["X_YOY"].iloc[0]))

    def test_yoy_quarterly(self) -> None:
        vals = [100.0] * 4 + [120.0]
        df = _quarterly_df({"X": vals})
        rule = yoy_rule("X_YOY", "X", 4)
        result = apply_transformations(df, [rule])
        self.assertAlmostEqual(result["X_YOY"].iloc[-1], 0.20)


class TestRollingSumRule(unittest.TestCase):
    def test_rolling_4(self) -> None:
        df = _quarterly_df({"X": [10.0, 20.0, 30.0, 40.0, 50.0]})
        rule = rolling_sum_rule("X_4Q", "X", 4)
        result = apply_transformations(df, [rule])
        # First 3 should be NaN
        self.assertTrue(pd.isna(result["X_4Q"].iloc[0]))
        self.assertTrue(pd.isna(result["X_4Q"].iloc[2]))
        # Sum of 10+20+30+40 = 100
        self.assertAlmostEqual(result["X_4Q"].iloc[3], 100.0)
        # Sum of 20+30+40+50 = 140
        self.assertAlmostEqual(result["X_4Q"].iloc[4], 140.0)


# -- Rule set tests ------------------------------------------


class TestNormalizeRules(unittest.TestCase):
    def test_generates_rules_for_monetary_series(self) -> None:
        catalog = {
            "A": {
                "display_name": "a",
                "providers": {"bde": {"code": "X", "unit": "M_EUR"}},
            },
            "B": {
                "display_name": "b",
                "providers": {"bde": {"code": "Y", "unit": "PCT"}},
            },
        }
        rules = normalize_rules(catalog)  # type: ignore[arg-type]
        names = [r.output_name for r in rules]
        self.assertIn("A_BN", names)
        # PCT series should not get a _BN rule
        self.assertNotIn("B_BN", names)

    def test_correct_divisors(self) -> None:
        catalog = {
            "K": {
                "display_name": "k",
                "providers": {"bde": {"code": "X", "unit": "K_EUR"}},
            },
            "M": {
                "display_name": "m",
                "providers": {"bde": {"code": "Y", "unit": "M_EUR"}},
            },
            "B": {
                "display_name": "b",
                "providers": {"bde": {"code": "Z", "unit": "BN_EUR"}},
            },
        }
        rules = normalize_rules(catalog)  # type: ignore[arg-type]
        df = _monthly_df({"K": [1e6], "M": [1e3], "B": [1.0]})
        result = apply_transformations(df, rules)
        # All should become 1.0 billion
        self.assertAlmostEqual(result["K_BN"].iloc[0], 1.0)
        self.assertAlmostEqual(result["M_BN"].iloc[0], 1.0)
        self.assertAlmostEqual(result["B_BN"].iloc[0], 1.0)

    def test_empty_catalog(self) -> None:
        self.assertEqual(normalize_rules({}), [])  # type: ignore[arg-type]


class TestAggregationRules(unittest.TestCase):
    def test_returns_rules(self) -> None:
        rules = aggregation_rules()
        self.assertGreater(len(rules), 0)
        names = [r.output_name for r in rules]
        self.assertIn("FLUJOS_TOTAL_BN", names)


class TestCompositionRules(unittest.TestCase):
    def test_returns_rules(self) -> None:
        rules = composition_rules()
        self.assertGreater(len(rules), 0)
        names = [r.output_name for r in rules]
        self.assertIn("CF_PCT_EFECTIVO", names)


class TestGrowthRateRules(unittest.TestCase):
    def test_returns_rules(self) -> None:
        rules = growth_rate_rules()
        self.assertGreater(len(rules), 0)


class TestRollingRules(unittest.TestCase):
    def test_returns_rules(self) -> None:
        rules = rolling_rules()
        self.assertGreater(len(rules), 0)


class TestAllRules(unittest.TestCase):
    def test_correct_order(self) -> None:
        catalog = {
            "X": {
                "display_name": "x",
                "providers": {"bde": {"code": "C", "unit": "M_EUR"}},
            },
        }
        rules = all_rules(catalog)  # type: ignore[arg-type]
        self.assertGreater(len(rules), 0)
        # First rule should be a normalization rule
        self.assertTrue(rules[0].output_name.endswith("_BN"))


if __name__ == "__main__":
    unittest.main()
