"""Tests for src.pipeline.engine."""

import unittest

import pandas as pd

from src.pipeline.engine import TransformationRule, apply_transformations


def _sample_df() -> pd.DataFrame:
    idx = pd.date_range("2025-01-01", periods=4, freq="MS")
    idx.name = "date"
    return pd.DataFrame(
        {"A": [1.0, 2.0, 3.0, 4.0], "B": [10.0, 20.0, 30.0, 40.0]},
        index=idx,
    )


class TestTransformationRule(unittest.TestCase):
    def test_frozen(self) -> None:
        rule = TransformationRule("X", ["A"], lambda df: df["A"] * 2)
        with self.assertRaises(AttributeError):
            rule.output_name = "Y"  # type: ignore[misc]


class TestApplyTransformations(unittest.TestCase):
    def test_single_rule(self) -> None:
        df = _sample_df()
        rules = [
            TransformationRule("C", ["A", "B"], lambda df: df["A"] + df["B"]),
        ]
        result = apply_transformations(df, rules)
        self.assertIn("C", result.columns)
        self.assertEqual(result["C"].iloc[0], 11.0)

    def test_does_not_modify_input(self) -> None:
        df = _sample_df()
        rules = [TransformationRule("C", ["A"], lambda df: df["A"] * 2)]
        apply_transformations(df, rules)
        self.assertNotIn("C", df.columns)

    def test_chained_rules(self) -> None:
        df = _sample_df()
        rules = [
            TransformationRule("C", ["A"], lambda df: df["A"] * 10),
            TransformationRule("D", ["C"], lambda df: df["C"] + 1),
        ]
        result = apply_transformations(df, rules)
        self.assertEqual(result["D"].iloc[0], 11.0)

    def test_missing_dependency_skipped(self) -> None:
        df = _sample_df()
        rules = [
            TransformationRule("X", ["MISSING"], lambda df: df["MISSING"]),
        ]
        with self.assertLogs("src.pipeline.engine", "WARNING"):
            result = apply_transformations(df, rules)
        self.assertNotIn("X", result.columns)

    def test_empty_rules(self) -> None:
        df = _sample_df()
        result = apply_transformations(df, [])
        pd.testing.assert_frame_equal(result, df)

    def test_empty_dataframe(self) -> None:
        df = pd.DataFrame()
        rules = [TransformationRule("X", [], lambda df: pd.Series([1.0]))]
        result = apply_transformations(df, rules)
        self.assertIn("X", result.columns)


if __name__ == "__main__":
    unittest.main()
