"""Tests for src.tables."""

# pyright: reportPrivateUsage=false

import tempfile
import unittest
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from src.tables import (
    _build_table,
    _format_date,
    _format_value,
    generate_tables,
)


def _sample_df() -> pd.DataFrame:
    """Monthly DataFrame with known values."""
    idx = pd.date_range("2025-01-01", periods=6, freq="MS")
    idx.name = "date"
    return pd.DataFrame(
        {
            "A": [100.0, 200.0, 300.0, 400.0, 500.0, 600.0],
            "B": [1.5, 2.5, 3.5, 4.5, 5.5, 6.5],
            "A_YOY": [0.05, 0.06, 0.07, 0.08, 0.09, 0.10],
        },
        index=idx,
    )


def _write_config(path: Path, tables: dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump({"tables": tables}, f)


class TestFormatValue(unittest.TestCase):
    def test_integer(self) -> None:
        self.assertEqual(_format_value(1234.0, 0), "1.234")

    def test_decimals(self) -> None:
        self.assertEqual(_format_value(1234.56, 2), "1.234,56")

    def test_nan(self) -> None:
        self.assertEqual(_format_value(float("nan"), 1), "")


class TestFormatDate(unittest.TestCase):
    def test_monthly(self) -> None:
        ts = pd.Timestamp("2025-03-01")
        result = _format_date(ts, "monthly")
        self.assertIn("25", result)
        self.assertIn("mar", result.lower())

    def test_quarterly(self) -> None:
        ts = pd.Timestamp("2025-04-01")
        result = _format_date(ts, "quarterly")
        self.assertEqual(result, "2T-2025")


class TestBuildTable(unittest.TestCase):
    def test_basic(self) -> None:
        df = _sample_df()
        cfg: dict[str, Any] = {
            "series": {"A": "Series A", "B": "Series B"},
            "periods": 4,
            "decimals": 1,
            "frequency": "monthly",
        }
        result = _build_table(df, cfg)
        self.assertEqual(result.shape[0], 2)
        self.assertEqual(result.shape[1], 4)
        self.assertIn("Series A", result.index)

    def test_with_yoy(self) -> None:
        df = _sample_df()
        cfg: dict[str, Any] = {
            "series": {"A": "Series A"},
            "periods": 4,
            "decimals": 0,
            "frequency": "monthly",
            "yoy_series": {"A_YOY": "A (% ia)"},
            "yoy_decimals": 1,
            "yoy_scale": 100,
        }
        result = _build_table(df, cfg)
        self.assertIn("A (% ia)", result.index)
        self.assertEqual(result.shape[0], 2)


class TestGenerateTables(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.tmp.name)
        df = _sample_df()
        self.feather = self.tmp_path / "data.feather"
        df.to_feather(self.feather)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_generates_feathers(self) -> None:
        config = self.tmp_path / "tables.yaml"
        _write_config(
            config,
            {
                "test_tbl": {
                    "series": {"A": "A", "B": "B"},
                    "periods": 3,
                    "decimals": 1,
                    "frequency": "monthly",
                }
            },
        )
        out = self.tmp_path / "tables"
        result = generate_tables(config, self.feather, out)
        self.assertEqual(result, ["test_tbl"])
        self.assertTrue((out / "test_tbl.feather").exists())

    def test_feather_with_date_column(self) -> None:
        """Feather with reset_index has a 'date' column."""
        df = _sample_df()
        alt = self.tmp_path / "with_date.feather"
        df.reset_index().to_feather(alt)

        config = self.tmp_path / "tables.yaml"
        _write_config(
            config,
            {
                "date_col": {
                    "series": {"A": "A"},
                    "periods": 3,
                    "decimals": 0,
                    "frequency": "monthly",
                }
            },
        )
        out = self.tmp_path / "tables2"
        result = generate_tables(config, alt, out)
        self.assertEqual(result, ["date_col"])

    def test_failed_table_logged(self) -> None:
        config = self.tmp_path / "tables.yaml"
        _write_config(
            config,
            {
                "bad": {
                    "series": {"NONEXISTENT": "X"},
                    "periods": 3,
                    "decimals": 1,
                }
            },
        )
        out = self.tmp_path / "tables"
        result = generate_tables(config, self.feather, out)
        self.assertEqual(result, [])
