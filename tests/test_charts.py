"""Tests for src.charts."""

# pyright: reportPrivateUsage=false

import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import yaml

from src.charts import (
    _clean_slice,
    _make_format,
    _make_legend,
    generate_charts,
)


def _mixed_freq_df() -> pd.DataFrame:
    """DataFrame mixing monthly and quarterly frequencies."""
    monthly = pd.date_range("2020-01-01", periods=12, freq="ME")
    df = pd.DataFrame(
        {"MONTHLY": range(1, 13)},
        index=monthly,
    )
    # Add a quarterly column: only values on quarter-end months.
    df["QUARTERLY"] = float("nan")
    for i in [2, 5, 8, 11]:  # Mar, Jun, Sep, Dec
        df.loc[df.index[i], "QUARTERLY"] = float(i * 10)
    df.index.name = "date"
    return df


def _write_config(path: Path, charts: dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump({"charts": charts}, f)


class TestCleanSlice(unittest.TestCase):
    def test_drops_all_nan_rows(self) -> None:
        df = _mixed_freq_df()
        clean = _clean_slice(df, ["QUARTERLY"], None, None)
        self.assertEqual(len(clean), 4)
        self.assertFalse(clean["QUARTERLY"].isna().any())

    def test_keeps_partial_nan(self) -> None:
        df = _mixed_freq_df()
        clean = _clean_slice(df, ["MONTHLY", "QUARTERLY"], None, None)
        # All rows have MONTHLY data, so none are all-NaN.
        self.assertEqual(len(clean), 12)

    def test_respects_date_range(self) -> None:
        df = _mixed_freq_df()
        clean = _clean_slice(df, ["MONTHLY"], "2020-06-01", "2020-09-30")
        self.assertTrue((clean.index >= pd.Timestamp("2020-06-01")).all())
        self.assertTrue((clean.index <= pd.Timestamp("2020-09-30")).all())


class TestMakeFormat(unittest.TestCase):
    def test_from_config(self) -> None:
        fmt = _make_format({"format": {"units": "%", "decimals": 2}})
        self.assertEqual(fmt.units, "%")
        self.assertEqual(fmt.decimals, 2)

    def test_defaults(self) -> None:
        fmt = _make_format({})
        self.assertEqual(fmt.units, "")
        self.assertEqual(fmt.decimals, 0)


class TestMakeLegend(unittest.TestCase):
    def test_from_config(self) -> None:
        leg = _make_legend({"legend": {"ncol": 3}})
        self.assertIsNotNone(leg)
        assert leg is not None
        self.assertEqual(leg.ncol, 3)

    def test_none_when_absent(self) -> None:
        self.assertIsNone(_make_legend({}))


class TestGenerateCharts(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.tmp.name)

        # Write a minimal feather.
        df = _mixed_freq_df()
        self.feather_path = self.tmp_path / "data.feather"
        df.to_feather(self.feather_path)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    @patch("src.charts.LinePlot")
    def test_line_chart_dispatch(self, mock_lp_cls: MagicMock) -> None:
        config_path = self.tmp_path / "charts.yaml"
        _write_config(
            config_path,
            {
                "test_line": {
                    "type": "line",
                    "series": {"MONTHLY": "Monthly"},
                    "format": {"units": "", "decimals": 0},
                }
            },
        )
        out_dir = self.tmp_path / "charts"
        result = generate_charts(config_path, self.feather_path, out_dir)
        self.assertEqual(result, ["test_line"])
        mock_lp_cls.assert_called_once()
        mock_lp_cls.return_value.plot.assert_called_once()

    def test_stacked_area_dispatch(self) -> None:
        config_path = self.tmp_path / "charts.yaml"
        _write_config(
            config_path,
            {
                "test_area": {
                    "type": "stacked_area",
                    "series": {"MONTHLY": "M"},
                    "format": {"units": "", "decimals": 0},
                }
            },
        )
        out_dir = self.tmp_path / "charts"
        result = generate_charts(config_path, self.feather_path, out_dir)
        self.assertEqual(result, ["test_area"])
        self.assertTrue((out_dir / "test_area.png").exists())

    def test_stacked_bar_dispatch(self) -> None:
        config_path = self.tmp_path / "charts.yaml"
        _write_config(
            config_path,
            {
                "test_bar": {
                    "type": "stacked_bar",
                    "series": {"MONTHLY": "M"},
                    "format": {"units": "", "decimals": 0},
                }
            },
        )
        out_dir = self.tmp_path / "charts"
        result = generate_charts(config_path, self.feather_path, out_dir)
        self.assertEqual(result, ["test_bar"])
        self.assertTrue((out_dir / "test_bar.png").exists())

    def test_unknown_type_skipped(self) -> None:
        config_path = self.tmp_path / "charts.yaml"
        _write_config(
            config_path,
            {
                "bad": {
                    "type": "pie_chart",
                    "series": {"MONTHLY": "M"},
                }
            },
        )
        out_dir = self.tmp_path / "charts"
        with self.assertLogs(level="WARNING"):
            result = generate_charts(config_path, self.feather_path, out_dir)
        self.assertEqual(result, [])

    @patch("src.charts.LinePlot")
    def test_failed_chart_logged(self, mock_lp_cls: MagicMock) -> None:
        mock_lp_cls.side_effect = RuntimeError("boom")
        config_path = self.tmp_path / "charts.yaml"
        _write_config(
            config_path,
            {
                "explode": {
                    "type": "line",
                    "series": {"MONTHLY": "M"},
                    "format": {"units": "", "decimals": 0},
                }
            },
        )
        out_dir = self.tmp_path / "charts"
        result = generate_charts(config_path, self.feather_path, out_dir)
        self.assertEqual(result, [])

    @patch("src.charts.LinePlot")
    def test_feather_with_date_column(self, mock_lp_cls: MagicMock) -> None:
        """Feather saved with reset_index() has a 'date' col."""
        df = _mixed_freq_df()
        alt = self.tmp_path / "with_date.feather"
        df.reset_index().to_feather(alt)

        config_path = self.tmp_path / "charts.yaml"
        _write_config(
            config_path,
            {
                "date_col": {
                    "type": "line",
                    "series": {"MONTHLY": "Monthly"},
                    "format": {"units": "", "decimals": 0},
                }
            },
        )
        out_dir = self.tmp_path / "charts2"
        result = generate_charts(config_path, alt, out_dir)
        self.assertEqual(result, ["date_col"])

    @patch("src.charts.LinePlot")
    def test_temp_feather_cleaned_up(self, mock_lp_cls: MagicMock) -> None:
        config_path = self.tmp_path / "charts.yaml"
        _write_config(
            config_path,
            {
                "clean": {
                    "type": "line",
                    "series": {"MONTHLY": "M"},
                    "format": {"units": "", "decimals": 0},
                }
            },
        )
        out_dir = self.tmp_path / "charts"
        generate_charts(config_path, self.feather_path, out_dir)
        # Temp feather should be deleted after chart generation.
        temp = out_dir / "_chart_data.feather"
        self.assertFalse(temp.exists())
