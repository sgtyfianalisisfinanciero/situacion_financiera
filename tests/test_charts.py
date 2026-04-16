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
    _resample_annual_recent,
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


class TestResampleAnnualRecent(unittest.TestCase):
    def test_closed_years_take_q4(self) -> None:
        idx = pd.date_range("2022-01-01", periods=8, freq="QS")
        idx.name = "date"
        df = pd.DataFrame({"X": [10, 20, 30, 40, 50, 60, 70, 80]}, index=idx)
        result = _resample_annual_recent(df)
        # 2022 closed: Q4 value = 40. 2023 is last year: 4 quarters.
        self.assertEqual(result.loc["2022", "X"], 40)
        self.assertIn("T1-2023", result.index)
        self.assertIn("T4-2023", result.index)

    def test_empty_df(self) -> None:
        df = pd.DataFrame(
            {"X": [float("nan")]},
            index=pd.DatetimeIndex(["2023-01-01"], name="date"),
        )
        result = _resample_annual_recent(df)
        self.assertTrue(result.empty)

    def test_incomplete_closed_year(self) -> None:
        """Closed year with <4 quarters takes last available."""
        idx = pd.DatetimeIndex(
            ["2022-01-01", "2022-04-01", "2023-01-01"],
            name="date",
        )
        df = pd.DataFrame({"X": [10, 20, 30]}, index=idx)
        result = _resample_annual_recent(df)
        # 2022 has 2 quarters: take last (20).
        self.assertEqual(result.loc["2022", "X"], 20)
        self.assertIn("T1-2023", result.index)

    def test_single_year(self) -> None:
        idx = pd.date_range("2025-01-01", periods=3, freq="QS")
        idx.name = "date"
        df = pd.DataFrame({"X": [1, 2, 3]}, index=idx)
        result = _resample_annual_recent(df)
        # Only year = last year, all quarters shown.
        self.assertEqual(len(result), 3)


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

    def test_type_curve(self) -> None:
        # Monthly data spanning 2 years.
        idx = pd.date_range("2024-01-01", periods=24, freq="MS")
        idx.name = "date"
        df = pd.DataFrame({"X": range(1, 25)}, index=idx)
        feather = self.tmp_path / "monthly.feather"
        df.to_feather(feather)

        config_path = self.tmp_path / "charts.yaml"
        _write_config(
            config_path,
            {
                "tc": {
                    "type": "type_curve",
                    "series": {"X": "Series X"},
                    "cumulative": True,
                    "start_year": 2024,
                    "format": {"units": "", "decimals": 0},
                    "legend": {"ncol": 2},
                }
            },
        )
        out_dir = self.tmp_path / "charts_tc"
        result = generate_charts(config_path, feather, out_dir)
        self.assertEqual(result, ["tc"])
        self.assertTrue((out_dir / "tc.png").exists())

    def test_type_curve_non_cumulative(self) -> None:
        idx = pd.date_range("2024-01-01", periods=12, freq="MS")
        idx.name = "date"
        df = pd.DataFrame({"X": range(1, 13)}, index=idx)
        feather = self.tmp_path / "monthly2.feather"
        df.to_feather(feather)

        config_path = self.tmp_path / "charts.yaml"
        _write_config(
            config_path,
            {
                "tc2": {
                    "type": "type_curve",
                    "series": {"X": "X"},
                    "start_year": 2024,
                    "format": {"units": "", "decimals": 0},
                }
            },
        )
        out_dir = self.tmp_path / "charts_tc2"
        result = generate_charts(config_path, feather, out_dir)
        self.assertEqual(result, ["tc2"])

    def test_stacked_bar_with_figsize(self) -> None:
        config_path = self.tmp_path / "charts.yaml"
        _write_config(
            config_path,
            {
                "sized": {
                    "type": "stacked_bar",
                    "series": {"MONTHLY": "M"},
                    "format": {"units": "", "decimals": 0},
                    "figsize": [14, 7],
                }
            },
        )
        out_dir = self.tmp_path / "charts_fs"
        result = generate_charts(config_path, self.feather_path, out_dir)
        self.assertEqual(result, ["sized"])

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

    def test_stacked_resampled_with_scale(self) -> None:
        idx = pd.date_range("2023-01-01", periods=8, freq="QS")
        idx.name = "date"
        df = pd.DataFrame({"A": range(1, 9)}, index=idx)
        feather = self.tmp_path / "q_scale.feather"
        df.to_feather(feather)

        config_path = self.tmp_path / "charts.yaml"
        _write_config(
            config_path,
            {
                "scaled": {
                    "type": "stacked_bar",
                    "resample": "annual_recent",
                    "series": {"A": "A"},
                    "format": {"units": "", "decimals": 0},
                    "scale": 1000,
                    "baseline": True,
                }
            },
        )
        out_dir = self.tmp_path / "charts_sc"
        result = generate_charts(config_path, feather, out_dir)
        self.assertEqual(result, ["scaled"])

    @patch("src.charts.LinePlot")
    def test_series_styles_passed(self, mock_lp_cls: MagicMock) -> None:
        config_path = self.tmp_path / "charts.yaml"
        _write_config(
            config_path,
            {
                "styled": {
                    "type": "line",
                    "series": {"MONTHLY": "M"},
                    "format": {"units": "", "decimals": 0},
                    "series_styles": {"MONTHLY": {"linestyle": "--"}},
                }
            },
        )
        out_dir = self.tmp_path / "charts_st"
        generate_charts(config_path, self.feather_path, out_dir)
        call_kwargs = mock_lp_cls.call_args.kwargs
        self.assertIn("series_styles", call_kwargs)

    def test_resampled_with_overlay(self) -> None:
        idx = pd.date_range("2023-01-01", periods=8, freq="QS")
        idx.name = "date"
        df = pd.DataFrame(
            {"A": range(1, 9), "B": range(11, 19), "T": range(21, 29)},
            index=idx,
        )
        feather = self.tmp_path / "q_overlay.feather"
        df.to_feather(feather)

        config_path = self.tmp_path / "charts.yaml"
        _write_config(
            config_path,
            {
                "overlaid": {
                    "type": "stacked_bar",
                    "resample": "annual_recent",
                    "series": {"A": "A", "B": "B"},
                    "overlay_series": {"T": "Total"},
                    "format": {"units": "", "decimals": 0},
                }
            },
        )
        out_dir = self.tmp_path / "charts_ov"
        result = generate_charts(config_path, feather, out_dir)
        self.assertEqual(result, ["overlaid"])

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

    def test_stacked_resampled_dispatch(self) -> None:
        """resample: annual_recent triggers resampled path."""
        # Build quarterly data spanning 2 years.
        idx = pd.date_range("2023-01-01", periods=8, freq="QS")
        idx.name = "date"
        df = pd.DataFrame({"A": range(1, 9), "B": range(11, 19)}, index=idx)
        feather = self.tmp_path / "quarterly.feather"
        df.to_feather(feather)

        config_path = self.tmp_path / "charts.yaml"
        _write_config(
            config_path,
            {
                "resampled": {
                    "type": "stacked_bar",
                    "resample": "annual_recent",
                    "series": {"A": "A", "B": "B"},
                    "format": {"units": "", "decimals": 0},
                }
            },
        )
        out_dir = self.tmp_path / "charts_rs"
        result = generate_charts(config_path, feather, out_dir)
        self.assertEqual(result, ["resampled"])
        self.assertTrue((out_dir / "resampled.png").exists())

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
