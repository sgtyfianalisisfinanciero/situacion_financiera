"""Tests for src.artists.stacked."""

import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

from src.artists.stacked import (
    StackedAreaPlot,
    StackedBarPlot,
)
from tesorotools.artists.line_plot import (  # pyright: ignore[reportMissingTypeStubs]
    Format,
    Legend,
)


def _quarterly_df() -> pd.DataFrame:
    """Small quarterly DataFrame for chart tests."""
    dates = pd.date_range("2020-01-01", periods=8, freq="QE")
    rng = np.random.default_rng(42)
    return pd.DataFrame(
        {
            "A": rng.uniform(10, 50, 8),
            "B": rng.uniform(-5, 20, 8),
            "C": rng.uniform(5, 30, 8),
        },
        index=dates,
    )


class TestStackedAreaPlotInit(unittest.TestCase):
    def test_rejects_non_png(self) -> None:
        df = _quarterly_df()
        with self.assertRaises(ValueError, msg=".png"):
            StackedAreaPlot(
                out_path=Path("out.jpg"),
                data=df,
                series={"A": "Label A"},
            )


class TestStackedAreaPlotPlot(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.out = Path(self.tmp.name) / "area.png"

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_creates_png(self) -> None:
        df = _quarterly_df()
        chart = StackedAreaPlot(
            out_path=self.out,
            data=df,
            series={"A": "Series A", "B": "Series B"},
            format=Format(units="%", decimals=0),
            legend=Legend(ncol=2),
        )
        chart.plot()
        self.assertTrue(self.out.exists())
        self.assertGreater(self.out.stat().st_size, 0)

    def test_with_scale_and_baseline(self) -> None:
        df = _quarterly_df()
        chart = StackedAreaPlot(
            out_path=self.out,
            data=df,
            series={"A": "A", "C": "C"},
            scale=100,
            baseline=True,
            format=Format(units="%", decimals=1),
        )
        chart.plot()
        self.assertTrue(self.out.exists())

    def test_with_date_range(self) -> None:
        df = _quarterly_df()
        chart = StackedAreaPlot(
            out_path=self.out,
            data=df,
            series={"A": "A"},
            start_date="2020-07-01",
            end_date="2021-06-30",
            format=Format(),
        )
        chart.plot()
        self.assertTrue(self.out.exists())


class TestStackedBarPlotInit(unittest.TestCase):
    def test_rejects_non_png(self) -> None:
        df = _quarterly_df()
        with self.assertRaises(ValueError, msg=".png"):
            StackedBarPlot(
                out_path=Path("out.pdf"),
                data=df,
                series={"A": "Label A"},
            )


class TestStackedBarPlotPlot(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.out = Path(self.tmp.name) / "bar.png"

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_creates_png(self) -> None:
        df = _quarterly_df()
        chart = StackedBarPlot(
            out_path=self.out,
            data=df,
            series={"A": "Series A", "B": "Series B"},
            format=Format(units="", decimals=0),
            legend=Legend(ncol=2),
        )
        chart.plot()
        self.assertTrue(self.out.exists())
        self.assertGreater(self.out.stat().st_size, 0)

    def test_with_negative_values(self) -> None:
        """Bars with mixed positive/negative stack correctly."""
        df = _quarterly_df()
        # Force some negatives.
        df["B"] = df["B"] - 15
        chart = StackedBarPlot(
            out_path=self.out,
            data=df,
            series={"A": "A", "B": "B"},
            baseline=True,
            format=Format(),
        )
        chart.plot()
        self.assertTrue(self.out.exists())

    def test_with_date_range(self) -> None:
        df = _quarterly_df()
        chart = StackedBarPlot(
            out_path=self.out,
            data=df,
            series={"A": "A", "C": "C"},
            start_date="2020-07-01",
            end_date="2021-06-30",
            format=Format(),
        )
        chart.plot()
        self.assertTrue(self.out.exists())

    def test_default_legend(self) -> None:
        """No explicit legend uses defaults without error."""
        df = _quarterly_df()
        chart = StackedBarPlot(
            out_path=self.out,
            data=df,
            series={"A": "A"},
            format=Format(),
        )
        chart.plot()
        self.assertTrue(self.out.exists())
