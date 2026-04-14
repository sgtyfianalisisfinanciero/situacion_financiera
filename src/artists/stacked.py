"""Stacked area and stacked bar chart artists.

These follow the same constructor/plot() pattern as
``tesorotools.artists.line_plot.LinePlot`` so they can be
swapped in when tesorotools absorbs them.

They reuse the tesorotools matplotlib style sheet, axis
configuration, and formatting helpers so that all charts
in the hogares report share the same visual identity.

TODO for tesorotools developer:
    Migrate StackedAreaPlot and StackedBarPlot into
    tesorotools.artists as first-class chart types.
    See hogares-python/src/artists/stacked.py for the
    reference implementation and API contract.
"""

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.axes import Axes
from matplotlib.figure import Figure

from tesorotools.artists.line_plot import (  # pyright: ignore[reportMissingTypeStubs]
    AX_CONFIG,
    FIG_CONFIG,
    Format,
    Legend,
    _style_baseline,  # pyright: ignore[reportPrivateUsage]
    _style_spines,  # pyright: ignore[reportPrivateUsage]
)

# Default legend config matching tesorotools line chart.
_DEFAULT_NCOL = 5
_DEFAULT_SEP = -0.125


class StackedAreaPlot:
    """Stacked area chart with the tesorotools visual style.

    Parameters match ``LinePlot`` where applicable so that
    chart configs can switch between ``line`` and
    ``stacked_area`` by changing a single field.
    """

    def __init__(
        self,
        out_path: Path,
        data: pd.DataFrame,
        series: dict[str, str],
        *,
        scale: float = 1,
        start_date: str | None = None,
        end_date: str | None = None,
        baseline: bool = False,
        format: Format | None = None,
        legend: Legend | None = None,
    ) -> None:
        if out_path.suffix != ".png":
            raise ValueError(f"out_path must be .png: {out_path}")
        self.out_path = out_path
        self.data = data
        self.series = series
        self.scale = scale
        self.start_date = start_date
        self.end_date = end_date
        self.baseline = baseline
        self.format = format or Format()
        self.legend = legend

    def plot(self) -> Axes:
        start = (
            pd.Timestamp(self.start_date)
            if self.start_date
            else self.data.index.min()
        )
        end = (
            pd.Timestamp(self.end_date)
            if self.end_date
            else self.data.index.max()
        )

        plot_data = self.data.loc[start:end, list(self.series.keys())].dropna()
        plot_data = plot_data * self.scale

        fig: Figure = plt.figure(  # pyright: ignore[reportUnknownMemberType]
            **FIG_CONFIG
        )
        ax: Axes = fig.add_subplot()

        labels = list(self.series.values())
        arrays: list[np.ndarray[tuple[int], np.dtype[np.float64]]] = [
            plot_data[col].to_numpy(dtype=np.float64) for col in self.series
        ]
        ax.stackplot(  # pyright: ignore[reportUnknownMemberType]
            plot_data.index,
            *arrays,
            labels=labels,
            alpha=0.85,
        )

        _style_spines(  # pyright: ignore[reportPrivateUsage]
            ax,
            decimals=self.format.decimals,
            units=self.format.units,
            **AX_CONFIG["spines"],
        )
        if self.baseline:
            _style_baseline(  # pyright: ignore[reportPrivateUsage]
                ax, 0, **AX_CONFIG["baseline"]
            )

        ncol = self.legend.ncol if self.legend else _DEFAULT_NCOL
        sep = self.legend.sep if self.legend else _DEFAULT_SEP
        ax.legend(  # pyright: ignore[reportUnknownMemberType]
            loc="upper center",
            bbox_to_anchor=(0.5, sep),
            ncol=ncol,
        )

        fig.savefig(  # pyright: ignore[reportUnknownMemberType]
            self.out_path
        )
        plt.close(fig)
        return ax


class StackedBarPlot:
    """Stacked bar chart with the tesorotools visual style.

    Positive and negative values are stacked separately so
    that bars extend in both directions from the baseline.
    """

    def __init__(
        self,
        out_path: Path,
        data: pd.DataFrame,
        series: dict[str, str],
        *,
        scale: float = 1,
        start_date: str | None = None,
        end_date: str | None = None,
        baseline: bool = True,
        format: Format | None = None,
        legend: Legend | None = None,
    ) -> None:
        if out_path.suffix != ".png":
            raise ValueError(f"out_path must be .png: {out_path}")
        self.out_path = out_path
        self.data = data
        self.series = series
        self.scale = scale
        self.start_date = start_date
        self.end_date = end_date
        self.baseline = baseline
        self.format = format or Format()
        self.legend = legend

    def plot(self) -> Axes:
        start = (
            pd.Timestamp(self.start_date)
            if self.start_date
            else self.data.index.min()
        )
        end = (
            pd.Timestamp(self.end_date)
            if self.end_date
            else self.data.index.max()
        )

        plot_data = self.data.loc[start:end, list(self.series.keys())].dropna()
        plot_data = plot_data * self.scale

        # Wider figure for bar charts to avoid cramped bars.
        fig: Figure = plt.figure(  # pyright: ignore[reportUnknownMemberType]
            figsize=(12, 6), **FIG_CONFIG
        )
        ax: Axes = fig.add_subplot()

        cols = list(self.series.keys())
        labels = list(self.series.values())

        # Use numeric x positions for clean bar alignment,
        # then set date tick labels manually.
        x = np.arange(len(plot_data))
        bar_width = 0.7

        pos_bottom: np.ndarray[tuple[int], np.dtype[np.float64]] = np.zeros(
            len(plot_data)
        )
        neg_bottom: np.ndarray[tuple[int], np.dtype[np.float64]] = np.zeros(
            len(plot_data)
        )

        for col, label in zip(cols, labels):
            values: np.ndarray[tuple[int], np.dtype[np.float64]] = plot_data[
                col
            ].to_numpy(dtype=np.float64)
            pos: np.ndarray[tuple[int], np.dtype[np.float64]] = np.where(
                values >= 0, values, 0.0
            )
            neg: np.ndarray[tuple[int], np.dtype[np.float64]] = np.where(
                values < 0, values, 0.0
            )

            color = (
                ax.bar(  # pyright: ignore[reportUnknownMemberType]
                    x,
                    pos,
                    bottom=pos_bottom,
                    width=bar_width,
                    label=label,
                )
                .patches[0]
                .get_facecolor()
            )
            ax.bar(  # pyright: ignore[reportUnknownMemberType]
                x,
                neg,
                bottom=neg_bottom,
                width=bar_width,
                color=color,
            )
            pos_bottom = pos_bottom + pos
            neg_bottom = neg_bottom + neg

        # Date tick labels: show year only, skip to avoid
        # overlap.
        dates = plot_data.index
        step = max(1, len(dates) // 12)
        tick_pos = list(range(0, len(dates), step))
        tick_labels = [dates[i].strftime("%Y") for i in tick_pos]
        ax.set_xticks(  # pyright: ignore[reportUnknownMemberType]
            tick_pos
        )
        ax.set_xticklabels(  # pyright: ignore[reportUnknownMemberType]
            tick_labels
        )

        _style_spines(  # pyright: ignore[reportPrivateUsage]
            ax,
            decimals=self.format.decimals,
            units=self.format.units,
            **AX_CONFIG["spines"],
        )
        if self.baseline:
            _style_baseline(  # pyright: ignore[reportPrivateUsage]
                ax, 0, **AX_CONFIG["baseline"]
            )

        ncol = self.legend.ncol if self.legend else _DEFAULT_NCOL
        sep = self.legend.sep if self.legend else _DEFAULT_SEP
        ax.legend(  # pyright: ignore[reportUnknownMemberType]
            loc="upper center",
            bbox_to_anchor=(0.5, sep),
            ncol=ncol,
        )

        fig.savefig(  # pyright: ignore[reportUnknownMemberType]
            self.out_path
        )
        plt.close(fig)
        return ax
