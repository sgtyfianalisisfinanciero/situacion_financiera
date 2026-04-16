# pyright: reportUnknownVariableType=false
# pyright: reportUnknownMemberType=false
# pyright: reportUnknownArgumentType=false
# pyright: reportAttributeAccessIssue=false
"""Chart generation driver.

Reads ``series/charts.yaml``, generates one PNG per entry
using the appropriate tesorotools artist (``LinePlot``,
``StackedAreaPlot``, ``StackedBarPlot``).

Mixed-frequency handling
------------------------
The hogares DataFrame mixes monthly (credit) and quarterly
(financial accounts) series.  Quarterly columns are NaN on
non-quarterly dates.  Before plotting, each chart's columns
are extracted and rows where *all* are NaN are dropped.
This prevents matplotlib from drawing invisible single-dot
line segments on NaN-surrounded dates.

Annual/quarterly resampling
---------------------------
Some charts in the original Excel report show annual totals
for closed years and quarterly data for the current year.
This is controlled by ``resample: annual_recent`` in the
chart YAML config.  When set, the driver:

1. Sums quarterly values within each calendar year.
2. For years with all 4 quarters: keeps one annual bar.
3. For the latest year (incomplete): keeps individual
   quarters with labels like "T1-2025".

This is a **visualization concern**, not a data
transformation.  The underlying data remains quarterly.
The resampling only affects how it is displayed in the
chart.  This breaks the general pattern of charts.py being
a thin passthrough — the justification is that the Excel
uses this mixed display and reproducing it requires
preprocessing that doesn't belong in the pipeline rules
(which are about computing magnitudes, not about display
granularity).
"""

import logging
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import pandas as pd
import yaml

from tesorotools.artists.line_plot import (
    AX_CONFIG,
    FIG_CONFIG,
    Format,
    Legend,
    LinePlot,
    style_spines,
)
from tesorotools.artists.stacked import StackedAreaPlot, StackedBarPlot

logger = logging.getLogger(__name__)


def _make_format(cfg: dict[str, Any]) -> Format:
    raw = cfg.get("format", {})
    return Format(
        units=raw.get("units", ""),
        decimals=raw.get("decimals", 0),
    )


def _make_legend(cfg: dict[str, Any]) -> Legend | None:
    raw = cfg.get("legend")
    if raw is None:
        return None
    return Legend(
        ncol=raw.get("ncol", 5),
        sep=raw.get("sep", -0.125),
    )


def _clean_slice(
    df: pd.DataFrame,
    cols: list[str],
    start: str | None,
    end: str | None,
) -> pd.DataFrame:
    """Slice by date and drop rows where all cols are NaN.

    This is the key fix for mixed-frequency data: quarterly
    series have NaN on monthly dates, so we drop those rows
    to get a clean DataFrame for plotting.
    """
    s = pd.Timestamp(start) if start else df.index.min()
    e = pd.Timestamp(end) if end else df.index.max()
    sliced = df.loc[s:e, cols]
    return sliced.dropna(how="all")


def _resample_annual_recent(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Resample rolling-4Q data to annual + recent quarters.

    Expects data that is already a rolling 4-quarter sum
    (annualized).  For closed years, takes the **Q4 value**
    (which equals the annual total because the rolling
    window covers Q1-Q4).  For the latest year, keeps
    individual quarters (each showing the trailing 4Q sum).

    This matches the Excel pattern where annual bars show
    the year-end rolling 4Q value, and recent quarterly
    bars show the same rolling 4Q metric per quarter.

    Returns a DataFrame with a string index (period labels).
    """
    clean = df.dropna(how="all")
    if clean.empty:
        return clean

    years = clean.index.year
    last_year = int(years.max())

    rows: dict[str, pd.Series] = {}

    for year in sorted(years.unique()):
        year_data = clean.loc[clean.index.year == year]
        if year == last_year:
            # Last year: show individual quarters.
            for ts, row in year_data.iterrows():
                q = (ts.month - 1) // 3 + 1  # type: ignore[union-attr]
                label = f"T{q}-{year}"
                rows[label] = row
        elif len(year_data) >= 4:
            # Closed year with all quarters: take Q4
            # (= rolling 4Q at year end = annual total).
            rows[str(year)] = year_data.iloc[-1]
        else:
            # Closed year with incomplete data: take last
            # available quarter.
            rows[str(year)] = year_data.iloc[-1]

    result = pd.DataFrame(rows).T
    result.index.name = "period"
    return result


_MONTH_LABELS = [
    "Ene",
    "Feb",
    "Mar",
    "Abr",
    "May",
    "Jun",
    "Jul",
    "Ago",
    "Sep",
    "Oct",
    "Nov",
    "Dic",
]


def _plot_type_curve(
    chart_id: str,
    cfg: dict[str, Any],
    df: pd.DataFrame,
    out_dir: Path,
) -> None:
    """Type curve: one line per year, x = month.

    If ``cumulative: true``, values are accumulated within
    each year (starts at 0 in January, rises through
    December).  Otherwise, raw monthly values.

    Uses matplotlib directly (not LinePlot) because the
    x-axis is categorical (month names), not DatetimeIndex.
    """
    col = list(cfg["series"].keys())[0]
    start_year = int(cfg.get("start_year", 2015))
    cumulative = cfg.get("cumulative", False)

    clean = df[[col]].dropna()
    clean = clean.loc[clean.index.year >= start_year]

    # Pivot: rows = month (1-12), cols = year.
    pivoted = clean.copy()
    pivoted["month"] = pivoted.index.month
    pivoted["year"] = pivoted.index.year
    table = pivoted.pivot_table(index="month", columns="year", values=col)
    table = table.reindex(range(1, 13))

    if cumulative:
        table = table.cumsum()

    fmt = _make_format(cfg)
    legend_cfg = _make_legend(cfg)

    fig = plt.figure(**FIG_CONFIG)  # pyright: ignore[reportUnknownMemberType]
    ax = fig.add_subplot()

    for year_col in table.columns:
        vals = table[year_col].dropna()
        ax.plot(  # pyright: ignore[reportUnknownMemberType]
            [_MONTH_LABELS[m - 1] for m in vals.index],
            vals.values,
            label=str(year_col),
        )

    style_spines(
        ax,
        decimals=fmt.decimals,
        units=fmt.units,
        **AX_CONFIG["spines"],
    )

    ncol = legend_cfg.ncol if legend_cfg else 6
    sep = legend_cfg.sep if legend_cfg else -0.125
    ax.legend(  # pyright: ignore[reportUnknownMemberType]
        loc="upper center",
        bbox_to_anchor=(0.5, sep),
        ncol=ncol,
    )

    out_path = out_dir / f"{chart_id}.png"
    fig.savefig(out_path)  # pyright: ignore[reportUnknownMemberType]
    plt.close(fig)
    logger.info("Chart: %s (type_curve)", out_path.name)


def _plot_line(
    chart_id: str,
    cfg: dict[str, Any],
    df: pd.DataFrame,
    out_dir: Path,
) -> None:
    """Generate a line chart via tesorotools LinePlot."""
    cols = list(cfg["series"].keys())
    clean = _clean_slice(df, cols, cfg.get("start_date"), cfg.get("end_date"))

    out_path = out_dir / f"{chart_id}.png"
    kwargs: dict[str, Any] = {}
    if "series_styles" in cfg:
        kwargs["series_styles"] = cfg["series_styles"]

    lp = LinePlot(
        out_path=out_path,
        data=clean,
        series=cfg["series"],
        scale=cfg.get("scale", 1),
        base_100=cfg.get("base_100", False),
        baseline=cfg.get("baseline", False),
        format=_make_format(cfg),
        legend=_make_legend(cfg),
        **kwargs,
    )
    lp.plot()
    logger.info("Chart: %s (line)", out_path.name)


def _plot_stacked(
    chart_id: str,
    cfg: dict[str, Any],
    df: pd.DataFrame,
    out_dir: Path,
    cls: type,
) -> None:
    """Generate a stacked chart (area or bar)."""
    out_path = out_dir / f"{chart_id}.png"
    kwargs: dict[str, Any] = {}
    for key in ("overlay_series", "bar_width", "x_rotation"):
        if key in cfg:
            kwargs[key] = cfg[key]
    if "figsize" in cfg:
        kwargs["figsize"] = tuple(cfg["figsize"])

    chart = cls(
        out_path=out_path,
        data=df,
        series=cfg["series"],
        scale=cfg.get("scale", 1),
        start_date=cfg.get("start_date"),
        end_date=cfg.get("end_date"),
        baseline=cfg.get("baseline", False),
        format=_make_format(cfg),
        legend=_make_legend(cfg),
        **kwargs,
    )
    chart.plot()
    logger.info("Chart: %s (%s)", out_path.name, cfg["type"])


class _ResampledBarPlot(StackedBarPlot):
    """StackedBarPlot with annual/quarterly resampling.

    Overrides ``_prepare_data`` to resample quarterly data
    into annual bars for closed years and quarterly for the
    most recent year.  Overrides ``_format_xticks`` to use
    string period labels instead of dates.

    This is an example of the P12 extensibility pattern:
    only the data preparation and tick formatting change,
    everything else (bar stacking, overlay, legend, spines)
    is inherited from tesorotools.
    """

    def _prepare_data(self) -> pd.DataFrame:  # type: ignore[override]
        """Resample then scale."""
        cols = list(self.series.keys())
        overlay_cols = (
            list(self.overlay_series.keys()) if self.overlay_series else []
        )
        all_cols = cols + overlay_cols
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
        sliced = self.data.loc[start:end, all_cols].dropna(how="all")
        resampled = _resample_annual_recent(sliced)
        return resampled * self.scale

    def _format_xticks(  # type: ignore[override]
        self,
        ax: Any,
        plot_data: pd.DataFrame,
        x: Any,
    ) -> None:
        """Use string period labels with rotation."""
        ax.set_xticks(x)
        ax.set_xticklabels(
            list(plot_data.index),
            rotation=self.x_rotation or 45,
            ha="right",
        )


_TYPE_MAP: dict[str, type] = {
    "stacked_area": StackedAreaPlot,
    "stacked_bar": StackedBarPlot,
}


def generate_charts(
    config_path: Path,
    data_path: Path,
    out_dir: Path,
) -> list[str]:
    """Generate all chart PNGs defined in *config_path*.

    Parameters
    ----------
    config_path
        Path to the charts YAML config.
    data_path
        Path to the transformed feather file (with
        DatetimeIndex).
    out_dir
        Directory where PNGs will be written.

    Returns
    -------
    list[str]
        Chart IDs that were generated (in config order).
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    with open(config_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    charts: dict[str, Any] = config["charts"]

    df = pd.read_feather(data_path)
    if "date" in df.columns:
        df = df.set_index("date")

    generated: list[str] = []

    for chart_id, cfg in charts.items():
        chart_type = cfg.get("type", "line")
        resample = cfg.get("resample")
        try:
            if chart_type == "line":
                _plot_line(chart_id, cfg, df, out_dir)
            elif chart_type == "type_curve":
                _plot_type_curve(chart_id, cfg, df, out_dir)
            elif chart_type in _TYPE_MAP:
                cls = _TYPE_MAP[chart_type]
                if resample == "annual_recent":
                    _plot_stacked(
                        chart_id,
                        cfg,
                        df,
                        out_dir,
                        _ResampledBarPlot,
                    )
                else:
                    _plot_stacked(chart_id, cfg, df, out_dir, cls)
            else:
                logger.warning(
                    "Unknown chart type '%s' for %s",
                    chart_type,
                    chart_id,
                )
                continue
            generated.append(chart_id)
        except Exception:
            logger.exception("Failed to generate %s", chart_id)

    logger.info(
        "Charts: %d/%d generated",
        len(generated),
        len(charts),
    )
    return generated
