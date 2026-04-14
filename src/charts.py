"""Chart generation driver.

Reads ``series/charts.yaml``, generates one PNG per entry
using the appropriate artist (tesorotools ``LinePlot`` for
line charts, local ``StackedAreaPlot`` / ``StackedBarPlot``
for stacked charts).

All artists receive a pre-loaded DataFrame so the feather
is read only once.

Mixed-frequency handling
------------------------
The hogares DataFrame mixes monthly (credit) and quarterly
(financial accounts) series.  Quarterly columns are NaN on
non-quarterly dates.  Before plotting, each chart's columns
are extracted and rows where *all* are NaN are dropped.
This prevents matplotlib from drawing invisible single-dot
line segments on NaN-surrounded dates.

For ``LinePlot`` (which requires a feather path), a
per-chart temporary feather is written with clean data.
"""

import logging
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from tesorotools.artists.line_plot import Format, Legend, LinePlot  # pyright: ignore[reportMissingTypeStubs]

from src.artists.stacked import StackedAreaPlot, StackedBarPlot

logger = logging.getLogger(__name__)

#: Temporary feather used to feed LinePlot clean data.
_TEMP_FEATHER = "_chart_data.feather"


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


def _plot_line(
    chart_id: str,
    cfg: dict[str, Any],
    df: pd.DataFrame,
    out_dir: Path,
) -> None:
    """Generate a line chart via tesorotools LinePlot.

    Writes a temporary feather with clean (NaN-dropped)
    data so that LinePlot sees only valid observations.
    """
    cols = list(cfg["series"].keys())
    clean = _clean_slice(df, cols, cfg.get("start_date"), cfg.get("end_date"))

    temp_path = out_dir / _TEMP_FEATHER
    clean.to_feather(temp_path)

    out_path = out_dir / f"{chart_id}.png"
    lp = LinePlot(
        out_path=out_path,
        data_path=temp_path,
        series=cfg["series"],
        scale=cfg.get("scale", 1),
        base_100=cfg.get("base_100", False),
        baseline=cfg.get("baseline", False),
        format=_make_format(cfg),
        legend=_make_legend(cfg),
    )
    lp.plot()

    temp_path.unlink(missing_ok=True)
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
    )
    chart.plot()
    logger.info("Chart: %s (%s)", out_path.name, cfg["type"])


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
        try:
            if chart_type == "line":
                _plot_line(chart_id, cfg, df, out_dir)
            elif chart_type in _TYPE_MAP:
                _plot_stacked(
                    chart_id,
                    cfg,
                    df,
                    out_dir,
                    _TYPE_MAP[chart_type],
                )
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

    logger.info("Charts: %d/%d generated", len(generated), len(charts))
    return generated
