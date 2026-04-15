"""Table generation for the hogares report.

Reads ``series/tables.yaml`` and produces feather files
that ``tesorotools.render.Table`` can consume.  Each table
config generates one feather with formatted string values,
ready for insertion into the Word document.

Tables are transposed: rows are series, columns are dates.
This matches the layout of the original Excel-based report.
"""

import logging
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

logger = logging.getLogger(__name__)


def _format_value(v: float, decimals: int) -> str:
    """Format a float to a locale-style string."""
    if pd.isna(v):
        return ""
    return f"{v:_.{decimals}f}".replace(".", ",").replace("_", ".")


def _format_date(ts: pd.Timestamp, freq: str) -> str:
    """Format a timestamp for column headers."""
    if freq == "quarterly":
        q = (ts.month - 1) // 3 + 1
        return f"{q}T-{ts.year}"
    return ts.strftime("%b-%y")


def _build_table(
    df: pd.DataFrame,
    cfg: dict[str, Any],
) -> pd.DataFrame:
    """Build a single formatted table DataFrame.

    Returns a DataFrame where rows are series labels and
    columns are date strings.  Values are formatted strings.
    """
    series: dict[str, str] = cfg["series"]
    periods: int = cfg.get("periods", 8)
    decimals: int = cfg.get("decimals", 1)
    freq: str = cfg.get("frequency", "monthly")

    cols = list(series.keys())
    subset = df[cols].dropna(how="all").tail(periods)

    date_labels = [_format_date(ts, freq) for ts in subset.index]

    rows: dict[str, list[str]] = {}
    for col, label in series.items():
        rows[label] = [_format_value(v, decimals) for v in subset[col].values]

    # Append YOY series if defined.
    yoy_series: dict[str, str] = cfg.get("yoy_series", {})
    yoy_decimals: int = cfg.get("yoy_decimals", 1)
    yoy_scale: float = cfg.get("yoy_scale", 1)

    if yoy_series:
        yoy_cols = list(yoy_series.keys())
        yoy_subset = df[yoy_cols].dropna(how="all").tail(periods)
        for col, label in yoy_series.items():
            raw = yoy_subset[col].to_numpy(dtype=float)
            scaled = raw * yoy_scale
            rows[label] = [
                _format_value(float(v), yoy_decimals) for v in scaled
            ]

    return pd.DataFrame(rows, index=date_labels).T


def generate_tables(
    config_path: Path,
    data_path: Path,
    out_dir: Path,
) -> list[str]:
    """Generate table feathers from config.

    Parameters
    ----------
    config_path
        Path to tables.yaml.
    data_path
        Path to the transformed feather.
    out_dir
        Directory for output feather files.

    Returns
    -------
    list[str]
        Table IDs that were generated.
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    with open(config_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    tables: dict[str, Any] = config["tables"]

    df = pd.read_feather(data_path)
    if "date" in df.columns:
        df = df.set_index("date")

    generated: list[str] = []

    for table_id, cfg in tables.items():
        try:
            table_df = _build_table(df, cfg)
            feather_path = out_dir / f"{table_id}.feather"
            table_df.to_feather(feather_path)
            generated.append(table_id)
            logger.info("Table: %s", table_id)
        except Exception:
            logger.exception("Failed to generate table %s", table_id)

    logger.info(
        "Tables: %d/%d generated",
        len(generated),
        len(tables),
    )
    return generated
