"""Entry point for the Household Financial Situation report.

This script orchestrates the data pipeline:

1. Reads the series catalog from ``series/instruments.yaml``.
2. Downloads data via ``BdeProvider`` (or updates incrementally
   if a local feather file already exists).
3. Renames columns from raw BdE codes to canonical IDs.
4. Exports the data to Excel and Feather.

The catalog maps canonical IDs (like ``STOCK_VIVIENDA``) to BdE
provider codes (like ``DCF_M.N.ES...``).  After download, the
DataFrame columns use canonical IDs so that downstream code
never needs to know about provider-specific codes.

Command line usage::

    uv run --active python generar_hogares.py
    uv run --active python generar_hogares.py --download-only
"""

import argparse
import logging
from pathlib import Path
from typing import TypedDict, cast

import pandas as pd
import yaml

from src.charts import generate_charts
from src.pipeline.rules import all_rules
from src.report import generate_report
from src.store import SeriesStore
from tesorotools.pipeline.engine import apply_transformations
from tesorotools.providers.bde import BdeProvider

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


class _BdeConfig(TypedDict):
    """Provider config for a BdE series."""

    code: str
    unit: str


class _Providers(TypedDict, total=False):
    """Provider block inside an instrument entry."""

    bde: _BdeConfig


class InstrumentEntry(TypedDict):
    """A single entry in the instruments catalog."""

    display_name: str
    providers: _Providers


#: The full catalog type: canonical_id -> entry.
InstrumentCatalog = dict[str, InstrumentEntry]

#: Project root directory (where this file lives).
ROOT = Path(__file__).parent

#: Path to the series catalog.
INSTRUMENTS_PATH = ROOT / "series" / "instruments.yaml"

#: Path to the charts config.
CHARTS_PATH = ROOT / "series" / "charts.yaml"

#: Path to the report template.
TEMPLATE_PATH = ROOT / "series" / "template.yaml"

#: Output directory for generated files.
OUTPUT_DIR = ROOT / "output"

#: Path to the persistent feather store (raw BdE codes only).
STORE_PATH = OUTPUT_DIR / "store_bde.feather"


def load_instruments(
    path: Path = INSTRUMENTS_PATH,
) -> InstrumentCatalog:
    """Read the YAML catalog and return a flat dict.

    Parameters
    ----------
    path
        Path to the YAML file.

    Returns
    -------
    InstrumentCatalog
        Mapping of canonical ID to series metadata.
    """
    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return cast(InstrumentCatalog, raw["instruments"])


def build_code_map(
    instruments: InstrumentCatalog,
) -> dict[str, str]:
    """Extract the canonical_id -> bde_code mapping.

    Parameters
    ----------
    instruments
        Catalog as returned by ``load_instruments``.

    Returns
    -------
    dict[str, str]
        ``{canonical_id: bde_code}`` for every series
        that has a ``bde`` provider entry.

    Raises
    ------
    RuntimeError
        If no series with a ``bde`` provider are found.
    """
    mapping: dict[str, str] = {}
    for inst_id, info in instruments.items():
        providers = info.get("providers", {})
        if "bde" in providers:
            mapping[inst_id] = providers["bde"]["code"]

    if not mapping:
        raise RuntimeError(
            "No series with provider 'bde' found in the instrument catalog"
        )
    return mapping


def export_excel(df: pd.DataFrame, path: Path) -> None:
    """Export a DataFrame to Excel.

    Creates the parent directory if needed.  Overwrites
    the file if it already exists.

    Parameters
    ----------
    df
        DataFrame to export.  The index is written as the
        first column.
    path
        Output .xlsx file path.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(  # pyright: ignore[reportUnknownMemberType]
        path, sheet_name="datos", index=True
    )
    logger.info("Excel saved: %s", path)


def main() -> None:
    """Parse arguments and run the pipeline."""
    parser = argparse.ArgumentParser(
        description=(
            "Download and export data for the Household "
            "Financial Situation report"
        ),
    )
    parser.add_argument(
        "--download-only",
        action="store_true",
        help="Download and export data only",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help=("Force a full re-download instead of an incremental update"),
    )
    parser.add_argument(
        "--lookback",
        type=int,
        default=4,
        metavar="Q",
        help=("Number of quarters to re-check for revisions (default: 4)"),
    )
    args = parser.parse_args()

    # 1. Read catalog
    instruments = load_instruments()
    code_map = build_code_map(instruments)
    logger.info("Catalog: %d instruments", len(code_map))

    # 2. Download (incremental or full)
    provider = BdeProvider()
    bde_codes = list(code_map.values())

    # The store works with raw BdE codes internally.
    # We rename to canonical IDs after loading.
    raw_store = SeriesStore(STORE_PATH)

    if args.full and raw_store.exists():
        # Delete existing feather to force full download.
        raw_store.path.unlink()
        logger.info("Deleted existing store (--full)")

    raw_store.update(
        provider,
        bde_codes,
        lookback_quarters=args.lookback,
    )

    # Load and rename to canonical IDs.
    rename_map = {v: k for k, v in code_map.items()}
    df = raw_store.load().rename(columns=rename_map)
    logger.info(
        "Data: %d rows x %d columns",
        len(df),
        len(df.columns),
    )

    # 3. Apply transformations
    rules = all_rules(instruments)
    df = apply_transformations(df, rules)
    logger.info(
        "After transforms: %d rows x %d columns",
        len(df),
        len(df.columns),
    )

    # 4. Export
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    export_excel(df, OUTPUT_DIR / "datos_hogares.xlsx")

    feather_path = OUTPUT_DIR / "datos_hogares.feather"
    df.reset_index().to_feather(feather_path)
    logger.info("Feather saved: %s", feather_path)

    # Save with DatetimeIndex for chart consumption.
    chart_feather = OUTPUT_DIR / "datos_transformados.feather"
    df.to_feather(chart_feather)
    logger.info("Chart feather saved: %s", chart_feather)

    if args.download_only:
        logger.info("--download-only mode. Done.")
        return

    # 5. Generate charts
    charts_dir = OUTPUT_DIR / "charts"
    generate_charts(CHARTS_PATH, chart_feather, charts_dir)

    # 6. Generate Word report
    report_path = OUTPUT_DIR / "informe_hogares.docx"
    generate_report(TEMPLATE_PATH, report_path)

    logger.info("Pipeline completed.")


if __name__ == "__main__":
    main()
