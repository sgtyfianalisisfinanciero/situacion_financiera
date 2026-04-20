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
from tesorotools.providers.base import DataProvider
from tesorotools.providers.bde import BdeProvider
from tesorotools.providers.ecb import EcbProvider

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


class _ProviderConfig(TypedDict):
    """Provider config for a series.

    Shared shape for every provider: a provider-specific
    ``code`` plus a free-form ``unit`` tag read by the
    unit normalization rules.
    """

    code: str
    unit: str


class _Providers(TypedDict, total=False):
    """Provider block inside an instrument entry."""

    bde: _ProviderConfig
    ecb: _ProviderConfig


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

#: Provider name -> factory. One feather store per provider
#: lives in OUTPUT_DIR as ``store_{name}.feather``.
PROVIDERS: dict[str, type[DataProvider]] = {
    "bde": BdeProvider,
    "ecb": EcbProvider,
}


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


def build_code_maps(
    instruments: InstrumentCatalog,
) -> dict[str, dict[str, str]]:
    """Group the catalog by provider.

    Returns
    -------
    dict[str, dict[str, str]]
        ``{provider_name: {canonical_id: provider_code}}``,
        restricted to providers listed in ``PROVIDERS`` that
        actually have entries.  An instrument with more than
        one provider block appears once per provider; the
        first one that downloads successfully wins at merge
        time (columns are concatenated; duplicates use the
        first non-NaN value).
    """
    maps: dict[str, dict[str, str]] = {p: {} for p in PROVIDERS}
    for inst_id, info in instruments.items():
        providers = info.get("providers", {})
        for prov_name in PROVIDERS:
            if prov_name in providers:
                maps[prov_name][inst_id] = providers[prov_name]["code"]

    filtered = {name: m for name, m in maps.items() if m}
    if not filtered:
        raise RuntimeError(
            "No instrument in the catalog uses a known provider "
            f"({', '.join(PROVIDERS)})"
        )
    return filtered


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

    # 1. Read catalog (explicit arg so tests can patch
    # INSTRUMENTS_PATH at the module level).
    instruments = load_instruments(INSTRUMENTS_PATH)
    code_maps = build_code_maps(instruments)
    total_codes = sum(len(m) for m in code_maps.values())
    logger.info(
        "Catalog: %d instruments across providers %s",
        total_codes,
        sorted(code_maps),
    )

    # 2. Download (incremental or full), one store per provider.
    # Each store holds raw provider codes; we rename to
    # canonical IDs when loading, then concat horizontally.
    frames: list[pd.DataFrame] = []
    for prov_name, cmap in code_maps.items():
        provider = PROVIDERS[prov_name]()
        store = SeriesStore(OUTPUT_DIR / f"store_{prov_name}.feather")

        if args.full and store.exists():
            store.path.unlink()
            logger.info("Deleted existing %s store (--full)", prov_name)

        store.update(
            provider,
            list(cmap.values()),
            lookback_quarters=args.lookback,
        )

        rename_map = {v: k for k, v in cmap.items()}
        frames.append(store.load().rename(columns=rename_map))

    df = pd.concat(frames, axis=1)
    df.sort_index(inplace=True)
    df.index.name = "date"
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
