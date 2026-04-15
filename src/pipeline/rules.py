"""Transformation rules for the hogares project.

Each public function returns a list of ``TransformationRule``
instances.  Rules are grouped by purpose: unit normalization,
aggregations, composition ratios, growth rates, and rolling
sums.

All rules operate on DataFrames whose columns are canonical
IDs (e.g. ``STOCK_VIVIENDA``).

Unit normalization
------------------
The BdE API returns monetary series in different units
depending on the series (K_EUR, M_EUR, BN_EUR).  The
``normalize_rules`` function reads the ``unit`` field from
the instrument catalog and generates rules that convert
every monetary series to a common unit: **billions of
euros** (suffix ``_BN``).  Percentage series are left as-is.
"""

from typing import TYPE_CHECKING

from tesorotools.pipeline.engine import TransformationRule
from tesorotools.pipeline.rules import (
    ratio_rule,
    rolling_sum_rule,
    scale_rule,
    sum_rule,
    yoy_rule,
)

if TYPE_CHECKING:
    from generar_hogares import InstrumentCatalog

# Divisor to convert each source unit to billions of euros.
_TO_BILLIONS: dict[str, float] = {
    "K_EUR": 1e6,  # thousands -> billions
    "M_EUR": 1e3,  # millions -> billions
    "BN_EUR": 1.0,  # already billions
}


# ----------------------------------------------------------
# Rule sets
# ----------------------------------------------------------


def normalize_rules(
    catalog: "InstrumentCatalog",
) -> list[TransformationRule]:
    """Generate unit conversion rules from the catalog.

    For each monetary series (unit in K_EUR, M_EUR, or
    BN_EUR), creates a rule that converts to billions and
    stores the result in a new column with suffix ``_BN``.

    Percentage series (PCT) are skipped.
    """
    rules: list[TransformationRule] = []
    for inst_id, info in catalog.items():
        unit = info["providers"].get("bde", {}).get("unit", "")
        divisor = _TO_BILLIONS.get(unit)
        if divisor is None:
            continue
        rules.append(scale_rule(f"{inst_id}_BN", inst_id, divisor))
    return rules


def aggregation_rules() -> list[TransformationRule]:
    """Derived totals from component series."""
    return [
        sum_rule(
            "FLUJOS_TOTAL_BN",
            [
                "FLUJOS_HIPOTECARIO_CON_RENEG_BN",
                "FLUJOS_CONSUMO_BN",
                "FLUJOS_OTROS_BN",
            ],
        ),
        sum_rule(
            "CF_OTROS_Y_PRESTAMOS_BN",
            [
                "CF_OTROS_ACTIVOS_BN",
                "CF_PRESTAMOS_ACTIVO_BN",
            ],
        ),
    ]


def composition_rules() -> list[TransformationRule]:
    """Asset composition as fraction of total."""
    return [
        ratio_rule(
            "CF_PCT_EFECTIVO",
            "CF_EFECTIVO_DEPOSITOS_BN",
            "CF_TOTAL_ACTIVO_BN",
        ),
        ratio_rule(
            "CF_PCT_VALORES",
            "CF_VALORES_DEUDA_BN",
            "CF_TOTAL_ACTIVO_BN",
        ),
        ratio_rule(
            "CF_PCT_PARTICIPACIONES",
            "CF_PARTICIPACIONES_BN",
            "CF_TOTAL_ACTIVO_BN",
        ),
        ratio_rule(
            "CF_PCT_SEGUROS",
            "CF_SEGUROS_BN",
            "CF_TOTAL_ACTIVO_BN",
        ),
        ratio_rule(
            "CF_PCT_OTROS",
            "CF_OTROS_Y_PRESTAMOS_BN",
            "CF_TOTAL_ACTIVO_BN",
        ),
    ]


def dudosidad_rules() -> list[TransformationRule]:
    """NPL ratios: doubtful / total credit."""
    return [
        ratio_rule(
            "DUDOSIDAD_HOGARES",
            "DUDOSOS_HOGARES",
            "CREDITO_HOGARES_TOTAL",
        ),
        ratio_rule(
            "DUDOSIDAD_VIVIENDA",
            "DUDOSOS_VIVIENDA",
            "CREDITO_VIVIENDA_TOTAL",
        ),
        ratio_rule(
            "DUDOSIDAD_CONSUMO",
            "DUDOSOS_CONSUMO",
            "CREDITO_CONSUMO_TOTAL",
        ),
    ]


def growth_rate_rules() -> list[TransformationRule]:
    """Year-over-year growth rates for key series."""
    return [
        # Credit stocks (monthly, 12-period yoy)
        yoy_rule("STOCK_VIVIENDA_YOY", "STOCK_VIVIENDA_BN", 12),
        yoy_rule("STOCK_CONSUMO_YOY", "STOCK_CONSUMO_BN", 12),
        yoy_rule("STOCK_OTROS_YOY", "STOCK_OTROS_BN", 12),
        yoy_rule("STOCK_PRESTAMOS_YOY", "STOCK_PRESTAMOS_BN", 12),
        # Financial accounts (quarterly, 4-period yoy)
        yoy_rule("CF_TOTAL_ACTIVO_YOY", "CF_TOTAL_ACTIVO_BN", 4),
        yoy_rule("CF_DEUDA_HOGARES_YOY", "CF_DEUDA_HOGARES_BN", 4),
        yoy_rule("CF_RIQUEZA_NETA_YOY", "CF_RIQUEZA_NETA_BN", 4),
    ]


def rolling_rules() -> list[TransformationRule]:
    """Rolling 4-quarter sums for annualization."""
    return [
        rolling_sum_rule("CF_VNA_4Q", "CF_VNA_BN", 4),
        rolling_sum_rule("CF_VNP_4Q", "CF_VNP_BN", 4),
        rolling_sum_rule("CF_OFN_4Q", "CF_OFN_BN", 4),
    ]


def all_rules(
    catalog: "InstrumentCatalog",
) -> list[TransformationRule]:
    """All transformation rules in correct order.

    Order matters: normalization first, then aggregations
    (which depend on normalized values), then ratios and
    growth rates.
    """
    return [
        *normalize_rules(catalog),
        *aggregation_rules(),
        *composition_rules(),
        *dudosidad_rules(),
        *growth_rate_rules(),
        *rolling_rules(),
    ]
