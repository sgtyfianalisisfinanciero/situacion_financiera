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

import pandas as pd

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


def mortgage_type_rules() -> list[TransformationRule]:
    """Mortgage volumes and proportions by rate type."""
    return [
        # Mixto = 1-5 years + 5-10 years fixation
        sum_rule(
            "FLUJOS_HIPOT_MIXTO",
            ["FLUJOS_HIPOT_1_5A", "FLUJOS_HIPOT_5_10A"],
        ),
        # Total for proportions (variable + mixto + fijo)
        sum_rule(
            "FLUJOS_HIPOT_TIPO_TOTAL",
            [
                "FLUJOS_HIPOT_1A",
                "FLUJOS_HIPOT_1_5A",
                "FLUJOS_HIPOT_5_10A",
                "FLUJOS_HIPOT_10A",
            ],
        ),
        # Proportions
        ratio_rule(
            "HIPOT_PCT_VARIABLE",
            "FLUJOS_HIPOT_1A",
            "FLUJOS_HIPOT_TIPO_TOTAL",
        ),
        ratio_rule(
            "HIPOT_PCT_MIXTO",
            "FLUJOS_HIPOT_MIXTO",
            "FLUJOS_HIPOT_TIPO_TOTAL",
        ),
        ratio_rule(
            "HIPOT_PCT_FIJO",
            "FLUJOS_HIPOT_10A",
            "FLUJOS_HIPOT_TIPO_TOTAL",
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


def stock_change_rules() -> list[TransformationRule]:
    """Stock changes and revaluation residuals.

    Total stock change = level_t - level_{t-1}.
    Revaluation = total change - net transactions (VNA/VNP).
    All in 4Q rolling sums for annualization.
    """

    def _delta(
        stock_col: str,
        shift: int,
        suffix: str,
    ) -> TransformationRule:
        """Stock change: level_t - level_{t-shift}."""

        def _compute(
            df: pd.DataFrame,
            s: str = stock_col,
            p: int = shift,
        ) -> pd.Series:
            clean = df[s].dropna()
            return clean - clean.shift(p)

        return TransformationRule(
            output_name=f"{stock_col}_DELTA_{suffix}",
            dependencies=[stock_col],
            compute=_compute,
        )

    def _reval(
        delta_col: str,
        flow_col: str,
        output: str,
    ) -> TransformationRule:
        """Revaluation = stock change - transactions."""

        def _compute(df: pd.DataFrame) -> pd.Series:
            d = df[delta_col].dropna()
            f = df[flow_col].dropna()
            common = d.index.intersection(f.index)
            return d.loc[common] - f.loc[common]

        return TransformationRule(
            output_name=output,
            dependencies=[delta_col, flow_col],
            compute=_compute,
        )

    return [
        # 4Q deltas (for 4Q rolling charts).
        _delta("CF_TOTAL_ACTIVO_BN", 4, "4Q"),
        _delta("CF_TOTAL_PASIVO_BN", 4, "4Q"),
        _reval(
            "CF_TOTAL_ACTIVO_BN_DELTA_4Q",
            "CF_VNA_4Q",
            "CF_REVAL_ACTIVO_4Q",
        ),
        _reval(
            "CF_TOTAL_PASIVO_BN_DELTA_4Q",
            "CF_VNP_4Q",
            "CF_REVAL_PASIVO_4Q",
        ),
        # Quarterly deltas (for annual_recent charts).
        _delta("CF_TOTAL_ACTIVO_BN", 1, "Q"),
        _delta("CF_DEUDA_HOGARES_BN", 1, "Q"),
        _reval(
            "CF_TOTAL_ACTIVO_BN_DELTA_Q",
            "CF_VNA_BN",
            "CF_REVAL_ACTIVO_Q",
        ),
        _reval(
            "CF_DEUDA_HOGARES_BN_DELTA_Q",
            "CF_VAR_PRESTAMOS_BN",
            "CF_REVAL_PRESTAMOS_Q",
        ),
        # 4Q versions for pasivos (préstamos only).
        _delta("CF_DEUDA_HOGARES_BN", 4, "4Q"),
        _reval(
            "CF_DEUDA_HOGARES_BN_DELTA_4Q",
            "CF_VAR_PRESTAMOS_4Q",
            "CF_REVAL_PRESTAMOS_4Q",
        ),
    ]


def amortization_rules() -> list[TransformationRule]:
    """Derived series for mortgage amortizations and
    cumulative renegotiations."""

    def _amortizaciones(df: pd.DataFrame) -> pd.Series:
        """Amort = stock_{t-1} - stock_t + flujos_t.

        Uses _BN columns (billions) for consistent units.
        Result is in billions of euros.
        """
        stock = df["STOCK_VIVIENDA_BN"].dropna()
        flujos = df["FLUJOS_HIPOTECARIO_CON_RENEG_BN"].dropna()
        common = stock.index.intersection(flujos.index)
        s = stock.loc[common]
        f = flujos.loc[common]
        return s.shift(1) - s + f

    def _reneg_acum(df: pd.DataFrame) -> pd.Series:
        reneg = df["RENEGOCIACIONES"].dropna()
        return reneg.cumsum()

    return [
        TransformationRule(
            output_name="AMORTIZACIONES_VIVIENDA",
            dependencies=[
                "STOCK_VIVIENDA_BN",
                "FLUJOS_HIPOTECARIO_CON_RENEG_BN",
            ],
            compute=_amortizaciones,
        ),
        TransformationRule(
            output_name="RENEGOCIACIONES_ACUM",
            dependencies=["RENEGOCIACIONES"],
            compute=_reneg_acum,
        ),
    ]


def deuda_pib_decomposition_rules() -> list[TransformationRule]:
    """Decompose YOY change in debt/GDP ratio.

    R = D/Y.  The change R_t - R_{t-4} splits into:
    - Debt contribution: (D_t - D_{t-4}) / Y_{t-4}
    - GDP contribution:  residual (total - debt)
    """

    def _debt_contrib(df: pd.DataFrame) -> pd.Series:
        d = df["CF_DEUDA_MILLONES_BN"].dropna()
        y = df["PIB_BN"].dropna()
        common = d.index.intersection(y.index)
        d, y = d.loc[common], y.loc[common]
        # Intertrimestral (quarter-on-quarter).
        return (d - d.shift(1)) / y.shift(1) * 100

    def _total_change(df: pd.DataFrame) -> pd.Series:
        r = df["CF_DEUDA_PIB"].dropna()
        # Intertrimestral.
        return r - r.shift(1)

    def _pib_contrib(df: pd.DataFrame) -> pd.Series:
        total = df["DEUDA_PIB_VAR_TOTAL"].dropna()
        debt = df["DEUDA_PIB_VAR_DEUDA"].dropna()
        common = total.index.intersection(debt.index)
        return total.loc[common] - debt.loc[common]

    return [
        TransformationRule(
            output_name="DEUDA_PIB_VAR_DEUDA",
            dependencies=["CF_DEUDA_MILLONES_BN", "PIB_BN"],
            compute=_debt_contrib,
        ),
        TransformationRule(
            output_name="DEUDA_PIB_VAR_TOTAL",
            dependencies=["CF_DEUDA_PIB"],
            compute=_total_change,
        ),
        TransformationRule(
            output_name="DEUDA_PIB_VAR_PIB",
            dependencies=[
                "DEUDA_PIB_VAR_TOTAL",
                "DEUDA_PIB_VAR_DEUDA",
            ],
            compute=_pib_contrib,
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
        # Credit flows (monthly, 12-period yoy)
        yoy_rule("FLUJOS_TOTAL_YOY", "FLUJOS_TOTAL_BN", 12),
        yoy_rule(
            "FLUJOS_HIPOTECARIO_YOY",
            "FLUJOS_HIPOTECARIO_CON_RENEG",
            12,
        ),
        yoy_rule("FLUJOS_CONSUMO_YOY", "FLUJOS_CONSUMO", 12),
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
        # VNA components (4Q accumulated)
        rolling_sum_rule("CF_VAR_EFECTIVO_4Q", "CF_VAR_EFECTIVO_BN", 4),
        rolling_sum_rule("CF_VAR_VALORES_4Q", "CF_VAR_VALORES_BN", 4),
        rolling_sum_rule("CF_VAR_ACCIONES_4Q", "CF_VAR_ACCIONES_BN", 4),
        rolling_sum_rule("CF_VAR_FONDOS_4Q", "CF_VAR_FONDOS_BN", 4),
        rolling_sum_rule("CF_VAR_SEGUROS_4Q", "CF_VAR_SEGUROS_BN", 4),
        # VNP components (4Q accumulated)
        rolling_sum_rule(
            "CF_VAR_PRESTAMOS_4Q",
            "CF_VAR_PRESTAMOS_BN",
            4,
        ),
        rolling_sum_rule(
            "CF_VAR_CRED_COMERCIALES_4Q",
            "CF_VAR_CRED_COMERCIALES_BN",
            4,
        ),
        rolling_sum_rule(
            "CF_VAR_OTROS_PASIVOS_4Q",
            "CF_VAR_OTROS_PASIVOS_BN",
            4,
        ),
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
        *mortgage_type_rules(),
        *composition_rules(),
        *dudosidad_rules(),
        *amortization_rules(),
        *deuda_pib_decomposition_rules(),
        *growth_rate_rules(),
        *rolling_rules(),
        *stock_change_rules(),
    ]
