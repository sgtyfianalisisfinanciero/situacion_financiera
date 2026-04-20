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

from typing import TYPE_CHECKING, cast

import pandas as pd

from tesorotools.pipeline.engine import TransformationRule
from tesorotools.pipeline.rules import (
    cumsum_rule,
    delta_rule,
    pct_change_rule,
    ratio_rule,
    rolling_sum_rule,
    scale_rule,
    sum_rule,
)


def _mean_rule(output: str, sources: list[str]) -> TransformationRule:
    """NaN-safe row-wise average of several columns.

    Used for BLS aggregations like
    ``Percepción del riesgo = mean(GEA, CWB, HMP)`` where the
    original spreadsheet computes ``=AVERAGE(...)`` over the
    relevant sub-components. ``.mean(axis=1)`` skips NaNs by
    default, so quarters where only some sub-items are
    reported still produce a value.
    """
    return TransformationRule(
        output_name=output,
        dependencies=list(sources),
        compute=lambda df, cols=list(sources): df[cols].mean(axis=1),
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

    Percentage series (PCT) are skipped.  The unit is read
    from whichever provider declares the instrument (bde or
    ecb); first declared provider wins.
    """
    rules: list[TransformationRule] = []
    for inst_id, info in catalog.items():
        providers = cast(dict[str, dict[str, str]], info["providers"])
        unit = ""
        for prov_cfg in providers.values():
            cfg_unit = prov_cfg.get("unit", "")
            if cfg_unit:
                unit = cfg_unit
                break
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
        # "Otras fuentes de financiación": promedio de los
        # 3 componentes alternativos (IFS, LOB, OSF) según
        # la fórmula `=AVERAGE(...)` del Excel de referencia
        # (BLS y CES/BLS/4T2025 - BLS.xlsx).
        _mean_rule(
            "BLS_DMD_OTROS",
            ["BLS_DMD_IFS", "BLS_DMD_LOB", "BLS_DMD_OSF"],
        ),
        # Agregados para el gráfico de estándares (vivienda).
        _mean_rule(
            "BLS_STD_COSTE",
            ["BLS_STD_CP", "BLS_STD_MF", "BLS_STD_LP"],
        ),
        _mean_rule(
            "BLS_STD_COMPET",
            ["BLS_STD_BC", "BLS_STD_NBC"],
        ),
        _mean_rule(
            "BLS_STD_PERCEP",
            ["BLS_STD_GEA", "BLS_STD_CWB", "BLS_STD_HMP"],
        ),
        # Consumo: "Uso financiación alternativa" = AVG(IFS, LOB, OSF)
        # (fórmula `=(F+G+H)/3` en la hoja "Demanda consumo").
        _mean_rule(
            "BLS_DMD_C_USO_ALT",
            ["BLS_DMD_C_IFS", "BLS_DMD_C_LOB", "BLS_DMD_C_OSF"],
        ),
        # Agregados para estándares consumo (hoja
        # "Estándares consumo"): Coste = AVG(CP,MF,LP),
        # Competencia = AVG(BC,NBC),
        # Percepción = AVG(GEA,CWC,RCD).
        _mean_rule(
            "BLS_STDC_COSTE",
            ["BLS_STDC_CP", "BLS_STDC_MF", "BLS_STDC_LP"],
        ),
        _mean_rule(
            "BLS_STDC_COMPET",
            ["BLS_STDC_BC", "BLS_STDC_NBC"],
        ),
        _mean_rule(
            "BLS_STDC_PERCEP",
            ["BLS_STDC_GEA", "BLS_STDC_CWC", "BLS_STDC_RCD"],
        ),
        # "Otros términos y condiciones" consumo =
        # AVG(SZL, NIC, MTY) — tamaño, no-intereses,
        # vencimiento.
        _mean_rule(
            "BLS_TCC_OTROS_TC",
            ["BLS_TCC_SZL", "BLS_TCC_NIC", "BLS_TCC_MTY"],
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

    Uses ``delta_rule`` from tesorotools for stock changes
    and ``difference_rule`` pattern for revaluations
    (stock_change - transactions).
    """
    from tesorotools.pipeline.rules import difference_rule

    return [
        # 4Q deltas.
        delta_rule("CF_TOTAL_ACTIVO_BN_DELTA_4Q", "CF_TOTAL_ACTIVO_BN", 4),
        delta_rule("CF_TOTAL_PASIVO_BN_DELTA_4Q", "CF_TOTAL_PASIVO_BN", 4),
        difference_rule(
            "CF_REVAL_ACTIVO_4Q",
            ["CF_TOTAL_ACTIVO_BN_DELTA_4Q", "CF_VNA_4Q"],
        ),
        difference_rule(
            "CF_REVAL_PASIVO_4Q",
            ["CF_TOTAL_PASIVO_BN_DELTA_4Q", "CF_VNP_4Q"],
        ),
        # Quarterly deltas.
        delta_rule("CF_TOTAL_ACTIVO_BN_DELTA_Q", "CF_TOTAL_ACTIVO_BN", 1),
        delta_rule("CF_DEUDA_HOGARES_BN_DELTA_Q", "CF_DEUDA_HOGARES_BN", 1),
        difference_rule(
            "CF_REVAL_ACTIVO_Q",
            ["CF_TOTAL_ACTIVO_BN_DELTA_Q", "CF_VNA_BN"],
        ),
        difference_rule(
            "CF_REVAL_PRESTAMOS_Q",
            ["CF_DEUDA_HOGARES_BN_DELTA_Q", "CF_VAR_PRESTAMOS_BN"],
        ),
        # 4Q for préstamos.
        delta_rule("CF_DEUDA_HOGARES_BN_DELTA_4Q", "CF_DEUDA_HOGARES_BN", 4),
        difference_rule(
            "CF_REVAL_PRESTAMOS_4Q",
            ["CF_DEUDA_HOGARES_BN_DELTA_4Q", "CF_VAR_PRESTAMOS_4Q"],
        ),
    ]


def amortization_rules() -> list[TransformationRule]:
    """Mortgage amortizations and cumulative renegotiations.

    Amortization = delta(stock, 1) negated + flows.
    Uses ``delta_rule`` + local compute for the identity.
    ``cumsum_rule`` from tesorotools for renegotiations.
    """

    def _amortizaciones(df: pd.DataFrame) -> pd.Series:
        stock = df["STOCK_VIVIENDA_BN"].dropna()
        flujos = df["FLUJOS_HIPOTECARIO_CON_RENEG_BN"].dropna()
        common = stock.index.intersection(flujos.index)
        s = stock.loc[common]
        f = flujos.loc[common]
        return s.shift(1) - s + f

    return [
        TransformationRule(
            output_name="AMORTIZACIONES_VIVIENDA",
            dependencies=[
                "STOCK_VIVIENDA_BN",
                "FLUJOS_HIPOTECARIO_CON_RENEG_BN",
            ],
            compute=_amortizaciones,
        ),
        cumsum_rule("RENEGOCIACIONES_ACUM", "RENEGOCIACIONES"),
        TransformationRule(
            output_name="AMORT_MA12",
            dependencies=["AMORTIZACIONES_VIVIENDA"],
            compute=lambda df: (
                df["AMORTIZACIONES_VIVIENDA"]
                .dropna()
                .rolling(12, min_periods=12)
                .mean()
            ),
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
        pct_change_rule("STOCK_VIVIENDA_YOY", "STOCK_VIVIENDA_BN", 12),
        pct_change_rule("STOCK_CONSUMO_YOY", "STOCK_CONSUMO_BN", 12),
        pct_change_rule("STOCK_OTROS_YOY", "STOCK_OTROS_BN", 12),
        pct_change_rule("STOCK_PRESTAMOS_YOY", "STOCK_PRESTAMOS_BN", 12),
        # Credit flows (monthly, 12-period yoy)
        pct_change_rule("FLUJOS_TOTAL_YOY", "FLUJOS_TOTAL_BN", 12),
        pct_change_rule(
            "FLUJOS_HIPOTECARIO_YOY",
            "FLUJOS_HIPOTECARIO_CON_RENEG",
            12,
        ),
        pct_change_rule("FLUJOS_CONSUMO_YOY", "FLUJOS_CONSUMO", 12),
        # Financial accounts (quarterly, 4-period yoy)
        pct_change_rule("CF_TOTAL_ACTIVO_YOY", "CF_TOTAL_ACTIVO_BN", 4),
        pct_change_rule("CF_DEUDA_HOGARES_YOY", "CF_DEUDA_HOGARES_BN", 4),
        pct_change_rule("CF_RIQUEZA_NETA_YOY", "CF_RIQUEZA_NETA_BN", 4),
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
