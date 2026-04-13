"""Transformation engine.

Replicates the pattern from the diariospython project: a
``TransformationRule`` dataclass that bundles the output name,
its dependencies, and a pure compute function, plus an
``apply_transformations`` function that applies rules
sequentially to a DataFrame.

This engine is generic and has no domain knowledge.  The
rules themselves (what to compute) are defined elsewhere.

When tesorotools absorbs this engine in the future, the
migration should be straightforward because the interface
is identical to the one in diariospython.
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TransformationRule:
    """A single transformation rule.

    Attributes
    ----------
    output_name
        Canonical ID of the derived series that this rule
        produces.  Will become a new column in the
        DataFrame.
    dependencies
        List of column names that must exist in the
        DataFrame for this rule to run.  If any dependency
        is missing, the rule is skipped gracefully.
    compute
        A pure function that receives the full DataFrame
        and returns a Series with the computed values.
        The function may read any column, but should only
        use those listed in ``dependencies``.
    """

    output_name: str
    dependencies: list[str]
    compute: Callable[[pd.DataFrame], pd.Series]


def apply_transformations(
    df: pd.DataFrame,
    rules: list[TransformationRule],
) -> pd.DataFrame:
    """Apply a list of rules to a DataFrame.

    Rules are applied in order.  Each rule may depend on
    columns produced by earlier rules, so ordering matters.

    If a rule's dependencies are not all present, it is
    skipped with a warning rather than raising an error.
    This allows partial execution when some upstream data
    is unavailable.

    Parameters
    ----------
    df
        Input DataFrame (not modified).
    rules
        Ordered list of rules to apply.

    Returns
    -------
    pd.DataFrame
        Copy of ``df`` with new columns added by the rules.
    """
    result = df.copy()
    for rule in rules:
        missing = [d for d in rule.dependencies if d not in result.columns]
        if missing:
            logger.warning(
                "Skipping %s: missing dependencies %s",
                rule.output_name,
                missing,
            )
            continue
        result[rule.output_name] = rule.compute(result)
    return result
