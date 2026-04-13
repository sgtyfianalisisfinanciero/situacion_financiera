"""Data pipeline: transformation engine and rules.

The engine (``engine.py``) provides a generic rule-based
transformation system, replicating the pattern from the
diariospython project.  It is domain-agnostic.

The rules (``rules.py``) define the specific transformations
for the hogares report: unit conversions, aggregations,
composition ratios, growth rates, and rolling sums.
"""
