"""Data providers: modules that download from external sources.

Each provider exposes a ``get_series()`` function that takes a
list of series codes and returns a ``pd.DataFrame`` with a
datetime index and one column per series.

Available providers:

- ``bde`` -- Bank of Spain (REST JSON API). Covers all 53
  series in this project, including ECB DCF series that the
  BdE redistributes.
"""
