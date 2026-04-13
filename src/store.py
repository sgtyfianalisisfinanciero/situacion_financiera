"""Local feather-based series store with incremental updates.

This module handles persistence of downloaded time series data.
It stores a single feather file where rows are dates and columns
are series codes (canonical IDs).

On subsequent runs, instead of re-downloading the full history,
it computes which data is missing, fetches only the new portion
(plus a configurable lookback window to catch statistical
revisions), merges with the existing data, and saves.

This is project-specific logic -- it does not belong in
tesorotools.  Each project may have its own storage strategy.
"""

import logging
from pathlib import Path

import pandas as pd

from src.providers.base import DataProvider

logger = logging.getLogger(__name__)

#: Default number of quarters to re-check for revisions.
DEFAULT_LOOKBACK_QUARTERS = 4


class SeriesStore:
    """Feather-backed store for time series data.

    The store manages a single feather file.  Its main
    method, ``update``, implements incremental downloads:
    only fetch what is new, plus a lookback window to
    capture revisions in previously published data.

    Parameters
    ----------
    path
        Path to the feather file.  Created on first save.

    Example
    -------
    >>> store = SeriesStore(Path("output/datos.feather"))
    >>> df = store.update(provider, codes)
    >>> df.shape
    (376, 53)
    """

    def __init__(self, path: Path) -> None:
        self._path = path

    @property
    def path(self) -> Path:
        """Path to the feather file."""
        return self._path

    def exists(self) -> bool:
        """Check whether the feather file exists."""
        return self._path.is_file()

    def load(self) -> pd.DataFrame:
        """Load the stored DataFrame from disk.

        Returns
        -------
        pd.DataFrame
            The stored data, or an empty DataFrame if the
            file does not exist.
        """
        if not self.exists():
            return pd.DataFrame()
        df = pd.read_feather(self._path)
        if "date" in df.columns:
            df = df.set_index("date")
        return df

    def save(self, df: pd.DataFrame) -> None:
        """Write a DataFrame to the feather file.

        Creates the parent directory if it does not exist.

        Parameters
        ----------
        df
            DataFrame to save.  The index must be a
            DatetimeIndex named ``"date"``.
        """
        self._path.parent.mkdir(parents=True, exist_ok=True)
        df.to_feather(self._path)
        logger.info("Store saved: %s", self._path)

    def update(
        self,
        provider: DataProvider,
        codes: list[str],
        lookback_quarters: int = DEFAULT_LOOKBACK_QUARTERS,
    ) -> pd.DataFrame:
        """Incrementally update the store.

        On the first run (no feather file), downloads the
        full history.  On subsequent runs:

        1. Loads the existing data.
        2. Computes ``start`` as the last date minus
           ``lookback_quarters`` quarters.
        3. Fetches from ``start`` to now.
        4. Merges: new data overwrites old data where dates
           overlap (to capture revisions).  New dates are
           appended.
        5. Adds any columns (series) that are in ``codes``
           but missing from the existing data, fetching
           their full history.
        6. Saves and returns the full DataFrame.

        Parameters
        ----------
        provider
            A DataProvider instance to fetch data from.
        codes
            Provider-specific series codes to download.
        lookback_quarters
            Number of quarters to re-download for revision
            checking.  For example, 4 means "re-check the
            last year of data".  Set to 0 to only fetch
            genuinely new data (no revision check).

        Returns
        -------
        pd.DataFrame
            The full updated DataFrame (all history, all
            series).
        """
        existing = self.load()

        if existing.empty:
            logger.info("Store: no existing data, downloading full history")
            df = provider.fetch(codes)
            self.save(df)
            return df

        # Determine which codes need full download (new
        # series not present in the existing data).
        existing_codes = [c for c in codes if c in existing.columns]
        new_codes = [c for c in codes if c not in existing.columns]

        # Incremental update for existing series.
        if existing_codes:
            start = _lookback_start(existing.index.max(), lookback_quarters)
            logger.info(
                "Store: incremental update from %s (%d existing series)",
                start.date(),
                len(existing_codes),
            )
            fresh = provider.fetch(
                existing_codes,
                start=str(start.date()),
            )
            existing = _merge(existing, fresh)

        # Full download for genuinely new series.
        if new_codes:
            logger.info(
                "Store: downloading %d new series",
                len(new_codes),
            )
            new_data = provider.fetch(new_codes)
            existing = _merge(existing, new_data)

        existing.sort_index(inplace=True)
        self.save(existing)
        return existing


def _lookback_start(
    last_date: pd.Timestamp,
    quarters: int,
) -> pd.Timestamp:
    """Compute the start date for the lookback window.

    Subtracts ``quarters * 3`` months from ``last_date``.
    """
    return last_date - pd.DateOffset(months=quarters * 3)


def _merge(old: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    """Merge new data into existing data.

    New values overwrite old values where both have data
    for the same (date, series) cell.  Dates and columns
    present only in one side are preserved.

    Uses ``combine_first`` with ``new`` as primary so that
    fresh values take precedence over stale ones.
    """
    if old.empty:
        return new
    if new.empty:
        return old
    return new.combine_first(old)
