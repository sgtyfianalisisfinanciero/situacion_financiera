"""Abstract base class for data providers.

A provider knows how to download time series from a specific
external source (Bank of Spain, Eikon, ECB, etc.).  It does
not know anything about local storage, incremental updates,
or presentation -- those concerns belong elsewhere.

The interface is intentionally minimal: ``fetch`` to download
data and ``is_available`` to check connectivity.  Any
provider-specific configuration (API keys, rate limits, etc.)
belongs in the concrete class constructor, not in the
abstract interface.

Every provider returns data in the same shape: a DataFrame
with a DatetimeIndex and one column per series code.

Design note
-----------
This ABC is designed to be portable to ``tesorotools`` in the
future.  The diario project currently uses a different
interface (``download(date, skip)``); when it migrates, its
``EikonProvider`` will implement this same ABC, with the
``skip`` parameter moving to the constructor.
"""

from abc import ABC, abstractmethod

import pandas as pd


class DataProvider(ABC):
    """Base class for all data providers.

    Subclasses must implement ``fetch`` and ``is_available``.
    """

    @abstractmethod
    def fetch(
        self,
        codes: list[str],
        start: str | None = None,
        end: str | None = None,
    ) -> pd.DataFrame:
        """Download series data for a date range.

        Parameters
        ----------
        codes
            Provider-specific series codes to download.
        start
            Start date as ISO string (e.g. ``"2025-01-01"``).
            If ``None``, the provider decides the earliest
            date (typically the full available history).
        end
            End date as ISO string.  If ``None``, the
            provider fetches up to the latest available data.

        Returns
        -------
        pd.DataFrame
            - Index: ``DatetimeIndex`` named ``"date"``,
              tz-naive, normalized to midnight.
            - Columns: one per code in ``codes``.
            - Values: ``float64``, with ``NaN`` for missing
              observations.
        """
        ...

    @abstractmethod
    def is_available(self) -> bool:
        """Check whether this provider can currently serve data.

        Returns ``True`` if the external service is reachable
        and ready.  Useful for fail-fast checks before
        starting a long download, or for choosing between
        a primary provider and a fallback.
        """
        ...
