"""Bank of Spain (Banco de Espana, BdE) data provider.

Downloads historical time series from the BdE public REST API.
No authentication required.

API reference
-------------
Endpoint:
    https://app.bde.es/bierest/resources/srdatosapp/listaSeries

Query parameters (names are fixed by the BdE API):
    ``idioma``  : ``"es"`` | ``"en"``
    ``series``  : comma-separated series codes
    ``rango``   : ``"MAX"`` | ``"30M"`` | ``"60M"``

Response format (JSON):
    The API returns a *list* of objects, one per series::

        [
          {
            "serie": "DN_1TI2TIE42",
            "fechas": ["2026-02-01T09:15:00Z", ...],
            "valores": [6572, 5962, ...],
            "decimales": 0,
            "codFrecuencia": "M"
          }
        ]

    The JSON fields ``"fechas"`` (dates) and ``"valores"``
    (values) are parallel arrays in reverse chronological
    order (most recent first).

Notes
-----
- ECB DCF series (credit stock data) are also available
  through this API because the BdE redistributes them.
- The API accepts multiple series per request, but may
  time out with too many.  Downloads are batched.
- Timestamps include timezone info (UTC).  They are
  normalized to tz-naive midnight for pandas compatibility.
"""

import logging
from typing import TypedDict, cast

import pandas as pd
import requests

from src.providers.base import DataProvider

logger = logging.getLogger(__name__)


class _BdeSeriesEntry(TypedDict):
    """Shape of a single series object in the BdE API
    JSON response.  Field names are in Spanish because
    they are fixed by the API."""

    serie: str
    fechas: list[str]
    valores: list[int | float | None]
    decimales: int
    codFrecuencia: str


_LIST_URL = "https://app.bde.es/bierest/resources/srdatosapp/listaSeries"
_PING_URL = "https://app.bde.es/bierest/resources/srdatosapp/favoritas"

#: Default HTTP timeout in seconds.
DEFAULT_TIMEOUT = 30

#: Maximum number of series per API request.
BATCH_SIZE = 10


class BdeProvider(DataProvider):
    """Provider that downloads series from the Bank of Spain.

    The BdE API is public and requires no credentials.

    Parameters
    ----------
    language
        Language for metadata in the API response.
        Sent as the ``idioma`` query parameter.
        ``"es"`` for Spanish, ``"en"`` for English.
    timeout
        Maximum seconds to wait per HTTP request.

    Example
    -------
    >>> provider = BdeProvider()
    >>> df = provider.fetch(["DN_1TI2TIE42"])
    >>> df.columns.tolist()
    ['DN_1TI2TIE42']
    """

    def __init__(
        self,
        *,
        language: str = "es",
        timeout: int = DEFAULT_TIMEOUT,
    ) -> None:
        self._language = language
        self._timeout = timeout

    def fetch(
        self,
        codes: list[str],
        start: str | None = None,
        end: str | None = None,
    ) -> pd.DataFrame:
        """Download one or more series from the BdE.

        Splits the series into batches of ``BATCH_SIZE``,
        downloads each batch in a single HTTP request, and
        merges the results into one DataFrame.

        The ``start`` parameter is translated to the BdE
        API ``rango`` parameter.  If ``None``, the full
        available history is downloaded.  The ``end``
        parameter is accepted for interface compatibility
        but is not supported by the BdE API (it always
        returns data up to the latest observation).

        Parameters
        ----------
        codes
            BdE series codes (e.g. ``["DN_1TI2TIE42"]``).
        start
            Earliest date to include (ISO format).
            If ``None``, fetches the full history.
        end
            Ignored by this provider.  Kept for interface
            compatibility.

        Returns
        -------
        pd.DataFrame
            DatetimeIndex named ``"date"``, one column per
            code, ``float64`` values.
        """
        if not codes:
            return pd.DataFrame()

        time_range = _compute_time_range(start)
        frames: list[pd.DataFrame] = []

        for batch in _batches(codes, BATCH_SIZE):
            df = self._download_batch(batch, time_range)
            if not df.empty:
                frames.append(df)

        if not frames:
            return pd.DataFrame()

        result = pd.concat(frames, axis=1)
        result.sort_index(inplace=True)

        if start:
            cutoff = pd.Timestamp(start)
            result = result.loc[result.index >= cutoff]

        return result

    def is_available(self) -> bool:
        """Ping the BdE API to check connectivity.

        Makes a lightweight request to the ``favoritas``
        endpoint and checks for a 200 response.
        """
        try:
            resp = requests.get(
                _PING_URL,
                params={
                    "idioma": self._language,
                    "series": "DN_1TI2TIE42",
                },
                timeout=5,
            )
            return resp.status_code == 200
        except requests.RequestException:
            return False

    # --------------------------------------------------
    # Internal helpers
    # --------------------------------------------------

    def _download_batch(
        self, codes: list[str], time_range: str
    ) -> pd.DataFrame:
        """Download a batch of series in one HTTP request.

        Raises
        ------
        RuntimeError
            If the API returns a business error.
        requests.HTTPError
            If the HTTP status is not 2xx.
        """
        params = {
            "idioma": self._language,
            "series": ",".join(codes),
            "rango": time_range,
        }
        logger.info("BdE: downloading %d series...", len(codes))
        resp = requests.get(
            _LIST_URL,
            params=params,
            timeout=self._timeout,
        )
        resp.raise_for_status()

        payload = resp.json()
        _check_api_error(payload)
        return _parse_series_list(cast(list[_BdeSeriesEntry], payload), codes)


# ----------------------------------------------------------
# Module-level helpers
# ----------------------------------------------------------


class _BdeErrorResponse(TypedDict, total=False):
    """Shape of an error response from the BdE API."""

    errNum: int  # noqa: N815
    errMsgUsr: str  # noqa: N815
    errMsgDebug: str  # noqa: N815


def _check_api_error(payload: object) -> None:
    """Raise RuntimeError if the API returned an error.

    On validation errors (e.g. bad range), the BdE API
    returns a JSON dict with an ``errNum`` field instead
    of the normal series list.
    """
    if not isinstance(payload, dict):
        return
    if "errNum" not in payload:
        return
    err = cast(_BdeErrorResponse, payload)
    raise RuntimeError(
        f"BdE API error {err.get('errNum', '?')}: {err.get('errMsgUsr', '')}"
    )


def _compute_time_range(start: str | None) -> str:
    """Translate a start date into a BdE API range value.

    The BdE API does not accept arbitrary date ranges.
    It only accepts a small set of predefined values for
    the ``rango`` query parameter: ``"30M"`` (30 months),
    ``"60M"`` (60 months), and ``"MAX"`` (full history).

    This function picks the smallest valid value that
    covers the requested start date.
    """
    if start is None:
        return "MAX"

    start_ts = pd.Timestamp(start)
    now = pd.Timestamp.now()
    months_needed = (
        (now.year - start_ts.year) * 12
        + (now.month - start_ts.month)
        + 3  # buffer for publication lag
    )

    if months_needed <= 30:
        return "30M"
    if months_needed <= 60:
        return "60M"
    return "MAX"


def _parse_series_list(
    series_list: list[_BdeSeriesEntry],
    requested: list[str],
) -> pd.DataFrame:
    """Convert the JSON series list into a DataFrame.

    Iterates over each entry, extracts the JSON fields
    ``"fechas"`` (dates) and ``"valores"`` (values), and
    assembles them into columns of a DataFrame with a
    datetime index.

    Logs a warning if any requested code is missing.
    """
    columns: dict[str, pd.Series] = {}

    for entry in series_list:
        code = entry["serie"]
        raw_dates = entry["fechas"]
        raw_values = entry["valores"]

        if not raw_dates:
            logger.warning("BdE: %s returned no data", code)
            continue

        dates = pd.to_datetime(raw_dates, utc=True)
        dates = dates.tz_localize(None).normalize()
        values = [_to_float(v) for v in raw_values]

        columns[code] = pd.Series(values, index=dates, name=code, dtype=float)

    missing = set(requested) - set(columns)
    if missing:
        logger.warning(
            "BdE: series not found: %s",
            ", ".join(sorted(missing)),
        )

    if not columns:
        return pd.DataFrame()

    df = pd.DataFrame(columns)
    df.index.name = "date"
    df.sort_index(inplace=True)
    return df


def _to_float(raw: int | float | None) -> float | None:
    """Convert a raw API value to float, or None."""
    if raw is None:
        return None
    try:
        return float(raw)
    except ValueError, TypeError:
        return None


def _batches(items: list[str], size: int) -> list[list[str]]:
    """Split a list into sublists of at most ``size``."""
    return [items[i : i + size] for i in range(0, len(items), size)]
