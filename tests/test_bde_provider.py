"""Tests for src.providers.bde."""

# pyright: reportPrivateUsage=false

import json
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
import requests

from src.providers.bde import (
    BdeProvider,
    _batches,
    _check_api_error,
    _compute_time_range,
    _parse_series_list,
    _to_float,
)


# -- Fixtures -----------------------------------------------


def _make_series_entry(
    code: str = "S1",
    dates: list[str] | None = None,
    values: list[int | float | None] | None = None,
) -> dict[str, object]:
    """Build a minimal BdE API series entry."""
    if dates is None:
        dates = ["2026-01-01T09:15:00Z"]
    if values is None:
        values = [100]
    return {
        "serie": code,
        "fechas": dates,
        "valores": values,
        "decimales": 0,
        "codFrecuencia": "M",
    }


def _make_response(
    entries: list[dict[str, object]] | None = None,
    status: int = 200,
    error: dict[str, object] | None = None,
) -> MagicMock:
    """Build a mock requests.Response."""
    resp = MagicMock()
    resp.status_code = status
    body = error if error else (entries or [])
    resp.json.return_value = body
    resp.text = json.dumps(body)
    resp.raise_for_status = MagicMock()
    if status >= 400:
        resp.raise_for_status.side_effect = Exception(f"HTTP {status}")
    return resp


# -- _to_float -----------------------------------------------


class TestToFloat(unittest.TestCase):
    def test_int(self) -> None:
        self.assertEqual(_to_float(42), 42.0)

    def test_float(self) -> None:
        self.assertEqual(_to_float(3.14), 3.14)

    def test_none(self) -> None:
        self.assertIsNone(_to_float(None))

    def test_non_numeric_returns_none(self) -> None:
        # _to_float declares int|float|None but the API
        # could theoretically send something unexpected.
        # The try/except handles it.
        self.assertIsNone(
            _to_float("not a number")  # type: ignore[arg-type]
        )


# -- _batches ------------------------------------------------


class TestBatches(unittest.TestCase):
    def test_exact_split(self) -> None:
        self.assertEqual(
            _batches(["a", "b", "c", "d"], 2),
            [["a", "b"], ["c", "d"]],
        )

    def test_remainder(self) -> None:
        self.assertEqual(
            _batches(["a", "b", "c"], 2),
            [["a", "b"], ["c"]],
        )

    def test_empty(self) -> None:
        self.assertEqual(_batches([], 5), [])

    def test_single_batch(self) -> None:
        self.assertEqual(
            _batches(["a", "b"], 10),
            [["a", "b"]],
        )


# -- _check_api_error ----------------------------------------


class TestCheckApiError(unittest.TestCase):
    def test_list_payload_no_error(self) -> None:
        _check_api_error([{"serie": "X"}])

    def test_dict_without_errnum_no_error(self) -> None:
        _check_api_error({"other": "value"})

    def test_dict_with_errnum_raises(self) -> None:
        payload = {"errNum": 412, "errMsgUsr": "bad range"}
        with self.assertRaises(RuntimeError) as ctx:
            _check_api_error(payload)
        self.assertIn("412", str(ctx.exception))
        self.assertIn("bad range", str(ctx.exception))

    def test_dict_with_errnum_no_msg(self) -> None:
        payload = {"errNum": 500}
        with self.assertRaises(RuntimeError):
            _check_api_error(payload)

    def test_non_dict_non_list_no_error(self) -> None:
        _check_api_error("something")
        _check_api_error(42)


# -- _compute_time_range -------------------------------------


class TestComputeTimeRange(unittest.TestCase):
    def test_none_returns_max(self) -> None:
        self.assertEqual(_compute_time_range(None), "MAX")

    def test_recent_returns_30m(self) -> None:
        recent = pd.Timestamp.now() - pd.DateOffset(months=6)
        result = _compute_time_range(str(recent.date()))
        self.assertEqual(result, "30M")

    def test_medium_returns_60m(self) -> None:
        medium = pd.Timestamp.now() - pd.DateOffset(months=35)
        result = _compute_time_range(str(medium.date()))
        self.assertEqual(result, "60M")

    def test_old_returns_max(self) -> None:
        result = _compute_time_range("2000-01-01")
        self.assertEqual(result, "MAX")


# -- _parse_series_list --------------------------------------


class TestParseSeriesList(unittest.TestCase):
    def test_single_series(self) -> None:
        entries = [
            _make_series_entry(
                "S1",
                dates=[
                    "2026-02-01T09:15:00Z",
                    "2026-01-01T09:15:00Z",
                ],
                values=[200, 100],
            )
        ]
        df = _parse_series_list(entries, ["S1"])  # type: ignore[arg-type]
        self.assertEqual(list(df.columns), ["S1"])
        self.assertEqual(len(df), 2)
        self.assertEqual(df.index.name, "date")
        # Sorted chronologically
        self.assertEqual(df["S1"].iloc[0], 100.0)
        self.assertEqual(df["S1"].iloc[1], 200.0)

    def test_multiple_series(self) -> None:
        entries = [
            _make_series_entry("A", values=[10]),
            _make_series_entry("B", values=[20]),
        ]
        df = _parse_series_list(entries, ["A", "B"])  # type: ignore[arg-type]
        self.assertEqual(sorted(df.columns), ["A", "B"])

    def test_empty_dates_skipped(self) -> None:
        entries = [
            _make_series_entry("S1", dates=[], values=[]),
        ]
        df = _parse_series_list(entries, ["S1"])  # type: ignore[arg-type]
        self.assertTrue(df.empty)

    def test_missing_series_logged(self) -> None:
        entries = [_make_series_entry("A")]
        with self.assertLogs("src.providers.bde", "WARNING"):
            _parse_series_list(entries, ["A", "B"])  # type: ignore[arg-type]

    def test_empty_list_returns_empty(self) -> None:
        df = _parse_series_list([], [])
        self.assertTrue(df.empty)

    def test_none_values_become_nan(self) -> None:
        entries = [
            _make_series_entry(
                "S1",
                dates=["2026-01-01T00:00:00Z"],
                values=[None],
            )
        ]
        df = _parse_series_list(entries, ["S1"])  # type: ignore[arg-type]
        self.assertTrue(pd.isna(df["S1"].iloc[0]))

    def test_dates_normalized_to_midnight(self) -> None:
        entries = [
            _make_series_entry(
                "S1",
                dates=["2026-03-15T14:30:00Z"],
                values=[1],
            )
        ]
        df = _parse_series_list(entries, ["S1"])  # type: ignore[arg-type]
        ts = df.index[0]
        self.assertEqual(ts.hour, 0)
        self.assertEqual(ts.minute, 0)
        self.assertIsNone(ts.tzinfo)


# -- BdeProvider.fetch ----------------------------------------


class TestBdeProviderFetch(unittest.TestCase):
    @patch("src.providers.bde.requests.get")
    def test_empty_codes_returns_empty(self, mock_get: MagicMock) -> None:
        provider = BdeProvider()
        df = provider.fetch([])
        self.assertTrue(df.empty)
        mock_get.assert_not_called()

    @patch("src.providers.bde.requests.get")
    def test_single_series(self, mock_get: MagicMock) -> None:
        mock_get.return_value = _make_response(
            [_make_series_entry("S1", values=[42])]
        )
        provider = BdeProvider()
        df = provider.fetch(["S1"])
        self.assertEqual(list(df.columns), ["S1"])
        self.assertEqual(df["S1"].iloc[0], 42.0)

    @patch("src.providers.bde.requests.get")
    def test_batching(self, mock_get: MagicMock) -> None:
        """With 12 codes, should make 2 HTTP calls."""
        codes = [f"S{i}" for i in range(12)]
        entries = [_make_series_entry(c) for c in codes]
        mock_get.return_value = _make_response(entries)
        provider = BdeProvider()
        provider.fetch(codes)
        self.assertEqual(mock_get.call_count, 2)

    @patch("src.providers.bde.requests.get")
    def test_start_filters_dates(self, mock_get: MagicMock) -> None:
        mock_get.return_value = _make_response(
            [
                _make_series_entry(
                    "S1",
                    dates=[
                        "2026-03-01T00:00:00Z",
                        "2025-01-01T00:00:00Z",
                    ],
                    values=[300, 100],
                )
            ]
        )
        provider = BdeProvider()
        df = provider.fetch(["S1"], start="2026-01-01")
        self.assertEqual(len(df), 1)
        self.assertEqual(df["S1"].iloc[0], 300.0)

    @patch("src.providers.bde.requests.get")
    def test_api_error_raises(self, mock_get: MagicMock) -> None:
        mock_get.return_value = _make_response(
            error={"errNum": 412, "errMsgUsr": "bad"}
        )
        provider = BdeProvider()
        with self.assertRaises(RuntimeError):
            provider.fetch(["S1"])

    @patch("src.providers.bde.requests.get")
    def test_all_empty_returns_empty(self, mock_get: MagicMock) -> None:
        mock_get.return_value = _make_response(
            [_make_series_entry("S1", dates=[], values=[])]
        )
        provider = BdeProvider()
        df = provider.fetch(["S1"])
        self.assertTrue(df.empty)


# -- BdeProvider.is_available ---------------------------------


class TestBdeProviderIsAvailable(unittest.TestCase):
    @patch("src.providers.bde.requests.get")
    def test_available(self, mock_get: MagicMock) -> None:
        mock_get.return_value = MagicMock(status_code=200)
        self.assertTrue(BdeProvider().is_available())

    @patch("src.providers.bde.requests.get")
    def test_not_available_bad_status(self, mock_get: MagicMock) -> None:
        mock_get.return_value = MagicMock(status_code=500)
        self.assertFalse(BdeProvider().is_available())

    @patch("src.providers.bde.requests.get")
    def test_not_available_exception(self, mock_get: MagicMock) -> None:
        mock_get.side_effect = requests.ConnectionError("down")
        self.assertFalse(BdeProvider().is_available())


if __name__ == "__main__":
    unittest.main()
