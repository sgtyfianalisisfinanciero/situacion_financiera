"""Tests for src.store."""

# pyright: reportPrivateUsage=false

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

from src.store import (
    SeriesStore,
    _lookback_start,
    _merge,
)


# -- Fixtures -----------------------------------------------


def _sample_df(
    cols: list[str] | None = None,
    n_rows: int = 3,
    start: str = "2025-01-01",
) -> pd.DataFrame:
    """Build a small DataFrame for testing."""
    cols = cols or ["A", "B"]
    idx = pd.date_range(start, periods=n_rows, freq="MS")
    idx.name = "date"
    data = {
        c: [float(i + j) for i in range(n_rows)] for j, c in enumerate(cols)
    }
    return pd.DataFrame(data, index=idx)


def _mock_provider(
    df: pd.DataFrame,
) -> MagicMock:
    """Build a mock DataProvider that returns *df*."""
    provider = MagicMock()
    provider.fetch.return_value = df
    return provider


# -- _lookback_start -----------------------------------------


class TestLookbackStart(unittest.TestCase):
    def test_four_quarters(self) -> None:
        last = pd.Timestamp("2026-04-01")
        result = _lookback_start(last, 4)
        self.assertEqual(result, pd.Timestamp("2025-04-01"))

    def test_zero_quarters(self) -> None:
        last = pd.Timestamp("2026-04-01")
        result = _lookback_start(last, 0)
        self.assertEqual(result, last)


# -- _merge --------------------------------------------------


class TestMerge(unittest.TestCase):
    def test_both_empty(self) -> None:
        result = _merge(pd.DataFrame(), pd.DataFrame())
        self.assertTrue(result.empty)

    def test_old_empty(self) -> None:
        new = _sample_df()
        result = _merge(pd.DataFrame(), new)
        pd.testing.assert_frame_equal(result, new)

    def test_new_empty(self) -> None:
        old = _sample_df()
        result = _merge(old, pd.DataFrame())
        pd.testing.assert_frame_equal(result, old)

    def test_new_overwrites_old(self) -> None:
        old = _sample_df(["A"], n_rows=2)
        new = pd.DataFrame(
            {"A": [999.0]},
            index=pd.DatetimeIndex([old.index[0]], name="date"),
        )
        result = _merge(old, new)
        self.assertEqual(result.loc[old.index[0], "A"], 999.0)
        # Second row from old is preserved
        self.assertEqual(len(result), 2)

    def test_new_columns_added(self) -> None:
        old = _sample_df(["A"])
        new = _sample_df(["B"])
        result = _merge(old, new)
        self.assertIn("A", result.columns)
        self.assertIn("B", result.columns)


# -- SeriesStore ---------------------------------------------


class TestSeriesStore(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.mkdtemp()
        self._path = Path(self._tmpdir) / "test.feather"
        self.store = SeriesStore(self._path)

    def test_exists_false_initially(self) -> None:
        self.assertFalse(self.store.exists())

    def test_path_property(self) -> None:
        self.assertEqual(self.store.path, self._path)

    def test_load_empty_when_no_file(self) -> None:
        df = self.store.load()
        self.assertTrue(df.empty)

    def test_save_and_load_roundtrip(self) -> None:
        original = _sample_df()
        self.store.save(original)
        self.assertTrue(self.store.exists())
        loaded = self.store.load()
        # Feather does not preserve DatetimeIndex freq,
        # so we compare without checking frequency.
        pd.testing.assert_frame_equal(loaded, original, check_freq=False)

    def test_load_with_date_column(self) -> None:
        """Feather files written externally may have
        'date' as a regular column instead of the index.
        The store should set it as index on load."""
        df = _sample_df()
        df_reset = df.reset_index()  # 'date' as column
        df_reset.to_feather(self._path)
        loaded = self.store.load()
        self.assertEqual(loaded.index.name, "date")
        self.assertNotIn("date", loaded.columns)

    def test_save_creates_parent_dirs(self) -> None:
        deep = Path(self._tmpdir) / "a" / "b" / "c.feather"
        store = SeriesStore(deep)
        store.save(_sample_df())
        self.assertTrue(deep.exists())


# -- SeriesStore.update --------------------------------------


class TestSeriesStoreUpdate(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.mkdtemp()
        self._path = Path(self._tmpdir) / "test.feather"
        self.store = SeriesStore(self._path)

    def test_first_run_full_download(self) -> None:
        data = _sample_df(["X", "Y"])
        provider = _mock_provider(data)

        result = self.store.update(provider, ["X", "Y"])
        provider.fetch.assert_called_once_with(["X", "Y"])
        self.assertEqual(list(result.columns), ["X", "Y"])
        self.assertTrue(self.store.exists())

    def test_incremental_update(self) -> None:
        # Pre-populate the store
        old = _sample_df(["A"], n_rows=3)
        self.store.save(old)

        fresh = _sample_df(["A"], n_rows=1, start="2025-03-01")
        fresh.iloc[0, 0] = 999.0
        provider = _mock_provider(fresh)

        result = self.store.update(provider, ["A"], lookback_quarters=1)
        # Provider was called with start parameter
        call_kwargs = provider.fetch.call_args
        self.assertIn("start", call_kwargs.kwargs)
        # Updated value is reflected
        self.assertEqual(
            result.loc[pd.Timestamp("2025-03-01"), "A"],
            999.0,
        )

    def test_new_series_full_download(self) -> None:
        # Store has column A
        old = _sample_df(["A"])
        self.store.save(old)

        new_data = _sample_df(["B"])
        provider = _mock_provider(new_data)

        result = self.store.update(provider, ["A", "B"])
        self.assertIn("A", result.columns)
        self.assertIn("B", result.columns)
        # fetch called twice: once for existing (A),
        # once for new (B)
        self.assertEqual(provider.fetch.call_count, 2)

    def test_lookback_zero(self) -> None:
        old = _sample_df(["A"], n_rows=3)
        self.store.save(old)

        provider = _mock_provider(_sample_df(["A"]))
        self.store.update(provider, ["A"], lookback_quarters=0)
        call_kwargs = provider.fetch.call_args
        # With lookback=0, start = last date itself
        self.assertIn("start", call_kwargs.kwargs)


if __name__ == "__main__":
    unittest.main()
