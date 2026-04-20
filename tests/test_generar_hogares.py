"""Tests for generar_hogares."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from generar_hogares import (
    build_code_maps,
    export_excel,
    load_instruments,
)


# -- Fixtures -----------------------------------------------


_MINIMAL_YAML = """\
instruments:
  MY_SERIES:
    display_name: "test series"
    providers:
      bde:
        code: "TEST_CODE_001"
  ANOTHER:
    display_name: "another"
    providers:
      bde:
        code: "TEST_CODE_002"
  AN_ECB_SERIES:
    display_name: "ecb series"
    providers:
      ecb:
        code: "BLS.Q.ES.ALL.Z.H.H.B3.ZZ.D.FNET"
"""

_NO_KNOWN_PROVIDER_YAML = """\
instruments:
  MY_SERIES:
    display_name: "test"
    providers:
      other:
        code: "X"
"""


def _write_yaml(content: str, directory: str) -> Path:
    """Write YAML content to a temp file and return path."""
    path = Path(directory) / "instruments.yaml"
    path.write_text(content, encoding="utf-8")
    return path


# -- load_instruments ----------------------------------------


class TestLoadInstruments(unittest.TestCase):
    def test_loads_catalog(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            path = _write_yaml(_MINIMAL_YAML, d)
            catalog = load_instruments(path)
        self.assertIn("MY_SERIES", catalog)
        self.assertIn("ANOTHER", catalog)
        self.assertEqual(len(catalog), 3)

    def test_entry_structure(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            path = _write_yaml(_MINIMAL_YAML, d)
            catalog = load_instruments(path)
        entry = catalog["MY_SERIES"]
        self.assertEqual(entry["display_name"], "test series")
        providers = entry["providers"]
        self.assertIn("bde", providers)
        bde = providers.get("bde")
        assert bde is not None
        self.assertEqual(bde["code"], "TEST_CODE_001")

    def test_file_not_found(self) -> None:
        with self.assertRaises(FileNotFoundError):
            load_instruments(Path("/nonexistent/x.yaml"))


# -- build_code_maps -----------------------------------------


class TestBuildCodeMaps(unittest.TestCase):
    def test_extracts_codes_per_provider(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            path = _write_yaml(_MINIMAL_YAML, d)
            catalog = load_instruments(path)
        code_maps = build_code_maps(catalog)
        self.assertIn("bde", code_maps)
        self.assertIn("ecb", code_maps)
        self.assertEqual(code_maps["bde"]["MY_SERIES"], "TEST_CODE_001")
        self.assertEqual(code_maps["bde"]["ANOTHER"], "TEST_CODE_002")
        self.assertEqual(
            code_maps["ecb"]["AN_ECB_SERIES"],
            "BLS.Q.ES.ALL.Z.H.H.B3.ZZ.D.FNET",
        )

    def test_omits_providers_with_no_entries(self) -> None:
        bde_only_yaml = """\
instruments:
  X:
    display_name: "x"
    providers:
      bde:
        code: "C"
"""
        with tempfile.TemporaryDirectory() as d:
            path = _write_yaml(bde_only_yaml, d)
            catalog = load_instruments(path)
        code_maps = build_code_maps(catalog)
        self.assertEqual(list(code_maps), ["bde"])

    def test_no_known_provider_raises(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            path = _write_yaml(_NO_KNOWN_PROVIDER_YAML, d)
            catalog = load_instruments(path)
        with self.assertRaises(RuntimeError):
            build_code_maps(catalog)


# -- export_excel --------------------------------------------


class TestExportExcel(unittest.TestCase):
    def test_creates_excel(self) -> None:
        df = pd.DataFrame(
            {"A": [1.0, 2.0]},
            index=pd.date_range("2025-01-01", periods=2, freq="MS"),
        )
        df.index.name = "date"
        with tempfile.TemporaryDirectory() as d:
            xlsx_path = Path(d) / "out.xlsx"
            export_excel(df, xlsx_path)
            self.assertTrue(xlsx_path.exists())

    def test_creates_parent_dir(self) -> None:
        df = pd.DataFrame(
            {"A": [1.0]},
            index=pd.DatetimeIndex(
                [pd.Timestamp("2025-01-01")],
                name="date",
            ),
        )
        with tempfile.TemporaryDirectory() as d:
            xlsx_path = Path(d) / "sub" / "dir" / "out.xlsx"
            export_excel(df, xlsx_path)
            self.assertTrue(xlsx_path.exists())


# -- main (smoke test) ---------------------------------------


_BDE_ONLY_YAML = """\
instruments:
  MY_SERIES:
    display_name: "test series"
    providers:
      bde:
        code: "TEST_CODE_001"
"""


class TestMain(unittest.TestCase):
    def _mock_store(self) -> MagicMock:
        store = MagicMock()
        store.exists.return_value = False
        store.load.return_value = pd.DataFrame(
            {"TEST_CODE_001": [1.0]},
            index=pd.DatetimeIndex(
                [pd.Timestamp("2025-01-01")],
                name="date",
            ),
        )
        return store

    @patch("generar_hogares.BdeProvider")
    @patch("generar_hogares.SeriesStore")
    def test_download_only(
        self,
        mock_store_cls: MagicMock,
        mock_provider_cls: MagicMock,
    ) -> None:
        """Smoke test: main() with --download-only runs
        without error when mocked."""
        mock_store = self._mock_store()
        mock_store_cls.return_value = mock_store
        mock_provider_cls.return_value = MagicMock()

        with (
            tempfile.TemporaryDirectory() as d,
            patch(
                "generar_hogares.INSTRUMENTS_PATH",
                _write_yaml(_BDE_ONLY_YAML, d),
            ),
            patch(
                "generar_hogares.OUTPUT_DIR",
                Path(d) / "output",
            ),
            patch(
                "sys.argv",
                ["generar_hogares.py", "--download-only"],
            ),
        ):
            from generar_hogares import main

            main()

        mock_store.update.assert_called_once()

    @patch("generar_hogares.BdeProvider")
    @patch("generar_hogares.SeriesStore")
    def test_full_flag(
        self,
        mock_store_cls: MagicMock,
        mock_provider_cls: MagicMock,
    ) -> None:
        """--full deletes existing store."""
        mock_store = self._mock_store()
        mock_store.exists.return_value = True
        mock_store_cls.return_value = mock_store
        mock_provider_cls.return_value = MagicMock()

        with (
            tempfile.TemporaryDirectory() as d,
            patch(
                "generar_hogares.INSTRUMENTS_PATH",
                _write_yaml(_BDE_ONLY_YAML, d),
            ),
            patch(
                "generar_hogares.OUTPUT_DIR",
                Path(d) / "output",
            ),
            patch(
                "sys.argv",
                [
                    "generar_hogares.py",
                    "--full",
                    "--download-only",
                ],
            ),
        ):
            from generar_hogares import main

            main()

        mock_store.path.unlink.assert_called_once()

    @patch("generar_hogares.BdeProvider")
    @patch("generar_hogares.SeriesStore")
    def test_default_mode(
        self,
        mock_store_cls: MagicMock,
        mock_provider_cls: MagicMock,
    ) -> None:
        """Without --download-only, pipeline continues
        past the download step."""
        mock_store = self._mock_store()
        mock_store_cls.return_value = mock_store
        mock_provider_cls.return_value = MagicMock()

        with (
            tempfile.TemporaryDirectory() as d,
            patch(
                "generar_hogares.INSTRUMENTS_PATH",
                _write_yaml(_BDE_ONLY_YAML, d),
            ),
            patch(
                "generar_hogares.OUTPUT_DIR",
                Path(d) / "output",
            ),
            patch(
                "sys.argv",
                ["generar_hogares.py"],
            ),
        ):
            from generar_hogares import main

            main()

        mock_store.update.assert_called_once()


if __name__ == "__main__":
    unittest.main()
