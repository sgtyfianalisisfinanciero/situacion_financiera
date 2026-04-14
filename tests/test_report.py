"""Tests for src.report."""

# pyright: reportPrivateUsage=false

import tempfile
import unittest
from pathlib import Path
from typing import Any

import yaml
from PIL import Image as PILImage

from src.report import (
    _build_row,
    _build_section,
    _load_chart_meta,
    generate_report,
)


def _create_dummy_png(path: Path) -> None:
    """Create a minimal valid PNG file."""
    img = PILImage.new("RGB", (100, 100), color="white")
    img.save(path, format="PNG")


def _write_charts_yaml(path: Path, charts: dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump({"charts": charts}, f)


def _minimal_charts_yaml(path: Path) -> None:
    _write_charts_yaml(
        path,
        {
            "chart_a": {
                "title": "Chart A",
                "subtitle": "Units A",
                "source": "Source A",
            },
            "chart_b": {
                "title": "Chart B",
                "subtitle": "Units B",
                "source": "Source B",
            },
        },
    )


class TestLoadChartMeta(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.config = Path(self.tmp.name) / "charts.yaml"
        _minimal_charts_yaml(self.config)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_loads_titles(self) -> None:
        meta = _load_chart_meta(self.config)
        self.assertEqual(meta["chart_a"]["title"], "Chart A")
        self.assertEqual(meta["chart_b"]["subtitle"], "Units B")

    def test_uses_id_as_fallback_title(self) -> None:
        path = Path(self.tmp.name) / "bare.yaml"
        _write_charts_yaml(path, {"bare": {}})
        meta = _load_chart_meta(path)
        self.assertEqual(meta["bare"]["title"], "bare")


class TestBuildRow(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.charts_dir = Path(self.tmp.name)
        # Create dummy PNGs.
        _create_dummy_png(self.charts_dir / "chart_a.png")
        _create_dummy_png(self.charts_dir / "chart_b.png")
        self.meta: dict[str, dict[str, str]] = {
            "chart_a": {
                "title": "A",
                "subtitle": "sub A",
                "source": "src",
            },
            "chart_b": {
                "title": "B",
                "subtitle": "sub B",
                "source": "src",
            },
        }
        self.available = {"chart_a", "chart_b"}

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_single_chart_returns_image(self) -> None:
        row_def: dict[str, Any] = {
            "charts": ["chart_a"],
            "source": "Fuente",
        }
        result = _build_row(
            row_def,
            self.charts_dir,
            self.meta,
            self.available,
        )
        # Single chart returns an Image, not Images.
        self.assertIsNotNone(result)
        self.assertEqual(type(result).__name__, "Image")

    def test_two_charts_returns_images(self) -> None:
        row_def: dict[str, Any] = {
            "charts": ["chart_a", "chart_b"],
            "source": "Fuente",
        }
        result = _build_row(
            row_def,
            self.charts_dir,
            self.meta,
            self.available,
        )
        self.assertIsNotNone(result)
        self.assertEqual(type(result).__name__, "Images")

    def test_no_available_returns_none(self) -> None:
        row_def: dict[str, Any] = {
            "charts": ["nonexistent"],
            "source": "Fuente",
        }
        result = _build_row(
            row_def,
            self.charts_dir,
            self.meta,
            self.available,
        )
        self.assertIsNone(result)

    def test_partial_availability(self) -> None:
        row_def: dict[str, Any] = {
            "charts": ["chart_a", "missing"],
            "source": "Fuente",
        }
        result = _build_row(
            row_def,
            self.charts_dir,
            self.meta,
            self.available,
        )
        # Only one chart available -> Image (not Images).
        self.assertIsNotNone(result)
        self.assertEqual(type(result).__name__, "Image")


class TestBuildSection(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.charts_dir = Path(self.tmp.name)
        _create_dummy_png(self.charts_dir / "c1.png")
        self.meta: dict[str, dict[str, str]] = {
            "c1": {
                "title": "C1",
                "subtitle": "",
                "source": "src",
            },
        }

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_builds_section_with_rows(self) -> None:
        rows = [
            {"charts": ["c1"], "source": "Fuente"},
        ]
        section = _build_section(
            "Test Section",
            rows,
            self.charts_dir,
            self.meta,
            {"c1"},
        )
        self.assertIsNotNone(section)

    def test_returns_none_when_all_rows_empty(self) -> None:
        rows = [
            {"charts": ["missing"], "source": "Fuente"},
        ]
        section = _build_section(
            "Empty",
            rows,
            self.charts_dir,
            self.meta,
            {"c1"},
        )
        self.assertIsNone(section)


class TestGenerateReport(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.tmp.name)
        self.charts_dir = self.tmp_path / "charts"
        self.charts_dir.mkdir()

        # Create dummy PNGs for charts referenced in
        # the report layout.
        for name in [
            "cap_nec_financiacion",
            "ahorro_bruto",
            "stock_credito_yoy",
            "ti_stock",
        ]:
            _create_dummy_png(self.charts_dir / f"{name}.png")

        self.config = self.tmp_path / "charts.yaml"
        charts: dict[str, Any] = {}
        for name in [
            "cap_nec_financiacion",
            "ahorro_bruto",
            "stock_credito_yoy",
            "ti_stock",
        ]:
            charts[name] = {
                "title": name.replace("_", " ").title(),
                "subtitle": "Test",
                "source": "Fuente test",
            }
        _write_charts_yaml(self.config, charts)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_creates_docx(self) -> None:
        output = self.tmp_path / "report.docx"
        generate_report(self.charts_dir, self.config, output)
        self.assertTrue(output.exists())
        self.assertGreater(output.stat().st_size, 0)

    def test_handles_no_charts(self) -> None:
        empty_dir = self.tmp_path / "empty"
        empty_dir.mkdir()
        output = self.tmp_path / "empty_report.docx"
        generate_report(empty_dir, self.config, output)
        self.assertTrue(output.exists())
