"""Tests for src.report."""

# pyright: reportPrivateUsage=false

import tempfile
import unittest
from pathlib import Path

import pandas as pd
from PIL import Image as PILImage

from src.report import _load_template, generate_report


def _create_dummy_png(path: Path) -> None:
    """Create a minimal valid PNG file."""
    img = PILImage.new("RGB", (100, 100), color="white")
    img.save(path, format="PNG")


def _create_dummy_feather(path: Path) -> None:
    """Create a minimal valid feather for Table."""
    df = pd.DataFrame(
        {"Jan-26": ["100", "200"]},
        index=["Serie A", "Serie B"],
    )
    df.to_feather(path)


def _write_template(
    path: Path,
    image_dir: Path,
    table_dir: Path,
) -> None:
    """Write a minimal template.yaml for testing."""
    # Paths relative to the template file.
    img_rel = image_dir.relative_to(path.parent)
    tbl_rel = table_dir.relative_to(path.parent)
    content = f"""\
imports:
  image: {img_rel.as_posix()}
  table: {tbl_rel.as_posix()}

report: !report
  title: !title
    title: "Test Report"

  charts: !section
    title: "Charts"
    chart_a.png: !image
      title: "Chart A"
      subtitle: "sub"
      width: 3

  datos: !section
    title: "Datos"
    test_tbl: !table
      title: "Test Table"
      index_name: true
"""
    path.write_text(content, encoding="utf-8")


class TestLoadTemplate(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.img_dir = self.root / "charts"
        self.img_dir.mkdir()
        self.tbl_dir = self.root / "tables"
        self.tbl_dir.mkdir()
        _create_dummy_png(self.img_dir / "chart_a.png")
        _create_dummy_feather(self.tbl_dir / "test_tbl.feather")
        self.template = self.root / "template.yaml"
        _write_template(self.template, self.img_dir, self.tbl_dir)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_loads_report(self) -> None:
        from tesorotools.render.report import Report

        report = _load_template(self.template)
        self.assertIsInstance(report, Report)


class TestGenerateReport(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.img_dir = self.root / "charts"
        self.img_dir.mkdir()
        self.tbl_dir = self.root / "tables"
        self.tbl_dir.mkdir()
        _create_dummy_png(self.img_dir / "chart_a.png")
        _create_dummy_feather(self.tbl_dir / "test_tbl.feather")
        self.template = self.root / "template.yaml"
        _write_template(self.template, self.img_dir, self.tbl_dir)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_creates_docx(self) -> None:
        output = self.root / "report.docx"
        generate_report(self.template, output)
        self.assertTrue(output.exists())
        self.assertGreater(output.stat().st_size, 0)

    def test_creates_parent_dir(self) -> None:
        output = self.root / "subdir" / "report.docx"
        generate_report(self.template, output)
        self.assertTrue(output.exists())
