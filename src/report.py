"""Word report generation from YAML template.

Reads ``series/template.yaml`` using the tesorotools
``TemplateLoader`` which interprets custom tags (``!report``,
``!section``, ``!image``, ``!images``, ``!table``, ``!text``,
``!title``).  The template defines the full document
structure declaratively — this module just loads it and
renders to a Word file.
"""

import logging
from pathlib import Path

from docx import Document

import tesorotools.render  # noqa: F401  # pyright: ignore[reportUnusedImport]
from tesorotools.render.report import Report
from tesorotools.utils.template import TemplateLoader

logger = logging.getLogger(__name__)


def _load_template(path: Path) -> Report:
    """Load a report template from YAML."""
    import yaml

    with open(path, encoding="utf-8") as f:
        config = yaml.load(f, Loader=TemplateLoader)  # noqa: S506
    return config["report"]  # type: ignore[no-any-return]


def generate_report(
    template_path: Path,
    output_path: Path,
) -> None:
    """Generate the Word document from a YAML template.

    Parameters
    ----------
    template_path
        Path to the template YAML (with ``!report``
        and ``imports`` for image/table directories).
    output_path
        Path for the output .docx file.
    """
    report = _load_template(template_path)

    doc = Document()
    doc = report.render(doc)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    doc.save(str(output_path))
    logger.info("Report saved: %s", output_path)
