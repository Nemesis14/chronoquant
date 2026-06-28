"""Build a single Quarto-renderable notebook from _doc_ markdown and notebooks."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
DOC_DIR = ROOT / "_doc_"
# Three-zone layout (epic_032): docs live in these subdirs + the _doc_ root globals.
# Walk order is by topic number across all zones, so methodology (X000/X100) still
# precedes its code reference (X110) in the consolidated reading order.
ZONE_DIRS = ("database_and_code_doc", "methodology_doc", "models_doc")
DEFAULT_OUT = ROOT / "_chronoquant_docs.ipynb"
MERMAID_FENCE_RE = re.compile(r"^(\s*)```mermaid\s*$", re.IGNORECASE | re.MULTILINE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge _doc_/*.md and _doc_/*.ipynb into one Quarto-renderable notebook.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUT,
        help=f"Output notebook path. Default: {DEFAULT_OUT}",
    )
    return parser.parse_args()


def raw_cell(source: str) -> dict[str, Any]:
    return {
        "cell_type": "raw",
        "metadata": {},
        "source": source.splitlines(keepends=True),
    }


def markdown_cell(source: str) -> dict[str, Any]:
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": normalize_quarto_mermaid(source).splitlines(keepends=True),
    }


def normalize_quarto_mermaid(source: str) -> str:
    """Convert GitHub-style Mermaid fences to Quarto executable Mermaid blocks."""
    return MERMAID_FENCE_RE.sub(r"\1```{mermaid}", source)


def frontmatter() -> str:
    return """---
title: "ChronoQuant Documentation"
format:
  html:
    theme: cosmo
    css: analyst/quarto/chronoquant_analysis.css
    toc: true
    toc-title: "Tartalom"
    toc-location: left
    toc-depth: 3
    toc-expand: 2
    number-sections: true
    page-layout: article
    smooth-scroll: true
    code-fold: true
    code-tools: true
    code-summary: "code"
    code-copy: true
    code-overflow: wrap
    df-print: paged
    fig-align: center
    fig-width: 10
    fig-height: 5
    fig-format: retina
    embed-resources: true
    self-contained: true
    grid:
      sidebar-width: 380px
      body-width: 900px
      margin-width: 140px
      gutter-width: 2rem
execute:
  enabled: false
---
"""


def is_frontmatter_cell(cell: dict[str, Any]) -> bool:
    source = "".join(cell.get("source", []))
    stripped = source.lstrip()
    return cell.get("cell_type") == "raw" and stripped.startswith("---")


def doc_sources() -> list[Path]:
    """Collect docs from the _doc_ root globals + the three zone subdirs.

    Numbered files are merged and sorted by their 4-digit topic prefix so the
    reading order is preserved across zones (methodology X000/X100 before code
    X110). Archive subdirs and the draft `_plans_/` are excluded.
    """
    sources: list[Path] = []
    readme = DOC_DIR / "README.md"
    if readme.exists():
        sources.append(readme)

    search_dirs = [DOC_DIR] + [DOC_DIR / zone for zone in ZONE_DIRS]
    numbered: list[Path] = []
    for base in search_dirs:
        if not base.is_dir():
            continue
        for path in base.iterdir():
            if (
                path.is_file()
                and path.suffix.lower() in {".md", ".ipynb"}
                and path.name[:4].isdigit()
            ):
                numbered.append(path)

    numbered.sort(key=lambda p: p.name)
    sources.extend(numbered)
    return sources


def append_markdown(cells: list[dict[str, Any]], path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    cells.append(markdown_cell(f"\n\n---\n\n<!-- Source: {path.name} -->\n\n"))
    cells.append(markdown_cell(text))


def append_notebook(cells: list[dict[str, Any]], path: Path) -> None:
    notebook = json.loads(path.read_text(encoding="utf-8"))
    cells.append(markdown_cell(f"\n\n---\n\n<!-- Source notebook: {path.name} -->\n\n"))
    for cell in notebook.get("cells", []):
        if is_frontmatter_cell(cell):
            continue
        if cell.get("cell_type") == "markdown":
            cell = dict(cell)
            cell["source"] = normalize_quarto_mermaid("".join(cell.get("source", []))).splitlines(
                keepends=True
            )
        cells.append(cell)


def build_notebook(out_path: Path) -> None:
    out_path = out_path if out_path.is_absolute() else ROOT / out_path
    out_path.parent.mkdir(parents=True, exist_ok=True)

    cells: list[dict[str, Any]] = [raw_cell(frontmatter())]
    for path in doc_sources():
        if path.resolve() == out_path.resolve():
            continue
        if path.suffix.lower() == ".md":
            append_markdown(cells, path)
        elif path.suffix.lower() == ".ipynb":
            append_notebook(cells, path)

    notebook = {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {
                "name": "python",
                "pygments_lexer": "ipython3",
            },
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    out_path.write_text(json.dumps(notebook, indent=2, ensure_ascii=False), encoding="utf-8")
    print(out_path)


def main() -> None:
    args = parse_args()
    build_notebook(args.out)


if __name__ == "__main__":
    main()
