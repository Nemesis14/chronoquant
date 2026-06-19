#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT_NOTEBOOK="${1:-_chronoquant_docs.ipynb}"
OUT_HTML="${ROOT_DIR}/${OUT_NOTEBOOK%.*}.html"

cd "$ROOT_DIR"
uv run python analyst/doc_renderer/build_doc_notebook.py --out "$OUT_NOTEBOOK"
quarto render "$ROOT_DIR/$OUT_NOTEBOOK" --to html

echo "Rendered: $OUT_HTML"
