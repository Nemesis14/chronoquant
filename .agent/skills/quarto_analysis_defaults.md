# Quarto Analysis Defaults for ChronoQuant

Create or update `_quarto.yaml` in the documentation/analysis root so Quarto handles numbering and rendering consistently.

Recommended `_quarto.yaml`:

```yaml
project:
  type: website

format:
  html:
    toc: true
    toc-depth: 3
    number-sections: true
    code-fold: true
    code-tools: true
    theme: cosmo
    fig-width: 9
    fig-height: 5.5
    df-print: paged

execute:
  echo: true
  warning: false
  message: false
  freeze: false

crossref:
  fig-title: "Figure"
  tbl-title: "Table"
  title-delim: ":"
```

Operational rules:

1. Do not manually number headings, figures, or tables.
2. Use Quarto cross-reference labels:
   - `fig-...` for figures
   - `tbl-...` for tables
3. Use `fig-cap`, `tbl-cap`, and where needed `fig-subcap`.
4. Render every notebook after execution:

```bash
quarto render _doc/<topic>/analysis_<spec_slug>.ipynb --execute
```

5. Treat failed render as failed task completion.
