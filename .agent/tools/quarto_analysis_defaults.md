# Quarto Analysis Defaults

Documents the Quarto configuration defaults for ChronoQuant analysis notebooks.
Numbering belongs in Quarto config — never write "Figure 1", "Table 2", etc. manually.

---

## `_quarto.yml`

Régi `_doc_/analysis/_quarto.yml` törölve. Minden notebook self-contained Raw-cell frontmattert
használ — nincs `_quarto.yml`-függőség. Az alábbi beállítások referenciaként:

CSS elérési út `_doc_/` notebookoknál: `../analyst/chronoquant_analysis.css`
CSS elérési út `src/modeling/` notebookoknál: `../../analyst/chronoquant_analysis.css`

```yaml
project:
  type: default

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
    embed-resources: true

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

---

## Label Conventions

### Figures

```python
#| label: fig-<kebab-case-name>
#| fig-cap: "Human-readable caption"
#| fig-alt: "Alt text for accessibility"
```

### Tables

```python
#| label: tbl-<kebab-case-name>
#| tbl-cap: "Human-readable caption"
```

### Multi-plot panels

```python
#| label: fig-<panel-name>
#| fig-cap: "Overall panel caption"
#| fig-subcap:
#|   - "Subcaption for plot 1"
#|   - "Subcaption for plot 2"
#| layout-ncol: 2
```

**Temporal subplot rule:** for time-ordered subplots (period box plots by year /
half-year, rolling time-series across years), use `layout-ncol: 1` instead of
`layout-ncol: 2`. Vertical stacking preserves the time axis width and makes
period comparisons readable. Side-by-side collapses the x-axis and obscures trends.

### Subtable groups (markdown)

```markdown
::: {#tbl-<panel-name> layout-ncol=2}

| col | val |
|---|---:|
| a | 1 |

: First subtable {#tbl-<name-1>}

| col | val |
|---|---:|
| b | 2 |

: Second subtable {#tbl-<name-2>}

Panel caption
:::
```

---

## Render Command

Always render after executing a notebook:

```bash
quarto render _doc_\models_doc\XXXX_<slug>.ipynb --execute
```

The rendered HTML is placed at `_doc_/models_doc/XXXX_<slug>.html` (same directory as the notebook).
Rendering must succeed before the task is complete. If it fails, fix the notebook.

Each notebook is self-contained: the full Quarto config lives in the Raw-cell frontmatter
(see `analyst_skill.md`). No `_quarto.yml` project discovery needed.
The CSS is referenced as `../../analyst/chronoquant_analysis.css` relative to `_doc_/models_doc/`.

---

## Consolidated Documentation Layout

For `_chronoquant_docs.ipynb` / `_chronoquant_docs.html`, update both
`analyst/_quarto.yml` and `analyst/doc_renderer/build_doc_notebook.py` when the
global layout must persist after notebook rebuild.

Working grid:

```yaml
grid:
  sidebar-width: 380px
  body-width: 900px
  margin-width: 140px
  gutter-width: 2rem
```

TOC CSS guidance:

```css
nav#TOC {
  width: 100% !important;
  font-size: 0.92rem;
}
```

Mermaid HTML guidance:

```css
.cell-output-display:has(svg.mermaid-js),
.cell-output-display:has(pre.mermaid-js) {
  align-items: stretch;
  overflow-x: visible;
  width: 100%;
}

.cell-output-display:has(svg.mermaid-js) > div,
.cell-output-display:has(pre.mermaid-js) > div,
.cell-output-display:has(svg.mermaid-js) figure,
.cell-output-display:has(pre.mermaid-js) figure,
.cell-output-display:has(svg.mermaid-js) figure > div,
.cell-output-display:has(pre.mermaid-js) figure > div {
  width: 100%;
  max-width: 100%;
}

pre.mermaid-js,
svg.mermaid-js {
  width: 100% !important;
  max-width: 100% !important;
}

svg.mermaid-js {
  height: auto !important;
  display: block;
  margin: 1rem auto 1.5rem;
}
```

Do not add a desktop width override that exceeds the body by default; it makes
large sequence diagrams overflow. Avoid emoji/status glyphs in Mermaid labels
because Quarto's bundled Mermaid parser can show `Syntax error in text`; use
`OK:`, `NO:`, `WARN:` inside Mermaid nodes instead.

---

## What NOT to Do

- Never write "Figure 1", "Table 2", or manual section numbers anywhere in captions or narrative.
- Never set `ax.set_title()` for figure titles — use `fig-cap` instead.
- Never omit `label` from result-producing cells.
- Never use `freeze: true` unless specifically caching a known slow cell.
