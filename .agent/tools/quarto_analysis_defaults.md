# Quarto Analysis Defaults

Documents the Quarto configuration defaults for ChronoQuant analysis notebooks.
Numbering belongs in Quarto config — never write "Figure 1", "Table 2", etc. manually.

---

## `_quarto.yml`

Régi `_doc_/analysis/_quarto.yml` törölve. Minden notebook self-contained Raw-cell frontmattert
használ — nincs `_quarto.yml`-függőség. Az alábbi beállítások referenciaként:

CSS elérési út `_doc_/` notebookoknál: `../src/analyst/chronoquant_analysis.css`  
CSS elérési út `src/modeling/` notebookoknál: `../analyst/chronoquant_analysis.css`

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
quarto render _doc_\XXXX_<slug>.ipynb --execute
```

The rendered HTML is placed at `_doc_/XXXX_<slug>.html` (same directory as the notebook).
Rendering must succeed before the task is complete. If it fails, fix the notebook.

Each notebook is self-contained: the full Quarto config lives in the Raw-cell frontmatter
(see `analyst_skill.md`). No `_quarto.yml` project discovery needed.
The CSS is referenced as `analysis/chronoquant_analysis.css` relative to `_doc_/`.

---

## What NOT to Do

- Never write "Figure 1", "Table 2", or manual section numbers anywhere in captions or narrative.
- Never set `ax.set_title()` for figure titles — use `fig-cap` instead.
- Never omit `label` from result-producing cells.
- Never use `freeze: true` unless specifically caching a known slow cell.
