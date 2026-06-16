# Analysis Presentation Skill

Shared visual, table, caption, and Quarto rendering rules for all ChronoQuant analysis notebooks.

This skill maps Urban Institute-style data communication principles to Python notebooks rendered by Quarto.

---

## Design Goals

Analysis notebooks must be readable as standalone reports after Quarto rendering. A reader should understand every table and figure without opening the code.

Each result-producing code cell must have:

1. A preceding markdown explanation.
2. A Quarto label and caption.
3. Consistent numeric formatting.
4. A nearby finding that interprets the result.

---

## Quarto Project Defaults

Add or update `_quarto.yaml` near the analysis docs root. The goal is to let Quarto handle numbering instead of manually writing numbers in headings or captions.

Recommended project config:

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

Rules:

- Do not manually number section headings, figures, or tables.
- Use `number-sections: true` for section numbering.
- Use `label`, `fig-cap`, and `tbl-cap` for figure/table numbering.
- Use `fig-subcap` for multi-plot panels.
- Use markdown `tbl-` div panels for subtable groups when needed.

---

## Required Cell Narrative Pattern

Before every table or plot code cell, add a markdown cell with this structure:

```markdown
### <Human-readable check title>

**Purpose.** This table/figure checks <what is being checked>.

**Method.** It uses <source tables/files/columns> and computes <metric> at <grain>.

**Interpretation.** Healthy output means <condition>. Suspicious or failing output means <condition>.

**Action rule.** If the check fails, <next action>.
```

Do not write placeholders. If the finding is data-dependent, compute it in the next code cell and display it as Markdown.

---

## Quarto Caption Examples

### Single figure

```python
#| label: fig-positive-rate-by-year
#| fig-cap: "Positive target rate by year"
#| fig-alt: "Line chart showing yearly positive target rate, with the expected range shown as reference bands."

fig, ax = plt.subplots(figsize=(9, 5.5))
ax.plot(yearly["year"], yearly["positive_rate"])
ax.axhline(0.08, linestyle="--", linewidth=1)
ax.axhline(0.12, linestyle="--", linewidth=1)
ax.set_xlabel("Year")
ax.set_ylabel("Positive rate")
ax.set_title("")
ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1.0, decimals=2))
ax.xaxis.set_major_locator(mtick.MaxNLocator(integer=True))
plt.show()
```

### Single table

```python
#| label: tbl-positive-rate-by-year
#| tbl-cap: "Positive target rate by year"

display_df = yearly.assign(
    year=lambda d: d["year"].astype("Int64").astype(str),
    row_count=lambda d: d["row_count"].round(0).astype("Int64"),
    positive_rate=lambda d: (d["positive_rate"] * 100).map(lambda x: f"{x:.2f}%"),
)
display_df
```

### Multi-figure panel

```python
#| label: fig-feature-null-pattern-panel
#| fig-cap: "Feature null-pattern diagnostics"
#| fig-subcap:
#|   - "Leading null count by rolling feature"
#|   - "Null-rate distribution by feature family"
#| layout-ncol: 2

fig, ax = plt.subplots(figsize=(7, 4))
ax.barh(nulls_by_feature["feature"], nulls_by_feature["leading_nulls"])
ax.set_xlabel("Leading nulls")
ax.set_ylabel("")
ax.set_title("")
plt.show()

fig, ax = plt.subplots(figsize=(7, 4))
ax.hist(nulls_by_family["null_rate"], bins=20)
ax.set_xlabel("Null rate")
ax.set_ylabel("Feature families")
ax.set_title("")
ax.xaxis.set_major_formatter(mtick.PercentFormatter(xmax=1.0, decimals=2))
plt.show()
```

---

## Python Visual Style

Use a clean, restrained style inspired by Urban Institute's chart conventions: strong readability, minimal clutter, direct labels, clear axes, and consistent palette.

### Palette

Use this project palette unless the user requests otherwise:

```python
CQ_COLORS = {
    "blue": "#1696d2",
    "black": "#000000",
    "gray_dark": "#353535",
    "gray": "#696969",
    "gray_light": "#d2d2d2",
    "cyan": "#55b748",
    "yellow": "#fdbf11",
    "orange": "#f15a24",
    "red": "#ec008b",
}

CQ_SEQUENCE = [
    CQ_COLORS["blue"],
    CQ_COLORS["yellow"],
    CQ_COLORS["orange"],
    CQ_COLORS["gray"],
    CQ_COLORS["red"],
]
```

Use color meaning consistently:

- main measured series: blue
- expected/acceptable reference: gray or gray light
- warning threshold: yellow or orange
- failure/violation: red
- neutral context: gray

### Matplotlib setup

Put this in the notebook setup cell:

```python
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

CQ_COLORS = {
    "blue": "#1696d2",
    "black": "#000000",
    "gray_dark": "#353535",
    "gray": "#696969",
    "gray_light": "#d2d2d2",
    "yellow": "#fdbf11",
    "orange": "#f15a24",
    "red": "#ec008b",
}
CQ_SEQUENCE = [CQ_COLORS["blue"], CQ_COLORS["yellow"], CQ_COLORS["orange"], CQ_COLORS["gray"], CQ_COLORS["red"]]

plt.rcParams.update({
    "figure.figsize": (9, 5.5),
    "figure.dpi": 120,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.edgecolor": CQ_COLORS["gray"],
    "axes.labelcolor": CQ_COLORS["gray_dark"],
    "xtick.color": CQ_COLORS["gray_dark"],
    "ytick.color": CQ_COLORS["gray_dark"],
    "grid.color": CQ_COLORS["gray_light"],
    "grid.linewidth": 0.8,
    "axes.grid": True,
    "axes.axisbelow": True,
    "legend.frameon": False,
})
```

### Chart rules

- Do not use `ax.set_title()` for the main title. Let Quarto captions title and number the figure.
- Use clear axis labels with units.
- Remove top and right spines.
- Use light gridlines, mainly on the value axis.
- Start bar charts at zero unless there is a documented reason not to.
- Prefer horizontal bars for long category names.
- Sort bars by the measured value unless chronological or logical order matters.
- Use direct labels or compact legends; avoid large legends when labels can be put near data.
- Show thresholds as reference lines or bands and explain them in the preceding markdown.
- Use red/orange only for actual warnings/failures.

---

## Table Style Rules

Tables must be compact and typed for reading.

Required formatting:

- `year`: full integer year, no thousands separator.
- `count(*)`, `count`, `n`, `row_count`: integer, zero decimals.
- rates/proportions/shares: percent string with exactly two decimals.
- Boolean pass/fail columns: use `PASS` / `FAIL` or compact icons plus text.
- Sort tables so the most important failures appear first.
- Show at most 20 rows in the main report; link or save full detail separately if needed.

Python formatter:

```python
def format_analysis_table(df):
    out = df.copy()
    for col in out.columns:
        lower = col.lower()
        if lower == "year":
            out[col] = out[col].astype("Int64").astype(str)
        elif lower in {"count", "count(*)", "n", "row_count", "rows", "violations"} or lower.endswith("_count"):
            out[col] = out[col].round(0).astype("Int64")
        elif any(token in lower for token in ["rate", "ratio", "share", "pct", "percent"]):
            values = out[col]
            if values.max(skipna=True) <= 1.0:
                values = values * 100
            out[col] = values.map(lambda x: f"{x:.2f}%")
    return out
```

---

## Rendering QA Checklist

Before handing off an analysis notebook, verify:

- One spec produced one notebook.
- Notebook executed from a clean kernel.
- Quarto render succeeded.
- No `futtatás után kitöltendő` or equivalent placeholder remains.
- Every table cell has `#| label: tbl-...` and `#| tbl-cap: ...`.
- Every plot cell has `#| label: fig-...` and `#| fig-cap: ...`.
- Multi-plot cells use `fig-subcap` or are split into separate labelled cells.
- Markdown before each output explains purpose, method, interpretation, and action rule.
- Numeric formatting follows project conventions.
- Headings/captions are not manually numbered.
