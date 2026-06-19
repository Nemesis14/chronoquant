# Analysis Presentation Skill

Visual, table, caption, and rendering rules for ChronoQuant analysis notebooks.
Applies Urban Institute-style data communication principles to Quarto-rendered notebooks.

Quarto config and label syntax → see `quarto_analysis_defaults.md`.
Notebook structure and cell patterns → see `analyst_skill.md`.

---

## Design Goals

Analysis notebooks must be readable as standalone reports after Quarto rendering.
A reader must understand every table and figure without opening the code.

Each result-producing code cell must have:

1. A preceding markdown explanation (purpose, method, interpretation, action rule).
2. A Quarto label and caption.
3. Consistent numeric formatting.
4. A nearby finding that interprets the result.

---

## Palette

Urban Institute-inspired project palette:

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

Color meaning (apply consistently):

- main measured series: blue
- expected/acceptable reference: gray or gray_light
- warning threshold: yellow or orange
- failure/violation: red
- neutral context: gray

---

## Seaborn Setup

Put this complete block in the notebook Setup cell:

```python
import seaborn as sns
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

sns.set_theme(
    style="whitegrid",
    rc={
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
        "axes.axisbelow": True,
        "legend.frameon": False,
    },
)
sns.set_palette(CQ_SEQUENCE)
```

---

## Chart Rules

- seaborn is the primary charting library; matplotlib is the fallback and customization layer (axis formatters, reference lines, multi-panel layout).
- Plotly is forbidden by default — use only if the spec explicitly requests interactive HTML output.
- Every plot must have a corresponding source dataframe or summary table computed in the same or adjacent code cell. Findings must derive from that data, not from visual inspection of the chart.
- Do not use `ax.set_title()` — let Quarto captions title and number figures.
- Use clear axis labels with units.
- Remove top and right spines (handled by `sns.set_theme` rc above).
- Use light gridlines on the value axis.
- Start bar charts at zero unless documented otherwise.
- Prefer horizontal bars for long category names.
- Sort bars by measured value unless chronological or logical order matters.
- Use direct labels or compact legends.
- Show thresholds as reference lines and explain them in the preceding markdown.
- Use red/orange only for actual warnings or failures.
- **Temporal subplot layout:** when a multi-panel cell contains two or more time-ordered
  subplots (period box plots by year/half-year, rolling time-series), always use
  `layout-ncol: 1` (vertical stacking). Side-by-side (`layout-ncol: 2`) compresses
  the time axis and makes period comparisons unreadable.

---

## Caption Examples

### Single figure

```python
#| label: fig-positive-rate-by-year
#| fig-cap: "Positive target rate by year"
#| fig-alt: "Line chart showing yearly positive target rate with reference bands."

fig, ax = plt.subplots(figsize=(9, 5.5))
sns.lineplot(data=yearly, x="year", y="positive_rate", ax=ax, color=CQ_COLORS["blue"])
ax.axhline(0.08, linestyle="--", linewidth=1, color=CQ_COLORS["gray"])
ax.axhline(0.12, linestyle="--", linewidth=1, color=CQ_COLORS["gray"])
ax.set_xlabel("Year")
ax.set_ylabel("Positive rate")
ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1.0, decimals=2))
ax.xaxis.set_major_locator(mtick.MaxNLocator(integer=True))
plt.show()
```

### Single table

```python
#| label: tbl-positive-rate-by-year
#| tbl-cap: "Positive target rate by year"

display_analysis_table(yearly)
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
sns.barplot(data=nulls_by_feature, y="feature", x="leading_nulls", ax=ax, color=CQ_COLORS["blue"], orient="h")
ax.set_xlabel("Leading nulls")
ax.set_ylabel("")
plt.show()

fig, ax = plt.subplots(figsize=(7, 4))
sns.histplot(data=nulls_by_family, x="null_rate", bins=20, ax=ax, color=CQ_COLORS["blue"])
ax.set_xlabel("Null rate")
ax.set_ylabel("Feature families")
ax.xaxis.set_major_formatter(mtick.PercentFormatter(xmax=1.0, decimals=2))
plt.show()
```

### Subtable group (markdown)

```markdown
::: {#tbl-feature-quality-panel layout-ncol=2}

| feature | leading_nulls |
|---|---:|
| feat_rsi_14 | 14 |

: Leading null counts {#tbl-leading-nulls}

| feature_family | null_rate_pct |
|---|---:|
| momentum | 2.31% |

: Null rates by family {#tbl-null-rates}

Feature quality diagnostics
:::
```

---

## Numeric Formatting

Apply consistently in all displayed tables and plot axes.

| Column type | Display format |
|---|---|
| `year` | Full integer, e.g. `2024` — never `2,024` or `2024.0` |
| `count`, `count(*)`, `n`, `row_count`, `rows`, `trades`, `volume`, `violations`, `*_count`, `*_n` | Integer, zero decimals |
| `rate`, `ratio`, `share`, `pct`, `percent` (name contains any of these) | Percent string, exactly 2 decimals, e.g. `23.24%`; auto-scaled ×100 if values ≤ 1 |
| all other float columns | 3 decimal string, e.g. `1.234` |

Shared helper module — import in every analysis notebook Setup cell:

```python
import sys
sys.path.insert(0, "analyst/src")
from table_formatting import format_analysis_table, display_analysis_table
```

`_doc_/analyst/src/table_formatting.py` is the canonical implementation.
Functions are also reproduced here for reference:

```python
from IPython.display import display
import pandas as pd

_COUNT_NAMES = {"count", "count(*)", "n", "row_count", "rows", "trades", "volume", "violations"}
_PCT_TOKENS  = ("rate", "ratio", "share", "pct", "percent")

def format_analysis_table(df):
    out = df.copy()
    for col in out.columns:
        lower = col.lower()
        if lower == "year":
            out[col] = pd.array(out[col], dtype="Int64").astype(str)
        elif lower in _COUNT_NAMES or lower.endswith("_count") or lower.endswith("_n"):
            out[col] = pd.array(out[col], dtype="Int64")
        elif any(t in lower for t in _PCT_TOKENS):
            values = out[col]
            if pd.api.types.is_numeric_dtype(values):
                if pd.notna(values.max(skipna=True)) and values.max(skipna=True) <= 1.0:
                    values = values * 100
                out[col] = values.map(lambda x: f"{x:.2f}%" if pd.notna(x) else "—")
        elif pd.api.types.is_float_dtype(out[col]):
            out[col] = out[col].map(lambda x: f"{x:.3f}" if pd.notna(x) else "—")
    return out

def display_analysis_table(df):
    display(format_analysis_table(df).style.hide(axis="index"))
```

Axis formatters (matplotlib ticker on seaborn axes):

```python
# Percent axis
ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1.0, decimals=2))

# Year axis
ax.xaxis.set_major_locator(mtick.MaxNLocator(integer=True))
ax.xaxis.set_major_formatter(mtick.FormatStrFormatter("%d"))
```

---

## Rendering QA Checklist

Before handing off an analysis notebook, verify:

- [ ] Notebook executed from a clean kernel.
- [ ] Quarto render succeeded and HTML is at `_doc_/<slug>.html`.
- [ ] No forbidden placeholder text remains (`futtatás után kitöltendő`, etc.).
- [ ] Every table cell has `#| label: tbl-...` and `#| tbl-cap: ...`.
- [ ] Every plot cell has `#| label: fig-...` and `#| fig-cap: ...`.
- [ ] Multi-plot cells use `fig-subcap` or are split into separate labelled cells.
- [ ] Markdown before each output explains purpose, method, and interpretation.
- [ ] Every report table cell ends with `display_analysis_table(df)` — never a bare variable, `display(df)`, `df.head()`, or raw `df.style`.
- [ ] No pandas index column visible in rendered HTML tables.
- [ ] Numeric formatting follows project conventions (ratio/rate → `22.33%`, floats → `1.234`, counts → `0` decimals).
- [ ] Headings and captions are not manually numbered.
