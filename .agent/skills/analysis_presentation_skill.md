# Analysis Presentation Skill

Visual, table, caption, and rendering rules for ChronoQuant analysis notebooks.
Applies Urban Institute-style data communication principles to Quarto-rendered notebooks.

Quarto config and label syntax → see `quarto_analysis_defaults.md`.
Notebook structure and workflow → see `analyst_skill.md`.

---

## Design Goals

Analysis notebooks must be readable as standalone reports after Quarto rendering.
A reader must understand every table and figure without opening the code.

Each result-producing code cell must have:

1. A preceding markdown explanation: purpose, source, method, interpretation.
2. A Quarto label and caption.
3. Consistent numeric formatting.
4. A nearby finding that interprets the result.

The notebook as a whole must also end with a short decision-oriented interpretation
that answers the user's actual analytical question.

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

Color meaning:

- main measured series: blue
- comparison or benchmark series: gray or black
- secondary comparison series: yellow or orange
- warnings, failures, problematic segments: red
- contextual shading, bands, neutral overlays: gray_light

Use color consistently across the notebook. Do not assign different semantics to the
same color in neighboring charts.

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
CQ_SEQUENCE = [
    CQ_COLORS["blue"],
    CQ_COLORS["yellow"],
    CQ_COLORS["orange"],
    CQ_COLORS["gray"],
    CQ_COLORS["red"],
]

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

- seaborn is the primary charting library; matplotlib is the fallback and customization layer.
- Plotly is forbidden by default.
- Every plot must have a corresponding source dataframe or summary table.
- Findings must derive from computed data, not only from visual inspection.
- Do not use `ax.set_title()`.
- Use clear axis labels with units or scale context.
- Remove top and right spines.
- Use light gridlines on the value axis.
- Use direct labels or compact legends.
- Explain thresholds and reference lines in the preceding markdown.
- Use red and orange only for genuine warnings, failures, or clearly highlighted segments.

---

## Analysis-Driven Chart Selection

Choose chart forms according to the analytical question, not convenience.

- **Időbeli alakulás:** line chart, rolling chart, éves overlay vagy faceted time series.
- **Éves összehasonlítás:** separate panels or aligned overlays with consistent scales.
- **Eloszlás:** histogram, KDE, box, violin, ECDF, quantile range, or density comparison.
- **Model performance:** train-valid metrics table plus calibration or prediction-vs-target views.
- **Seasonality:** monthly or quarterly aggregation with aligned panels or heatmap.
- **Split periods:** visually mark train, valid, OOS or regime periods with shading, bands, or explicit legends when relevant.

If the user asks whether years or periods are comparable, the presentation must make
those comparisons visually and numerically obvious.

---

## Temporal And Split Presentation Rules

- When a chart mixes multiple time segments, explicitly distinguish them with shading,
  facet panels, or stable color semantics.
- When train and valid periods matter, mark them on the time axis or separate them into panels.
- If yearly comparison is central, keep axes aligned across years.
- For time-ordered multi-panel layouts, prefer `layout-ncol: 1` unless side-by-side
  genuinely remains readable.
- If a csonka year is being compared to full years, say so explicitly in nearby text.

---

## Regression And Target Diagnostics

For continuous target analysis or regression model evaluation, prefer the following
best-practice views when the data supports them:

- train vs valid metric table: `RMSE`, `MAE`, `R²`, and sample counts;
- prediction vs fact scatter with ideal reference line;
- binned calibration style chart: average prediction vs average fact;
- residual summary table or residual distribution view;
- daily or periodic aggregation chart when regime following is important.

The purpose is not to maximize the number of charts, but to answer:

- is there signal;
- does it survive on valid;
- is the model calibrated or only rank-useful;
- are there unstable periods;
- can the same target behavior be assumed across years.

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
```

### Quarto panel layout

When a result naturally consists of multiple separate outputs, prefer Quarto panel
layout over manually squeezing everything into one subplot grid.

Use this for:

- train vs valid paired charts;
- before vs after comparisons;
- same metric on multiple splits or horizons;
- a compact pair such as scatter + density heatmap.

Supported executable-cell layout options:

- `layout-ncol: 2` for equal-width side-by-side panels;
- `layout-nrow: 2` for stacked outputs;
- `layout: "[[1,1],[1]]"` or similar for asymmetric custom panels;
- `layout-align` and `layout-valign` when vertical or horizontal alignment matters;
- `fig-subcap` to caption each panel separately under one shared figure caption.

Pattern:

```python
#| label: fig-train-valid-panel
#| fig-cap: "Train-valid comparison"
#| fig-subcap:
#|   - "Train sample"
#|   - "Validation sample"
#| layout-ncol: 2

fig, ax = plt.subplots()
...
plt.show()

fig, ax = plt.subplots()
...
plt.show()
```

If the outputs share axes and are only readable together, one matplotlib subplot
figure is still acceptable. If each panel needs its own legend, scale treatment,
or dense annotation, Quarto panels are usually cleaner.

For more complex layouts, Quarto also supports custom `layout` arrays such as
`[[70,30],[100]]` and negative spacer columns. Source: official Quarto Figures
documentation (`authoring/figures`, subfigures, figure panels, custom layouts)
and Jupyter cell layout reference (`reference/cells/cells-jupyter`).

---

## Numeric Formatting

Apply consistently in all displayed tables and plot axes.

| Column type | Display format |
|---|---|
| `year` | Full integer, e.g. `2024` |
| `count`, `count(*)`, `n`, `row_count`, `rows`, `trades`, `volume`, `violations`, `*_count`, `*_n` | Integer, zero decimals |
| `rate`, `ratio`, `share`, `pct`, `percent` | Percent string, exactly 2 decimals; auto-scaled ×100 if values ≤ 1 |
| all other float columns | 3 decimal string |

Shared helper module:

```python
import sys
sys.path.insert(0, str(_root))
from analyst.table_formatting import format_analysis_table, display_analysis_table
```

---

## Interpretation Rules

- Every major chart or table must have a nearby interpretation.
- Interpretations must be concrete: mention direction, magnitude, stability, and implication.
- Do not stop at "visible difference" or "there is correlation"; quantify it if the data allows.
- The final interpretation must answer the user's decision question, not only summarize outputs.
- If the results are mixed, say what is strong and what is weak.
- If the valid view is weaker than train, say whether the degradation is acceptable.

Good interpretation answers questions like:

- mennyire más évente a target;
- elég egységesek-e az évek a modellhez;
- a csonka aktuális év mely korábbi évekre hasonlít;
- van-e olyan év vagy rezsim, amit érdemes kizárni;
- a modell inkább rangsorolásra vagy abszolút becslésre alkalmas.

---

## Rendering QA Checklist

Before handing off an analysis notebook, verify:

- [ ] Notebook executed from a clean kernel.
- [ ] Quarto render succeeded and HTML is at `_doc_/<slug>.html`.
- [ ] No forbidden placeholder text remains.
- [ ] Every table cell has `#| label: tbl-...` and `#| tbl-cap: ...`.
- [ ] Every plot cell has `#| label: fig-...` and `#| fig-cap: ...`.
- [ ] Markdown before each output explains purpose, source, method, and interpretation.
- [ ] Every report table cell ends with `display_analysis_table(df)`.
- [ ] No pandas index column is visible in rendered HTML tables.
- [ ] Numeric formatting follows project conventions.
- [ ] Train-valid, in-sample/OOS, or yearly comparison is visually explicit when relevant.
- [ ] The final notebook includes a decision-oriented interpretation based on the executed results.
