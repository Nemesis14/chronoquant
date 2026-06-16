# Analyst Skill

Shared execution guide for producing Quarto-renderable analysis notebooks. Used by `analyst_agent`.

---

## Required Notebook Contract

Every `analysis_<spec_slug>.ipynb` must be executable, rendered, and presentation-quality.

Required sections in order:

```markdown
# Analysis: <Title>
## Summary
## Setup
## <Check 1>
## <Check N>
```

Do not create a separate `Conclusion` or `Conclusions` section. Findings belong directly next to the check that produced them.

Write the `Summary` section last, after the notebook has run, but place it first in the notebook. It must contain real findings, not placeholders.

---

## Mandatory Markdown Before Code

Every code cell that produces a table, figure, metric summary, or diagnostic output must be preceded by a markdown cell that answers these questions:

1. **Purpose:** What does this table/plot/check measure?
2. **Method:** Which columns, model artifacts, time windows, or thresholds are used?
3. **Interpretation:** What result is healthy, suspicious, or failing?
4. **Action rule:** What should happen if the check fails?

Template:

```markdown
### Embargo gap by fold

This table checks whether each validation fold starts far enough after the end of its training fold. The minimum acceptable gap is the forward target window, currently 60 bars for `fw60` targets.

Interpretation: each `gap_bars` value must be at least 60. Values below 60 indicate potential target leakage because validation labels may overlap the training horizon. If any fold fails, the CV splitter or dataset materialization must be reviewed before model performance is trusted.
```

---

## Forbidden Placeholder Text

Never write placeholders such as:

- `futtatás után kitöltendő`
- `to be filled after running`
- `TODO after execution`
- `interpret after running`
- `pending result`

If the exact finding is unknown before execution, write code that generates it. Use `IPython.display.Markdown` for computed findings.

Example:

```python
from IPython.display import Markdown, display

failed = int((fold_gaps["gap_bars"] < 60).sum())
if failed == 0:
    display(Markdown("**Finding:** ✅ All folds satisfy the 60-bar embargo rule."))
else:
    display(Markdown(f"**Finding:** ⚠️ {failed} folds violate the 60-bar embargo rule; model validation may be leaked."))
```

---

## Quarto Cell Labels and Captions

Do not put manual titles inside plots or tables when Quarto can provide numbering and captions.

For figures produced by Python code cells:

```python
#| label: fig-embargo-gap-by-fold
#| fig-cap: "Embargo gap by validation fold"
#| fig-alt: "Bar chart showing embargo gap in bars for each validation fold."

ax = fold_gaps.plot.bar(x="fold", y="gap_bars", legend=False)
ax.set_xlabel("Fold")
ax.set_ylabel("Gap (bars)")
ax.set_title("")
plt.show()
```

For tables produced by Python code cells:

```python
#| label: tbl-class-balance-by-year
#| tbl-cap: "Target class balance by year"

class_balance_display
```

Labels must be lower-case, kebab-case, and prefixed correctly:

- figures: `fig-...`
- tables: `tbl-...`

For multiple plots from one code cell, use subcaptions:

```python
#| label: fig-null-pattern-panel
#| fig-cap: "Windowed feature null-pattern diagnostics"
#| fig-subcap:
#|   - "Leading null count by feature"
#|   - "Null-rate distribution by feature family"
#| layout-ncol: 2

fig1.show()
fig2.show()
```

For subtables in markdown, use a `tbl-` panel div when needed:

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

## Quarto Rendering Requirement

After writing or updating a notebook:

1. Execute all cells from a clean kernel.
2. Render with Quarto.
3. Verify the rendered output exists.
4. Search the notebook and rendered output for forbidden placeholders.
5. Verify all result-producing cells have Quarto labels/captions.

Recommended command:

```bash
quarto render _doc/<topic>/analysis_<spec_slug>.ipynb --execute
```

If rendering fails, the task is not complete. Fix the notebook until it renders.

---

## Numeric Formatting Conventions

Apply these conventions consistently in displayed tables and plot axes:

- `year`: display as a full integer year, e.g. `2024`, never `2,024` and never `2024.0`.
- `count`, `n`, `row_count`, `count(*)`: integer with zero decimals.
- rates, proportions, shares, positive rates, violation rates: display as percentages with exactly two decimals, e.g. `23.24%`.
- monetary or price values: use context-appropriate precision, but do not use thousands separators for years.

Python helpers:

```python
def fmt_year(s):
    return s.astype("Int64").astype(str)


def fmt_count(s):
    return s.round(0).astype("Int64")


def fmt_pct(s):
    return (s * 100).map(lambda x: f"{x:.2f}%")
```

For matplotlib percentage axes:

```python
import matplotlib.ticker as mtick
ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1.0, decimals=2))
```

For year axes:

```python
import matplotlib.ticker as mtick
ax.xaxis.set_major_locator(mtick.MaxNLocator(integer=True))
ax.xaxis.set_major_formatter(mtick.FormatStrFormatter("%d"))
```

---

## Code Style in Notebooks

- Use `duckdb.connect(db_path, read_only=True)` — never write to the DB.
- Prefer `polars` for transformation and `pandas` for display/plotting.
- Chart library: `matplotlib` for static Quarto-friendly output; `plotly.express` only when interactivity is useful in HTML.
- Every table code cell must end with a displayed dataframe or a styled dataframe.
- Every chart code cell must call `fig.show()` or `plt.show()`.
- No bare `print()` for analytical results — use displayed tables, charts, or generated Markdown findings.
- Avoid hard-coded manual numbering in headings, captions, or labels. Let Quarto number figures and tables.

---

## Finding Pattern

Each check should close with a local finding, not a global conclusion section.

```markdown
**Finding:** ✅ No leakage detected. All feature availability timestamps are less than or equal to `open_time`.
```

or generated by code:

```python
display(Markdown(f"**Finding:** ⚠️ {violations:,} rows have `available_ts > open_time`."))
```

---

## Output Location

Notebooks go in `_doc_/<topic>/analysis_<spec_slug>.ipynb`.
Rendered HTML goes next to the notebook unless the project-level Quarto config routes outputs elsewhere.

Topic naming mirrors `src/` module names:

- `_doc_/store/` — DB integrity, stats
- `_doc_/modeling/` — CV structure, sample independence, feature null patterns
- `_doc_/data_pipeline/` — leakage, sync correctness
- `_doc_/analysis/` — cross-cutting audits spanning multiple modules

---

## Flagging Issues

If the analysis finds a critical issue such as leakage, broken CV, or wrong null count:

1. Document it in the local check's finding.
2. Add it to the Summary.
3. Append to the triggering jira/spec notes:

```markdown
[analyst] Critical finding — <YYYY-MM-DD>
Issue: <one-line description>
Notebook: _doc_/<topic>/analysis_<spec_slug>.ipynb
Rendered: _doc_/<topic>/analysis_<spec_slug>.html
```

4. Create a `todo_` ticket for the responsible agent if a fix is required.
