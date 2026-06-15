# Analyst Skill

Shared execution guide for producing analysis notebooks. Used by `analyst_agent`.

---

## Notebook Structure (required sections in order)

Every `analysis_<slug>.ipynb` must contain these sections:

```
# Analysis: <Title>
## Summary          ← key findings up front, 3–5 bullet points
## Setup            ← imports, DB connection, config loading
## <Section 1>      ← topic-specific analysis with code + narrative
## <Section N>      ← ...
## Conclusions      ← what was found, what it means, any required actions
```

Write the **Summary** section last (after the analysis), but place it first in the notebook.

---

## Code Style in Notebooks

- Use `duckdb.connect(db_path, read_only=True)` — never write to the DB
- Prefer `polars` for transformation, `pandas` for display/plotting
- Chart library: `plotly.express` (interactive) or `matplotlib` (static)
- Every code cell that produces a table: end with `df.head(20)` or `.to_pandas()`
- Every code cell that produces a chart: call `fig.show()` or `plt.show()`
- No bare `print()` for results — use display or chart output

---

## Finding the DB Path

```python
import sys
sys.path.insert(0, "../../src")   # adjust depth from _doc_/<topic>/
import utils
cfg     = utils.load_asset_config()
db_path = cfg["database"]["db_path"]
```

---

## Methodology Markers

Each checklist item from the agent manifest gets its own subsection.
Start each with a one-line **Goal** statement, then code, then a **Finding** callout:

```markdown
**Finding:** ✅ No violations — all 2,847,201 rows have available_ts ≤ open_time.
```
or
```markdown
**Finding:** ⚠️ 142 rows where available_ts > open_time. See table below.
```

---

## Output Location

Notebooks go in `_doc_/<topic>/analysis_<slug>.ipynb`.

If the topic directory does not exist, create it. Topic naming mirrors `src/` module names:
- `_doc_/store/` — DB integrity, stats
- `_doc_/modeling/` — CV structure, sample independence, feature null patterns
- `_doc_/data_pipeline/` — leakage, sync correctness

---

## Flagging Issues

If the analysis finds a critical issue (leakage, broken CV, wrong null count):
1. Document it in the **Conclusions** section
2. Append to the triggering jira ticket's `## Notes`:
   ```
   [analyst] Critical finding — <date>
   Issue: <one-line description>
   Notebook: _doc_/<topic>/analysis_<slug>.ipynb
   ```
3. Create a `todo_` ticket for the responsible agent if a fix is required
