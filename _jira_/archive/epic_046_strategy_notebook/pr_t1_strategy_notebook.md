# pr_t1 — Strategy Notebook: 04_strategy.ipynb

## Metadata
- epic: 046
- task: t1
- assignee: analyst_agent
- status: pr
- blocked_by: []
- blocks: []

## Description
Create `artifacts/lgbm_solusdt_l_fw60_2101_2605/analysis/04_strategy.ipynb` —
a Quarto-rendered notebook for strategy signal visualization.

## Deliverables
- `artifacts/lgbm_solusdt_l_fw60_2101_2605/analysis/04_strategy.ipynb` — 8-cell notebook
- `artifacts/lgbm_solusdt_l_fw60_2101_2605/04_strategy.html` — self-contained HTML (2.8 MB)

## Implementation Notes
- Matplotlib-only (seaborn whitegrid), no Plotly
- Long signals only (lgbm_solusdt_l_fw60_2101_2605)
- Validation period charts: 2025-06-09 to 2025-06-15 (Bootstrap 5 nav-tabs, base64 PNG)
- Outcome-colored dots on triggers: green (TP hit), yellow (timeout+profit), red (timeout+loss)
  - TP_LOG = 0.012363 (bucket_median_mfe at score_pct >= 0.98)
  - MAX_HOLD = 60 bars
- Weekly trade summary table (from 2025-05-01): plain HTML table via display(HTML(...))
  - Columns: Week, Trades, Green (TP), Yellow (TO+), Red (TO-), TP Rate
  - Color-coded counts (inline style, no pandas Styler caption to avoid Quarto Lua bug)
- YAML: number-sections removed to avoid Quarto 1.4 cross-ref Lua filter crash on table HTML
- Duplicate cells dd07bdc3 and 6e47891b removed (artifacts of prior session insertions)
