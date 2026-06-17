---
epic: epic_5_analysis_table_formatting
id: t1
title: Add display_analysis_table helper and enforce consistent table display in notebooks
assignee: analyst_agent
status: pr
---

## Goal
Eliminate raw pandas DataFrame expressions in analysis notebook report cells and
enforce consistent numeric formatting (index hidden, pct columns, float precision,
integer count columns).

## Scope
- `_doc_/analysis/src/table_formatting.py` — new shared helper module
- `.agent/skills/analysis_presentation_skill.md` — updated format helpers, QA checklist
- `.agent/skills/analyst_skill.md` — added mandatory "Displayed table rules" section
- `_doc_/analysis/0024_ohlcv_table.ipynb` — 12 table cells replaced with `display_analysis_table(df)`

## Acceptance Criteria
- [x] `format_analysis_table` handles: `year` → str, count/trades/volume → Int64, rate/pct/ratio → `xx.xx%`, floats → 3 decimals
- [x] `display_analysis_table` hides pandas index via `.style.hide(axis="index")`
- [x] `analysis_presentation_skill.md` references shared module import pattern
- [x] `analyst_skill.md` has explicit "Displayed table rules" prohibiting bare df expressions
- [x] All 12 table cells in `0024_ohlcv_table.ipynb` end with `display_analysis_table(df)`
- [x] No other analysis notebooks have bare df display patterns

## Notes
- Other notebooks in `_doc_/analysis/` had no bare df cell endings — only 0024 required changes.
- Cells with already-string-formatted values (stats_df, top_returns, sb_display) are safe to
  pass through display_analysis_table — string columns are left untouched.
- Import path assumes notebook kernel CWD = `_doc_/analysis/` (consistent with DB_PATH `../../database/`).
