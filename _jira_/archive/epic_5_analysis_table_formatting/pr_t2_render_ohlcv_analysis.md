---
epic: epic_5_analysis_table_formatting
id: t2
title: Execute and render OHLCV table analysis notebook
assignee: analyst_agent
status: pr
---

## Goal
Execute `_doc_/analysis/0024_ohlcv_table.ipynb` from a clean kernel via Quarto render,
producing the final HTML analysis report.

## Scope
- `_doc_/analysis/0024_ohlcv_table.ipynb` — executed (32 cells)
- `_doc_/0024_ohlcv_table.html` — rendered output (2.6 MB)

## Acceptance Criteria
- [x] All 32 cells executed successfully from clean kernel
- [x] HTML output at `_doc_/0024_ohlcv_table.html`
- [x] No forbidden placeholder text in rendered HTML
- [x] All figures and tables carry Quarto `#| label:` tags and auto-numbered captions

## Notes
Re-rendered 2026-06-16 (latest). Quarto 1.4.551. Fresh kernel, full re-execution. All 32 cells passed.

Fix: pandas Styler generates `<table id="T_uuid">` without `class="dataframe"`, so CSS
`table.dataframe` selectors never matched. Borders and right-alignment now injected via
`set_table_styles` in `display_analysis_table` — applies regardless of class name.

Key findings from execution:
- Check 1 (Time coverage): 3,022,861 rows, 0 missing minutes, 0 duplicates, 0 gaps > 1 min — CLEAN
- Check 2 (Candle validity): 0 violations across all 16 geometry/price checks — CLEAN
- Check 3 (Return sanity): max |log_ret| = 0.185 (2023-01-02), 10 bars > 10% — known market events
- Check 4 (Activity): 382 zero-volume bars, avg taker buy ratio 0.4935 — within normal range
- Check 5 (Downstream alignment): all 4 tables fully aligned, 0 orphan rows — CLEAN
- Check 6 (Regime): 7 years 2020–2026, volume 3x growth, avg trades 52→2027/bar
- Check 7 (Volatility): rolling vol 26%–395% annualized, no year exceeds 2x dataset-wide std

Overall status: ✅ All checks passed — no critical issues detected.
