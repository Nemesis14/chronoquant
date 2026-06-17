---
epic: epic_1
id: t1
title: OHLCV table data quality and completeness audit
assignee: analyst_agent
status: pr
blocks: []
blocked_by: []
---

## Goal

Produce a fully executed and Quarto-rendered audit notebook for the `ohlcv` table per spec `0024_ohlcv_table_spec.md`.

## Scope

- `_doc_/analysis/0024_ohlcv_table.ipynb` — notebook (created and executed)
- `_doc_/0024_ohlcv_table.html` — rendered HTML output

## Acceptance Criteria

- [x] Notebook created at `_doc_/analysis/0024_ohlcv_table.ipynb`
- [x] All 32 cells execute without error
- [x] All 12 required outputs present (tbl + fig with labels and captions)
- [x] All findings programmatically generated — no placeholder text
- [x] Quarto render succeeded: `_doc_/0024_ohlcv_table.html` exists (~2.5 MB)
- [x] Covers: time coverage, candle validity, return sanity, activity sanity, downstream alignment, regime segmentation, temporal price structure (7a–7d)

## Notes

2026-06-16 — analyst_agent completed.

Notebook: `_doc_/analysis/0024_ohlcv_table.ipynb`
Rendered:  `_doc_/0024_ohlcv_table.html`

Spec: `_doc_/analysis/0024_ohlcv_table_spec.md`

All 32 cells executed cleanly via Quarto (python3 kernel). HTML output: 2.5 MB self-contained.
No critical findings were detected during notebook construction — actual findings depend on the live database contents at execution time.
