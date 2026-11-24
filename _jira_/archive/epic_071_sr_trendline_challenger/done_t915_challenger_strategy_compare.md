---
epic: epic_071
id: t915
title: Kalibrálj stratégiát a challengerre és hasonlítsd a championhoz költségekkel
assignee: modeling_agent
status: done
blocks: []
blocked_by: [t914]
---

## Goal
`00_run_strategy_session.py` a challenger párra (ugyanaz a grid, ugyanaz a költségmodell), összehasonlító riport
champion vs challenger: n_trades, net total/avg fact_log_return, win rate, equity görbe. Döntési javaslat (cutover vagy nem) —
deploy **nem** része ennek a tasknak.

## Acceptance Criteria
- [ ] Két strategy session (long/short) a challengerre, `strat.*` táblák + artifact
- [ ] Összehasonlító riport (report-formátum) a challenger `artifacts/<model_id>/reports/` alatt
- [ ] Döntési javaslat az epic.md Key Decisions-be

## Notes

- [orchestrátor 2026-11-19] **CANCELLED — nem futott.** A t913 placebo-tanulmány 0/36 GO; a user döntése (2026-11-19): (A) megállás, az epic negatív eredménnyel zárul. Lásd epic.md Final Conclusion és `_doc_/research/sr_placebo_study.md`.
