---
epic: epic_033
id: t37
title: strategy 6xxx kód-referencia (új)
assignee: code_doc_agent
status: done
blocks: [t39]
blocked_by: []
---

## Goal
A `src/strategy/` teljesen lefedetlen a database_and_code_doc zónában — kód-referencia írása.

## Scope
- `src/strategy/strategy/build_table.py, calibrate.py, optimize.py, artifacts.py, session_naming.py`
- `src/strategy/00_run_strategy_session.py`
- Számozás: 6xxx kód-ref

## Acceptance Criteria
- [x] Minden strategy .py-hoz kód-ref: Overview diagram + függvény-szintű leírás
- [x] Felfelé link a `../methodology_doc/6000_strategy.md`-re (és 6100, 6200)
- [x] Min. 2–3 Mermaid/fájl; `strat.*` DuckDB táblák + artifact-fájlok pontosan dokumentálva
- [x] Entry Gate: 6000/6100/6200 methodology doc létezik → nincs blokk, nincs todo szükséges

## Notes

### Végrehajtás (2026-06-22)

Létrehozott fájlok (`_doc_/database_and_code_doc/`):
- `6100_strategy_module.md` — modul-szintű overview + session_naming + CLI
- `6110_build_table.md` — build_table.py, DuckDB join struktúra
- `6120_calibrate.md` — calibrate.py, rank lookup + isotonic fitting
- `6130_optimize.md` — optimize.py, Optuna sweep + state machine
- `6140_artifacts.md` — artifacts.py, strat.* DuckDB táblák + JSON + registry

Entry Gate: `methodology_doc/6000_strategy.md`, `6100_strategy_calibration.md`,
`6200_strategy_optimization.md` mind létezik → nincs methodology todo szükséges.

Minden fájl tartalmaz: Overview flowchart, sequenceDiagram, funkció-szintű
paraméter tábla, return leírás. `strat.__trades/__equity/__cutoffs` sémák
teljesen dokumentálva a 6140-ben. Upward link megvan minden fájlban.
