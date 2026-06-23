---
id: epic_036
title: Strategy Live Activation & UI Display
status: in_progress
assignee: orchestrator
created: 2026-06-22
---

## Goal

Az epic_035-ben elkészült grid search stratégiát (`strat_solusdt_fw60_combo_2101_2605`)
beköti az éles trading service-be és az UI-ba. Dokumentálja a módszertant.

**Triggering change:** epic_035 után a `strategy_artifact.json` `decision_params`
formátuma megváltozott (nincs `long_entry_pct` / `short_entry_pct` / `rearm_pct` / `min_edge_gap`
— helyette `entry_cutoff`, `tp_spec`, `sl_spec`). Ez a trading service-t és az UI-t törte.

## Scope

- Trading service strategy evaluator: new entry_cutoff logic + inverted short signal
- UI data.py: artifact metrics olvasás fix
- UI strategy info kártya: kulcsmetrikák megjelenítése
- Metodológia dokumentáció: execution-aware grid search

## Out of scope

- Intrabar TP/SL monitoring a live service-ben (külön epic)
- Bracket order placement (exchange-szintű TP/SL)

## Tasks

| ID | Title | Assignee | Status | Blocks |
|----|-------|----------|--------|--------|
| t56 | Trading service strategy evaluator refactor | ui_agent | todo | t57 |
| t57 | UI data.py strategy display fix | ui_agent | todo | t58 |
| t58 | UI strategy info kártya | ui_agent | todo | t60 |
| t59 | Metodológia dokumentáció | methodology_agent | todo | t60 |
| t60 | Validáció | validator_agent | todo | — |
