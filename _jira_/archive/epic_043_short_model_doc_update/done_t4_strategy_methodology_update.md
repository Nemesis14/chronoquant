---
epic: epic_043
id: t4
title: Strategy methodology frissítés — két session, külön cutoff (6300)
assignee: methodology_agent
status: pr
blocks: []
blocked_by: []
---

## Goal

`_doc_/methodology_doc/6300_strategy_grid_search.md` frissítése: a korábbi kombinált stratégia helyett két önálló session dokumentálása.

## Scope

- `_doc_/methodology_doc/6300_strategy_grid_search.md`

## Változások

- Régi: egyetlen `strategy_session_id`, shared `entry_cutoff`
- Új: `strat_solusdt_fw60_long_2101_2605` (cutoff=0.98) + `strat_solusdt_fw60_short_2101_2605` (cutoff=0.94)
- Motiváció: a long és short signal erőssége eltér → külön optimális cutoff

## Acceptance Criteria

- [x] Két session dokumentálva külön-külön (névvel, cutoff-fal, fő metrikákkal)
- [x] Miért szétválasztás: külön optimalizáció indoklása
- [x] Grid search paraméterek mindkét irányra megadva

## Notes

2026-06-28 — methodology_agent végrehajtotta.

Változtatások a `6300_strategy_grid_search.md`-ben:
- Bevezető bekezdés kibővítve: architekturális megjegyzés a kombinált session kivezetéséről
- Overview flowchart frissítve: két önálló artifact node (long + short session)
- Új "Dual-Session Architektúra" szekció hozzáadva a "## Overview" után:
  - Miért két session? (motiváció + Mermaid diagram)
  - Aktuális session-ek táblázata metrikákkal (cutoff, trade-szám, win rate, total_log_ret, compounded)
  - Grid search futtatási flowchart irányonként
- Paraméter táblázat: `session szétválasztás` sor hozzáadva az aktuális optimumokkal
- Validációs checklist: 3 új ellenőrzési pont a dual-session követelményekhez
