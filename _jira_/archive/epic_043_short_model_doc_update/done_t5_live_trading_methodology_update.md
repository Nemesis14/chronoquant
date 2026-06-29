---
epic: epic_043
id: t5
title: Live trading methodology frissítés — config split, service változás (7100)
assignee: methodology_agent
status: pr
blocks: []
blocked_by: [t4]
---

## Goal

`_doc_/methodology_doc/7100_live_trading.md` frissítése a dual-strategy architektúra bevezetésével.

## Scope

- `_doc_/methodology_doc/7100_live_trading.md`

## Változások

- `config/trading.json`: `strategy_session_id` → `strategy_session_long_id` + `strategy_session_short_id`
- `strategy.evaluate()`: két külön cutoff paraméter (`entry_cutoff_long`, `entry_cutoff_short`)
- `service.py`: két artifact betöltés, két rank lookup, külön cutoff átadás

## Acceptance Criteria

- [x] Config struktúra frissítve (új kulcsok megnevezve)
- [x] `evaluate()` szignatúra változás dokumentálva
- [x] Két rank lookup (long/short session-ből) magyarázva

## Notes

2026-06-28 — methodology_agent végrehajtotta.

Változtatások a `7100_live_trading.md`-ben:

- **Overview flowchart frissítve**: PCT és DEC node-ok jelzik a 2x lookup-ot és az irány-specifikus cutoff-okat
- **Új "Dual-Strategy Architektúra" szekció** az Overview után:
  - Bevezető bekezdés: miért két session (long és short signal erőssége eltér)
  - Mermaid flowchart: config → session ID-k → artifactok → cutoff-ok → evaluate()
  - Config struktúra alszekció: régi vs. új kulcsok táblázata, backward-compat megjegyzés
  - `strategy.evaluate()` szignatúra alszekció: Python kódrészlet + belső logika (fallback _base_cutoff)
  - Két rank lookup alszekció: Mermaid flowchart long/short lookup → np.interp pipeline
- **Paraméter táblázat frissítve**: `strategy_session_id` lecserélve `strategy_session_long_id` + `strategy_session_short_id`-ra; `entry_cutoff_long` (0.98) és `entry_cutoff_short` (0.94) új sorokként hozzáadva
- **Kockázat táblázat bővítve**: két új sor (session verzió eltérés, mindkét signal egyidejű aktiválása)
- **Validációs checklist frissítve**: 8 pont, dual-session specifikus ellenőrzési pontokkal
- **Fő runtime szerződés tábla bővítve**: long/short rank lookup, `entry_cutoff_long`, `entry_cutoff_short`, max_hold_minutes külön sorokként
