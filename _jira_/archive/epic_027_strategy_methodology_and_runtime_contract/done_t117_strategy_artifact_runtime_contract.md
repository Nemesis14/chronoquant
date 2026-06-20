---
epic: epic_027
id: t117
title: Definiald a strategy artifact runtime contractot
assignee: modeling_agent
status: done
blocks: [t119, t120]
blocked_by: [t116]
---

## Goal

Véglegesíteni, hogy a `src/strategy/` milyen artifactokat ír ki, és ezekből pontosan mit
és hogyan tölt be a `src/trading/` runtime.

## Scope

- `src/strategy/strategy/artifacts.py`
- `artifacts/<session_id>/` struktúra
- `strategy_artifact.json` séma
- rank / decile lookup perzisztencia
- `same_window` evaluation mode jelölése

## Acceptance Criteria

- [x] A `strategy_artifact.json` világosan tartalmazza a runtime által szükséges mezőket
- [x] Az artifact explicit tartalmazza a `fit_period` és `evaluation_mode: "same_window"` mezőket
- [x] A rank/decilis lookup tárolási formája rögzített és implementálható
- [x] A long/short signal conflict rule explicit és géppel alkalmazható
- [x] A trading runtime bemeneti contractja dokumentált és stabil

## Notes

Kiindulás: `_doc_/6000_strategy.md`

### Implementáció (t117)

`src/strategy/strategy/artifacts.py` teljesen átírva a rank-first strategy contract alapján:

**Eltávolított régi mezők:**
- `calib_period`, `eval_period` — lecserélve `fit_period`-ra
- `entry_threshold_long`, `entry_threshold_short` — raw isotonic threshold, nem rank-based
- `conflict_priority: "long"` — hardcoded, lecserélve `decision_params.conflict_rule: "highest_edge"`-re
- `best_params` paraméter — lecserélve `decision_params`-ra

**Új JSON séma mezők:**
- `signal_mode: "rank_first"` — explicit rank-first metodológia jelölése
- `evaluation_mode: "same_window"` — a metrikák ugyanarra az ablakra vonatkoznak
- `fit_period: {"start": ..., "end": ...}` — az session ablak
- `rank_lookup_long_path`, `rank_lookup_short_path` — rank calibration lookup fájlok
- `decision_params` — optimizer által kiválasztott paraméterek (long_entry_pct, short_entry_pct, min_edge_gap, min_hold_minutes, max_hold_minutes, cooldown_minutes, rearm_pct, conflict_rule)

`src/strategy/tests/strategy/smoke/test_artifacts.py` frissítve az új sémára:
- `write_strategy_artifact` hívások az új szignatúra szerint
- `required_keys` lista az új JSON séma kulcsaival
- Explicit ellenőrzések: `signal_mode == "rank_first"`, `evaluation_mode == "same_window"`, `decision_params` összes mezője

**Validáció:** pyright 0 errors, ruff clean, 2/2 smoke test passed.

### Validator (t121) — 2026-06-20

- ruff check src/strategy/: 1 auto-fixed, 0 remaining
- pyright src/strategy/strategy/: 0 errors, 0 warnings
- pytest src/strategy/tests/: 8/8 passed
- status: done
