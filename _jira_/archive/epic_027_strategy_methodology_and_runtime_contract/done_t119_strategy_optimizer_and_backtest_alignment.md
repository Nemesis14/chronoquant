---
epic: epic_027
id: t119
title: Igazitsd a strategy optimizert a runtime logikahoz
assignee: modeling_agent
status: done
blocks: [t120, t121]
blocked_by: [t117, t118]
---

## Goal

Az offline optimizer és backtest pontosan ugyanarra a signal- és state-machine contractra
optimalizáljon, amit később a live trading alkalmaz.

## Scope

- `src/strategy/strategy/optimize.py`
- objective definíció
- conflict rule
- exit / cooldown / rearm alignment

## Acceptance Criteria

- [x] A backtest és a live decision contract azonos szabályokon alapul
- [x] Az objective nem csak score-szummára, hanem használható strategy quality proxyra épül
- [x] Az optimizer ugyanazon strategy session időablakon dolgozik, mint a calibration
- [x] A riportok explicit `same_window` módként jelölik ezt a futást
- [x] A strategy report metrikák neve és szemantikája megfelel a tényleges számításnak

## Notes

### Implementált változások

**`src/strategy/strategy/optimize.py`** — teljes átírás rank-first logikára:
- `_simulate_strategy()`: új paraméterek: `long_entry_pct`, `short_entry_pct`, `min_edge_gap`, `max_hold_minutes`, `cooldown_minutes`, `min_hold_minutes`, `rearm_pct`. Bemeneti oszlopok: `score_pct_long`, `score_pct_short`, `bucket_mean_mfe_long`, `bucket_mean_mfe_short`. Conflict resolution: `highest_edge`. Trade record: `score_pct_at_entry`, `bucket_mean_mfe`, `n_bars`.
- `_objective()`: új sweep range-ek (0.70-0.99 percentile), objective = mean `bucket_mean_mfe` at entry.
- `_compute_metrics()`: `bucket_mean_mfe` alapú számítás (`total_return`, `win_rate`).
- `optimize_strategy()`: új szignatúra — `long_model_id`, `short_model_id`, `start`, `end` paraméterek. `read_strategy_artifact` hívás eltávolítva. `write_strategy_artifact` az új `fit_period` + `decision_params` (conflict_rule: "highest_edge") szignatúrával hívva.

**`src/strategy/02_optimize_strategy.py`** — CLI frissítve:
- `--eval-start`/`--eval-end` → `--start`/`--end`
- Új required paraméterek: `--long-model`, `--short-model`
- Print output: új paraméterneveket mutatja

**`src/strategy/tests/strategy/smoke/test_optimize.py`** — frissítve:
- Szintetikus DataFrame rank-first oszlopokkal (`score_pct_*`, `bucket_mean_mfe_*`)
- `_simulate_strategy` új szignatúrával tesztelve
- `optimize_strategy` integrációs smoke test: ellenőrzi `signal_mode: "rank_first"`, `evaluation_mode: "same_window"`, `conflict_rule: "highest_edge"` mezőket

### Validáció
- ruff check: 0 hiba (mindkét fájl)
- pyright: 0 errors, 0 warnings
- pytest: 4/4 passed (1.71s)

### Validator (t121) — 2026-06-20

- ruff check src/strategy/: 1 auto-fixed, 0 remaining
- pyright src/strategy/strategy/: 0 errors, 0 warnings
- pytest src/strategy/tests/: 8/8 passed
- status: done
