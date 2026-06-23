---
id: t52
title: artifact contract frissítése artifacts.py + search.py
epic: epic_035_strategy_execution_search
assignee: modeling_agent
status: pr
blocks: [t53]
blocked_by: [t50]
---

## Description

`write_strategy_artifact()` signature és az artifact JSON tartalom frissítése a grid-search kontraktushoz.

## Változások

### `write_strategy_artifact()` signature
`optuna_best: dict` paraméter → `search_info: dict` paraméter.

### Artifact JSON — új struktúra
**`decision_params`:**
```json
{
  "entry_cutoff": 0.95,
  "tp_spec": "bucket_mean_mfe",
  "sl_spec": "0.5x_tp",
  "directions": ["long", "short"],
  "max_hold_minutes": 60,
  "same_bar_conflict_rule": "sl_first"
}
```

**`search_info`** (új kulcs, `optuna_best_trial` helyett):
```json
{
  "search_type": "grid",
  "n_setups_evaluated": 200,
  "best_objective": "total_fact_log_return",
  "best_value": 0.342
}
```

**Új mezők:**
- `summary_table`: `strat."<session>__summary"` ref
- `grid_results_table`: `strat."<session>__grid_results"` ref

**Törölt mező:** `optuna_best_trial`

### `register_strategy()` frissítése
A strat loop kibővítve: `summary` és `grid_results` kind is regisztrálódik `reg.artifacts`-ban.

### `read_strategy_artifact()` docstring frissítve
Az új kulcsok (`search_info`, `summary_table`, `grid_results_table`) dokumentálva.

## Notes

Implementálva. 23/23 pytest zöld. ruff + pyright 0 error.
