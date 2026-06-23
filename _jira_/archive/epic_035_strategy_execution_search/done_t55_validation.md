---
id: t55
title: végső validáció és konzisztencia-ellenőrzés
epic: epic_035_strategy_execution_search
assignee: validator_agent
status: todo
blocks: []
blocked_by: [t53]
---

## Description

A `pr_` státuszú t49–t54 ticketek validálása:
- ruff check
- pyright
- pytest
- konzisztencia-ellenőrzés

## Validálandó

```bash
uv run ruff check src/strategy/ --fix
uv run pyright src/strategy/
uv run pytest src/strategy/tests/ -v
```

## Konzisztencia-ellenőrzés

1. `strategy_artifact.json` contract: `decision_params` tartalmaz `entry_cutoff`, `tp_spec`, `sl_spec`, `directions`, `max_hold_minutes`, `same_bar_conflict_rule`
2. `decision_params`-ban NEM szerepel `long_entry_pct`, `short_entry_pct`, `cooldown_minutes`, `rearm_pct`, `min_edge_gap` (régi Optuna mezők)
3. `optimize.py` nincs import-olva sehol (ha törölve lett)
4. `search.py` létezik és importálható
5. `strat.__summary` tábla az epics report contract mezőivel rendelkezik

## Done kritérium

Minden check zöld → `todo_t55` → `done_t55` rename.
