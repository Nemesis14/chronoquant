---
id: t57
epic: epic_036
title: UI data.py strategy display fix
assignee: ui_agent
status: pr
blocks: [t58]
blocked_by: [t56]
---

## Context

`src/ui/data.py` két helyen olvasta az artifact-ot elavult kulcsokkal.
Mindkettő javítva az új `strategy_artifact.json` formátumhoz.

## Changes

### `src/ui/data.py`

1. `load_long_short_strategies()` — régi `long_entry_pct` / `short_entry_pct` / `rearm_pct`
   kulcsok helyett `entry_cutoff` (egyforma küszöb long és short irányhoz):
   ```python
   entry_cutoff = params.get("entry_cutoff")
   long_cfg     = {"entry_cutoff": entry_cutoff}
   short_cfg    = {"entry_cutoff": entry_cutoff}
   ```

2. `backtest_summary()` — `summary.json` helyett `strategy_artifact.json → metrics` blokkból olvas.

3. `load_strategy_artifact()` — új helper, a teljes artifact-ot adja vissza (graceful empty dict ha nem elérhető).

### `src/ui/main.py`

`render_asset_chart()` — `prediction_price_figure()` hívásban `entry_pct` / `rearm_pct`
helyett `entry_cutoff`; `rearm_threshold` és `short_rearm_threshold` → `None`
(az új stratégia nem használ rearm küszöböt).

## Validation

- `ruff check src/ui/ --fix` — All checks passed
- `uv run pyright src/ui/` — 0 errors, 0 warnings, 0 informations

## Notes

Aktív artifact (`strat_solusdt_fw60_combo_2101_2605/strategy_artifact.json`):
- `decision_params.entry_cutoff = 0.97`
- `metrics.win_rate = 0.6332`, `metrics.compounded_return_pct = 49.2641`
