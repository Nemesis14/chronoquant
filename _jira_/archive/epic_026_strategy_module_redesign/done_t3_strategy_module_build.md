---
epic: epic_026
id: t3
title: src/strategy/ modul létrehozása
assignee: modeling_agent
status: pr
blocks: [t5]
blocked_by: [t2]
---

## Goal

Létrehozni az `src/strategy/` modult, amely a strategy table build → score calibration →
entry/exit optimalizálás teljes folyamatát kezeli.
A t2 methodology doc alapján implementálni.

## Scope

```
src/strategy/
  __init__.py
  00_build_strategy_table.py    CLI wrapper
  01_calibrate_scores.py        CLI wrapper
  02_optimize_strategy.py       CLI wrapper
  strategy/
    __init__.py
    build_table.py              quant_train → strategy table parquet
    calibrate.py                isotonic regression fit + apply
    optimize.py                 Optuna sweep, entry/exit/cooldown
    artifacts.py                strategy_artifact.json írás/olvasás
```

## CLI Interface

```bash
# 1. Strategy table build
uv run python src/strategy/00_build_strategy_table.py \
  --long-model lgbm_solusdt_l_fw60_2101_2605 \
  --short-model lgbm_solusdt_s_fw60_2101_2605 \
  --start 2025-10-01 --end 2026-05-31 \
  --session-id strategy_2101_2605_202605

# 2. Score calibration (isotonic regression)
uv run python src/strategy/01_calibrate_scores.py \
  --session-id strategy_2101_2605_202605 \
  --calib-start 2025-10-01 --calib-end 2026-02-28

# 3. Strategy optimization
uv run python src/strategy/02_optimize_strategy.py \
  --session-id strategy_2101_2605_202605 \
  --eval-start 2026-03-01 --eval-end 2026-05-31
```

## Artifact directory

`artifacts/strategy_{long_model}_{short_model}_{date}/`

Tartalom:
- `strategy_table.parquet` — open_time | pred_long_raw | pred_short_raw | pred_long_cal | pred_short_cal | long_mfe_fw60 | short_mfe_fw60
- `isotonic_long.pkl` — fitted IsotonicRegression
- `isotonic_short.pkl` — fitted IsotonicRegression
- `strategy_artifact.json` — végső strategy params + metrics
- `sweep_results.csv` — Optuna trial log

## Acceptance Criteria

- [x] `build_table.py`: betölti mindkét modell `features.json`-ját, quant_train-ből feature select, predict(), target join → parquet
- [x] `calibrate.py`: IsotonicRegression fit a calibration perióduson, apply az egész táblára, `pred_{dir}_cal` oszlopok hozzáadva a parquet-hoz
- [x] `optimize.py`: Optuna sweep, objektívum a t2 doc alapján, output: strategy_artifact.json
- [x] Mindhárom CLI wrapper működik end-to-end
- [ ] `ruff check src/strategy/` tiszta (validator_agent feladata)
- [ ] `pyright src/strategy/` tiszta (validator_agent feladata)

## Notes

2026-06-20 — modeling_agent implementálta az src/strategy/ modult.

Létrehozott fájlok:
- src/strategy/__init__.py
- src/strategy/00_build_strategy_table.py  — CLI wrapper, argparse, logging
- src/strategy/01_calibrate_scores.py      — CLI wrapper, argparse, logging
- src/strategy/02_optimize_strategy.py     — CLI wrapper, argparse, --n-trials arg
- src/strategy/strategy/__init__.py
- src/strategy/strategy/artifacts.py       — write_strategy_artifact() + read_strategy_artifact()
- src/strategy/strategy/build_table.py     — build_strategy_table(): model load, union feature query, predict, target join, parquet write
- src/strategy/strategy/calibrate.py       — fit_calibration(): IsotonicRegression fit, pkl save, apply to full table, parquet overwrite
- src/strategy/strategy/optimize.py        — optimize_strategy(): Optuna TPE, _simulate_strategy() state machine, _compute_metrics(), sweep_results.csv

Implementációs döntések:
- union feature query: mindkét modell feature-listájának uniója egyetlen DuckDB lekérésben
- conflict_priority="long": bekerülve az artifact sémába (long tüzelésnél short nem lép be)
- Optuna verbosity=WARNING: csak a study szintű log jelenik meg
- read_strategy_artifact() hívás az optimize-ban: ha nincs artifact.json, üres string-ekkel folytatja
- relative import a subpackage-en belül: from strategy.artifacts import ...
