---
epic: epic_021
id: t3
title: Pipeline.py + 03_fit_model.py CLI update
assignee: modeling_agent
status: pr
blocks: [t4]
blocked_by: [t1, t2]
---

## Goal

A `pipeline.py` `step_train` és a `03_fit_model.py` CLI frissítése az új fit funkcióra.
A régi CV-alapú flow eltávolítása a pipeline-ból; az új flow egyetlen fit + OOS scoring.

## Scope

- `src/modeling/pipeline.py` — `step_train()` function
- `src/modeling/03_fit_model.py` — CLI wrapper
- `src/modeling/training/train.py` — dispatcher (train_model)

## Acceptance Criteria

- [ ] `pipeline.py step_train(model_id)` az új `fit_lightgbm_from_search(model_id)` + OOS scoring-t hívja
- [ ] `03_fit_model.py` CLI: csak `--model` arg, minden más az artifact-ból/config-ból jön
- [ ] `train.py train_model()` átirányítva az új fit funkcióra
- [ ] `pipeline.py` manifest update: `"train_done"` state megmarad
- [ ] `ALL_STEPS` listában a `train` step továbbra is szerepel
- [ ] Az old `_load_param_profile` / tuning_values logika nem hívódik meg a train step-ből
- [ ] `ruff check` + `pyright` tiszta a módosított fájlokra

## Notes

**03_fit_model.py jelenleg:**
Thin wrapper, meghívja `train_model(args.model_id)` majd kiírja a result dict-et.
Frissítés minimális — a `--model-id` arg átnevezése `--model`-re (konzisztens pipeline-nal),
vagy megtartható ahogy van, mivel csak a belső `train_model` implementáció változik.

**Régi print-ek:**
A jelenlegi `main()` `tuning_param` és `best_tuning_value` értékeket ír ki — ezek
az új flow-ban nem relevánsak. Cserélni kell pl. n_estimators + selected features count + oos_year outputra.

**Validálandó:** `uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_q90_2021 --step train`
sikeres futása artifact megjelenésével.
