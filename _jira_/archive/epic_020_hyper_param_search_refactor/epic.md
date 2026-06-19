---
id: epic_020
title: 02_hyper_param_search — refaktor (yearly sample + feature_set.json)
status: in_progress
---

## Goal

A `02_hyper_param_search.py` és `search/lgbm_search.py` refaktorálása az új yearly
sample struktúrára. Minden legacy expanding-window logika és parquet-alapú feature audit
eltávolítva. Az új search kizárólag a yearly sample + feature_set.json inputokra épül.

## Emelt be: story_hyper_param_search_refactor

Az alábbi acceptance criteria a `_jira_/story_hyper_param_search_refactor.md` alapján:

- Input: `sample_train_valid.parquet` (yearly sample) + `feature_set.json` (csak `selected`)
- CV: `selected_valid_weeks` alapján (12 fold), purge sorok kizárva, train segment = training set
- Bináris label: `long_mfe_fw60 >= quantile(train, q)` / `short_mfe_fw60 <= quantile(train, q)`,
  ahol `q` a model ID-ből parsolt érték (pl. `q90` → 0.90)
- Output: `search/search_best.json` (teljes trial rekord) + `search/best_params.json` (csak params dict)
- CLI: `uv run python src/modeling/02_hyper_param_search.py --model lgbm_solusdt_l_fw60_q90_2021 --stage smoke`
- Pipeline illeszt: `pipeline.py --model ... --step search` is működik
- `ruff check` + `pyright` tiszta
- Smoke teszt átmegy (vagy új smoke teszt kerül be)

## Eltávolított legacy

- Stage 0 Feature Audit (parquet-based, `data_dir/features/*.parquet`)
- `load_sample_definition` / `validate_sample_definition` (folds.json — legacy format)
- `_load_champion_prauc` (local_v2 → stable_v1 mapping — nem aktív modell nevek)
- `_KNOWN_DUPLICATES` (hardcoded lista — feature_set.json váltja ki)
- `--model-id` CLI arg (→ `--model`, konzisztens a pipeline-nal)
- `asset-id` CLI arg (mindig a model config-ból jön)

## Taskok

| ID | Cím | Assignee |
|----|-----|----------|
| t1 | lgbm_search.py + 02_hyper_param_search.py + pipeline.py refaktor | modeling_agent |
| t2 | _doc_/5500_hyper_param_search.md | methodology_agent |
| t3 | Validáció (ruff + pyright + smoke) | validator_agent |

Sorrend: t1 → t3, t2 párhuzamosan
