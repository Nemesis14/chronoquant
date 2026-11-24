# Modeling Agent

Owns model sampling, training, hyperparameter search, prediction pipeline and strategy calibration.

---

## Role

Feature generation logic, model training, cross-validation, prediction artifact
management. Reads from and writes to the store only
via the defined interfaces in `src/data_handling/store/`. Does not touch the store layer
directly or UI code.

---

## Required Skills and Tools

Read these before starting work:

- `.agent/general_principles.md`
- `.agent/skills/coding_skill.md`
- `.agent/skills/jira_skill.md`
- `.agent/tools/lsp_tool.md`
- `.agent/tools/ast_grep_tool.md`
- `.agent/tools/uv_tool.md`

Load on demand (only when relevant):

- `.agent/skills/model_lifecycle_skill.md` — új modell build, részleges retrain döntés
- `.agent/skills/deploy_skill.md` — élesítés, cutover, rollback

Load relevant module docs (only for affected modules):

- `_doc_/database_and_code_doc/5700_offline_prediction.md`, `_doc_/database_and_code_doc/0004_model_lifecycle.md` — if touching `src/modeling/`
- `_doc_/database_and_code_doc/6100_strategy_module.md` ... `6140_artifacts.md` — if touching `src/strategy/`
- `_doc_/database_and_code_doc/5800_scout_runtime.md` — if touching `src/scout/` / `artifacts/scout_s0c_r1/` (Impulse Scout)
- `_doc_/database_and_code_doc/8330_reports.md` — a pipeline `feature_engineering`/`analyze` lépései a `src/reports` buildereket hívják

---

## Scope

| Path | Responsibility |
|------|---------------|
| `src/modeling/` | Training, CV, dataset preparation, model artifacts |
| `src/strategy/` | Isotonic calibration, Optuna threshold sweep, strategy artifact |
| `src/data_handling/sync_tables/_features_polars.py` | Feature computation logic |
| `src/data_handling/sync_tables/sync_features.py` | Feature sync into store |
| `src/data_handling/sync_tables/sync_predictions.py` | Prediction sync into store |
| `artifacts/` | Generated model artifacts (`artifacts/<model_id>/`) |
| `src/scout/`, `artifacts/scout_s0c_r1/` | Impulse Scout runtime + befagyasztott artifact (kísérleti, NO-GO); új verzió = új artifact (pl. `scout_s0c_r2`) |
| `src/modeling/tests/` | Modeling tests for training and sampling |
| `src/modeling/pipeline.py` | Pipeline orchestrator; a `feature_engineering` és `analyze` lépések hívják a `src/reports/builders/*`-t (a builderek tartalma az analyst_agent-é) |

---

## Out of Scope

Minden egyéb domain: lásd delegation table — `CLAUDE.md`.

---

## Key Patterns

- Feature columns: `feat_` prefix; primary target columns: `long_mfe_fw60`, `short_mfe_fw60`
- Model naming: `lgbm_{asset}_{direction}_fw{horizon}_{start}_{end}` (pl. `lgbm_solusdt_l_fw60_2101_2607`); config: `config/models.json`
- Egyetlen támogatott `sampling_mode`: `train_valid_split`
- Riportok (sampling, FE, search, strategy) NEM notebookok: `src/reports` builderek -> `artifacts/<model_id>/reports/`
- Model artifacts: `artifacts/<model_id>/`
- Candidate evaluation output stays separate from live predictions table
- Use Polars for feature computation — do not mix with pandas in same step
- Apply t-1 lag on all features before training to prevent data leakage
- Aktív assetek: `solusdt` (default), `bchusdt`, `btcusdt` (élő modellpár); `ethusdt` data-only (nincs quant modell); a modell assetje `models.json` `asset_id` (`utils.model_asset_id`)

---

## Notes

<!-- Modeling Agent-specific notes here as the role evolves. -->
