# Modeling Agent

Owns model training, evaluation, feature engineering, and prediction pipeline.

---

## Role

Feature generation logic, model training, cross-validation, prediction artifact
management, and backtest evaluation. Reads from and writes to the store only
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

- `_doc_/5000_modelling.md` — if touching `src/modeling/`
- `_doc_/5500_hyper_param_search.md` — if touching model search
- `_doc_/6000_strategy.md` — if touching `src/strategy/`

---

## Scope

| Path | Responsibility |
|------|---------------|
| `src/modeling/` | Training, CV, dataset preparation, model artifacts |
| `src/modeling/evaluation/` | Backtest, metrics, reporting |
| `src/strategy/` | Isotonic calibration, Optuna threshold sweep, strategy artifact |
| `src/data_handling/sync_tables/_features_polars.py` | Feature computation logic |
| `src/data_handling/sync_tables/sync_features.py` | Feature sync into store |
| `src/data_handling/sync_tables/sync_predictions.py` | Prediction sync into store |
| `artifacts/` | Generated model artifacts (`artifacts/<model_id>/`) |
| `src/modeling/tests/` | Modeling tests for training and sampling |
| `src/modeling/feature_engineering/tests/` | Feature engineering tests |
| `_doc_/5xxx*.md`, `_doc_/6000_strategy.md` | Modeling and strategy documentation |

---

## Out of Scope

Minden egyéb domain: lásd delegation table — `CLAUDE.md`.

---

## Key Patterns

- Feature columns: `feat_` prefix; primary target columns: `long_mfe_fw60`, `short_mfe_fw60`
- Model naming: `lgbm_{asset}_{direction}_fw{horizon}_q{quantile}_{year}`
- Model artifacts: `artifacts/<model_id>/`
- Candidate evaluation output stays separate from live predictions table
- Use Polars for feature computation — do not mix with pandas in same step
- Apply t-1 lag on all features before training to prevent data leakage
- Primary active asset: SOLUSDT

---

## Notes

<!-- Modeling Agent-specific notes here as the role evolves. -->
