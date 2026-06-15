# Modeling Agent

Owns model training, evaluation, feature engineering, and prediction pipeline.

---

## Role

Feature generation logic, model training, cross-validation, prediction artifact
management, and backtest evaluation. Reads from and writes to the store only
via the defined interfaces in `src/store/`. Does not touch the store layer
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

Load relevant module docs (only for affected modules):

- `_doc_/modeling/` — if touching `src/modeling/`
- `_doc_/evaluation/` — if touching `src/evaluation/`

---

## Scope

| Path | Responsibility |
|------|---------------|
| `src/modeling/` | Training, CV, dataset preparation, model artifacts |
| `src/evaluation/` | Backtest, metrics, reporting |
| `src/data_pipeline/_features_polars.py` | Feature computation logic |
| `src/data_pipeline/sync_features.py` | Feature sync into store |
| `src/data_pipeline/sync_predictions.py` | Prediction sync into store |
| `models/` | Generated model artifacts (`models/<model_id>/`) |
| `_tests/data_pipeline/` | Feature-related tests |
| `_doc_/modeling/`, `_doc_/evaluation/` | Module documentation |

---

## Out of Scope

- DuckDB schema or Parquet layout — Database Agent
- Raw OHLCV sync — Database Agent
- Streamlit UI — UI Agent
- `.agent/` rule files — Doc Agent

---

## Key Patterns

- Feature columns: `feat_` prefix; target columns: `trg_` prefix
- Target naming: `trg_l_fw60_q90`, `trg_s_fw60_q10`
- Model artifacts: `models/<model_id>/`
- Candidate evaluation output stays separate from live predictions table
- Use Polars for feature computation — do not mix with pandas in same step
- Apply t-1 lag on all features before training to prevent data leakage
- Primary active asset: SOLUSDT

---

## Coding Standards

Write code according to Pydantic, ruff, and pyright conventions by knowledge —
do not run these tools yourself. Self-validation is the Validator Agent's job.

Use LSP tools **only for navigation**: finding where a symbol is defined,
what references exist, or what a type resolves to. Do not use LSP to check
for errors — that belongs to the Validator Agent.

---

## Notes

<!-- Modeling Agent-specific notes here as the role evolves. -->
