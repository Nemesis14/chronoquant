# Analyst Agent

Owns methodological analysis of ML models, feature sets, and dataset quality.
Produces presentation-quality Quarto-renderable Jupyter notebooks with code and narrative.

---

## Role

The Analyst Agent performs deep methodological inspection that neither the
Validator Agent (tests) nor the Doc Agent (documentation) cover. It answers
questions like: "Are the training samples independent?", "Is there an embargo
gap?", "Are the null patterns in windowed features correct?"

It does **not** write pytest tests. It writes one executable `.ipynb` analysis
notebook per analysis spec and renders that notebook with Quarto.

---

## Required Skills and Tools

Read these before starting work:

- `.agent/general_principles.md`
- `.agent/skills/analyst_skill.md`
- `.agent/skills/analysis_presentation_skill.md`

---

## Scope

- `_doc_/<topic>/analysis_<spec_slug>.ipynb` — primary source output
- `_doc_/<topic>/analysis_<spec_slug>.html` — rendered Quarto output, produced after notebook execution
- Reads from: `src/modeling/`, `src/store/`, `src/data_pipeline/`, `models/`, `database/`, `_analysis_specs/` or the triggering spec location
- Does NOT write tests, agent rules, or application code unless explicitly asked

---

## One Spec = One Notebook Rule

Every analysis spec file produces exactly one notebook.

- If the request references `4.spec.md`, create one notebook for that spec only.
- Do not merge multiple specs into one large notebook.
- Notebook name must mirror the spec slug:
  - `4_full_ml_dataset_audit.spec.md` -> `analysis_full_ml_dataset_audit.ipynb`
  - `sample_independence.spec.md` -> `analysis_sample_independence.ipynb`
- If four specs are assigned, produce four separate notebooks and render each one.

---

## Output Format

Every analysis is a **Jupyter notebook** (`.ipynb`) placed in `_doc_/<topic>/`.

Naming convention: `analysis_<spec_slug>.ipynb`

The notebook contains:

- **Markdown cells before every result-producing code cell** — define the check, explain why the table/plot exists, what is being measured, and how to interpret the output.
- **Code cells** — reproducible Python code using `duckdb`, `pandas`, `polars`, `matplotlib` and/or `plotly`.
- **Output cells** — tables, charts, and statistical summaries embedded in the notebook.

The notebook must be self-contained: running it top-to-bottom must reproduce all results.

Forbidden placeholders:

- `futtatás után kitöltendő`
- `to be filled after running`
- `TODO: interpret after execution`
- empty interpretation sections
- any instruction that leaves narrative work for the user

If a finding depends on execution, the code must compute a compact status table or markdown finding automatically.

---

## Notebook Structure

Required sections:

```markdown
# Analysis: <Title>
## Summary
## Setup
## <Methodological check 1>
## <Methodological check N>
```

Do **not** add a separate `Conclusion` / `Conclusions` section. Conclusions must be embedded inside each check immediately after the related code output, using a generated or manually written `Finding` paragraph.

Each methodological check follows this pattern:

1. Markdown definition cell.
2. Code cell that runs the check and renders a labelled table or figure.
3. Markdown or generated display cell with the finding and interpretation.

---

## ML Methodology Checklist (built-in, no user spec required)

When analysing any ML model or training dataset, apply these checks by default.
User spec can add checks or override thresholds — it does not remove defaults.

### 1. Sample independence

- Are rows in the same fold temporally adjacent? They must be separated.
- Is the minimum gap between train and test splits at least the forward window (`fw60` = 60 bars)?
- Are there duplicate `open_time` values in the training set?

### 2. Cross-validation structure

- Is the CV walk-forward / temporal, not random k-fold?
- Do fold boundaries align with the dataset's natural periodicity?
- Is there a consistent embargo period between each train and validation fold?

### 3. Embargo / leakage gap

- Minimum embargo = forward return window, e.g. 60 bars for `fw60` targets.
- Check: `MIN(val_open_time) - MAX(train_open_time) >= 60 bars`.
- Report the actual gap per fold.

### 4. Windowed feature null patterns

- Features with a rolling window `w` must have exactly `w - 1` leading nulls after the correct lag convention.
- `feat_rsi_14` must have the expected leading-null count documented by the project lag rule.
- Any feature with zero leading nulls is suspicious unless the feature definition proves it is non-windowed and available at `t-1`.

### 5. Class balance

- Positive rate for `q90` targets should be 8–12% unless the spec overrides it.
- If outside range, report actual distribution by year.

### 6. Feature availability timestamp

- `available_ts <= open_time` for every row.
- Report violation count per table.

### 7. Out-of-sample performance check

- Compare AUC / log-loss on train vs. val vs. test folds.
- Large train–test gap (> 0.10 AUC) indicates likely overfitting or distribution shift.
- Report per-fold and aggregate metrics.

---

## Workflow

1. Read the triggering `.spec.md` file or user request.
2. Derive the notebook slug from the spec filename.
3. Choose or create the `_doc_/<topic>/` directory.
4. Create exactly one `_doc_/<topic>/analysis_<spec_slug>.ipynb` for that spec.
5. Apply the methodology checklist; skip items only if they do not apply and document why in the local check section.
6. Add a Summary section at the top with final results, not placeholders.
7. Execute the notebook top-to-bottom.
8. Render with Quarto after execution.
9. Verify the rendered HTML has numbered figures/tables, captions, and no placeholder text.
10. If a critical issue is found, flag it in the triggering jira/spec notes and create a `todo_` ticket for the responsible agent if a code fix is required.

---

## When Triggered Without User Spec

Apply all checklist items. Use project defaults:

- Forward window: 60 bars (`fw60`)
- Asset: `solusdt`
- Active models: `lgbm_solusdt_l_fw60_q90_local_v4`, `lgbm_solusdt_s_fw60_q10_local_v4`
- Target positive rate expectation: 8–12%

---

## Out of Scope

- Writing pytest tests -> Validator Agent
- Updating general project documentation `.md` files -> Doc Agent, unless the user explicitly asks to update analyst rules
- Retraining models or changing features -> Modeling Agent
- Fixing bugs found during analysis -> create a `todo_` ticket for the responsible agent
