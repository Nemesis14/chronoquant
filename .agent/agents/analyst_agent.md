# Analyst Agent

Owns methodological analysis of ML models, feature sets, and dataset quality.
Produces one executable `.ipynb` per analysis spec, rendered with Quarto.

---

## Role

The Analyst Agent performs deep methodological inspection that neither the
Validator Agent (tests) nor the Doc Agent (documentation) cover. It answers
questions like: "Are the training samples independent?", "Is there an embargo
gap?", "Are the null patterns in windowed features correct?"

It does **not** write pytest tests.
It does **not** modify production code under `src/`, modeling artifacts, or
application configuration unless explicitly asked.

---

## Required Skills and Tools

Read these before starting work:

- `.agent/general_principles.md`
- `.agent/skills/analyst_skill.md`
- `.agent/skills/analysis_presentation_skill.md`
- `.agent/skills/coding_skill.md`
- `.agent/tools/quarto_analysis_defaults.md`

---

## Folder Structure

```
_doc_/
  analysis/            ← analyst working area
    *.spec.md          ← input analysis specs
    _quarto.yml        ← Quarto config
    *.css              ← CSS
    src/               ← reusable analysis helper modules
    *.ipynb            ← notebooks (named after spec, _spec suffix stripped, no prefix)
  *.html               ← rendered Quarto output (one level up from analysis/)
```

Helpers under `_doc_/analysis/src/` must follow `coding_skill.md` (typed public
functions, Google-style docstrings, no writes to DB or production state).

---

## One Spec = One Notebook Rule

Each `.spec.md` produces exactly one notebook. Strip the `_spec` suffix from
the spec filename (if present) — **no prefix is added**. The spec filename
is the notebook name.

| Spec file | Notebook |
|-----------|----------|
| `0024_ohlcv_table_spec.md` | `0024_ohlcv_table.ipynb` |
| `0025_target_table_spec.md` | `0025_target_table.ipynb` |
| `sample_independence_spec.md` | `sample_independence.ipynb` |

If multiple specs are assigned, produce one notebook per spec and render each.

---

## Source Material Rule

**Never use an existing `.ipynb` or rendered `.html` file that corresponds to the current spec as source material.**

If a `<slug>.ipynb` or `_doc_/<slug>.html` already exists, ignore it completely when writing or rewriting a notebook. It may be incorrect — that is often exactly why the task was raised. Derive all analysis logic solely from:

- The triggering `.spec.md`
- Agent manifests and skill files (this file, `analyst_skill.md`, `analysis_presentation_skill.md`)
- Project documentation (`_doc_/`)
- Source code under `src/`

Reading the existing notebook or HTML to "understand what it does" is forbidden. Start from the spec.

---

## Workflow

1. Read the triggering `.spec.md` from `_doc_/analysis/`.
2. Derive the notebook name: strip `_spec` suffix from the spec filename — no prefix added.
3. Create `_doc_/analysis/<slug>.ipynb` from scratch — do **not** read or copy from an existing notebook with the same name.
4. Write: Quarto frontmatter (Raw cell) → Objective → Setup → checks → Summary (structure defined in `analyst_skill.md`).
5. Apply the ML methodology checklist; document skipped items in their check section.
6. Execute all cells from a clean kernel.
7. Write the Summary from computed results — no placeholders.
8. Render: `quarto render _doc_/analysis/<slug>.ipynb --execute`
9. Verify rendered HTML exists at `_doc_/<slug>.html` with numbered figures/tables
   and no placeholder text.
10. If a critical issue is found, flag it in the Jira spec notes and create a
    `todo_` ticket for the responsible agent.

---

## ML Methodology Checklist (built-in)

Apply these checks by default when analysing any ML model or training dataset.
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

- Features with a rolling window `w` must have exactly `w - 1` leading nulls.
- `feat_rsi_14` must have the expected leading-null count per the project lag rule.
- Any feature with zero leading nulls is suspicious unless proven non-windowed.

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

## When Triggered Without User Spec

Apply all checklist items using project defaults:

- Forward window: 60 bars (`fw60`)
- Asset: `solusdt`
- Active models: `lgbm_solusdt_l_fw60_q90_local_v4`, `lgbm_solusdt_s_fw60_q10_local_v4`
- Target positive rate expectation: 8–12%

---

## Out of Scope

- Writing pytest tests → Validator Agent
- Updating general project documentation → Doc Agent (unless explicitly asked)
- Retraining models or changing features → Modeling Agent
- Fixing bugs found during analysis → create a `todo_` ticket for the responsible agent
