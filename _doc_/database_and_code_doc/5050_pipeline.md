# 5050 — Pipeline Orchestrátor

`src/modeling/pipeline.py`

A `pipeline.py` a modell fejlesztési pipeline belépési pontja. Egyetlen CLI paranccsal
futtatja az összes lépést sorban, vagy egy kiválasztott lépést önállóan.

> Metodológiai háttér:
> - [5000_modelling.md](../methodology_doc/0000_overview.md) — modellezési pipeline overview
> - [5400_feature_engineering.md](../methodology_doc/4000_feature_engineering.md) — feature engineering módszertan
> - [5500_sampling.md](../methodology_doc/3000_sampling.md) — sampling módszertan
> - [5600_model_training.md](../methodology_doc/5100_training.md) — training módszertan

Részletes kód-referencia (step implementációk, predict, provenance):
→ [5530_pipeline_predict_provenance.md](5530_pipeline_predict_provenance.md)

---

## Overview

```mermaid
flowchart TD
  CLI["pipeline.py\n--model model_id\n--step step"]
  SETUP["setup\nartifact_dir + manifest.json"]
  SAMPLE["sample\nmodel.__sample FROM snap"]
  FE["feature_engineering\nreports.builders.feature_engineering"]
  SEARCH["search\nlgbm_search.run_search"]
  TRAIN["train\ntrain_model -> model.pkl"]
  PREDICT["predict\npredict_offline -> model.__pred"]
  ANALYZE["analyze\nreports.builders.* -> reports/"]

  CLI --> SETUP --> SAMPLE --> FE --> SEARCH --> TRAIN --> PREDICT --> ANALYZE
```

---

## CLI Reference

```bash
# Összes lépés sorban
uv run python src/modeling/pipeline.py --model <model_id>

# Egyetlen lépés
uv run python src/modeling/pipeline.py --model <model_id> --step <step>

# Search step — stage és trial count megadásával
uv run python src/modeling/pipeline.py --model <model_id> --step search --stage explore --n-trials 60

# Sample step — snapshot override
uv run python src/modeling/pipeline.py --model <model_id> --step sample --snapshot <snapshot_id>
```

### CLI argumentumok

| Argumentum | Default | Leírás |
|------------|---------|--------|
| `--model` | kötelező | Modell ID a `config/models.json`-ból |
| `--step` | `None` (összes) | Egyetlen step futtatása: `setup`, `sample`, `feature_engineering`, `search`, `train`, `predict`, `analyze` |
| `--stage` | `None` | Search stage: `smoke`, `explore`, `refine` (csak `search` stepnél) |
| `--n-trials` | `100` | Max Optuna trial (csak `search` stepnél) |
| `--timeout-hours` | `None` | Falióra korlát órában (csak `search` stepnél) |
| `--snapshot` | `None` | Snapshot ID override (sample és feature_engineering stepnél) |

---

## Lépések

| Lépés | Modul | Input | Output |
|-------|-------|-------|--------|
| `setup` | `pipeline.step_setup` | `config/models.json` meta | `artifact_dir/manifest.json`; `reg.models` draft sor |
| `sample` | `modeling.sampling.create_model_sample` | `snap."<snapshot_id>"` + `config/models.json` sampling config | `model."<model_id>__sample"` DuckDB tábla; `reg.feature_sets` bejegyzés |
| `feature_engineering` | `reports.builders.feature_engineering.build` | `snap ⋈ model.__sample` | `artifact_dir/feature_engineering/feature_set.json`; riport `artifact_dir/reports/02_feature_engineering/` |
| `search` | `modeling.search.lgbm_search.run_search` | `model.__sample` + `feature_set.json` | `artifact_dir/search/search_best.json`; `reg.search_runs` bejegyzés |
| `train` | `modeling.training.train.train_model` | `search_best.json` + `feature_set.json` + `snap ⋈ model.__sample` | `artifact_dir/model.pkl`, `features.json`, `params.json`, `metrics.json` |
| `predict` | `modeling.predict.predict_offline` | `model.pkl` + `features.json` + `snap."<snapshot_id>"` (teljes range) | `model."<model_id>__pred"` DuckDB tábla; `reg.models` status → `predicted` |
| `analyze` | `pipeline.step_analyze` -> `reports.builders.{sampling,feature_engineering,hyperparameter_search,strategy}` | artifactok (előfeltétel-fájlonként; hiányzót kihagy) | `artifact_dir/reports/{01_sampling,02_feature_engineering,03_hyperparameter_search,04_strategy}/` (report-formátum) |

```mermaid
sequenceDiagram
  participant CLI as pipeline.py CLI
  participant SETUP as step_setup
  participant SAMPLE as step_sample
  participant FE as step_feature_engineering
  participant SEARCH as step_search
  participant TRAIN as step_train
  participant PRED as step_predict
  participant DB as DuckDB lab
  participant FS as artifact_dir/

  CLI ->> SETUP: model_id, meta, artifact_dir
  SETUP ->> FS: manifest.json
  SETUP ->> DB: reg.models draft
  CLI ->> SAMPLE: model_id, snapshot_id
  SAMPLE ->> DB: model.__sample FROM snap
  SAMPLE ->> DB: reg.feature_sets + reg.models sampled
  CLI ->> FE: model_id, meta, artifact_dir
  FE ->> FS: feature_set.json, reports/02_feature_engineering/
  FE ->> DB: reg.feature_sets link
  CLI ->> SEARCH: model_id, stage, n_trials
  SEARCH ->> FS: search_best.json
  SEARCH ->> DB: reg.search_runs
  CLI ->> TRAIN: model_id
  TRAIN ->> FS: model.pkl, features.json, params.json, metrics.json
  TRAIN ->> DB: reg.models trained + reg.artifacts
  CLI ->> PRED: model_id
  PRED ->> DB: model.__pred (full snapshot range)
  PRED ->> DB: reg.models predicted
  CLI ->> FS: analyze -> reports/*/ (manifest, report.md, charts, tables)
```

---

## Manifest státusz

A `manifest.json` `pipeline_status` mezője minden lépés után frissül:

`setup` → `sample_done` → `feature_engineering_done` → `search_done` → `train_done` → `predict_done` → `analyze_done`

---

## Kapcsolódó dokumentumok

| Fájl | Tartalom |
|------|----------|
| [5530_pipeline_predict_provenance.md](5530_pipeline_predict_provenance.md) | Pipeline step implementációk, predict, provenance részletes kód-ref |
| [8330_reports.md](8330_reports.md) | `reports.builders.*` (FE és analyze lépések riportjai) |
| [5510_training.md](5510_training.md) | `train_model()` és training submodule kód-ref |
| [5520_search.md](5520_search.md) | Hyperparameter search kód-ref |
| [5300_create_sample.md](5300_create_sample.md) | `create_model_sample` snap-natív sampling kód-ref |
