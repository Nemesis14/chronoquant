---
id: t2
epic: epic_042
title: MI bevezetése + feature engineering pipeline csere
status: pr
assignee: analyst_agent
created: 2026-06-26
---

## Leírás

MI (Mutual Information) bevezetése az összes feature csoportra a scatter notebookba,
és a scatter notebook cseréje az `01_feature_engineering.ipynb` pipeline templatere.

## Elvégzett munka

### 1. Scatter notebook → pipeline template csere

- `feature_engineering_scatter.ipynb` átnevezve `01_feature_engineering.ipynb`-ra (artifact + src/modeling template)
- Régi `01_feature_engineering.ipynb` (Spearman + stability alapú) törölve
- `src/modeling/feature_engineering/` Python package törölve (quality.py, stability.py, redundancy.py, target_relation.py, config.py)

### 2. Notebook módosítások

**Papermill paraméterek:**
- Hozzáadva `pm-parameters` cella (`ARTIFACT_DIR`, `SAMPLE_DIR`, `MODEL_ID`, `SNAPSHOT_ID`)
- Setup cella hardcoded konstansok → params + fallback a korábbi értékekre
- `ARTIFACT_DIR_PATH` levezetése, `TARGET` levezetése MODEL_ID directionből

**Quality pre-filter:**
- Null rate > 1%, inf rate > 0.1%, variance < 1e-8 → `feat_cols_quality_dropped`
- `feat_gap_open_abs_sma_*` kizárva quality alapján

**Globális akkumulátorok:**
- `_fs_selected`, `_fs_dropped_mi`, `_fs_dropped_corr`, `_fs_dropped_qual`
- Loop végén feltöltve per-group döntések alapján

**feature_set.json output cella** (`co-feature-set-output`):
- `selected`: MI > 0.001 AND nem korreláció-duplikátum (csoporton belül)
- `dropped`: quality + MI threshold + korrelációs dedup okai
- `review`: üres (stability eltávolítva)
- `thresholds`: `{mi_threshold: 0.001, corr_threshold: 0.98, ...}`

### 3. feature_set.json eredmény

| Kategória | Darab |
|-----------|-------|
| selected  | 131   |
| top10     | 10    |
| top1_per_group | 17 |
| dropped (quality) | 2 |
| dropped (MI ≤ 0.001) | ~60 |
| dropped (duplikátum) | ~15 |
| review | 0 |

Korábbi állapot: 124 selected, 30 review (stability false positive), 54 dropped.
Az összes volatilitás-feature (hml_range MI=0.099, returns_std_14 MI=0.093) mostantól SELECTED.

### 4. Metodológiai dokumentáció frissítése

- `_doc_/methodology_doc/2010_feature_engineering.md`: stability eltávolításának indoklása,
  MI_THRESHOLD + CORR_THRESHOLD paraméter leírás, frissített flowchart, checklist

## Notes

- Stability eltávolításának indoka: vol clustering miatt az összes legjobb volatilitás-feature REVIEW-ba
  kerül, miközben valóban gyenge feature-ök (MI≈0) SELECTED maradnak — szisztematikus false negative
- A pipeline (`src/modeling/pipeline.py`) változatlan: papermill + quarto render ugyanúgy fut,
  csak a template tartalom cserélődött
- A `feature_engineering` Python package törlése nem töri el az importokat: a downstream modulok
  (search, training, sampling) csak a JSON fájlt olvassák, nem importálnak a packageből
