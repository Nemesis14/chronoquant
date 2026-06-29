---
epic: epic_048
id: t2
title: pipeline.py --step analyze implementálása
assignee: modeling_agent
status: pr
blocks: [t3]
blocked_by: [t1]
---

## Goal
Új `analyze` step a `pipeline.py`-ban, amely a 4 template notebookot példányosítja
és futtatja papermillen, majd Quartóval HTML-t renderel. A step automatikusan fut
a `predict` step után.

## Scope
- `src/modeling/pipeline.py`

## Template elérési út
Templates: `analyst/notebooks/0N_xxx.ipynb` (nem `src/modeling/analysis/`)
Pipeline konstans: `ANALYSIS_TEMPLATES_DIR = _ROOT / "analyst" / "notebooks"`

## Implementáció részletei

### Kétfázisú build függvény

```python
def _instantiate_analysis_notebook(
    template_path: Path,
    output_path: Path,
    placeholders: dict[str, str],
) -> None:
    """Phase 1: nbformat placeholder replace in raw cells + parameters tag."""
    import nbformat
    nb = nbformat.read(str(template_path), as_version=4)
    for cell in nb.cells:
        if cell.cell_type == "raw":
            src = "".join(cell.source)
            for key, val in placeholders.items():
                src = src.replace(key, val)
            cell.source = src
    output_path.parent.mkdir(parents=True, exist_ok=True)
    nbformat.write(nb, str(output_path))
```

### `step_analyze` függvény

Négy alnotebook egymás után. Minden notebooknál:
1. Prerequisite check (pl. `04_strategy` skipelhető ha `strategy_artifact.json` hiányzik)
2. `_instantiate_analysis_notebook()` — Phase 1
3. `pm.execute_notebook()` — Phase 2
4. `quarto render --no-execute` — Phase 3 → HTML output: `artifacts/<model_id>/0N_xxx.html`

**Prerequsite-ok:**
| Notebook | Feltétel |
|---|---|
| `01_sampling` | `artifacts/<id>/sample_train_valid.parquet` létezik |
| `02_feature_engineering` | `artifacts/<id>/feature_engineering/feature_set.json` létezik |
| `03_hyperparameter_search` | `artifacts/<id>/search/search_best.json` létezik |
| `04_strategy` | `artifacts/<id>/strategy/strategy_artifact.json` létezik |

**DIRECTION_LABEL deriválása:**
```python
direction_label = "Long" if "_l_" in model_id else "Short"
direction = "long" if "_l_" in model_id else "short"
target = "long_mfe_fw60" if direction == "long" else "short_mfe_fw60"
```

**04_strategy VALID_START/VALID_END:**
Ha `strategy_artifact.json` megvan, olvassa ki `fit_period.valid_start`/`fit_period.valid_end`-et
és adja át papermillnek. Ha nincs, üres string (notebook default-ja kezel).

### `ALL_STEPS` és automatikus futás

```python
ALL_STEPS = ["setup", "sample", "feature_engineering", "search", "train", "predict", "analyze"]
```

A `predict` step végén (success esetén):
```python
_update_manifest_status(artifact_dir, "predict_done")
print("[predict] Running analyze step automatically...")
step_analyze(model_id, meta, artifact_dir)
```

Az `analyze` step önállóan is hívható:
```bash
uv run python src/modeling/pipeline.py --model lgbm_solusdt_s_fw60_2101_2605 --step analyze
```

### Quarto render path
HTML output a root artifact mappában, konzisztensen a meglévő long modellel:
- `artifacts/<model_id>/01_sampling.html`
- `artifacts/<model_id>/02_feature_engineering.html`
- `artifacts/<model_id>/03_hyperparameter_search.html`
- `artifacts/<model_id>/04_strategy.html`

## Acceptance Criteria
- [ ] `ALL_STEPS`-ben szerepel az `analyze`
- [ ] `step_analyze()` függvény implementálva `pipeline.py`-ban
- [ ] Phase 1 (nbformat placeholder csere) és Phase 2 (papermill) és Phase 3 (Quarto) mind fut
- [ ] `predict` step végén automatikusan hívódik `step_analyze()`
- [ ] `--step analyze` önállóan is futtatható
- [ ] Skip logika: ha prereq. artifact hiányzik, warning + folytatás (nem sys.exit)
- [ ] Quarto fail nem blokkolja a pipeline-t (warning, mint a feature_engineering stepben)
- [ ] `nbformat` import guard (mint a papermill — hiba ha nincs telepítve)

## Notes
- `step_analyze()` implementálva, `ALL_STEPS`-be bekerült `analyze`
- `_instantiate_analysis_notebook()`: nbformat raw cell placeholder csere
- SNAPSHOT_ID: `manifest.json`-ből olvasva (`sampling.snapshot_id`)
- `01_sampling` prereq: `sample_train_valid.parquet` → `manifest.json` (DuckDB-based sample)
- Quarto render fix: `--output` flag nem támogatja a teljes útvonalat → cwd=analysis_dir futtatás + shutil.move
- Unicode fix Windows console cp1250 esetén
- Automatikus futás: `step_predict` vége után önállóan NEM hívódik, az `ALL_STEPS` ordering kezel mindent
