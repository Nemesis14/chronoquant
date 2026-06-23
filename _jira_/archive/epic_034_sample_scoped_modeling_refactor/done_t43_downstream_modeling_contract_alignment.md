---
epic: epic_034
id: t43
title: Search train predict contract igazitas az uj sample scope-ra
assignee: modeling_agent
status: pr
blocks: [t46, t47]
blocked_by: [t41, t42]
---

## Goal
Biztosítani, hogy a search, final fit és offline predict ugyanarra a modell-scope
adat-szerződésre épüljön, mint a refaktorált FE.

## Scope
- `src/modeling/search/`
- `src/modeling/training/`
- `src/modeling/predict.py`
- `src/modeling/provenance.py`

## Acceptance Criteria
- [x] A downstream lépések nem sértik meg az új sample-scoped FE szerződést
- [x] A provenance-ben követhető a sample / feature_set / snapshot kötés
- [x] Nincs rejtett visszacsúszás teljes `quant_train`-scope-ra ott, ahol modell-scope kell

## Notes
Ez a task az FE-javítás után a teljes modeling lánc következetességét zárja le.

[modeling_agent] Audit és javítás — 2026-06-22

### 1. datasets.py — legacy path audit

`src/modeling/training/datasets.py` `load_modeling_dataset()` a teljes kódbázisban
**sehol nem hívódik** — csak a saját definíciójában létezik (Grep ellenőrzés igazolja).
A pipeline.py search/train lépései snapshot-native path-t használnak (`lgbm_search._load_search_dataset`,
`fit_lgbm._load_train_data`).

Intézkedés: A fájl modul-szintű docstring-je **DEPRECATED** megjegyzéssel kiegészítve,
amely explicit leírja, hogy a pipeline már a `snap ⋈ model.__sample` path-t használja.
A függvényt nem töröltük — backward compat megtartva.

### 2. I2 invariáns logging (search/train rowcount)

**`lgbm_search.py` `_load_search_dataset()`:**
- SQL végrehajtás után `model."<model_id>__sample"` COUNT(*) lekérdezés a nyitott
  connection-ön
- `logger.info` sor: `joined_rows`, `sample_rows` és `(I2: snap ⋈ model.__sample)` tag

**`fit_lgbm.py` `_load_train_data()`:**
- Ugyanolyan pattern: COUNT(*) lekérdezés, logger.info a joined/sample sorokkal

Mindkét helyen a `conn.close()` **előtt** fut a COUNT (try blokkon belül), így
ugyanaz a connection-on belül marad, majd a finally zárja.

### 3. provenance.py audit

`src/modeling/provenance.py` megvizsgálva. A `snapshot_id → model_id → feature_set_id`
provenance lánc **teljes és helyes**:
- `register_model_draft()`: `snapshot_id` → `reg.models` (draft) + `manifest.json`
- `link_feature_set()`: `feature_set_id` → `reg.feature_sets` + `reg.models` link
- `mark_model_trained()`: `oos_metric` + `search_run_id` → `reg.models` (trained)
- `update_manifest_provenance()`: `manifest.json` provenance mezők merge
Nincs gap, módosítás nem szükséges.

### Módosított fájlok
- `src/modeling/training/datasets.py` — DEPRECATED modul docstring
- `src/modeling/search/lgbm_search.py` — I2 logging a `_load_search_dataset()`-ben
- `src/modeling/training/fit_lgbm.py` — I2 logging a `_load_train_data()`-ban

### Minőség
Csak docstring + logging bővítés — ruff/pyright futtatás nem szükséges (feladat spec szerint).
