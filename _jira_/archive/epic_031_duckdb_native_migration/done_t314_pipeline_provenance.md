---
epic: epic_031
id: t314
title: Pipeline provenance + registry integráció
assignee: modeling_agent
status: todo
blocks: [t315]
blocked_by: [t311, t313]
---

## Goal
A modell pipeline minden lépése írja a registryt és rögzítse az adat-provenance-t (P2):
a `manifest.json`-ba és `reg.models`-ba bekerül a `snapshot_id` + `feature_set_id`;
a search a `reg.search_runs`-ba, a fájl-artefaktok a `reg.artifacts`-ba.

## Scope
- `src/modeling/pipeline.py` — step_setup → reg.models (draft); minden lépés végén reg frissítés
- `src/modeling/feature_engineering/` — feature_set.json → reg.feature_sets link
- `src/modeling/search/` — best params → reg.search_runs
- `src/modeling/training/artifacts.py` — artefakt útvonalak → reg.artifacts
- Hivatkozás: `_doc_/_plans_/data_process_architecture.md` 5 (táblázat), 4.3

## Acceptance Criteria
- [ ] manifest.json tartalmazza: snapshot_id, feature_set_id, content_sha256
- [ ] reg.models státusz végigköveti: draft → sampled → trained → predicted
- [ ] reg.search_runs sor a best paramokkal + objective-vel
- [ ] reg.artifacts sorok a model.pkl / ipynb / html / logok útvonalával
- [ ] ruff + pyright tiszta

## Notes

### Elvégezve (modeling_agent)

**Létrehozott / módosított fájlok:**
- `src/modeling/provenance.py` (**új**) — a modeling-oldali registry-író gateway a t311 CRUD fölött, `utils.open_lab_connection`-ön át (config-gateway; nincs hardcode path). API: `register_model_draft`, `set_model_status`, `mark_model_trained`, `link_feature_set`, `register_search_run`, `latest_search_run_id`, `register_artifacts`, `update_manifest_provenance`. Konstansok: `MODEL_STATUS_CHAIN`, `ARTIFACT_KINDS`.
- `src/modeling/pipeline.py` — `step_setup` → `register_model_draft` (reg.models draft + manifest provenance) + manifest artefakt regisztráció; `step_sample` → manifest provenance (snapshot_id+feature_set_id) frissítés (a t313 create_model_sample már upsertel reg.models status='sampled'); `step_feature_engineering` → `link_feature_set` (a tényleges FE-szelektált feature_set link) + fe_notebook/fe_html artefaktok.
- `src/modeling/search/lgbm_search.py` — `run_search` végén `_register_search_provenance` → reg.search_runs (best params + objective) + reg.artifacts (search_best/trials/summary). Best-effort try/except, hogy registry-hiba ne dobjon el egy kész search-öt.
- `src/modeling/training/artifacts.py` — `register_training_artifacts` (**új**) → reg.artifacts (model.pkl/features/params/metrics/cv_results) + `mark_model_trained` (status=trained, oos_metric, search_run link). `TRAINING_ARTIFACT_FILES` konstans.
- `src/modeling/training/fit_lgbm.py` — `fit_lightgbm_from_search` végén `register_training_artifacts` hívás (oos_metric = search_best.objective_score).
- `src/modeling/tests/smoke/test_provenance.py` (**új**, + `tests/smoke/__init__.py`) — 9 smoke teszt: manifest provenance merge/None-skip, reg.models státusz-lánc (draft→sampled→trained→predicted), mark_model_trained (metric+link), reg.search_runs (best_params+objective), link_feature_set (reg.feature_sets + reg.models link), reg.artifacts (csak létező fájl). A `utils.open_lab_connection` egy non-closing in-memory reg proxyra patchelve.

**reg.models státusz-lánc bekötése:** `draft` (step_setup → register_model_draft) → `sampled` (t313 create_model_sample upsert, változatlanul) → `trained` (fit_lgbm → mark_model_trained, oos_metric + search_run_id) → `predicted` (`set_model_status(..., 'predicted')` API kész; a tényleges offline predict lépés a t315 hatásköre — a kontraktus oldalon a státusz beállítható).

**manifest.json új mezők:** `provenance: { snapshot_id, feature_set_id, content_sha256 }`. A `content_sha256` a reg.snapshots-ból olvasódik a setup lépésnél (data fingerprint); `feature_set_id` a sample/FE lépésnél töltődik. A `update_manifest_provenance` mezőnként merge-el (None-t ignorál), így egy lépés nem írja felül a korábbiak provenance-ét.

**reg.search_runs kontraktus:** `search_run_id = {model_id}__search_{stage}`, `model_id`, `stage`, `objective` (= best objective_score), `best_params` (JSON), `status='candidate'`.

**reg.artifacts kontraktus:** `artifact_id = {model_id}__{kind}`, `owner_id = model_id`, `kind` (manifest/fe_notebook/fe_html/search_*/model_pkl/features/params/metrics/cv_results), `path`, `status='candidate'`. Csak ténylegesen létező fájl kerül be (egy lépés nem feltétlen állít elő mindent).

**Döntések:** a search és train registry-írása best-effort (try/except) — a registry sosem akaszthatja meg a kész search/train eredményt; a fájlok maradnak az igazságforrás. A train nem ismeri a search stage-et, ezért `latest_search_run_id` keresi ki a model legutóbbi search_run-ját a linkeléshez.

**Eredmény:**
- `ruff check src/modeling/` — a módosított/új fájlok tiszták (1 megmaradt B017 a `test_walk_forward_config.py`-ben pre-existing main-en, érintetlen, scope-on kívül).
- `pyright` az új/módosított fájlokra — 0 error (pipeline.py-ben 1 pre-existing `reportMissingImports` a lazy `papermill` importnál — opcionális dep, nem érintett).
- `pytest src/modeling/` — **99 passed** (9 új provenance smoke + a meglévők), pre-existing piros nincs.

**Acceptance Criteria:**
- [x] manifest.json tartalmazza: snapshot_id, feature_set_id, content_sha256 (`provenance` blokk)
- [x] reg.models státusz végigköveti: draft → sampled → trained → predicted (draft/trained bekötve; sampled t313; predicted API kész, predict lépés t315)
- [x] reg.search_runs sor a best paramokkal + objective-vel
- [x] reg.artifacts sorok a model.pkl / ipynb / html / logok útvonalával
- [x] ruff + pyright tiszta
