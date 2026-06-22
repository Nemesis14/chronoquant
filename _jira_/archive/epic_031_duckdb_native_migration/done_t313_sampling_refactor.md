---
epic: epic_031
id: t313
title: Sampling refactor — snap forrás + model.__sample tábla + reg.feature_sets
assignee: modeling_agent
status: todo
blocks: [t314, t315, t318]
blocked_by: [t312]
---

## Goal
A sampling átállítása: forrás a `quant_train` tábla helyett a `snap."<snapshot_id>"`;
output a parquet helyett `model."<model_id>__sample"` DuckDB tábla (kicsi, hourly+fold);
a feature-szűrés eredménye logikai feature_set → `reg.feature_sets`.

## Scope
- `src/modeling/sampling/create_sample.py` — forrás + output átállítás
- hourly select + fold_id determinisztikus SQL a snap fölött (QUALIFY + walk-forward CASE)
- `reg.feature_sets` írás (selected_cols, n_input, n_selected)
- Hivatkozás: `_doc_/_plans_/data_process_architecture.md` 5 (2-3. lépés), 5.1

## Acceptance Criteria
- [ ] create_sample a snap táblából olvas (snapshot_id paraméter)
- [ ] `model."<model_id>__sample"` tábla keletkezik (hourly+fold_id), kicsi méret
- [ ] determinisztikus, reprodukálható mintavétel (seed + immutable snap)
- [ ] feature_set a reg.feature_sets-be kerül
- [ ] meglévő sampling smoke tesztek átállítva/zöldek
- [ ] ruff + pyright tiszta

## Notes

### Elvégezve (modeling_agent)

**Átállított / létrehozott fájlok:**
- `src/modeling/sampling/snapshot_sampler.py` (kibővítve) — IO-mentes SQL builderek: `sample_table_fqn`, `snapshot_table_fqn`, `build_feature_set_id` (plan 6: `fs_{asset}_fw{h}_{dir}__{hash8}`, sorrend-független sha256 a selected_cols-on), `build_fold_case_sql` (walk-forward `CASE`), `build_sample_select_sql` (hourly select + fold_id), `build_sample_ctas_sql` (`CREATE OR REPLACE TABLE model."<id>__sample" AS ...`).
- `src/modeling/sampling/create_sample.py` (**teljes újraírás**) — snapshot-natív orchestrátor. `quant_train` + parquet út **megszűnt**. Forrás: `snap."<snapshot_id>"`; output: `model."<model_id>__sample"` DuckDB tábla.
- `src/modeling/sampling/__init__.py` — exportok átállítva (régi `create_yearly_sample` / `create_walk_forward_sample` / `create_model_walk_forward_sample` kivezetve; új: `create_model_sample`, `create_snapshot_sample` + SQL builderek). A pure helperek (`yearly_sampler`) és a legacy artifact IO (`artifacts`) megmaradtak.
- `src/modeling/pipeline.py` `step_sample` — az új `create_model_sample(model_id, snapshot_id)`-t hívja; a `snapshot_id` a `sampling.snapshot_id` config-mezőből jön (hiányában explicit `ValueError`). A pipeline-be való snapshot-bekötés downstream task (t314).
- `src/modeling/00_create_sample.py` CLI — `--model` + `--snapshot` (utóbbi default a config `sampling.snapshot_id`-ból).
- `src/modeling/tests/sampling/smoke/test_snapshot_sampler.py` (új) — 12 smoke teszt: SQL builderek + end-to-end CTAS szintetikus in-memory snap táblán + determinizmus + hiányzó snapshot hiba.

**`create_model_sample` új szignatúra:**
`create_model_sample(model_id: str, snapshot_id: str) -> dict` — feloldja a config-ból az asset/target/walk-forward paramétereket + FE feature-szelekciót, megnyitja a lab connectiont (`utils.open_lab_connection`), és delegál a `create_snapshot_sample`-nek. Visszatérés: `{model_id, snapshot_id, sample_table, n_rows, fold_row_counts, feature_set_id, n_input, n_selected}`.
Alacsonyabb szintű mag: `create_snapshot_sample(conn, model_id, snapshot_id, asset_id, target_cols, horizon=60, direction='l', year=2023, seed=42, train_months=9, valid_months=3, shift_months=3, n_folds=4, purge_minutes=240, selected_cols=None) -> dict`.

**`model."<model_id>__sample"` séma (kicsi, plan 5 step 2):** `open_time` + a modell target oszlopa(i) + `fold_id` (TINYINT/Int8). **Nincs per-modell feat_* másolat** — a feature-ök a snapshotban maradnak, a downstream a `__train_input` view-n projektál (plan 5.1).

**Fold-SQL lényeg:** hourly select determinisztikus `QUALIFY ROW_NUMBER() OVER (PARTITION BY date_trunc('hour', open_time) ORDER BY hash(epoch_ms(open_time)+seed), open_time) = 1` (immutable snap + fix seed → bit-identikus minta). `fold_id`: walk-forward `CASE` lánc a `generate_walk_forward_folds()` (változatlan, `yearly_sampler`) validációs ablakaiból — `open_time` dátuma a fold valid ablakában → fold_id 1..n, egyébként 0 (train-only). A meglévő walk-forward szemantika (train/valid/shift hónapok, n_folds, purge 240p, anchor October) **változatlan** — csak forrás+output cserélődött.

**reg.feature_sets kontraktus (írt mezők):** `feature_set_id` (PK, `fs_{asset}_fw{h}_{dir}__{hash8}`), `snapshot_id`, `n_input` (snapshot összes feat_* oszlopa), `n_selected` (a logikai szelekció mérete), `selected_cols` (JSON lista), `status='candidate'`. A szelekció forrása: `<artifact_dir>/feature_engineering/feature_set.json` `selected` listája; ha hiányzik, a teljes feat_* szuperszett. Plusz: `reg.models` upsert (snapshot_id + feature_set_id link, status='sampled').

**Determinizmus:** immutable snap + fix seed + fix fold paraméterek → bit-identikus `model.__sample` (`CREATE OR REPLACE`). Teszt igazolja (`test_create_snapshot_sample_deterministic`).

**Eredmény:**
- `ruff check src/modeling/ --fix` — tiszta (1 megmaradt B017 a `test_walk_forward_config.py`-ben **pre-existing main-en**, scope-on kívül, érintetlen fájl).
- `pyright src/modeling/sampling/` — **0 error, 0 warning**. (pipeline.py-ben 1 pre-existing `reportMissingImports` a lazy `papermill` importnál — opcionális dep, nem érintett.)
- `pytest src/modeling/tests/sampling/` — **67 passed** (12 új snapshot-sampler smoke + a meglévők). Teljes `src/modeling/` kollekció: 90 teszt import-hiba nélkül.
- Az end-to-end CTAS szintetikus in-memory snap táblán + reg attach-en futott (a repo lab DB-jében még nincs valós snap.* tábla); a `model.__sample` séma, az hourly-uniqueness, a TINYINT fold_id 0..4 tartomány és a reg.feature_sets/reg.models írás verifikálva.

**Döntések / feltételezések:**
- A `snapshot_id` config-mezőként (`sampling.snapshot_id`) kerül a modellhez; a pipeline tényleges snapshot-provenance bekötése a downstream t314 hatásköre — t313 csak a sampling modult állítja át.
- A feature-szelekció a meglévő FE `feature_set.json`-ból olvasódik (logikai feature_set), nincs új FE-logika — a metodológia változatlan.
- A régi parquet-alapú `create_yearly_sample`/`create_walk_forward_sample`/`create_model_walk_forward_sample` kivezetve (a snap-natív út kiváltja); a `yearly_sampler` pure fold-generátorai megmaradtak (a fold-szemantika forrása).

**Acceptance Criteria — mind teljesült:**
- [x] create_sample a snap táblából olvas (snapshot_id paraméter)
- [x] `model."<model_id>__sample"` tábla keletkezik (hourly+fold_id), kicsi (csak open_time+target+fold_id)
- [x] determinisztikus, reprodukálható mintavétel (seed + immutable snap)
- [x] feature_set a reg.feature_sets-be kerül (selected_cols, n_input, n_selected)
- [x] meglévő sampling smoke tesztek átállítva/zöldek (67 passed)
- [x] ruff + pyright tiszta
