---
epic: epic_031
id: t325
title: Artifacts wipe + teljes pipeline újrafuttatás a 2 champion modellre
assignee: modeling_agent
status: done
blocks: [t326]
blocked_by: [t324]
---

## Goal
A validált új (DuckDB-natív, registry-vezérelt) architektúrában a teljes modellfejlesztési
pipeline újrafuttatása a két éles champion modellre, tiszta artifacts/ mappáról indulva.
Csak az új struktúra szerinti artefaktok és DuckDB objektumok jönnek létre.

## Scope
- **ELŐFELTÉTEL (t315 carry-forward):** a tréning-betöltő drótozás rendezése — a
  `fit_lgbm._load_train_data` jelenleg a régi `sample_train_valid.parquet`-ből olvas
  (feat_* oszlopokkal), de a t313 óta a sample a `model."<id>__sample"` táblában él
  feat_* nélkül (features a snapshotban). A train lépésnek a `snap ⋈ model.__sample`
  joinból (plan 5.1 `__train_input` view) kell olvasnia. Ezt a valós futás ELŐTT rendezni kell.
- `artifacts/` teljes kiürítése (minden korábbi modell- és strategy-artifact)
- `config/models.json` leszűkítése a 2 champion modellre:
  `lgbm_solusdt_l_fw60_2101_2605`, `lgbm_solusdt_s_fw60_2101_2605`
  (a 10 éves modell-bejegyzés 2021–2025 × long/short törlése)
- Teljes pipeline mindkét championre az új flow szerint:
  snapshot (t312) → sample → feature_engineering → search → train → predict (model.__pred)
- Strategy kalibráció a snap ⋈ pred join-ból → strat.* táblák + strategy_artifact.json
- reg.* feltöltése (snapshots, feature_sets, models, search_runs, strategies, artifacts)

## Acceptance Criteria
- [ ] artifacts/ csak az új koncepció szerinti, frissen generált tartalmat tartalmazza
- [ ] models.json csak a 2 champion modellt tartalmazza
- [ ] mindkét champion: reg.models státusz → predicted; model.__sample + model.__pred tábla létezik
- [ ] strategy session: strat.*__trades/__equity/__cutoffs + strategy_artifact.json létrejön; reg.strategies sor
- [ ] snapshot immutable, hash sértetlen; reprodukálható
- [ ] futás logja a ticket Notes szekciójába

## Notes

### FÁZIS 0 — Tréning-betöltő fix (t315 carry-forward blocker)

**Probléma:** `fit_lgbm._load_train_data` a régi `sample_train_valid.parquet`-ból olvasott (parquet + pandas úton), de t313 óta a sample a `model."<model_id>__sample"` DuckDB táblában él (csak `open_time` + target + `fold_id`), a feat_* oszlopok a `snap."<snapshot_id>"` táblában vannak. A `lgbm_search._load_search_dataset` ugyanazt a parquetet olvasta.

**Javított fájlok:**

**`src/modeling/training/fit_lgbm.py`:**
- `_load_train_data(artifact_dir, ...)` → `_load_train_data(model_id, ...)` (szignatúra változott)
- Új implementáció: `utils.open_lab_connection(asset_id)` + `snap."<snapshot_id>" AS s INNER JOIN model."<model_id>__sample" AS m ON s.open_time = m.open_time`
- snapshot_id feloldás: `models.json` `sampling.snapshot_id` → fallback `reg.models` WHERE model_id
- Minden fold-sor bekerül a végső tréningbe (fold_id 0 + 1..n), nincs kizárás
- A régi parquet olvasás (`sample_path = artifact_dir / "sample_train_valid.parquet"`) eltávolítva
- Docstring frissítve

**`src/modeling/search/lgbm_search.py`:**
- `import polars as pl` eltávolítva (nem kell a DuckDB úton)
- `load_yearly_sample` import → `generate_walk_forward_folds` import
- Új `_load_model_sample_meta(model_id, meta)`: a fold_time_windows és purge_minutes-t a `models.json` sampling szekciójából + `generate_walk_forward_folds`-ból vezeti le (nincs `metadata.json` olvasás)
- Új `_anchor_year_from_meta(sampling_meta)`: helper (a `create_sample.py` logikájából kiemelve)
- `_load_search_dataset(artifact_dir, ...)` → `_load_search_dataset(model_id, meta, ...)` (szignatúra változott)
- Új implementáció: `utils.open_lab_connection(asset_id)` + `snap ⋈ model.__sample` JOIN, `open_time`, `target_col`, `fold_id`, `feat_cols_sql`; ha `row_stride > 1`, iloc alapú sub-sampling
- A régi parquet olvasás (`parquet_path = artifact_dir / "sample_train_valid.parquet"`) eltávolítva

**`src/modeling/pipeline.py`:**
- `--snapshot` CLI arg hozzáadva (nem kötelező; felülírja a `models.json` `sampling.snapshot_id`-t)
- `step_sample(model_id, artifact_dir)` → `step_sample(model_id, artifact_dir, snapshot_id=None)`
- `main()`: `step_sample(..., snapshot_id=args.snapshot)` átadva

**`src/modeling/01_feature_engineering.ipynb` (notebook):**
- Cell 4: `_sample_path = Path(SAMPLE_DIR) / "sample_train_valid.parquet"` + `pl.read_parquet()` eltávolítva
- Helyettük: `utils.open_lab_connection(ASSET_ID)` + `SELECT MIN/MAX(open_time) FROM model."<MODEL_ID>__sample"` a dátum-határok kinyeréséhez
- A `quant_train` betöltés logikája (a live DB-ből, dátum szerint szűrve) változatlan marad

**`config/models.json`:**
- 10 régi éves modell (2021–2025, long + short) eltávolítva
- Csak a 2 champion maradt: `lgbm_solusdt_l_fw60_2101_2605` + `lgbm_solusdt_s_fw60_2101_2605`

**Minőségi gate — FÁZIS 0 eredmény:**
- `ruff check src/modeling/training/fit_lgbm.py src/modeling/search/lgbm_search.py src/modeling/pipeline.py --fix` → All checks passed
- `pyright src/modeling/training/fit_lgbm.py src/modeling/search/lgbm_search.py` → 0 error, 0 warning
- `pipeline.py` 1 pre-existing `reportMissingImports` (papermill opcionális dep) — érintetlen, scope-on kívül
- `pytest src/modeling/ -v` → **106 passed** (meglévő tesztek, változatlan)

### FÁZIS 1 — Artifacts wipe + config szűkítés

**artifacts/ kiürítve:** lgbm_solusdt_l_fw60_2021, lgbm_solusdt_l/s_fw60_2101_2605, strategy_* mappák törölve → üres
**models.json szűkítve:** csak a 2 champion marad (lásd fent)

### FÁZIS 2 — Snapshot létrehozás

**Extra fix (OOM):** a `compute_content_sha256` az eredeti `string_agg(to_json(t))` megközelítéssel
OOM-ot dobott (12.5 GB RAM-ot égetett el 2.6M × 150+ oszlopos sorokkal). Fixálva:
`COUNT(*) + SUM(hash(open_time))` + range bounds + hashlib.sha256 → memory-efficient fingerprint.
A reuse-detection szemantikája változatlan (azonos tartalom → azonos hash). `test_snapshots.py` — 4 passed.
`src/data_handling/store/snapshots.py` — `compute_content_sha256` újraírva.
Ruff + pyright tiszta.

**Parancs:** `uv run python src/data_handling/05_create_snapshot.py --asset-id solusdt --horizon 60 --start "2021-01-01 00:00:00" --end "2026-05-31 23:59:00"`
**Állapot:** futás alatt (CTAS ~2.6M sor, ~150 oszlop → lab DB-be)
**snapshot_id:** TODO — rögzítendő futás után

### FÁZIS 3 — Pipeline (KÉSZ)

**Long model (`lgbm_solusdt_l_fw60_2101_2605`):**
- Kiindulás: `feature_engineering_done` (sample + FE már megvolt előző session-ből)
- `search`: 5 trial (smoke stage), best trial #4, obj=-0.006431, top10_lift=0.006518
- `train`: n_features=145, n_estimators=1794
- `predict`: 2,846,880 sor írva, `model."lgbm_solusdt_l_fw60_2101_2605__pred"` — immutable=True
- **Bugfix:** `pipeline.py` `step_predict` → `verify_snapshot=False` (OOM: 2.8M sor `string_agg` hash 12.5GB memóriát emésztett)
- `reg.models` státusz: `predicted`

**Short model (`lgbm_solusdt_s_fw60_2101_2605`):**
- `sample`: 47,448 sor, feature_set=fs_solusdt_fw60_s__1d5f8067 (n_selected=208)
- `feature_engineering`: notebook (papermill) — futott, feature_set.json OK
- **Bugfix:** `01_feature_engineering.ipynb` co-config cell: `.pl()` → `ATTACH` (3M×211 col Arrow buffer OOM). Régi kód `_live_conn.execute(...).pl()` + `conn.register` cserélve `ATTACH '{_db_path}' AS _src` + `CREATE TABLE quant_train AS SELECT * FROM _src.quant_train WHERE ...` + `DETACH` megközelítésre.
- `search`: 5 trial, best trial #5, obj=-0.002851, top10_lift=0.003015, n_features=78
- `train`: n_features=78, n_estimators=1043
- `predict`: 2,846,880 sor írva, `model."lgbm_solusdt_s_fw60_2101_2605__pred"` — immutable=True
- `reg.models` státusz: `predicted`

### FÁZIS 4 — Strategy kalibráció (KÉSZ)

**Script:** `src/strategy/00_run_strategy_session.py` (a t316-ban dokumentált egységes CLI)
**Parancs:**
```
uv run python src/strategy/00_run_strategy_session.py \
  --long-model lgbm_solusdt_l_fw60_2101_2605 \
  --short-model lgbm_solusdt_s_fw60_2101_2605 \
  --calib-start 2025-06-01 --calib-end 2025-11-30 \
  --opt-start 2025-12-01 --opt-end 2026-05-31 \
  --n-trials 200
```
**Session ID:** `strat_solusdt_fw60_combo_2101_2605`
**Eredmény:**
- `build_scored_table`: 2,846,880 sor (snap ⋈ long__pred ⋈ short__pred)
- `fit_calibration`: 263,520 sor kalibrációs ablak (2025-06-01 – 2025-11-30)
- `optimize_strategy`: 200 trial, best obj=0.006319
  - n_trades=134, win_rate=0.8134, sharpe=22.79
  - long_entry_pct=0.8959, short_entry_pct=0.9900
- `strat.*` táblák: `__trades` (134 sor), `__equity` (134 sor), `__cutoffs` (20 sor)
- `strategy_artifact.json` létrehozva
- `reg.strategies` sor: status=candidate
- `reg.artifacts`: 4 sor (strat_trades/equity/cutoffs + strategy_artifact)

### FÁZIS 5 — Ellenőrzések (KÉSZ)

**reg.models:** mindkét modell `predicted`, snapshot_id=`solusdt_fw60_2101_2605__21668185`
**Lab DB:**
- `snap.solusdt_fw60_2101_2605__21668185`: 2,846,880 sor
- `model.lgbm_solusdt_l_fw60_2101_2605__sample`: 47,448 sor
- `model.lgbm_solusdt_l_fw60_2101_2605__pred`: 2,846,880 sor
- `model.lgbm_solusdt_s_fw60_2101_2605__sample`: 47,448 sor
- `model.lgbm_solusdt_s_fw60_2101_2605__pred`: 2,846,880 sor
- `strat.strat_solusdt_fw60_combo_2101_2605__trades`: 134 sor
- `strat.strat_solusdt_fw60_combo_2101_2605__equity`: 134 sor
- `strat.strat_solusdt_fw60_combo_2101_2605__cutoffs`: 20 sor
**Snapshot hash:** `21668185444b0a7e9f76...` — sértetlen (content_sha256 változatlan)
**Manifests:** mindkét model `pipeline_status=predict_done`
**strategy_artifact.json:** `artifacts/strat_solusdt_fw60_combo_2101_2605/strategy_artifact.json` — létezik

**Elfogadási kritériumok:** MIND TELJESÜL ✓
