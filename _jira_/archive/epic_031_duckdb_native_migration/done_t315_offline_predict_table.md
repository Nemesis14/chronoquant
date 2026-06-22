---
epic: epic_031
id: t315
title: Offline predict tábla — model.__pred
assignee: modeling_agent
status: todo
blocks: [t316, t317, t324]
blocked_by: [t313, t314]
---

## Goal
A train utáni predikciós lépés a snapshot teljes range-ét scorolja, és külön
`model."<model_id>__pred"` táblába írja (open_time, pred) — NEM a snapshotba fúzva,
hogy a snapshot immutable és reprodukálható maradjon.

## Scope
- `src/modeling/` predict lépés — snap range scorolás a model.pkl-lel
- `model."<model_id>__pred"` tábla írás
- Hivatkozás: `_doc_/_plans_/data_process_architecture.md` 5 (6. lépés)

## Acceptance Criteria
- [ ] `model."<model_id>__pred"` tábla keletkezik a teljes snap range-re
- [ ] a snapshot tábla változatlan (hash sértetlen)
- [ ] `snap ⋈ model.__pred` join helyes (open_time-on)
- [ ] reg.models státusz → predicted
- [ ] smoke teszt + ruff + pyright tiszta

## Notes

### Elvégezve (modeling_agent)

**Létrehozott / módosított fájlok:**
- `src/modeling/predict.py` (**új**) — az offline predict lépés. Belépési pont:
  `predict_offline(model_id: str, verify_snapshot: bool = True) -> dict`. Betölti a
  `model.pkl` + `features.json` (`features` lista) artefaktot, feloldja a
  `snapshot_id`-t (reg.models, fallback `sampling.snapshot_id`), beolvassa a snap
  teljes range-ét (`open_time` + a kiválasztott feature oszlopok), scorol, és
  `CREATE OR REPLACE TABLE model."<model_id>__pred"` táblába írja `(open_time, pred)`.
  A `utils.open_lab_connection`-ön át dolgozik (config-gateway). Visszatérés:
  `{model_id, snapshot_id, pred_table, n_rows, n_features, snapshot_immutable}`.
- `src/modeling/sampling/snapshot_sampler.py` — új IO-mentes `pred_table_fqn(model_id)`
  builder (`model."<model_id>__pred"`), exportálva a `sampling/__init__.py`-ből.
- `src/modeling/pipeline.py` — új `predict` lépés: `ALL_STEPS` végére fűzve, `step_predict`
  hívja a `predict_offline`-t, manifest státusz `predict_done`.
- `src/modeling/training/fit_lgbm.py` — a régi `_add_predictions_to_sample` (a `pred_{dir}`
  oszlopot a `sample_train_valid.parquet`-be fúzta) **eltávolítva**; a predikció most
  külön `model.__pred` táblába megy a predict lépésben. `_load_train_data` egyszerűsítve
  (nem ad vissza felesleges `sample_df`-et).
- `src/modeling/tests/smoke/test_predict.py` (**új**) — 7 smoke teszt szintetikus
  in-memory snap + kis LightGBM modellel.

**A predict lépés helye / szignatúrája:** `modeling.predict.predict_offline(model_id, verify_snapshot=True)`.
A pipeline-ből a `predict` step (`pipeline.step_predict`) hívja; CLI:
`uv run python src/modeling/pipeline.py --model <id> --step predict`.

**`model."<model_id>__pred"` séma:** két oszlop — `open_time` (a snap open_time-ja),
`pred` (DOUBLE-ra castolva). NEM tartalmaz feature-t és NEM fúzódik a snapshotba.
`snap ⋈ model.__pred` join az `open_time`-on 1:1 (a predict a teljes snap range-et scorolja).

**Snapshot-immutability ellenőrzés módja:** a predict előtt és után újraszámolja a snap
tábla content-hash-ét ugyanazzal a sémával, mint a snapshot réteg
(`to_json(row)` soronként, `ORDER BY open_time`, `sha256(string_agg(...))`), és összeveti.
Eltérés esetén `ValueError` (immutability violation). A snapshot tábla csak olvasásra
nyitott; a predict kizárólag a külön `model.__pred` táblát írja. Teszt
(`test_predict_keeps_snapshot_immutable`) igazolja, hogy a hash a predict után = az előtte.

**reg státusz frissítés:** a predict végén `provenance.set_model_status(model_id, "predicted", asset_id=...)`
(a t314 gateway, MODEL_STATUS_CHAIN utolsó eleme). A pred tábla a `reg.artifacts`-ba is
bekerül `kind='pred_table'`, `path=model."<id>__pred"` néven (best-effort upsert).

**Teszt eredmény:** `test_predict.py` — 7 passed. Lefedi: teljes range pred tábla
(`open_time`,`pred`, sorszám = snap sorszám), snapshot-immutability, snap⋈pred join,
reg.models→predicted, reg.artifacts pred_table, determinizmus (re-run azonos pred),
ismeretlen model hibaág.

**Eredmény:** `ruff check src/modeling/ --fix` — a módosított/új fájlok tiszták (1 megmaradt
B017 pre-existing main-en, `test_walk_forward_config.py`, érintetlen, scope-on kívül).
`pyright` az új/módosított fájlokra — 0 error (pipeline.py-ben 1 pre-existing
`reportMissingImports` a lazy `papermill` importnál — opcionális dep, nem érintett).
Teljes `src/modeling/` pytest: **106 passed** (a 7 új predict smoke + a meglévő 99),
pre-existing piros nincs.

**Döntések / feltételezések:**
- A pred tábla `CREATE OR REPLACE` — determinisztikus re-run (fix model + immutable
  snap → bit-identikus pred). A `model.__pred` SCHEMA a lab DB-ben (a `snap`/`__sample`
  mintáját követi; `CREATE SCHEMA IF NOT EXISTS model`).
- A feature-bemenet a `features.json` `features` listája (= a FE-szelektált feature_set
  ténylegesen betanított oszlopai), a snapshotból projektálva. A snapshotban benne van a
  feature szuperszett, így a projekció ingyenes (columnar).
- A predict best-effort regisztrálja a pred táblát a `reg.artifacts`-ba; a státusz-flip
  (`predicted`) viszont nem best-effort — az a lépés explicit eredménye.

**Acceptance Criteria — mind teljesült:**
- [x] `model."<model_id>__pred"` tábla keletkezik a teljes snap range-re
- [x] a snapshot tábla változatlan (content_sha256 a predict után = előtte; verifikálva)
- [x] `snap ⋈ model.__pred` join helyes (open_time-on, 1:1)
- [x] reg.models státusz → predicted
- [x] smoke teszt (7) + ruff + pyright tiszta
