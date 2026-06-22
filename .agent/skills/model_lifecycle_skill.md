# Model Lifecycle Skill

Playbook az új modell buildeléshez és a részleges retrain döntésekhez.
A „miértek" és a teljes architektúra-leírás: `_doc_/database_and_code_doc/0004_model_lifecycle.md`.

---

## Mikor kell ezt a skill-t betölteni

- Új modell indítása (új range / új feature_set / új verzió)
- Részleges retrain döntés (mi változott → mit kell újrafuttatni)
- Meglévő modell újra-élesítése (kész modell, nincs retrain)

---

## Részleges-retrain döntési tábla

A registry hash-ek (`content_sha256` a snapshotban, `feature_set_id` a feature_sets-ben)
automatikusan detektálják, mit lehet újrahasználni — ha a hash egyezik, a lépés kihagyható.

| Mi változott | Snapshot | Sample | FE | Search | Train | Predict | Deploy |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| Csak hyperparam | — | — | — | Futtasd | Futtasd | Futtasd | Futtasd |
| Új feature_set (ugyanaz a range) | — | — | Futtasd | Futtasd | Futtasd | Futtasd | Futtasd |
| Új range | Futtasd | Futtasd | Futtasd | Futtasd | Futtasd | Futtasd | Futtasd |
| Csak újra-élesítés (kész modell) | — | — | — | — | — | — | Futtasd |

**Amit sosem kell újra:** ha a `snapshot_id` + `feature_set_id` ugyanaz és a hash egyezik
a registryben, a korábbi sample / FE / search eredmény újrahasználható. A snapshot immutable —
ne DROP-old, csak archivált státuszra állítsd.

---

## Pipeline lépések sorrendben

### 0. Setup — manifest + reg.models draft

- [ ] `models.json`-ba bejegyzés: `model_id`, `snapshot_id`, `direction`, `horizon`
- [ ] Pipeline futtatás: `uv run python src/modeling/pipeline.py --model <model_id> --step setup`
- [ ] Eredmény: `artifacts/<model_id>/manifest.json` létrejön; `reg.models` status=`draft`

### 1. Snapshot (ha kell — lásd döntési tábla)

- [ ] `uv run python src/data_handling/05_create_snapshot.py --asset solusdt --range <YYMM_start>_<YYMM_end>`
- [ ] Ellenőrizd: `reg.snapshots` bejegyzés megvan, `content_sha256` kitöltve
- [ ] A `snapshot_id` (`{asset}_fw{h}_{range}__{hash8}`) kerül a `models.json` `sampling.snapshot_id` mezőjébe

### 2. Sample (ha kell — lásd döntési tábla)

- [ ] `uv run python src/modeling/pipeline.py --model <model_id> --step sample`
- [ ] Ellenőrizd: `model."<model_id>__sample"` tábla létrejött a lab DB-ben (hourly + fold_id)
- [ ] `reg.models` status=`sampled`; `reg.feature_sets` bejegyzés létrejött

### 3. Feature Engineering (ha kell — lásd döntési tábla)

- [ ] `uv run python src/modeling/pipeline.py --model <model_id> --step feature_engineering`
- [ ] Ellenőrizd: `artifacts/<model_id>/feature_engineering/feature_set.json` megvan
- [ ] `reg.feature_sets` `selected_cols` + `n_selected` frissítve; `feature_set_id` a manifest-ben

### 4. Search (ha kell — lásd döntési tábla)

- [ ] `uv run python src/modeling/pipeline.py --model <model_id> --step search`
- [ ] Ellenőrizd: `artifacts/<model_id>/search/search_best.json` megvan
- [ ] `reg.search_runs` bejegyzés: `best_params` + `objective`

### 5. Train

- [ ] `uv run python src/modeling/pipeline.py --model <model_id> --step train`
- [ ] Ellenőrizd: `artifacts/<model_id>/model.pkl` megvan; `metrics.json` OOS metrika elfogadható
- [ ] `reg.models` status=`trained`, `oos_metric` kitöltve

### 6. Predict (offline, teljes snap range)

- [ ] `uv run python src/modeling/pipeline.py --model <model_id> --step predict`
- [ ] Ellenőrizd: `model."<model_id>__pred"` tábla létrejött (`open_time`, `pred`)
- [ ] Snapshot hash sértetlen (a predict nem módosítja a snap táblát)
- [ ] `reg.models` status=`predicted`

### Deploy — lásd `deploy_skill.md`

- [ ] A modell `predicted` státuszban van → átadhatsz a deploy flow-nak

---

## Registry állapot-ellenőrzés (bármely lépés után)

```sql
SELECT model_id, status, snapshot_id, feature_set_id, oos_metric
FROM reg.models WHERE model_id = '<model_id>';
```

---

## Hivatkozás

- Teljes lifecycle rationale és architektúra: `_doc_/database_and_code_doc/0004_model_lifecycle.md`
- Snapshot + registry kód-referencia: `_doc_/database_and_code_doc/1410_snapshots_code.md`, `1510_registry_code.md`
- Deploy lépések: `.agent/skills/deploy_skill.md`
