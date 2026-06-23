# Arch Spec t41 — Sample-scoped Modeling Architecture

Epic: epic_034 | Status: specifikált | Dátum: 2026-06-22

---

## 1. Döntés: A vs B

**Döntés: A megközelítés — a FE inputja közvetlenül a `model.__sample` sorokra épített
temp tábla, nem egy külön nézet/materialized tábla.**

Konkrétan: `snap."<snapshot_id>"` INNER JOIN `model."<model_id>__sample"` ON `open_time`
→ TEMP TABLE `quant_train` — csak az egyező sorok kerülnek bele.

### Indoklás a kódbázis tényeivel

Az A megközelítés infrastruktúrája **már létezik és helyes** a kódbázisban:

- `src/modeling/feature_engineering/sample_scope.py` — `materialize_sample_scoped_quant_train()`
  pontosan ezt végzi: `snap ⋈ model.__sample` INNER JOIN on `open_time`, majd ellenőrzi
  hogy a kapott rowcount == `sample_row_count`. Ez garantálja, hogy sem extra sor nem
  kerül be, sem sor el nem vész (a `RuntimeError` pont erre figyel).

- `src/modeling/01_feature_engineering.ipynb` — a config cella hívja ezt a függvényt,
  és a `feature_set.json`-ba beírja a `provenance.source_contract: "snap ⋈ model.__sample"`
  mezőt.

- `src/modeling/search/lgbm_search.py` `_load_search_dataset()` és
  `src/modeling/training/fit_lgbm.py` `_load_train_data()` — mindkettő ugyanezt a
  `snap ⋈ model.__sample` join-t alkalmazza.

- `src/modeling/predict.py` `predict_offline()` — a teljes snapshot range-en futtat,
  **nem** a sample sorain (ez helyes: a predict step célja az összes historikus bar
  előrejelzése, nem csak a training sample-é).

A B megközelítés (külön nézet/tábla) felesleges komplexitást adna: a `quant_train` TEMP
TABLE már az A megközelítés eredménye, és a FE modulok (`analyze_quality`,
`analyze_target_relation`, `analyze_redundancy`, `analyze_stability`) ezen operálnak —
nincs szükség egy újabb indirects rétegre.

### Meglévő technikai adósság (t42 célpontja)

A t42 task notes azt rögzíti: "a `model.__sample`-ből csak MIN/MAX időhatárt olvas, majd
azon a tartományon teljes `quant_train`-t használ." Ez a **régi állapot** leírása. A
jelenlegi kódban a `materialize_sample_scoped_quant_train` már az új, helyes logikát
implementálja. A t42 feladata ennek **teljes körű és konzisztens alkalmazásának
biztosítása** — elsősorban annak ellenőrzése, hogy a notebook valóban ezt a függvényt
hívja és a régi (időhatár-alapú) path sehol nem marad fenn.

---

## 2. Hivatalos adatfolyam

```
live.quant_train (mutable, teljes history)
    │
    ▼ 05_create_snapshot.py
snap."<snapshot_id>" (immutable, frozen range)
    │
    ├─── [sample step] 00_create_sample.py
    │        → model."<model_id>__sample"
    │          (open_time, target_col(s), fold_id — hourly select + WF folds)
    │          Regisztráció: reg.feature_sets (selected_cols), reg.models (sampled)
    │
    ├─── [feature_engineering step] 01_feature_engineering.ipynb (via pipeline.py)
    │        Input: snap ⋈ model.__sample → TEMP TABLE quant_train
    │        Műveletek: quality → target_relation → redundancy → stability
    │        Output: artifacts/<model_id>/feature_engineering/feature_set.json
    │        Regisztráció: reg.feature_sets update (selected_cols végleges)
    │                      reg.models (feature_set_id link)
    │
    ├─── [search step] 02_hyper_param_search.py
    │        Input: snap ⋈ model.__sample (feat_* + target + fold_id)
    │               Feature lista: feature_set.json["selected"]
    │        Output: artifacts/<model_id>/search/best_params.json, search_best.json
    │        Regisztráció: reg.search_runs
    │
    ├─── [train step] 03_fit_model.py
    │        Input: snap ⋈ model.__sample (minden fold sor, fold_id 0..n)
    │               Params: best_params.json; Features: feature_set.json["selected"]
    │        Output: artifacts/<model_id>/model.pkl, features.json, params.json
    │        Regisztráció: reg.models (trained)
    │
    └─── [predict step] pipeline.py --step predict
             Input: snap."<snapshot_id>" TELJES range (nem sample-korlátozott!)
                    Features: features.json["features"]
             Output: model."<model_id>__pred" (open_time, pred)
             Regisztráció: reg.models (predicted), reg.artifacts
```

**Fontos asszimmetria a predict lépésnél:** A predict step szándékosan az egész
snapshot range-et score-olja, nem csak a sample sorait. Ez helyes: az offline prediction
célja az összes historikus bar előrejelzése (utólagos kiértékeléshez, strategy
backtesthez). A sample scope csak a **modell-fejlesztési** lépéseket köti (FE, search,
train).

---

## 3. Invariánsok

### I1 — Sample rowcount conservation (FE input)
A `materialize_sample_scoped_quant_train` hívásakor:
`COUNT(quant_train) == COUNT(model."<model_id>__sample")`

Ha a count nem egyezik: `RuntimeError` — nem szabad csendben folytatni.
A jelenlegi implementáció már kikényszeríti ezt (`sample_scope.py` L72-76).

### I2 — Sample rowcount conservation (search / train input)
A `snap ⋈ model.__sample` JOIN a search és train lépésekben pontosan annyi sort ad,
mint amennyi a `model.__sample`-ben van. Ennek ellenőrzése logging szinten
dokumentálandó (jelenleg nincs explicit assert — t43 pontosíthatja).

### I3 — Snapshot immutability
A `snap."<snapshot_id>"` tábla írás után soha nem módosul. A `predict_offline` ezt
aktívan ellenőrzi (content_sha256 before/after összehasonlítás, ha `verify_snapshot=True`).
Más lépések (FE, search, train) nem írnak a snap sémába.

### I4 — Feature-scope konzisztencia
A `feature_set.json["selected"]` lista ugyanazokat a `feat_*` oszlopokat tartalmazza,
amelyeket a search, a train és a `features.json` is használ. Ez a provenance lánc:
`reg.feature_sets.selected_cols == feature_set.json["selected"] == features.json["features"]`

### I5 — fold_id traceability
A `model."<model_id>__sample"` tartalmaz `fold_id` oszlopot (Int8, 0 = train-only,
1..n = walk-forward validation fold). A search lépés erre épít. A train lépés az összes
fold sort (0..n) felhasználja a final production fit-hez.

### I6 — target_col egy modellhez rögzített
Egy `model_id`-hez pontosan egy `target_name` tartozik (config/models.json). A FE,
search, train, predict mind ugyanezt a target_col-t használja. A `model.__sample`
tartalmazza a target_col-t (nem csak open_time + fold_id).

### I7 — Provenance trace-ability
Minden FE output (`feature_set.json`) tartalmaz egy `provenance` blokkot:
- `snapshot_id`: melyik snapshot-ból jött az input
- `sample_table`: melyik `model.__sample` tábla volt a scope
- `sample_rows` / `joined_rows`: rowcount (== konzisztencia ellenőrzés)
- `min_open_time` / `max_open_time`: az input időtartama
- `source_contract: "snap ⋈ model.__sample"`: explicit contract deklaráció

---

## 4. Kötelező provenance mezők és intermediate objektumok

### 4.1 Intermediate DuckDB objektumok

| Objektum | Schema | Séma | Élettartam | Gazdája |
|---|---|---|---|---|
| `<snapshot_id>` | `snap` | `open_time, feat_*, target_cols, ...` | Perzisztens, immutable | snapshot layer |
| `<model_id>__sample` | `model` | `open_time, target_col(s), fold_id` | Perzisztens (újraírható) | sampling step |
| `quant_train` (TEMP) | — | `open_time, feat_*, target_col(s), fold_id` | Session-scoped TEMP TABLE | FE step (csak a notebookban) |
| `<model_id>__pred` | `model` | `open_time, pred` | Perzisztens (újraírható) | predict step |

**A TEMP TABLE `quant_train` scope-ja**: csak az FE notebook session-jén belül él.
A search/train/predict nem erre támaszkodik — azok direkt SQL-lel olvasnak a snap + model
sémákból.

### 4.2 File artifacts

| Artifact | Path | Producer | Consumer |
|---|---|---|---|
| `manifest.json` | `artifacts/<model_id>/manifest.json` | setup step | minden lépés |
| `feature_set.json` | `artifacts/<model_id>/feature_engineering/feature_set.json` | FE step | sampling (re-run), search, train |
| `best_params.json` | `artifacts/<model_id>/search/best_params.json` | search step | train |
| `search_best.json` | `artifacts/<model_id>/search/search_best.json` | search step | train |
| `model.pkl` | `artifacts/<model_id>/model.pkl` | train step | predict, strategy |
| `features.json` | `artifacts/<model_id>/features.json` | train step | predict |

### 4.3 Registry táblák

| Tábla | Kulcs | Mikor íródik |
|---|---|---|
| `reg.snapshots` | `snapshot_id` | snapshot létrehozáskor |
| `reg.feature_sets` | `feature_set_id` | sampling (draft), FE (végleges) |
| `reg.models` | `model_id` | setup (draft) → sample (sampled) → train (trained) → predict (predicted) |
| `reg.search_runs` | `search_run_id` | search step |
| `reg.artifacts` | `artifact_id` | setup, FE, search, train, predict |

### 4.4 Kötelező provenance mezők a feature_set.json-ban

```json
{
  "provenance": {
    "snapshot_id":     "<az adott model snapshot_id-je>",
    "sample_table":    "model.\"<model_id>__sample\"",
    "sample_rows":     <int — model.__sample rowcount>,
    "joined_rows":     <int — snap ⋈ model.__sample rowcount, == sample_rows>,
    "min_open_time":   "<YYYY-MM-DD HH:MM:SS>",
    "max_open_time":   "<YYYY-MM-DD HH:MM:SS>",
    "source_contract": "snap ⋈ model.__sample"
  }
}
```

A `sample_rows == joined_rows` feltétel a `materialize_sample_scoped_quant_train`
RuntimeError-rel kikényszeríti (I1 invariáns).

---

## 5. Aktuális állapot vs. kívánt állapot összefoglalása

| Lépés | Jelenlegi állapot | Kívánt állapot | Gap |
|---|---|---|---|
| snapshot | OK | OK | — |
| sample | OK (`create_model_sample` snap-native) | OK | — |
| FE input materializáció | OK (`materialize_sample_scoped_quant_train` implementálva) | OK | — |
| FE notebook pipeline hívás | Megvizsgálandó — az `01_feature_engineering.ipynb` config cellája hívja a `materialize_sample_scoped_quant_train`-t, de a teljes pipeline-on belüli integráció konzisztenciája t42 feladata | OK | t42 ellenőrzi |
| search input | OK (snap ⋈ model.__sample JOIN, `lgbm_search.py`) | OK | — |
| train input | OK (snap ⋈ model.__sample JOIN, `fit_lgbm.py`) | OK | — |
| predict input | OK (teljes snap range, szándékosan) | OK | — |
| `datasets.py` load_modeling_dataset | LEGACY — `feat_ohlcv_quant` + `target` táblákat olvas, nem snapshot-native | Nem aktív path a pipeline.py-ban, de t43 auditálandó | t43 scope |
| provenance feature_set.json | OK (source_contract mező benne van) | OK | — |
| strategy contract | t44 auditálja | — | t44 scope |

---

## 6. Implementációs útmutatás a következő taskokhoz

### t42 (FE input refaktor)
- Ellenőrizni, hogy a `pipeline.py step_feature_engineering` által futtatott notebook
  **valóban** a `materialize_sample_scoped_quant_train` hívással indul (a jelenlegi
  notebook config cellája igen, de papermill-paraméterezés és a teljes execution path
  auditálandó).
- Biztosítani, hogy a FE library modulok (`analyze_quality`, stb.) kizárólag a TEMP
  `quant_train` táblán operálnak, és nem olvasnak semmilyen másik DuckDB táblát
  közvetlenül.
- A `SampleScopeMeta` visszatérési értéke (különösen a `row_count` és `sample_row_count`)
  kerüljön a `feature_set.json["provenance"]` blokkba (jelenleg igen, ellenőrzés szükséges).

### t43 (downstream modeling contract)
- A `datasets.py` `load_modeling_dataset` legacy path auditálása: a pipeline.py
  nem hívja ezt a search/train lépéseknél (azok snapshot-native path-t használnak),
  de el kell dönteni, hogy deprecated-ként kell-e jelölni.
- Assert-ek / logging hozzáadása az I2 invariánshoz (search/train rowcount vs.
  sample rowcount).

### t44 (strategy contract)
- Ellenőrizni, hogy a strategy réteg a `model."<model_id>__pred"` táblát olvassa
  (a teljes snapshot range predikciói), és nem `model.__sample` soraira támaszkodik.
- A predict step scope (teljes snapshot range, nem sample-korlátozott) explicit
  dokumentálása a strategy contract szempontjából helyes.

### t45 (docs + provenance)
- `_doc_/database_and_code_doc/` frissítése az itt rögzített adatfolyammal.
- Az I1-I7 invariánsok és a provenance mezők a metodológiai docba kerüljenek.
