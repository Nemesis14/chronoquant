# ChronoQuant Engineering Workflow

Ez a dokumentum a teljes modell- és stratégiafejlesztési folyamat térképe.
Részletes szabályok a domain-specifikus útmutatókban:

- `docs/modeling/sampling.md`: adattartomány, sample, split, holdout szabályok.
- `docs/modeling/lightgbm_development.md`: LightGBM modell fejlesztés és promóció.
- `docs/evaluation/strategy_evaluation.md`: trigger sweep, backtest, stratégia kiértékelés.

## Általános szabályok

- Parancsokat a repo gyökérből futtasd.
- Vékony scriptek, újrafelhasználható logika `src/` alatt.
- Config betöltés kizárólag `src/utils.py`-on keresztül.
- Generált artifactok a meglévő artifact könyvtárakban maradnak.
- Kerüld a feladathoz nem kapcsolódó refaktorokat.

---

## Mikor kell új modellt fejleszteni?

Új fejlesztési ciklus indul, ha:

- Az adatforrás lényegesen megváltozott (pl. spot → futures OHLCV átállás).
- Elegendő új adat áll rendelkezésre (általában 6+ hónap).
- A jelenlegi modell teljesítménye romlott (live monitoring jelzi).
- Új feature-ok vagy target definíciók kerülnek bevezetésre.

---

## Teljes end-to-end folyamat

A `[COLAB]` jelölésű lépések számításigényesek és Google Colab-on futnak.
A `[LOCAL]` jelölésűek a helyi gépen futnak.

### 1. Adataudit `[LOCAL]`

Ellenőrizd a features tábla állapotát mielőtt bármit elindítasz:

The legacy SQLite feature audit entry point was removed. Use the current
Parquet/DuckDB validation path before model work.

Kézzel is ellenőrizhető:

```python
import sqlite3, pandas as pd, sys; sys.path.insert(0, "src")
import utils
db_cfg = utils.load_asset_config("solusdt_fw60")["database"]
with sqlite3.connect(db_cfg["db_path"]) as conn:
    r = pd.read_sql_query(
        "SELECT MIN(open_time) as start, MAX(open_time) as end, COUNT(*) as rows, "
        "SUM(CASE WHEN trg_l_fw60_q90 IS NULL THEN 1 ELSE 0 END) as null_l "
        "FROM solusdt_1m_features",
        conn,
    )
print(r.to_string(index=False))
```

Ellenőrzési szempontok: adattartomány, sorszám, target null arány, duplikált
`open_time` értékek, hiányok az idősorban.

---

### 2. Sample és split `[LOCAL]`

#### Mikor kell új sample_id?

Új `sample_id` kell, ha az alábbiak bármelyike lényegesen megváltozott:
- adatforrás (tábla, asset, spot/futures váltás)
- target horizon
- label definíció
- holdout határok
- adat minőségi javítás, ami a tartomány nagy részét érinti

Összehasonlítható modellek (pl. long és short azonos horizon-on) **ugyanazt**
a `sample_id`-t használják.

#### Sample létrehozása

```bash
python scripts/create_sample_splits.py \
    --sample-id base_solusdt_fw60_futures_v1 \
    --asset-id  solusdt_fw60 \
    --target-horizon-minutes 60
```

Ellenőrzés:

```bash
python -c "
import json
d = json.load(open('samples/base_solusdt_fw60_futures_v1/folds.json'))
print(len(d['folds']), 'folds, test:', d['test'])
"
```

---

### 3. Model registry bejegyzés `[LOCAL]`

Add hozzá az új kandidáltakat `config/models.json`-hoz `active: false` jelzéssel
mielőtt Colab-on futtatod a keresést. A notebook ellenőrzi, hogy megvannak-e.

```json
"lgbm_solusdt_l_fw60_q90_local_v4": {
    "asset_id": "solusdt_fw60",
    "target_name": "trg_l_fw60_q90",
    "family": "lightgbm",
    "variant": "local_v4",
    "trainer": "lightgbm_binary",
    "paths": {
        "model_dir": "models/lgbm_solusdt_l_fw60_q90_local_v4",
        "model_file": "model.pkl",
        "features_file": "features.json"
    },
    "predict": { "method": "predict_proba", "proba": true },
    "training": {
        "sample_id": "base_solusdt_fw60_futures_v1",
        "sample_dir": "samples/base_solusdt_fw60_futures_v1",
        "row_stride": 60
    },
    "active": false
}
```

---

### 4. Colab előkészítés `[LOCAL]`

#### 4a. Parquet export + Drive másolás

The legacy SQLite sample export entry point was removed. Use the current
Parquet/DuckDB sample export path once it is redefined.

Ez létrehozza: `samples/base_solusdt_fw60_futures_v1/dataset.parquet`
és átmásolja: `F:\My Drive\chronoquant\samples\base_solusdt_fw60_futures_v1\dataset.parquet`

#### 4b. Commit + push

A notebook klónozza a GitHub repót — ezért a model registry bejegyzéseket,
a sample definíciókat, és a config módosításokat push-olni kell mielőtt elindítod.

```bash
git add config/models.json samples/base_solusdt_fw60_futures_v1/
git commit -m "add v4 model registry entries and updated sample"
git push
```

#### 4c. Colab URL megnyitása

```
https://colab.research.google.com/github/Nemesis14/chronoquant/blob/main/notebooks/colab_training.ipynb
```

Frissítsd a notebook **CONFIG** celláját a helyes model ID-kkal és sample ID-val,
majd futtasd az összest: **Ctrl+F9**.

---

### 5. Hyperparameter search `[COLAB]`

A notebook automatikusan elvégzi:
- Smoke test (5 trial, 2 fold) — long + short modellekre
- Explore (60 trial, összes fold, ~2-3 óra) — long + short modellekre
- Artifacts mentése: `Drive/models/<model_id>/search/`

Ha a Colab session megszakad, a search automatikusan folytatódik a legutóbbi
trial-tól (`--resume` implicit módon működik).

---

### 6. Artifacts visszamásolása `[LOCAL]`

```
F:\My Drive\chronoquant\models\<model_id>\  →  models\<model_id>\
```

Ellenőrizd, hogy megvannak-e:

```bash
python -c "
from pathlib import Path
for mid in ['lgbm_solusdt_l_fw60_q90_local_v4', 'lgbm_solusdt_s_fw60_q10_local_v4']:
    p = Path(f'models/{mid}/search')
    files = ['search_best.json', 'search_trials.jsonl', 'features_search.json']
    for f in files:
        fp = p / f
        print(mid, f, fp.stat().st_size if fp.exists() else 'MISSING')
"
```

---

### 7. Model validáció `[LOCAL]`

#### Search review

```bash
python -c "
import json
for mid in ['lgbm_solusdt_l_fw60_q90_local_v4', 'lgbm_solusdt_s_fw60_q10_local_v4']:
    best = json.load(open(f'models/{mid}/search/search_best.json'))
    print(f'{mid}')
    print(f'  trial:  #{best[\"trial_no\"]}')
    print(f'  prauc:  {best.get(\"mean_valid_prauc\", \"N/A\")}')
    print(f'  val_ll: {best.get(\"mean_valid_ll\", \"N/A\"):.4f}')
    print(f'  gap:    {best.get(\"mean_gap\", \"N/A\"):.4f}')
"
```

Promóciós minőségű kandidált jellemzői:
- Validációs log loss és train/valid gap alacsony
- PR AUC és ROC AUC elfogadható
- Top percentilis lift megvan
- Fold-to-fold stabilitás
- Usable predikció eloszlás a várható strategy threshold-ok körül

#### Feature importance review

Lásd `docs/modeling/lightgbm_development.md` — 4. lépés.

---

### 8. Final fit `[LOCAL]`

A kiválasztott feature lista és paraméterek alapján production artifact elkészítése.
Lásd `docs/modeling/lightgbm_development.md` — 5. lépés.

---

### 9. Prediction sync `[LOCAL]`

```python
import sys; sys.path.insert(0, "src")
from data_pipeline.sync_predictions import sync_predictions
sync_predictions(start_time="2025-06-01 00:00:00", end_time=None, asset_id="solusdt_fw60")
```

Csak a runtime modell predikciói kerülnek a live predictions táblába.
Kandidált predikciók a live táblától elkülönítve tárolódnak.

---

### 10. Strategy evaluation `[LOCAL]` (sweep compute-igényes esetén `[COLAB]`)

#### Trigger sweep

```bash
python scripts/sweep_strategy.py \
    --model-id lgbm_solusdt_l_fw60_q90_local_v4 \
    --asset-id solusdt_fw60 \
    --start 2024-01-01 \
    --end   2025-12-31 \
    --side  long \
    --top-n 20
```

Sweep dátumokat a window policy szerint válaszd meg — lásd
`docs/evaluation/strategy_evaluation.md`.

#### Holdout report

A kiválasztott trigger **változtatás nélkül** futtatva az untouched holdout-on.
Ne tunningolj a holdout eredménye alapján.

---

### 11. Model card generálás `[LOCAL]`

```bash
python scripts/generate_model_card.py \
    --model-id      lgbm_solusdt_l_fw60_q90_local_v4 \
    --side          long \
    --holdout-start "2025-06-05 00:00:00" \
    --holdout-end   "2026-06-09 00:00:00" \
    --entry         0.45 \
    --max-hold      60
```

---

### 12. Config update `[LOCAL]`

Kizárólag validáció és holdout review után:

```bash
# config/models.json: régi active=false, új active=true
# config/env.json:    runtime model_id frissítés
# config/strategies.json: új strategy bejegyzés első helyre
```

---

### 13. UI verification `[LOCAL]`

```python
import sys; sys.path.insert(0, "src")
from streamlit_app.data import load_dashboard_config
cfg = load_dashboard_config(asset_id="solusdt_fw60")
assert cfg["runtime_model_id"] == "lgbm_solusdt_l_fw60_q90_local_v4"
print("entry_threshold:", cfg["strategy"]["entry_threshold"])
```

Indítsd el a dashboardot és ellenőrizd a legfrissebb predikciót, aktív stratégiát,
és a backtest/report összefoglalókat.

```bash
streamlit run src/streamlit_app/main.py
```

---

## Adatváltozás workflow

1. Config és implementáció együtt frissül.
2. Derived tables rebuild bounded intervallumon ha lehetséges.
3. Validáld a sorokat, szükséges oszlopokat, duplikált `open_time` értékeket és
   hiányokat mielőtt az adatot modellezésre használod.

## Modellváltozás összefoglalás

1. Shared dataset loading és sampling definíciók.
2. Kandidáltak először inactive modellként.
3. Kandidált értékelési outputok a live predictions táblától elkülönítve.
4. Runtime modellek promóciója csak validáció, holdout review és
   strategy/backtest összehasonlítás után.

Új modellek kizárólag LightGBM-mel fejleszthetők.
Logisztikus regressziós trainerek (lasso, p-value) legacy alapvonalak;
ne kezdj új fejlesztést velük.
