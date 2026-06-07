# LightGBM Model Development & Promotion Workflow

Ez a dokumentum lépésről lépésre leírja a folyamatot egy új SOLUSDT LightGBM modell
fejlesztéséhez, validálásához és élesbe helyezéséhez. A `lgbm_solusdt_l_fw60_q90_local_v2`
fejlesztésén alapul (2026-06-07).

---

## Áttekintés

```
Feature audit → Hyperparameter search → Model validálás → Final fit → Predictions sync → Strategy sweep → Élesítés → Dokumentáció
```

---

## 0. Előfeltételek

### 0a. Feature táblázat ellenőrzése

Ellenőrizd, hogy az összes tervezett feature elérhető-e a DB-ben:

```bash
python scripts/feature_audit.py --asset-id solusdt_fw60
```

Vagy manuálisan:

```python
import sqlite3, pandas as pd
with sqlite3.connect("database/solusdt_data_dev.db") as conn:
    cols = pd.read_sql_query("PRAGMA table_info(solusdt_1m_features)", conn)
feat_cols = cols[cols['name'].str.startswith('feat_')]
print(f"feat_ cols: {len(feat_cols)}")
print(cols[cols['name'].str.startswith('trg_')]['name'].tolist())  # target oszlopok
```

Várt eredmény: 208 `feat_` oszlop, 0% null rate a backfill activity feature-öknél.

### 0b. Samples ellenőrzése

```bash
# Fold definíciók megléte
python -c "import json; d=json.load(open('samples/base_solusdt_fw60_dev/folds.json')); print(len(d['folds']), 'folds, test:', d['test'])"
```

### 0c. Modell ID regisztrálása

Add hozzá az új modellt `config/models.json`-ba **active=false**-szal:

```json
"lgbm_solusdt_l_fw60_q90_local_v3": {
    "asset_id": "solusdt_fw60",
    "target_name": "trg_l_fw60_q90",
    "family": "lightgbm",
    "variant": "local_v3",
    "trainer": "lightgbm_binary",
    "paths": {
        "model_dir": "models/lgbm_solusdt_l_fw60_q90_local_v3",
        "model_file": "model.pkl",
        "features_file": "features.json"
    },
    "predict": { "method": "predict_proba", "proba": true },
    "training": {
        "sample_id": "base_solusdt_fw60_dev",
        "sample_dir": "samples/base_solusdt_fw60_dev",
        "row_stride": 60
    },
    "active": false
}
```

---

## 1. Feature Audit (Stage 0 a search-ben)

Az `lgbm_search.py` automatikusan elvégzi az auditot az első futáskor:

- **Null rate > 1%**: kizárás
- **Std < 1e-6** (valóban konstans): kizárás
- **Ismert duplikátumok** (`_KNOWN_DUPLICATES`): kizárás

Eredmény: `models/<model_id>/search/features_search.json` — ez a search-ben és a final fit-ben is ez lesz az inputja.

**Fontos:** az 1e-6 threshold helyes (ne strict < 0.001, mert az kizárja a kis értékű ratio feature-öket mint `feat_natr_14`).

---

## 2. Hyperparameter Search

### Smoke stage (gyors szanity check, ~5 perc)

```bash
python scripts/search_lgbm.py \
    --model-id lgbm_solusdt_l_fw60_q90_local_v3 \
    --stage smoke
```

- 5 trial, row_stride=60, 2 fold
- Ellenőrzi: feature loading, LightGBM fit, metric számítás

### Explore stage (fő keresés, ~2-3 óra)

```bash
python scripts/search_lgbm.py \
    --model-id lgbm_solusdt_l_fw60_q90_local_v3 \
    --stage explore \
    --n-trials 60
```

- 60 trial, row_stride=60, mind az 5 fold
- Random seed + TPE guide (20 startup trial után top-25% vezérli a mintavételt)
- **Resume automatikus**: ha megszakad, `--resume` flag-gel folytatható

### Refine stage (opcionális, ~2-4 óra)

```bash
python scripts/search_lgbm.py \
    --model-id lgbm_solusdt_l_fw60_q90_local_v3 \
    --stage refine \
    --n-trials 30
```

- 30 trial, row_stride=10 (6x több adat), szűkebb eloszlás a top-10 körül

### Eredmények értékelése

```bash
python -c "
import json
with open('models/lgbm_solusdt_l_fw60_q90_local_v3/search/search_best.json') as f:
    best = json.load(f)
print(f'Trial #{best[\"trial_no\"]}')
print(f'  mean_valid_ll:   {best[\"mean_valid_ll\"]:.4f}')
print(f'  mean_train_ll:   {best[\"mean_train_ll\"]:.4f}')
print(f'  mean_gap:        {best[\"mean_gap\"]:.4f}  (< 0.03 OK)')
print(f'  mean_valid_prauc:{best[\"mean_valid_prauc\"]:.4f}')
"
```

**Promotion küszöbök (champion guard):**
- `mean_valid_prauc` > champion − 5%
- `mean_gap` < 0.03 (vagy gap_penalty = 0)
- `std_valid_ll` alacsony (stabil a foldok között)

### Feature importance lekérése

A search nem menti automatikusan a feature importance-t. Manuálisan:

```python
import sys, json, pickle
sys.path.insert(0, 'src')
from modeling.datasets import load_modeling_dataset
import lightgbm as lgb

MODEL_ID = 'lgbm_solusdt_l_fw60_q90_local_v3'

with open(f'models/{MODEL_ID}/search/features_search.json') as f:
    features = json.load(f)['features']
with open(f'models/{MODEL_ID}/search/search_best.json') as f:
    best = json.load(f)

ds = load_modeling_dataset(
    target_col='trg_l_fw60_q90', feature_cols=features,
    row_stride=60, asset_id='solusdt_fw60',
)

# Fold 5 (legteljesebb training set)
import json
from pathlib import Path
folds = json.loads(Path('samples/base_solusdt_fw60_dev/folds.json').read_text())['folds']
fd = folds[-1]
mask_tr = (ds.open_time >= fd['train_start']) & (ds.open_time < fd['train_end'])
X_tr = ds.X[mask_tr]; y_tr = ds.y[mask_tr]

params = {**best['params'], 'objective':'binary', 'metric':'binary_logloss',
          'verbosity':-1, 'n_jobs':4}
params['n_estimators'] = round(
    sum(f['best_iteration'] for f in best['fold_summary']) / len(best['fold_summary']) * 1.12
)
model = lgb.LGBMClassifier(**params)
model.fit(X_tr, y_tr, callbacks=[lgb.log_evaluation(-1)])

gain = model.booster_.feature_importance(importance_type='gain')
for feat, g in sorted(zip(features, gain), key=lambda x: -x[1])[:25]:
    print(f'{feat:<50} {g:>10.1f}')
```

**Figyelj arra:**
- 0 gain feature-ök nem játszanak szerepet — a refine stage-ből kizárhatók
- A top feature-ök konzisztensek-e foldok között? (ha egy feature csak 1 foldban domináns, overfit jele)

---

## 3. Final Model Fit

A search-ben row_stride=60-at használtunk (51k sor). A final fit ugyan azt használja — gyors és konzisztens a validált viselkedéssel. (row_stride=5 is megpróbálható, de 818MB memóriát igényel.)

```python
import sys, json, pickle
sys.path.insert(0, 'src')
from modeling.datasets import load_modeling_dataset
import lightgbm as lgb
from pathlib import Path

MODEL_ID = 'lgbm_solusdt_l_fw60_q90_local_v3'
TRAIN_END = '2025-06-04 23:59:00'  # test period előtti utolsó nap

with open(f'models/{MODEL_ID}/search/features_search.json') as f:
    features = json.load(f)['features']
with open(f'models/{MODEL_ID}/search/search_best.json') as f:
    best = json.load(f)

# n_estimators = CV átlag + 12% buffer
mean_best_iter = sum(f['best_iteration'] for f in best['fold_summary']) / len(best['fold_summary'])
n_est = round(mean_best_iter * 1.12)

final_params = {
    **best['params'],
    'objective': 'binary', 'boosting_type': 'gbdt',
    'metric': 'binary_logloss', 'n_estimators': n_est,
    'subsample_freq': 1, 'force_col_wise': True,
    'verbosity': -1, 'n_jobs': 4,
}

ds = load_modeling_dataset(
    target_col='trg_l_fw60_q90', feature_cols=features,
    row_stride=60, asset_id='solusdt_fw60', end=TRAIN_END,
)
print(f'Train shape: {ds.X.shape}')

model = lgb.LGBMClassifier(**final_params)
model.fit(ds.X, ds.y, callbacks=[lgb.log_evaluation(-1)])

out = Path(f'models/{MODEL_ID}')
out.mkdir(parents=True, exist_ok=True)
with open(out / 'model.pkl', 'wb') as f:
    pickle.dump(model, f)
with open(out / 'features.json', 'w') as f:
    json.dump({'features': features, 'input_features': features}, f, indent=4)
with open(out / 'params.json', 'w') as f:
    json.dump(final_params, f, indent=4)
print(f'Saved model.pkl, features.json, params.json')
```

**Ellenőrzés:**
```bash
python -c "
from pathlib import Path
p = Path('models/lgbm_solusdt_l_fw60_q90_local_v3')
for f in ['model.pkl','features.json','params.json']:
    fp = p / f
    print(f, ':', fp.stat().st_size if fp.exists() else 'MISSING')
"
```

---

## 4. Config frissítés (Promotálás)

### 4a. models.json

```python
import json
from pathlib import Path

path = Path('config/models.json')
cfg = json.loads(path.read_text())

OLD_ID   = 'lgbm_solusdt_l_fw60_q90_local_v2'   # korábbi champion
MODEL_ID = 'lgbm_solusdt_l_fw60_q90_local_v3'   # új champion

cfg['models'][OLD_ID]['active'] = False
cfg['models'][MODEL_ID]['active'] = True

path.write_text(json.dumps(cfg, indent=4), encoding='utf-8')
print('models.json updated')
```

### 4b. env.json

```python
path = Path('config/env.json')
cfg = json.loads(path.read_text())
cfg['runtime']['models']['solusdt_fw60'] = MODEL_ID
path.write_text(json.dumps(cfg, indent=4), encoding='utf-8')
print('env.json updated')
```

---

## 5. Predictions szinkronizálás

A `sync_predictions` egyszerre tölt be minden sort → MemoryError 3M+ sornál.
**Mindig csak az utolsó 7 napot szinkronizáld** kézzel. A dashboard auto-syncje inkrementális lesz.

```python
import sys; sys.path.insert(0, 'src')
from data_pipeline.sync_predictions import sync_predictions

sync_predictions(
    start_time='2025-06-01 00:00:00',   # ~7 nappal ezelőtt
    end_time=None,
    asset_id='solusdt_fw60',
)
```

Ellenőrzés:
```python
import sqlite3, pandas as pd
with sqlite3.connect('database/solusdt_data_dev.db') as conn:
    df = pd.read_sql_query(
        'SELECT open_time, prediction FROM solusdt_1m_predictions ORDER BY open_time DESC LIMIT 3',
        conn
    )
print(df)
```

---

## 6. Strategy Threshold Sweep

A `build_backtest_frame` chunked loading-ot használ (50k soronként, fix MemoryError):

```bash
python scripts/sweep_strategy.py \
    --model-id lgbm_solusdt_l_fw60_q90_local_v3 \
    --asset-id solusdt_fw60 \
    --start 2024-01-01 \
    --end 2025-12-31 \
    --side long \
    --top-n 20
```

**Paraméterek magyarázata:**
- `entry_threshold`: ennél magasabb predikciónál lépünk be (tipikusan 0.40–0.55)
- `max_hold_minutes`: maximális tartási idő (ajánlott: target_window_minutes × 1.5–2.0)
- `take_profit_pct`: 0.0 általában jobb magas win rate-nél (hagyd futni a nyerőket)
- `rearm_threshold`: kilépés utáni újra-felvértezés küszöbe (általában ~0.18)
- `exit_threshold`: korai kilépés ha predikció erre esik (általában ~0.10)

**Kiválasztási szempontok:**
1. Legalább 50 trade (statisztikai szignifikancia)
2. Win rate > 65%
3. Profit factor > 2.0
4. Max drawdown < 20%
5. Nem a legjobb score ha csak 20-30 trade-del éri el (alacsony szignifikancia)

---

## 7. strategies.json frissítés

Az új stratégiát a lista **elejére** kell tenni az adott `asset_id`-hoz,
mert `active_strategy()` az első találatot adja vissza az UI-nak.

```python
import json
from pathlib import Path

path = Path('config/strategies.json')
cfg = json.loads(path.read_text())

new_id = 'solusdt_long_fw60_q90_local_v3'
new_strategy = {
    "description": "...",
    "asset_id": "solusdt_fw60",
    "model_id": "lgbm_solusdt_l_fw60_q90_local_v3",
    "side": "long",
    "start": "2024-01-01 00:00:00",
    "end": "2025-12-31 23:59:00",
    "initial_equity": 10000.0,
    "entry_threshold": 0.45,      # sweep eredménye
    "rearm_threshold": 0.18,
    "exit_threshold": 0.10,
    "min_hold_minutes": 5,
    "max_hold_minutes": 120,      # sweep eredménye
    "take_profit_pct": 0.0,       # sweep eredménye
    "stop_loss_pct": 0.0,
    "trailing_activation_pct": 0.0,
    "trailing_stop_pct": 0.0,
    "cooldown_minutes": 60,
    "fee_bps_per_side": 10.0,
    "slippage_bps_per_side": 2.0,
    "output_dir": f"backtests/{new_id}",
}

# Az elejére szúrjuk
cfg['strategies'] = {new_id: new_strategy, **cfg['strategies']}
path.write_text(json.dumps(cfg, indent=4), encoding='utf-8')
```

Részletes backtest + HTML report mentése:

```bash
python -c "
import sys; sys.path.insert(0, 'src')
from evaluation.backtest import run_configured_strategy
r = run_configured_strategy('solusdt_long_fw60_q90_local_v3')
print('return:', r['total_return'], 'wr:', r['win_rate'], 'pf:', r['profit_factor'])
"
```

---

## 8. UI Ellenőrzés

```python
import sys; sys.path.insert(0, 'src')
from streamlit_app.data import load_dashboard_config

cfg = load_dashboard_config(asset_id='solusdt_fw60')
assert cfg['runtime_model_id'] == 'lgbm_solusdt_l_fw60_q90_local_v3'
assert cfg['strategy_id'] == 'solusdt_long_fw60_q90_local_v3'
print('entry_threshold:', cfg['strategy']['entry_threshold'])
print('OK — UI felveszi az uj modellt')
```

Az UI nem igényel újraindítást — a `load_dashboard_config` minden render-ciklusnál olvassa a configot.

---

## 9. Dokumentáció

Másold a plan fájlt a `docs/plans/completed/` mappába és töltsd ki:

```
docs/plans/completed/<plan_neve>.md
```

Minimálisan dokumentálandó:
- CV metrikák (valid/train LL, PR AUC, gap per fold)
- Best hyperparameterek + magyarázat (miért ezek?)
- Feature importance top-15 és zero-gain lista
- Strategy sweep eredmény (best konfig + backtest summary)
- Config változások listája

---

## Gyors referencia — fájlok és szerepük

| Fájl | Szerepe |
|------|---------|
| `config/models.json` | Model registry — active=true/false itt dől el |
| `config/env.json` | Runtime live model per asset_id |
| `config/strategies.json` | Strategy konfig — első solusdt_fw60 bejegyzés az aktív |
| `models/<id>/model.pkl` | Mentett LightGBM objektum |
| `models/<id>/features.json` | Feature lista (prediction-höz) |
| `models/<id>/search/search_best.json` | Legjobb trial paraméterei |
| `models/<id>/search/search_trials.jsonl` | Összes trial — resume-hoz |
| `models/<id>/search/features_search.json` | Auditált feature lista a search-hez |
| `backtests/<strategy_id>/` | Trades CSV, equity curve, HTML report |
| `backtests/sweep_<model_id>.csv` | Teljes threshold sweep eredménye |
| `src/modeling/lgbm_search.py` | Search engine (distributions, TPE guide, objective) |
| `scripts/search_lgbm.py` | CLI wrapper a search-hez |
| `scripts/sweep_strategy.py` | Strategy threshold sweep CLI |
| `src/evaluation/backtest.py` | Backtest engine (chunked loading) |

---

## Tipikus hibák és megoldásuk

| Hiba | Ok | Megoldás |
|------|----|----------|
| `MemoryError` feature load-nál | Túl sok sor × feature | `row_stride=60` vagy chunked loading |
| Feature hiányzik auditból | std < 1e-6 threshold helyett < 0.001 | Ellenőrizd az 1e-6 küszöböt a `_run_feature_audit`-ban |
| 0 completed trials search után | Minden trial hibás | Nézd a `failed_trials.jsonl`-t |
| `UnicodeEncodeError` Windows console-on | → karakter print-ben | ASCII karakterek a print-ekben |
| `sync_predictions` MemoryError | 3M+ sor egyszerre | Csak az utolsó 7 napot szinkronizáld kézzel |
| Strategy sweep MemoryError | 1M+ sor × 200 feature | `build_backtest_frame` chunked verziót használ — OK |
| UI nem mutatja az új stratégiát | Nem az első bejegyzés az asset_id-hoz | `strategies.json`-ban az elejére szúrd |
