---
epic: epic_025
id: t5
title: 5600 modell teljes újrafuttatása samplingtól kezdve
assignee: modeling_agent
status: todo
blocks: [t6, t8, t9]
blocked_by: [t4]
---

## Goal

A 5600-as modellpipeline teljes újrafuttatása az új metodológia szerint,
samplingtól indulva, searchön és trainen át, új artifactokkal.

## Scope

- sample létrehozás
- feature engineering reuse vagy rerun szükség szerint
- hyperparameter search
- final fit
- prediction / analysis artifactok

## Lépések

1. Régi inkompatibilis search/train artifactok azonosítása
2. Új sample generálása az új fold sémával
3. Search futtatása Top10 Lift objective-fel
4. Final fit és scoring
5. Kimeneti artifactok ellenőrzése

## Acceptance Criteria

- [ ] A 5600-as modell új sample-ről újrafutott
- [ ] Új search artifactok elkészültek
- [ ] Új final model artifact elkészült
- [ ] A rerun reprodukálható parancsokkal dokumentált

## Notes

### Pipeline frissítések

**`src/modeling/pipeline.py` — `step_sample()` frissítve:**
- `sampling_mode: walk_forward` a models.json sampling blokkban → `create_model_walk_forward_sample()` hívódik
- Egyébként → legacy `create_model_sample()` (backward compatible)

**`config/models.json`:**
- `lgbm_solusdt_l_fw60_2021` sampling blokkba `"sampling_mode": "walk_forward"` hozzáadva

### Futtatott parancsok (reprodukálható)

```bash
uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_2021 --step sample
uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_2021 --step search --stage smoke
uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_2021 --step search --stage explore
uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_2021 --step train
```

### Sample eredmény

Fold séma (2021 anchor):
| fold | train_start | train_end  | valid_start | valid_end  |
|------|-------------|------------|-------------|------------|
| 1    | 2021-01-01  | 2021-09-30 | 2021-10-01  | 2021-12-31 |
| 2    | 2021-04-01  | 2021-12-31 | 2022-01-01  | 2022-03-31 |
| 3    | 2021-07-01  | 2022-03-31 | 2022-04-01  | 2022-06-30 |
| 4    | 2021-10-01  | 2022-06-30 | 2022-07-01  | 2022-09-30 |

Összes sor: 15,312 (6 fold-ablakból összegyűjtve, óránkénti mintavételezéssel)

### Smoke search eredmény (5 trial, 2 fold)

Best trial #63: `objective_score = -0.005958` (lift = 0.0063)
- fold1 top10_lift = 0.0056, spearman = 0.275, mono = 0.889
- fold2 top10_lift = 0.0069, spearman = 0.236, mono = 1.000

Összes trial top10_lift > 0 ✓, nincs crash ✓

### Explore search eredmény (60 trial, 4 fold)

60 trial futtatva 10:59-11:07 között (~8 perc), 4 fold.
Best trial marad #63 (smoke-ból): `objective_score = -0.005958`

Top-5 explore trial:
- #0104 obj=-0.005208, lift=0.0073, rho=0.256
- #0117 obj=-0.005183, lift=0.0073, rho=0.255
- #0096 obj=-0.005153, lift=0.0072, rho=0.255
- #0067 obj=-0.005152, lift=0.0073, rho=0.256

### Train eredmény

- `n_features = 114`
- `n_estimators = 2118` (átlag best_iter × 1.1)
- `oos_year = 2022`
- Artifacts: `model.pkl` (1.3 MB), `features.json`, `params.json`, `sample_oos.parquet` (394 MB)

### Short direction modell

`lgbm_solusdt_s_fw60_2021` a "5600 pipeline" részeként opcionális volt. A modell config-ban nincs `sampling_mode: walk_forward` beállítva — ez egy külön feladatként kezelendő, ha szükséges. Csak a long direction modellt futtattuk újra.

