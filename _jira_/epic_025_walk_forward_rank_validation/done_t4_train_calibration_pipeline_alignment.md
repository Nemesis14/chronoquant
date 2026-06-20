---
epic: epic_025
id: t4
title: Train és calibration pipeline alignment az új fold sémához
assignee: modeling_agent
status: todo
blocks: [t5, t7]
blocked_by: [t3]
---

## Goal

A training és calibration réteg összehangolása az új walk-forward foldokkal és
az új rank-alapú search outcome-mal.

## Scope

- `src/modeling/training/`
- `src/trading/calibration/`
- artifact contractok

## Design

- A final fit az új search által választott paramétereket használja.
- A calibration réteg kezelje, hogy a score nem probability, hanem rangsorolási
  célú regressziós score.
- A threshold / percentile kalibráció későbbi stratégiai sweephez kompatibilis legyen.

## Acceptance Criteria

- [ ] A training pipeline az új fold/search artifactokkal működik
- [ ] A calibration réteg nem probability-skálát feltételez hardcode-olva
- [ ] Az artifactok kompatibilisek a rerunnal és a 5600 analysis inputjaival

## Notes

### Training réteg ellenőrzés

**`src/modeling/training/cv.py`** — `PurgedEmbargoCV` független a fold sémától. Nem használ `fold_week_assignments`-t; saját expanding-window logikája van. Nem érintett.

**`src/modeling/training/train.py`** — Csak dispatcher, nem referenciál fold struktúrákat. Nem érintett.

**`src/modeling/training/fit_lgbm.py`** — `_load_train_data()` betölti az összes sort a `sample_train_valid.parquet`-ból (mindenfajta `fold_id`-val, 0-tól 4-ig), és azon trainál. Ez a walk-forward parquettel is kompatibilis — az összes sor (train-only és valid) felhasználásra kerül a final fithez. **Nincs módosítás szükséges.**

A `_score_oos()` nyers score-t (nem sigmoid-dal átalakítottat) ír a parquet-be — kompatibilis.

### Calibration réteg ellenőrzés

**`src/trading/calibration/backtest.py`** — A `simulate_long_probability_strategy` névben "probability" szerepel, de a threshold logika raw regressziós score-t vár — nem sigmoid-olt. Nincs sigmoid alkalmazás sehol.

**`src/trading/calibration/calibrate.py`** — `entry_threshold = 0.45` hardcode-olva van a `_DEFAULT_STRATEGY`-ban. Ez **NEM probability-hardcode** abban az értelemben, hogy nem feltételezi [0,1] tartományt implicit módon — de a threshold értéke az MFE scale-hez (0.45 = ~0.45% MFE) van kalibrálva.

**Módosítás elvégezve:** Kommentet adtam hozzá a `_DEFAULT_STRATEGY` előtt, hogy egyértelmű legyen: az entry/rearm/exit threshold-ok raw regressziós score-ok (nem probability-k), és threshold kalibrációhoz percentile-alapú sweep ajánlott.

### Összefoglalás

Nincs blocker a rerunhoz. A training pipeline az új walk-forward parquettel kompatibilis, a calibration réteg nem alkalmaz sigmoid/softmax átalakítást.

