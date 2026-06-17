# Target layer refaktor javaslat: full-history q90/q10 bináris targetről folytonos fw60 max/min logreturn outcome-okra

## 1. Vezetői összefoglaló

A jelenlegi ChronoQuant target pipeline a forward 60 perces long/short eseményeket bináris targetként állítja elő:

```text
trg_l_fw60_q90 = 1, ha a következő 60 perc max long move-ja eléri a teljes history q90 küszöbét
trg_s_fw60_q10 = 1, ha a következő 60 perc min short move-ja eléri a teljes history q10 küszöbét
```

Ez működőképes első verzió, de model development és időrendi CV szempontból több problémát okozhat:

- a q90/q10 threshold a teljes elérhető historyból számolódik;
- validációs vagy későbbi időszak eloszlása visszahat a múltbeli label definíciójára;
- a binarizálás információt veszít a move nagyságáról;
- a target cutoff és a trading/prediction cutoff összekeveredik;
- a target definíció erősen függ attól, mely időszakból becsüljük a percentilist.

A javasolt to-be állapotban a target table elsődleges forrása ne bináris label legyen, hanem folytonos forward outcome:

```text
fw60_max_logret   = log(max(close[t+1:t+60]) / close[t])
fw60_min_logret   = log(min(close[t+1:t+60]) / close[t])
fw60_close_logret = log(close[t+60] / close[t])
```

Ezekből később tetszőlegesen képezhető:

- regressziós target,
- quantile regression target,
- fold/as-of q90 bináris target,
- fix trading-threshold target,
- ordinal bucket target,
- triple-barrier target.

A lényeg: a target source-of-truth objektív forward outcome legyen, ne full-history percentilis alapján binarizált label.

---

## 2. As-is állapot

### 2.1. Jelenlegi target config

A jelenlegi target config lényege:

```json
{
  "direction": "long",
  "name": "trg_l_fw60_q90",
  "rolling_window": 60,
  "percentile": 0.9
}
```

Short oldalon:

```json
{
  "direction": "short",
  "name": "trg_s_fw60_q10",
  "rolling_window": 60,
  "percentile": 0.1
}
```

A `rolling_window = 60` itt a forward target horizon: 60 darab 1 perces bar.

### 2.2. Jelenlegi forward window logika

A jelenlegi target pipeline helyesen kizárja az aktuális bart a forward ablakból:

```sql
ROWS BETWEEN 1 FOLLOWING AND 60 FOLLOWING
```

Ez azt jelenti:

```text
t időpont labelje a t+1 ... t+60 időszakból készül
```

Long oldalon:

```text
future_max_return =
  max(close[t+1:t+60]) / close[t] - 1
```

Short oldalon:

```text
future_min_return =
  min(close[t+1:t+60]) / close[t] - 1
```

### 2.3. Jelenlegi bináris labeling

A jelenlegi folyamat:

```text
1. Számold ki minden sorra:
   future_max_return
   future_min_return

2. Dobd a null sorokat.

3. Számold ki:
   global_long_threshold = q90(future_max_return teljes elérhető historyn)
   global_short_threshold = q10(future_min_return teljes elérhető historyn)

4. Képezd a bináris targetet:
   trg_l_fw60_q90 = future_max_return >= global_long_threshold
   trg_s_fw60_q10 = future_min_return <= global_short_threshold
```

Tehát a mostani target nem csak azt tárolja, hogy mi történt a következő 60 percben, hanem egy teljes history alapján becsült target policyt is beéget a labelbe.

---

## 3. Az as-is állapot problémái

### 3.1. Full-history target-definition leakage

A supervised learningben normális, hogy a label forward-looking. Az nem probléma, hogy a `t` időpont labelje a `t+1:t+60` időszakból készül.

A probléma az, hogy a target definíciós küszöb a teljes elérhető historyból számolódik.

Példa:

```text
Adat: 2025-01-01 -> 2026-06-17

CV fold:
  train: 2025 Q1
  valid: 2025 Q2
```

Ha a q90 threshold a teljes 2025–2026-06-17 időszakból készül, akkor a 2025 Q2 valid targetje már 2026-os eloszlásinformációt is tartalmaz a label definícióban.

Ez nem klasszikus feature leakage, hanem:

```text
target-definition leakage
global normalization leakage
```

Következmények:

```text
- torzulhat a foldonkénti target rate
- torzulhat a CV score
- torzulhat a model selection
- torzulhat a feature importance
- a threshold nem production-like as-of módon készül
```

### 3.2. Rezimfüggő piacnál a full-history percentilis különösen sérülékeny

Crypto / OHLCV adatoknál a volatilitás rezsimenként változik.

Ha egy későbbi időszak nagyon volatilis, akkor a full-history q90 magasabb lesz. Ez visszamenőleg csökkentheti a korábbi, csendesebb időszak pozitív labeljeit.

Ha egy későbbi időszak csendesebb, akkor a full-history q90 alacsonyabb lehet, és visszamenőleg túl sok pozitív labelt adhat korábbi volatilis időszakokra.

Ezért a full-history quantile label nem ideális időrendi validációhoz.

### 3.3. A bináris target információvesztő

A bináris target azonosként kezel két nagyon eltérő esetet:

```text
future_max_return = +0.91%
future_max_return = +4.50%
```

Ha a q90 threshold +0.90%, mindkettő:

```text
target = 1
```

Trading szempontból ez nem ugyanaz. A második sokkal nagyobb opportunity.

Ugyanez short oldalon:

```text
future_min_return = -0.95%
future_min_return = -6.00%
```

mindkettő lehet pozitív short event, de a magnitude és a risk context teljesen más.

### 3.4. A target cutoff és a trading cutoff összekeveredik

A jelenlegi classifieres rendszerben két küszöb van:

```text
1. target cutoff:
   full-history q90/q10 move threshold

2. model decision cutoff:
   probability threshold, például p > 0.6
```

A modell outputja így:

```text
P(full-history q90 event)
```

Ez kevésbé közvetlen trading jelentésű, mint:

```text
predicted fw60 long opportunity ≈ +0.6%
```

---

## 4. To-be állapot

### 4.1. Source-of-truth target layer: folytonos forward outcome-ok

A target table elsődleges outcome oszlopai:

```text
fw60_max_logret
fw60_min_logret
fw60_close_logret
```

Definíciók:

```text
fw60_max_logret(t) =
  log(max(close[t+1:t+60]) / close[t])

fw60_min_logret(t) =
  log(min(close[t+1:t+60]) / close[t])

fw60_close_logret(t) =
  log(close[t+60] / close[t])
```

Ezek objektív forward outcome-ok. Nem tartalmaznak percentilis küszöböt.

### 4.2. Long és short értelmezés

A három nyers outcome-ból származtatható long/short értelmezés:

```text
long_mfe_fw60_log = fw60_max_logret
long_mae_fw60_log = fw60_min_logret

short_mfe_fw60_log = -fw60_min_logret
short_mae_fw60_log = -fw60_max_logret
```

Magyarázat:

```text
MFE = maximum favorable excursion
MAE = maximum adverse excursion
```

Long oldal:

```text
long_mfe = mennyit ment felfelé, tehát kedvező irányba
long_mae = mennyit ment lefelé, tehát ellenünk
```

Short oldal:

```text
short_mfe = mennyit ment lefelé, tehát shortnak kedvező irányba
short_mae = mennyit ment felfelé, tehát short ellen
```

### 4.3. Nem kell 4 modellt építeni

A 4 értelmezett mennyiség nem 4 kötelező modell.

Első körben:

```text
Long model:
  target = fw60_max_logret
```

Később:

```text
Short model:
  target = -fw60_min_logret
```

A `fw60_min_logret` a long modellnél risk/evaluation auditként használható, nem feltétlenül külön targetként.

---

## 5. Miért jobb a logreturn outcome target?

### 5.1. Megszűnik a percentilis-időszak kérdés

Folytonos outcome esetén nincs beégetve, hogy:

```text
q90 honnan jön?
- teljes historyból?
- csak trainből?
- fold-trainből?
- rolling windowból?
- 2025-ből?
- 2026-ból?
```

A target egyszerűen a tényleges forward outcome.

A percentilis később opcionális derived label lehet, például:

```text
y = fw60_max_logret > q90(fold_train_fw60_max_logret)
```

De ez már modellezési policy, nem a target source része.

### 5.2. A jelenlegi target lényege megmarad

A jelenlegi long binary target alapja:

```text
max(close[t+1:t+60]) / close[t] - 1
```

A javasolt long folytonos target:

```text
log(max(close[t+1:t+60]) / close[t])
```

Tehát nem váltunk át teljesen más célra. Továbbra is a következő 60 perc long oldali opportunity-ját mérjük, csak binarizálás nélkül.

### 5.3. A magnitude információ megmarad

A modell nem csak azt látja, hogy volt-e q90 event, hanem azt is, hogy mekkora volt a tényleges move.

Ez lehetővé teszi:

```text
- regressziós modellezést
- quantile regressiont
- ranking alapú evaluationt
- top-k opportunity kiválasztást
- trading cutoff közvetlenebb megadását
```

### 5.4. A model output közvetlenebbül értelmezhető

Classifier:

```text
output = P(q90 event)
```

Regresszor:

```text
output = predicted fw60_max_logret
```

Példa:

```text
predicted fw60_max_logret = 0.006
```

Ez kb.:

```text
exp(0.006) - 1 ≈ +0.60%
```

Ez közvetlenül összevethető:

```text
fee + slippage + risk margin + minimum profit expectation
```

---

## 6. Modellezési javaslat

### 6.1. As-is modell

```text
LGBMClassifier
target = trg_l_fw60_q90
objective = binary
metric = logloss / AUC / PR AUC
output = probability
```

### 6.2. To-be long modell

```text
LGBMRegressor
target = fw60_max_logret
objective = regression / huber / quantile
output = predicted long opportunity size
```

Javasolt objective-ek:

```text
Baseline:
  objective = regression

Robusztus:
  objective = huber

Tail/opportunity fókusz:
  objective = quantile
  alpha = 0.8 vagy 0.9
```

### 6.3. Fontos evaluation metrikák

Nem elég RMSE-t nézni. Tradinghez fontosabb:

```text
- MAE
- Huber loss
- Spearman correlation
- top 5% realized fw60_max_logret
- top 10% realized fw60_max_logret
- top-decile lift
- prediction bucket realized average
- adverse move audit top bucketben
```

Long modellnél a top predicted jeleknél nézni kell:

```text
- realized fw60_max_logret
- realized fw60_min_logret
- adverse move p10/p25
- hány esetben ment túl mélyen ellenünk
```

---

## 7. Repo változtatások taskok szintjén

## Epic: Replace full-history quantile binary target source with continuous fw60 forward outcome layer

---

### Task 1 — Add continuous fw60 target outcome schema

**Cél:** a `target` tábla képes legyen folytonos forward outcome-ok tárolására.

**Érintett fájl:**

```text
src/database/store/duckdb_store.py
```

**Új oszlopok:**

```text
fw60_max_logret DOUBLE
fw60_min_logret DOUBLE
fw60_close_logret DOUBLE
```

**Backward compatibility miatt maradhatnak:**

```text
trg_l_fw60_q90 BOOLEAN
trg_s_fw60_q10 BOOLEAN
```

**Acceptance criteria:**

```text
- target table tartalmazza az új logreturn oszlopokat
- régi bináris target oszlopok nem törnek el
- schema migration / ensure_tables kezeli az új oszlopokat
```

---

### Task 2 — Persist forward max/min/close logreturns in sync_targets

**Cél:** a jelenlegi köztes forward returnök perzisztált target outcome-ok legyenek.

**Érintett fájl:**

```text
src/database/sync_tables/sync_targets.py
```

**Kiszámolandó:**

```text
fw60_max_logret =
  log(future_max_close / close)

fw60_min_logret =
  log(future_min_close / close)

fw60_close_logret =
  log(future_close_horizon / close)
```

**Acceptance criteria:**

```text
- current bar excluded from forward window
- t+1..t+60 window used
- last 60 rows are NULL
- output DataFrame contains the new logreturn columns
- target table receives the new columns
```

---

### Task 3 — Keep legacy q90/q10 binary labels as derived compatibility columns

**Cél:** a régi classifier pipeline ne törjön el azonnal.

**Érintett fájl:**

```text
src/database/sync_tables/sync_targets.py
```

**Logika:**

```text
trg_l_fw60_q90 =
  fw60_max_logret >= full_history_q90(fw60_max_logret)

trg_s_fw60_q10 =
  fw60_min_logret <= full_history_q10(fw60_min_logret)
```

**Acceptance criteria:**

```text
- régi trg_l_fw60_q90 és trg_s_fw60_q10 továbbra is készül
- metadata jelöli, hogy legacy full-history quantile derived label
- új modeling pipeline már nem erre épül elsődlegesen
```

---

### Task 4 — Add metadata for continuous target outcomes

**Cél:** a metadata ne csak threshold auditot tartalmazzon, hanem outcome definíciót is.

**Érintett fájlok:**

```text
src/database/sync_tables/sync_targets.py
database/<asset>/<asset>.json
```

**Javasolt metadata:**

```json
{
  "target_outcomes": {
    "fw60": {
      "columns": [
        "fw60_max_logret",
        "fw60_min_logret",
        "fw60_close_logret"
      ],
      "horizon": 60,
      "window": "t+1..t+60",
      "computed_from": "...",
      "computed_to": "...",
      "null_tail_rows": 60
    }
  },
  "derived_binary_targets": {
    "trg_l_fw60_q90": {
      "source": "fw60_max_logret",
      "policy": "full_history_quantile_legacy",
      "quantile": 0.9,
      "threshold": "..."
    },
    "trg_s_fw60_q10": {
      "source": "fw60_min_logret",
      "policy": "full_history_quantile_legacy",
      "quantile": 0.1,
      "threshold": "..."
    }
  }
}
```

**Acceptance criteria:**

```text
- outcome definíciók auditálhatók
- legacy thresholdök külön mezőbe kerülnek
- computed_from/computed_to továbbra is mentve van
```

---

### Task 5 — Add tests for continuous fw60 outcomes

**Cél:** determinisztikus tesztekkel ellenőrizni az új target outcome-okat.

**Érintett tesztek:**

```text
src/database/tests/store/sanity/test_target_window.py
src/database/tests/sync_tables/smoke/test_sync_targets.py
```

**Acceptance criteria:**

```text
Given deterministic OHLCV sample:
  fw60_max_logret = log(max future close / current close)
  fw60_min_logret = log(min future close / current close)
  fw60_close_logret = log(close[t+60] / current close)

Also:
  current bar excluded
  last horizon rows are NULL
  no look-ahead beyond t+60
```

---

### Task 6 — Allow modeling dataset to use float target columns

**Cél:** a modeling dataset loader binary és continuous targetet is kezeljen.

**Érintett fájl:**

```text
src/modeling/quantitative/datasets.py
```

**Acceptance criteria:**

```text
- load_modeling_dataset(target_col="fw60_max_logret") működik
- y dtype numeric float
- null target rows dropped
- nincs binary-only feltételezés a dataset loadingban
```

---

### Task 7 — Add LightGBM regression trainer

**Cél:** új regression/quantile LightGBM tréner bevezetése.

**Érintett fájlok:**

```text
src/modeling/quantitative/lightgbm_regression.py
src/modeling/quantitative/train.py
config/models.json
config/model_params.json
```

**Javasolt config:**

```json
{
  "model_type": "lightgbm_regression",
  "target_name": "fw60_max_logret",
  "objective": "huber",
  "metric": ["l1", "huber"],
  "sample_id": "..."
}
```

**Támogatott objective-ek:**

```text
regression
huber
quantile
```

**Acceptance criteria:**

```text
- LGBMRegressor tud tanulni fw60_max_logret targetre
- artifacts mentése működik
- validation predictions mentése működik
- regression metrics report készül
```

---

### Task 8 — Add regression/ranking evaluation metrics

**Cél:** binary metrics mellett regression és trading-oriented metrikák.

**Érintett fájl:**

```text
src/modeling/quantitative/metrics.py
```

**Új metrikák:**

```text
MAE
RMSE
Huber loss
Spearman correlation
top_decile_realized_mean
top_decile_lift
bucketed realized target summary
adverse move audit
```

**Acceptance criteria:**

```text
- valid predictions alapján kiszámolható top-k lift
- bucketed report mutatja predikciós sávonként a realized targetet
- long modellnél fw60_min_logret audit is elérhető
```

---

### Task 9 — Add model card/report section for continuous target models

**Cél:** regression modellek riportja ne binary classifier szemléletű legyen.

**Érintett fájlok:**

```text
src/modeling/quantitative/reports.py
docs/modeling/model_cards/
```

**Report tartalom:**

```text
- target definition
- objective
- target distribution
- prediction distribution
- MAE / Huber / Spearman
- top-k realized performance
- adverse move diagnostics
- comparison against binary baseline
```

**Acceptance criteria:**

```text
- regression model card generálható
- top bucket performance látszik
- adverse move distribution dokumentált
```

---

### Task 10 — Update target documentation

**Cél:** dokumentációban szétválasztani forward outcome, derived label és model target fogalmakat.

**Érintett fájlok:**

```text
docs/concepts/targets.md
_doc_/0224_sync_targets.md
docs/modeling/lightgbm_development.md
```

**Új fogalmak:**

```text
Forward outcome:
  objektív jövőbeli mérés, például fw60_max_logret

Derived label:
  modellezési célra képzett bináris / bucket / barrier target

Model target:
  az adott modell által használt y oszlop
```

**Acceptance criteria:**

```text
- docs leírja, hogy full-history quantile binary label legacy
- docs leírja az új fw60 max/min logreturn outcome-okat
- docs leírja, hogyan lehet bináris labelt fold/as-of módon származtatni
```

---

### Task 11 — Benchmark legacy classifier vs continuous target models

**Cél:** ne elméleti alapon döntsünk, hanem összehasonlítással.

**Benchmark modellek:**

```text
Baseline A:
  current binary classifier
  target = trg_l_fw60_q90

Candidate B:
  LGBMRegressor
  target = fw60_max_logret
  objective = regression

Candidate C:
  LGBMRegressor
  target = fw60_max_logret
  objective = huber

Candidate D:
  LGBMRegressor
  target = fw60_max_logret
  objective = quantile alpha=0.8/0.9

Candidate E:
  binary classifier
  target = fw60_max_logret > fold_train_q90
```

**Acceptance criteria:**

```text
- ugyanazon sample/split mellett összehasonlítható
- top-k realized performance alapján is értékel
- adverse move audit szerepel
- legacy binary target megtartható benchmarkként
```

---

## 8. Javasolt implementációs sorrend

```text
1. Add target outcome schema.
2. Persist fw60_max_logret / fw60_min_logret / fw60_close_logret.
3. Add tests for forward window correctness.
4. Keep legacy q90/q10 labels for compatibility.
5. Add metadata for outcome definitions.
6. Verify dataset loader works with float target.
7. Add LightGBM regression trainer.
8. Add regression/ranking metrics.
9. Add regression model report/model card.
10. Run benchmark against legacy classifier.
11. Decide final production target policy.
```

---

## 9. Rövid végső döntési indoklás

A váltás azért indokolt, mert a jelenlegi `trg_l_fw60_q90` / `trg_s_fw60_q10` targetek a teljes historyból számolt percentilis küszöböt égetik bele a labelbe. Ez időrendi CV-ben target-definition leakage-et, rezsimfüggő torzítást és információvesztést okozhat.

A `fw60_max_logret` / `fw60_min_logret` folytonos outcome-ok ugyanazt a forward 60 perces long/short opportunity-t mérik, de küszöb, percentilis-időszak és binarizáció nélkül. Ez tisztább target layer, rugalmasabb modellezést tesz lehetővé, és közvetlenebbül kapcsolható a trading decision layerhez.
