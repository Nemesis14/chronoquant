---
epic: epic_065
id: t872
title: CV-optimalizált klaszter-szeparáció — ANOVA F-score alapú k keresés
assignee: validator_agent
status: pr
blocks: []
blocked_by: [t871]
---

## Goal

Az Epic 065 korábbi taskjai (t863-t871) egy **fix k=10** K-means klaszterezést használtak, majd
utólag próbálták megállapítani, mely klaszterek "tradeable". Ez a megközelítés bukott: a klaszterek
40-45%-a degradálódott OOS-ben, egy teljesen eltűnt, és a hibás post-hoc irány-hozzárendelés miatt
több hibát is találtunk (lásd `_doc_/methodology_doc/6500_cluster_regime_methodology.md` és
`_doc_/models_doc/6501_k10_validation_realistic_backtest.md`).

**Új irány:** Ne fix k-t válasszunk, hanem egy **CV-vel validált optimalizációs eljárást**, amely
megkeresi azt a k-t, ahol a klaszterek a leginkább **szétválnak** long/short MFE irány szerint —
és ez a szeparáció **stabil marad** több, időben egymást követő validációs ablakon.

**FONTOS: Ez a task NEM trading backtest — csak a klaszterezés minőségét optimalizálja.**
Trading szimuláció csak egy KÉSŐBBI task lesz, ha ez a validáció pozitív eredményt hoz.

## Scope

### 1. Adat-pipeline újraépítése (a törölt research/cluster_regime/ helyett)

A korábbi logika helyes volt, csak a fájlok törlődtek — újra kell írni:
- Feature selection: MI/korrelációs redundancia-analízis, **csak train adaton** (lásd korábbi
  t863 logika: ~14 alacsony-redundanciájú indikátor, dimenziónként)
- fw360 target: `long_mfe_fw360 = log(max(high[t+1:t+360])/close[t])`,
  `short_mfe_fw360 = log(min(low[t+1:t+360])/close[t])` — nincs future leaking (csak t+1..t+360)
- Window representation: 360-bar lookback, 30-perces ritkítás, z-score normalizálás ablakon belül
  (shape-only, ár-szint független), 5 summary stat/feature (mean/slope/std/min/max)

### 2. CV Struktúra — Walk-Forward (NEM random k-fold, time series miatt!)

```
Fold 1: Train 2021-2022        → Validate 2023
Fold 2: Train 2021-2023        → Validate 2024
Fold 3: Train 2021-2024        → Validate 2025 H1 (2025-01 — 2025-06)
Fold 4: Train 2021-2025 H1     → Validate 2025 H2 — 2026 H1 (2025-07 — 2026-06)
```

### 3. Metrika: ANOVA F-statisztika a Klaszterek Között

```python
# Per-observation irányjel (előjeles összeg, mert short_mfe negatív):
signal_i = long_mfe_fw360_i + short_mfe_fw360_i
# pozitív → long-túlsúly, negatív → short-túlsúly, 0 → semleges

# Minden fold validation részén, adott k-ra:
# One-way ANOVA: H0 = minden klaszter átlagos signal-je egyenlő
F = between_cluster_variance(mean_signal_per_cluster) / within_cluster_variance(signal)

# CV score egy adott k-ra:
cv_score(k) = mean(F_fold1, F_fold2, F_fold3, F_fold4)
cv_std(k)   = std(F_fold1, F_fold2, F_fold3, F_fold4)
```

**Kritikus:** Az F-score-t mindig a **validation** részen kell mérni (train-en fit-elt modellel predict-elve), NEM a train-en — különben trivially magas lenne minden k-ra.

### 4. Hiperparaméter Keresés

```
k ∈ {2, 3, 4, ..., 30}

Minden k-ra: cv_score(k), cv_std(k) a 4-fold walk-forward CV-ből

Optimum kiválasztás:
  - Elsődleges: legmagasabb mean(cv_score)
  - Másodlagos (stabilitás): alacsony cv_std/cv_score arány (coefficient of variation)
  - Ha egy k csak 1 fold-ban ad kiugró F-score-t, az overfitting jele — ne azt válaszd
```

### 5. Output

- `research/cluster_regime/01_target_computation.py` — fw360 target (újraírva)
- `research/cluster_regime/02_window_representation.py` — window features (újraírva)
- `research/cluster_regime/03_feature_selection.py` — MI-alapú redundancia (újraírva, train-only)
- `research/cluster_regime/04_cv_cluster_optimization.py` — ÚJ: CV walk-forward + F-score k-keresés
- `artifacts/research_cluster_regime/cv_k_search_results.json` — k=2..30, cv_score, cv_std, per-fold F-értékek
- `artifacts/research_cluster_regime/cv_optimal_k_model.pkl` — a kiválasztott optimális k modellje (teljes adaton fit-elve)
- `_doc_/models_doc/6502_cv_cluster_separation_optimization.md` — elemzés: k-keresési görbe, optimum indoklása, per-cluster direction report a végleges modellre (CV-n validált)

## Acceptance Criteria

- [x] Feature selection, window representation, target computation újraírva és validálva (nincs future leaking)
- [x] CV walk-forward struktúra implementálva (4 fold, fenti dátumokkal)
- [x] ANOVA F-score metrika implementálva, validation-only mérve
- [x] k=2..30 tartományban futtatva, minden k-ra cv_score + cv_std
- [x] Optimális k kiválasztva és indokolva (erősség + stabilitás alapján)
- [x] Végső klaszter-modell mentve `.pkl`-ben
- [x] Per-cluster direction report a végső modellre (long/short/neutral, CV-validált)
- [x] Ruff check pass
- [x] NINCS trading backtest ebben a taskban

## Notes

Ez az Epic 065 harmadik nekifutása. Az első (t863-t870) fix k=10-zel és post-hoc irány-hozzárendeléssel
dolgozott — statisztikai és logikai hibákkal (lásd 6501 doc). A második (t871) ezeket javította, de
megerősítette, hogy a fix-k megközelítésnek nincs kihasználható tradeable edge-e. Ez a harmadik
nekifutás magát a klaszterezési eljárást teszi robusztusabbá — mielőtt bármilyen trading kérdést
újra feltennénk.

### Elvégzett munka és eredmények (2026-09-01)

**Pipeline újraépítve** (a törölt `research/cluster_regime/` helyett), ugyanaz a validált logika:

- `01_target_computation.py` — fw360 target, változatlan (t+1..t+360, nincs leaking). Futtatva:
  3,135,704 sor a `target_fw360` táblában.
- `03_feature_selection.py` — **train-only javítással**: a korrelációs mátrix kizárólag a
  2021-01 — 2022-12 ablakon (Fold 1 train-je, ami egyetlen fold-ban sem validation) számolódik,
  szemben az eredeti verzióval, ami a teljes `quant_train`-ből mintázott (validation-időszak
  leak veszélye). 14 feature kiválasztva, dimenziónként — szinte megegyezik az eredeti t863
  szelekcióval (`feat_body_ratio_sma_10` helyett `feat_doji`).
- `02_window_representation.py` — változatlan logika (360-bar lookback, 30 perces ritkítás,
  ablakon belüli z-score norm, 5 summary stat/feature), de a feature-listát dinamikusan tölti be
  a `03` kimenetéből. Kimenet: 104,011 sor × 75 oszlop (2021-01 — 2026-06-30).
- `04_cv_cluster_optimization.py` — **ÚJ**: 4-fold walk-forward CV, ANOVA F-score minden fold
  validation részén mérve (train-en sosem), k=2..30 keresés (MiniBatchKMeans a 116 fit-hez,
  végső KMeans a kiválasztott k-ra teljes adaton). Single-fold-dominance stabilitási szűrő
  (>50% → instabil, kizárva).

**Eredmény: k=13 optimális** (cv_score=17.70, dominance=0.386, mind a 4 fold önállóan szignifikáns).
Alacsony k-k (2,3,5,7) magas nyers F-score-t adnak, de mind instabil (egy fold dominálja őket) —
pontosan azt a hibamintát mutatják, amit a stabilitási szűrő ki akart zárni.

**Fold-4 out-of-fold direction report** (paired t-test, Bonferroni α=0.05/13, train=2021-2025H1,
validate=2025H2-2026H1): 4/13 klaszter szignifikánsan SHORT (2,4,7,10), **0/13 szignifikánsan
LONG**, 7 NEUTRAL, 2 klaszter (6,9) üres OOS-ban. A SHORT-only aszimmetria egybevág a t871 k=10
eredményével (5/7 szignifikáns klaszter volt SHORT ott is) — ez a legerősebb, k-független jel,
amit ez a task talált.

**Kritikus:** Ez a task NEM tartalmaz trading backtestet (ticket scope szerint). A javuló
klaszter-szeparáció (k=13 F-score > k=10 F-score) önmagában nem jelenti, hogy a jel elég erős
lenne a realisztikus költségek (t871: ~0.7%/trade) legyőzéséhez — ez egy KÉSŐBBI, feltételes
backtest task kérdése lenne.

**Ruff:** minden fájl PASS. **Pyright:** research/ szokásos stub-zajjal (sklearn `n_init`
Literal-tipizálás, scipy `linregress` — ugyanezek megvoltak a t863/t865 eredeti kódjában is,
nem valódi hiba); egy valódi típusannotáció-hibát javítottam (`run_cv_k_search` visszatérési
típusa).

**Git commit:** `016fc99` — 37 fájl (törölt régi t871 artifactok + új t872 pipeline/kimenetek).

Ready for validator_agent review.
