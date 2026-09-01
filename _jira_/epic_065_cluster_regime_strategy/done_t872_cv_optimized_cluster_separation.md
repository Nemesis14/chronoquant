---
epic: epic_065
id: t872
title: CV-optimalizált klaszter-szeparáció — ANOVA F-score alapú k keresés
assignee: user
status: done
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

---

## Lezárás (2026-09-01) — user által validálva, validator_agent session nélkül

A user a fenti "Ready for validator_agent review" állapot után három további follow-up
vizsgálatot kért közvetlenül, majd ezek eredménye alapján maga hagyta jóvá a végleges lezárást
(explicit kérésére validator_agent session lefuttatása nélkül).

**Follow-up #1 — Per-cluster n_obs (train + valid, mind a 4 fold):**
`research/cluster_regime/05_cv_per_cluster_counts.py` →
`artifacts/research_cluster_regime/cv_per_cluster_train_valid_counts.json`.
Eredmény: az OOS-ben eltűnő/gyenge klaszterek már a train mintán is a legkisebbek közé
tartoztak (1.1-1.9% train share) — base-rate jelenség, nem váratlan összeomlás.

**Follow-up #2 — Irány-stabilitás (train vs. valid, foldonként, mind a 13 klaszterre):**
`research/cluster_regime/06_cv_direction_stability.py` →
`artifacts/research_cluster_regime/cv_direction_stability_train_vs_valid.json`.
Eredmény: 52 cluster×fold kombinációból csak 16 (30.77%) egyezik train↔valid között. 6 valódi
FLIP eset (LONG↔SHORT megfordulás), 10 gyengülés, és 14 "megjelenő" jel (NEUTRAL→szignifikáns
validation-ön) — az utóbbi a legnagyobb kategória, ami inkább zajra, mint valódi perzisztens
jelre utal.

**Follow-up #3 — Cross-fold cluster identity (centroid matching):**
`research/cluster_regime/07_cross_fold_signal_identity.py` →
`artifacts/research_cluster_regime/cv_cross_fold_signal_identity.json`.
Mivel minden fold külön K-means fit, a cluster ID-k nem garantáltan azonosak fold-ok között.
Centroid cosine similarity + mutual-nearest-neighbor matching + union-find grouping, egy
formális validity check-kel (within-fold baseline vs. cross-fold match distribúció —
RELIABLE, de szerény szeparációval egy erősen 1.0-hoz komprimált skálán). Eredmény: 11
komponens (2-4 fold-ot lefedő jelölt-mintázat), ebből csak **1 (`pattern_3`) valóban
robusztus** (mindhárom matched tagja független szignifikáns SHORT, n≈620-790/fold, ~1-2% az
adatból). Az egyetlen LONG jelölt (`pattern_10`) nem valódi — a 3 fold-ból 2-ben a matched
klaszter üres volt validation-ön, tehát a "konzisztencia" hiányzó adatból, nem egyetértésből
fakad.

**Végső konklúzió:** A CV-optimalizált k=13 klaszterezés objektíven jobb szeparációt ad, mint
a fix k=10, de ez a javulás nem stabil sem méretben, sem irányban, sem cross-fold
azonosságban. Legfeljebb egyetlen kicsi (~1-2% az adatból), valóban megbízható SHORT jel
azonosítható; LONG oldalon nincs hasonló. Ez, a t871 realisztikus backtest eredményével
(nincs edge semmilyen konfigurációban) együtt, **elégséges alap az Epic 065 végleges
lezárásához, további klaszter-alapú trading backtest nélkül.**

Teljes dokumentáció: `_doc_/models_doc/6502_cv_cluster_separation_optimization.md` (Section 8-9).
Epic összefoglaló: `_jira_/epic_065_cluster_regime_strategy/epic.md`.

**Ticket lezárva: `pr_` → `done_` (user explicit kérésére, validator_agent session nélkül).**
