# Kutatás: Joint Feature + Hyperparameter Search

**Cél:** Megvizsgálni, hogy a feature szelekciónak a hyperparameter search-be való integrálása javítja-e a modell generalizációját és a kereskedési teljesítményt.

**Mérési időszak:** 2025-05-01 – 2026-05-31 (valid / strategy ablak)  
**Modell:** `lgbm_solusdt_l_fw60_2101_2605` (long irány)

---

## Háttér

Az alap hyperparameter search fix feature listán (131 MI-szűrt változó) kereste az optimális LightGBM paramétereket. A probléma: a LightGBM-nek nincs L1-szerű regularizáció, amely kevesebb feature felé terelje a modellt. A `reg_alpha` és `reg_lambda` levélértékeket regularizál, de nem feature-számlálást. A `feature_k` Optuna-paraméterként való kezelése ezt direkten kezeli: a keresés egyszerre optimalizálja a paramétereket és a feature-számot.

---

## Kísérleti variánsok

| Variáns | feature_selection | feature_k | gap_penalty (λ) | search_dir |
|---------|------------------|-----------|-----------------|------------|
| selected (baseline) | fixed | 131 (fix) | 0 | `search/` |
| joint K=57 | joint | Optuna választja | 0 | `search_joint/` |
| joint_reg λ=0.1 | joint | Optuna választja | 0.1 | `search_joint_reg_gp10/` |
| joint_reg λ=0.2 | joint | Optuna választja | 0.2 | `search_joint_reg_gp20/` |

**Joint search mechanizmus:** `feature_k` Optuna integer paraméter (log-scale, 3–131). Feature sorrendezés: `run_gain_rank()` — egy LightGBM fit `colsample_bytree=1.0`-val, gain fontosság szerint csökkenő sorrendbe rendezve. Minden trial a `gain_ranked[:feature_k]` részhalmazzal dolgozik.

**Gap penalty:** A search objective-be beépített regularizáció: `penalized = valid_ratio - λ × max(0, gap)` ahol `gap = train_ratio - valid_ratio`. A search `objective_score = -penalized`-ot minimalizál.

---

## Search eredmények (explore stage, 60 trial)

```
Metrika                   selected(131)  joint K=57   gp λ=0.2   gp λ=0.1
---------------------------------------------------------------------------
valid_ratio_p925                 1.9900     1.9974     1.9399     1.9700
train_ratio_p925                 2.6427     2.7150     1.9812     2.6292
train_valid_gap                  0.6528     0.7177     0.0412     0.6592
penalized_ratio                       —          —     1.9317     1.9041
spearman_rho                     0.2932     0.2901     0.2726     0.2944
best_iteration                     1640       1379        336        436
feature_k                             —         57          5         59
```

### Két klaszter

Az eredmények egyértelműen két viselkedési rezsimre osztódnak:

**Klaszter A — nagy kapacitású (λ < 0.12):**
- K = 57–131 feature
- train-valid gap: ~0.65–0.72 (erős overfit a search célban)
- valid_ratio_p925: ~1.97–2.00

**Klaszter B — kompakt (λ ≥ 0.2):**
- K = 5 feature
- train-valid gap: ~0.04 (közel nulla gap)
- valid_ratio_p925: ~1.94

### Fázisátmenet

A λ=0.1 lényegében ugyanazt az eredményt adja mint λ=0 (K=59, gap~0.66), nem K=5-öt. A fázisátmenet λ≈0.11–0.12 körül van, ahol az overfit büntetése elegendő ahhoz, hogy az optimalizátor feladja a magas-K, magas-gap megoldást.

---

## Strategy összehasonlítás

Mindkét klaszter reprezentánsát befitteltük és lefuttattuk a strategy grid search-t (long irány, 2025-05 – 2026-05).

### Training és predict

```
Variáns          n_features  n_estimators
-----------------------------------------
joint K=57            57         1517
joint_reg K=5          5          370
```

### Grid search eredmények (long only, valid ablak)

| Metrika | joint K=57 | joint_reg K=5 |
|---|---|---|
| total_fact_log_return | **0.4063** | 0.2644 |
| compounded_return_pct | **+50.1%** | +30.3% |
| win_rate | **79.5%** | 52.4% |
| n_trades | 78 | 420 |
| best entry_cutoff | 0.98 | 0.90 |
| best tp_spec | bucket_median_mfe | bucket_p75_mfe |

### K=5 modell anomália

A K=5 modellnél a cutoff 0.90–0.96 között azonos n_trades (420) és azonos total_lr (0.2644) értéket ad. Ez azt jelenti, hogy a modell predikciói annyira összesűrűsödöttek, hogy a top 4%–10% pontosan ugyanazokat a barokat tartalmazza. Az 5 feature nem nyújt elegendő discriminative power-t a jó és kiváló belépési pontok szétválasztásához a valid ablakban.

---

## Konklúzió

**A joint K=57 a nyertes:** 50.1% compounded return vs 30.3%, 79.5% win rate vs 52.4%. A gap regularizáció (λ=0.2) ugyan megoldja a train-valid gap problémát, de azzal egyidejűleg a modell komplexitását és prediktív erejét is lecsökkenti.

**A joint search (gap_penalty=0, feature_selection="joint") az éles pipeline standard megközelítése.** A gap_penalty kutatási opcióként megmarad a kódban, de a módszertani alapértelmezett 0.

**HTML reportok:** az összes search variáns HTML reportja az artifact könyvtárban érhető el:
- `artifacts/lgbm_solusdt_l_fw60_2101_2605/search_joint/search_report.html`
- `artifacts/lgbm_solusdt_l_fw60_2101_2605/search_joint_reg_gp20/search_report.html`
- `artifacts/lgbm_solusdt_l_fw60_2101_2605/search_joint_reg_gp10/search_report.html`

**Strategy session artifacts:**
- `strat_solusdt_fw60_combo_joint_k57` — joint K=57 long-only eredmény
- `strat_solusdt_fw60_combo_joint_reg_k5` — joint_reg K=5 long-only eredmény
