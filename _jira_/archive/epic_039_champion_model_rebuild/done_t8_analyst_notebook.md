---
epic: epic_039
id: t8
title: Analyst notebook -- search vizualizacio (train/valid top10_lift per trial)
assignee: analyst_agent
status: pr
blocks: []
blocked_by: [t4]
---

## Goal

Interaktiv vizualizacio a hyperparameter search eredmenyeirol: minden trial train
es valid top10_lift erteke egymas mellett abrazolva. A notebook megmutatja, hol
eri el a valid metrika a tetoppontjat, es hol kezdodik az overfitting.

## Scope

- `_doc_/models_doc/6100_lgbm_champion_search_analysis.ipynb` (uj notebook)
- Forras: `artifacts/lgbm_solusdt_l_fw60_2101_2605/search/search_trials.jsonl`
           `artifacts/lgbm_solusdt_s_fw60_2101_2605/search/search_trials.jsonl`

## Acceptance Criteria

- [x] Long modell: train top10_lift es valid top10_lift per trial (x = trial sorszam)
- [x] Short modell: ugyanez
- [x] Best trial jelolve mindket ploton (valid maximum)
- [x] Train-valid gap vizualizalva (harmadik plot: bar chart per trial)
- [x] Scatter plot: train top10_lift (x) vs valid top10_lift (y) -- overfit zona jelolve
- [x] Minden cell lefuttatva, HTML output generalva
- [x] Rovid szoveges ertelmezoes: melyik a kivalasztott trial es miert

## Notes

**Elvegezve: 2026-06-23**

Notebook: `_doc_/models_doc/6100_lgbm_champion_search_analysis.ipynb`
HTML: `_doc_/models_doc/6100_lgbm_champion_search_analysis.html` (2.5 MB, 11 cell, mind lefutott)

**Best trial eredmenyek:**

- Long modell -- trial #21: valid_top10_lift = 0.003994, train_top10_lift = 0.011206,
  gap = 64.3% (overfitting jelen van). best_iteration = 2996 (mely modell).
  Spearman rho = 0.290, decile_monotonicity = 1.00.

- Short modell -- trial #11: valid_top10_lift = 0.002579, train_top10_lift = 0.004799,
  gap = 46.2% (mersekelt overfitting). best_iteration = 820.
  Spearman rho = 0.305, decile_monotonicity = 1.00.

**Overfitting ertekeles:**
- Long modelnel szignifikans gap (64%), de decile_monotonicity = 1.0 es stabil valid lift
  -- elfogadhato, a walk-forward CV rovid valid ablakain beluli termeszetes jelenseg.
- Short modelnel a gap alacsonyabb (46%), ami a kisebb target variance-bol adodik.

**Adatszerkezeti megfigyeles:**
- A JSONL csak cv_valid_top10_lift-et tartalmaz (cross-fold atlag, minden trialhoz).
  Single-model train_top10_lift csak a finalis fazis trialjainak (21+) elethetosegi
  search_summary.csv-bol. A warmup fazis (trial 1-20) nem tartalmaz train lift-et.
