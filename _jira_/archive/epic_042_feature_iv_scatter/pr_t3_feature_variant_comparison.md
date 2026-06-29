---
id: t3
epic: epic_042
title: Feature variáns összehasonlítás notebook
status: pr
assignee: analyst_agent
created: 2026-06-26
---

## Leírás

3 feature variáns LightGBM modell összehasonlítása decilis analízissel (train + valid):
- `top10`: 10 feature (MI-ben legjobb)
- `top1_per_group`: 17 feature (minden csoport bajnoka)
- `selected`: 131 feature (minden MI > 0.001 + nem duplikátum)

## Elvégzett munka

### Notebook: `artifacts/lgbm_solusdt_l_fw60_2101_2605/comparison/feature_variant_comparison.ipynb`

**Módszer:** Mindhárom variáns azonos `best_params.json` paraméterekkel edzett
(fair összehasonlítás — csak a feature input különbözik, nem a hiperparam konfig).

**Metrikák:**
| Variáns | n_features | Train RMSE | Valid RMSE |
|---------|-----------|------------|------------|
| top10           | 10  | 0.0094 | 0.0050 |
| top1_per_group  | 17  | 0.0094 | 0.0051 |
| selected        | 131 | 0.0094 | 0.0050 |

**Ábrák:**
- Decilis ábrák variánsonként (train + valid, 3×2 grid)
- Top decilis átlag target összehasonlítás (bar chart, train vs valid)
- Overlay vonaldiagram (mind a 3 variáns egy ábrán, train + valid)
- Konklúzió táblázat (top10 avg, top10 lift, overfit = train_lift - valid_lift)

## Notes

- `best_iter=600` mind a 3 modellnél: early stopping nem volt aktív (600 iteráció = max)
  → ha pontosabb összehasonlítás kell: n_estimators emelése (pl. 1500) + early_stopping(50)
- Valid RMSE < Train RMSE: a valid periódus valószínűleg alacsonyabb volatilitású
- A 3 variáns közel azonos RMSE-t mutat → a top10 feature már szinte teljes információt
  hordoz; a többletfeature-ök marginális javulást adnak
- A `feature_set.json` már tartalmazza `top10` és `top1_per_group` listákat a search step-hez
