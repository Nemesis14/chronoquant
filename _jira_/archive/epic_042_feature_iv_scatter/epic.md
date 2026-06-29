# Epic 042 — Feature IV Scatter Notebook

**Status:** pr  
**Created:** 2026-06-24  
**Assignee:** analyst_agent

## Leírás

Feature engineering elemzés a `lgbm_solusdt_l_fw60_2101_2605` long champion modellhez.
Minden feature-re `ContinuousOptimalBinning` (max 10 bin), scatter plot bin átlag target vs bin,
regressziós egyenessel és IV értékkel. Notebook struktúra: H2 = feature csoport, tabset = feature.

## Taskok

- t1: feature_engineering_scatter.ipynb újraírása IV + bin scatter spec szerint → analyst_agent
- t2: MI bevezetése + feature engineering pipeline csere → analyst_agent
- t3: Feature variáns összehasonlítás notebook (top10 / top1_pg / selected) → analyst_agent
