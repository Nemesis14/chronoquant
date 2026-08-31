---
epic: epic_054
id: t308
title: 22 alt-data feature számítása és bekötése a quant_train-be
assignee: database_agent
status: pr
blocks: [t309, t310]
blocked_by: [t307]
---

## Goal

Az `altdata_5m` nyers oszlopaiból kiszámolni a 22 modellezési változót, és
elérhetővé tenni őket a meglévő modellezési pipeline számára.

## Scope

- `src/data_handling/sync_tables/_altdata_features_polars.py` — új feature modul
- `database/solusdt/solusdt.duckdb` → `feat_altdata` tábla (1 perces rács)
- `src/data_handling/03_build_quant_train.py` — join kiegészítése

## A 22 változó

**A) Tőkeáttétel-állomány (7)**

| név | képlet | ablak |
|---|---|---|
| `feat_oi_to_volume_60` | oi_coin / Σperp_volume | 60m |
| `feat_oi_delta_15` | oi_coin relatív változás | 15m |
| `feat_oi_delta_60` | oi_coin relatív változás | 60m |
| `feat_oi_delta_240` | oi_coin relatív változás | 240m |
| `feat_oi_accel_60` | Δoi(0–60) − Δoi(60–120) | 60m |
| `feat_oi_unwind_60` | min(oi) / max(oi) az ablakban | 60m |
| `feat_oi_price_div_60` | Δoi% × sign(perp return 60m) | 60m |

**B) Pozicionáltsági aszimmetria (5)**

| név | képlet | ablak |
|---|---|---|
| `feat_ls_retail_vs_top_60` | log(ls_count) − log(ls_top_sum), átlag | 60m |
| `feat_ls_count_z_240` | ls_count trailing z-score | 240m |
| `feat_ls_top_sum_z_240` | ls_top_sum trailing z-score | 240m |
| `feat_ls_top_conc_60` | log(ls_top_count) − log(ls_top_sum) | 60m |
| `feat_ls_count_delta_60` | ls_count változás | 60m |

**C) A tőkeáttétel ára (6)**

| név | képlet | ablak |
|---|---|---|
| `feat_prem_mean_15` | prem_close átlag | 15m |
| `feat_prem_mean_60` | prem_close átlag | 60m |
| `feat_prem_std_60` | prem_close szórás | 60m |
| `feat_prem_pctile_240` | prem_close percentilis-rang | 240m |
| `feat_funding_countdown` | percek a következő elszámolásig | — |
| `feat_funding_dev` | funding_last − 0.0001 (alapérték-eltérés) | — |

**D) Spot vs perp (4)**

| név | képlet | ablak |
|---|---|---|
| `feat_basis_60` | (perp_close − spot_close)/spot_close, átlag | 60m |
| `feat_basis_z_240` | bázis trailing z-score | 240m |
| `feat_spot_perp_vol_ratio_60` | Σspot_volume / Σperp_volume | 60m |
| `feat_spot_taker_imb_60` | spot taker buy / total | 60m |

## ⚠️ Kötelező szabályok

1. **Minden ablak ≤ 240 perc** — a normalizálás is. Hosszabb baseline tilos,
   mert a `feature_lookback_embargo_minutes: 240` az egész projektben ennyi.
   Nincs 30 napos z-score.
2. **`t-1` lag kötelező**, a projekt feature-konvenciójával egyezően.
3. **Az 5m → 1m átvezetés forward-fill az utolsó LEZÁRT 5 perces barból.**
   A sampler óránként vesz egy sort, tehát a ffill nem hoz létre ismétlődő
   mintasorokat; a frissesség legfeljebb 5 perc.
4. **Warmup NULL-lal jelölve**, nem 0-val.

## Acceptance Criteria

- [ ] `feat_altdata` tábla feltöltve, 1 perces rács, 22 oszlop
- [ ] Shift-teszt: jövőbeli adat megváltoztatása nem változtatja a múltbeli
      feature-értéket — explicit teszt
- [ ] Egyetlen ablak sem hosszabb 240 percnél — teszt ellenőrzi
- [ ] `quant_train` tartalmazza a 22 oszlopot
- [ ] `ruff check` + `pyright` tiszta

## Notes
