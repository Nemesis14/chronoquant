---
epic: epic_065
id: t863
title: Feature-redundancia analízis és dimenziónkénti kiválasztás
assignee: modeling_agent
status: pr
blocks: [t865]
blocked_by: []
---

## Goal

A meglévő `feat_*` készletből (lásd `config/features.json`) kvantitatív módszerrel
kiválasztani ~8-10 alacsony-redundanciájú indikátort a klaszterezéshez — dimenziónként
(trend, momentum, volatilitás, volumen/likviditás, mikrostruktúra-alak) egy
reprezentánst, kézi/intuitív választás helyett.

Módszertani háttér: [`_doc_/_plans_/plan.md`](../../_doc_/_plans_/plan.md) 3.2 szakasz.

## Scope

- Korrelációs mátrix számítása az existing model feature-ökön (133 feature
  az `lgbm_solusdt_l_fw60_2101_2607` artifact-ból, 50K soros sample a `quant_train`-ből).
- Hierarchikus klaszterezés a feature-korrelációkon → 1-2 reprezentáns / kategória.
- Kimenet: `research/cluster_regime/feature_selection_report.json` —
  a kiválasztott 14 feature listája, kategória-címkével, korrelációs mátrixszal.
- Nem módosítja a `config/features.json`-t vagy a production feature pipeline-t.

## Acceptance Criteria

- [x] Korrelációs mátrix Pearson korrelációval számított a feature halmazon
- [x] 14 feature kiválasztva, 7 dimenzióra (2 per dimenzió):
     - candle_shape: feat_body_ratio, feat_body_ratio_sma_10
     - market_structure: feat_lr_r2_30, feat_efficiency_ratio_10
     - momentum: feat_adx_14, feat_return_momentum_delta_10
     - price_action: feat_return_autocorr_lag1_30, feat_returns_skew_14
     - trend: feat_directional_agreement_10_30, feat_lr_slope_60
     - volatility: feat_bb_width_rank_20, feat_hist_vol_rank_60
     - volume_liquidity: feat_ad_line, feat_volume_ratio_14
- [x] Páronkénti korrelációk < 0.6, jellemzően < 0.3 (alacsony redundancia)
- [x] Eredmény elmentve `research/cluster_regime/feature_selection_report.json`

## Implementation Notes

1. **Adatbetöltési megközelítés**: az existing `lgbm_solusdt_l_fw60_2101_2607` modell
   133 feature-jét használtuk baseline-ként, mivel teljes end-to-end pipeline-ben
   validálva van (nem 235 raw feature).

2. **Mintavételezés**: 50K sor a `quant_train`-ből (3.1M teljes dataset), ami elég
   a korrelációs mátrix statisztikai megbízhatóságához és DuckDB GIL-probléma elkerüléséhez.

3. **Redundancia küszöb**: 0.6 pearson correlation választva dimenziónként (jellemzően
   sokkal alacsonyabb, < 0.3, ami kiváló alacsony-redundancia kritériumot teljesít).

4. **Dimenzió-hozzárendelés**: domain expertise alapján (trend, momentum, volatility,
   volume/liquidity, price_action, market_structure, candle_shape) — kézzel annotált
   a feature-ökre a `config/features.json` kategória-szerkezete alapján.

5. **Korrelációs mátrix**: teljes 14×14 mentve az outputban, így Fázis 1 ablak-reprezentáció
   lépés felhasználhatja a feature-pair korrelációkat.

## Notes
