---
epic: epic_054
id: t310
title: Modell T (kezelés) — 208 + 22 alt-data feature
assignee: modeling_agent
status: pr
blocks: [t311]
blocked_by: [t308]
---

## Goal

Megmérni, hogy a 22 alt-data változó hozzáad-e bármit a meglévő
feature-készlethez.

**Ez a döntő futás.** A FE lépés maga fogja megmondani, melyik alt-változó
marad bent és melyik esik ki — pont az az elemzés, ami a meglévő
modellfejlesztési folyamat része.

## Konfiguráció

```
model_id     : lgbm_solusdt_l_fw60_alt_2201_2607
target       : long_mfe_fw60
train/valid  : AZONOS a t309 kontrolléval
feature-ök   : 208 meglévő + 22 alt-data = 230
```

## Amit a FE lépéstől várunk

Az `optbinning` IV + `mutual_info_regression` MI + korrelációs redundancia-szűrő
változónként. A `feature_set.json`-ból ki kell derülnie:

- melyik alt-változó **maradt bent**
- melyik esett ki **gyenge magyarázóerő** miatt (alacsony IV/MI)
- melyik esett ki **redundancia** miatt — és **melyik meglévő feature-rel**
  korrelált

⚠️ Az utolsó pont a legfontosabb eredmény. Ha például a `feat_oi_delta_60`
kiesik, mert 0,9 fölött korrelál egy meglévő volatilitás-feature-rel, az
önmagában válasz: az OI-információ már benne van a modellben, más néven.

## Acceptance Criteria

- [ ] Mind a 7 pipeline lépés lefutott
- [ ] `feature_set.json` tartalmazza a bent maradt / kiesett alt-változókat
      **kiesési okkal együtt**
- [ ] A 22 alt-változó IV és MI értéke táblázatban, a 208 meglévővel egy
      rangsorban — hol állnak a mezőnyben?
- [ ] Feature importance a betanított modellből, alt-változók kiemelve
- [ ] Valid `top10_lift` rögzítve, a t309 kontrolléval összevethetően

## Notes
