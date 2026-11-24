---
epic: epic_054
id: t309
title: Modell C (kontroll) — 208 meglévő feature, 2022-01-től
assignee: modeling_agent
status: pr
blocks: [t311]
blocked_by: [t308]
---

## Goal

Tiszta viszonyítási alapot építeni a t310 kezelt modellhez.

**Miért kell:** az élő champion 2021-01-01-től tanul, az alt-data viszont csak
2021-12-01-től létezik. Ha közvetlenül a championt hasonlítanánk a kezelt
modellhez, két dolog változna egyszerre — bekerülne 22 változó, **és** eltűnne
11 hónap tanítóadat. A kontroll ezt a második hatást izolálja.

C és T között így **egyetlen dolog különbözik: a 22 alt-változó.**

## Konfiguráció

```
model_id     : lgbm_solusdt_l_fw60_ctrl_2201_2607
target       : long_mfe_fw60
sampling_mode: train_valid_split
train        : 2022-01-01 → 2026-01-31
valid        : 2026-02-01 → 2026-07-31   (a championnel AZONOS)
embargo      : 240 perc     purge: 60 perc     seed: 42
feature-ök   : a meglévő 208 (feat_ohlcv_quant), alt-data NÉLKÜL
```

## Végrehajtás

A meglévő pipeline, változtatás nélkül:

```
setup → sample → feature_engineering → search → train → predict → analyze
```

## Acceptance Criteria

- [ ] `config/models.json` bejegyzés létrehozva
- [ ] Mind a 7 pipeline lépés lefutott
- [ ] `search_best.json` + valid `top10_lift` rögzítve
- [ ] Az analyze notebookok legenerálva
- [ ] A valid ablak bit-pontosan azonos a championével

## Notes
