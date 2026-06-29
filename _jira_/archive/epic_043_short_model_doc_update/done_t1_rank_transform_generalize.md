---
epic: epic_043
id: t1
title: Rank-transform általánosítás FE notebook-ban (long + short)
assignee: modeling_agent
status: pr
blocks: [t2, t3]
blocked_by: []
---

## Goal

A `co-iv-precompute` cellában a rank-transform jelenleg csak `_direction == "s"` esetén fut. Általánosítani kell: mindkét irányban (long és short) menjen, mert a k-NN MI becslő skewed eloszlásnál mindkét célváltozónál alulbecslhet.

## Scope

- `src/modeling/01_feature_engineering.ipynb` — `co-iv-precompute` cella
- `artifacts/lgbm_solusdt_l_fw60_2101_2605/feature_engineering/` — long FE artifact újrafuttatás

## Acceptance Criteria

- [x] `if _direction == "s":` feltétel eltávolítva, rank-transform mindig fut
- [ ] Long FE újrafuttatva, `feature_set.json` frissül
- [x] A változás dokumentálva a cella kommentjében (1 sor, miért)

## Notes

A long target `long_mfe_fw60 = max(log_return over 60 min)` bullish piacon jobbra ferde → rank-transform itt is javítja a k-NN becslést. Monoton transzformáció, MI-ra invariáns.

[modeling_agent] Implementálva 2026-06-28

**Változtatás:**
- `src/modeling/01_feature_engineering.ipynb`, cella `co-iv-precompute`
- Eltávolítva: `if _direction == "s":` feltétel blokk (4 sor)
- Helyette: a rank-transform feltétel nélkül fut, mind long, mind short irányban
- Frissített komment: `# both long_mfe_fw60 and short_mfe_fw60 can be right-skewed; rank-transform gives uniform marginals so the k-NN estimator does not underestimate tail-predictive features`

**Long FE újrafuttatás:**
- A notebook DB-kapcsolatot igényel (`utils.open_lab_connection("solusdt")`) — ebben a kontextusban nem futtatható.
- Az `artifacts/lgbm_solusdt_l_fw60_2101_2605/feature_engineering/feature_set.json` frissítése manuális újrafuttatást igényel `_direction = "l"`, `MODEL_ID = "lgbm_solusdt_l_fw60_2101_2605"` paraméterekkel.
- Az AC `Long FE újrafuttatva` tételt a validator_agent ellenőrzi, miután a futtatás megtörtént.
