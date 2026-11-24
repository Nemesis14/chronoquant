---
epic: epic_078
id: t963
title: Placebo-ellenőrzés a nyertes változókra
assignee: analyst_agent
status: todo
blocks: [t964]
blocked_by: [t962]
---

## Goal
Ugyanaz a pipeline a sign-placebo szintetikus áron: az AUC-javulás eloszlása a véletlenben, és a valós javulás empirikus p-értéke.

## Acceptance Criteria
- [ ] Placebo-eloszlás csoportonként és skálánként (a volumen/flow csoportnál a placebo mérethű kezelése dokumentálva).
- [ ] Empirikus p, és világos: melyik csoport marad meg a placebo után.
- [ ] ruff + pyright tiszta.

## Notes

[analyst] 2026-11-24 — kész.
- Kód: `src/reports/builders/cand_pred_placebo.py`; táblák `research."cand_pred_v1_placebo / _placebo_univar / _placebo_selected / _compare / _compare_univar"`.
- Placebo: a teljes lánc (zigzag → jelöltek → 48 változó → idő szerinti modellek, greedy + GBM is) 80 szintetikus áron, véletlen előjellel (|r| marad, volumen/flow/funding valós, az ár irányához nem kötött). A `qd_` változók záróár-alapúak, ezért a placebo-áron azonosan újraszámolhatók.
- Fő eredmény (cél: nagy mozgás, pooled 2023–25): valós AUC(all) 0,523 vs placebo 0,527 [0,512; 0,547], p = 0,69; GBM 0,522 vs 0,524. A placebón a mechanikus geometriai csoport (z, főleg z_risk) önmagában 0,533 → a +0,03 AUC a zigzag-definíció mechanikája (minél nagyobb a megerősítésig tartó visszapattanás, annál közelebb a 4σ cél), nem piaci információ. A placebón a greedy 99 / 80-szor a z-t választja.
- Csoportonként (real vs placebo AUC, big): z 0,521/0,533, v 0,512/0,519, w 0,519/0,500 (p_two 0,049; p_up 0,037), f 0,492/0,518 (p 0,012, a valós rosszabb), q 0,527/0,523, s 0,494/0,512. Egyetlen enyhén pozitív jel a VWAP-csoport (w_anch_low, w_anch_hp egyváltozós AUC 0,526 / 0,522 vs placebo 0,497 / 0,496, p = 0,025), 16 modell + 48 változó mellett a véletlen szintje (várt ~2,4 db p<0,05 a 48-ból, kijött 4 / 1 a két célnál); nem szignifikáns korrekció után és a +1R célon sem jön elő (hit1: all 0,499 vs 0,499).
- Cél +1R: minden modell ≈ placebo (all 0,499 vs 0,499, GBM 0,510 vs 0,500).
- Verdikt: a vizsgált hat változócsoport egyike sem ad a jelölt-nevezőhöz a véletlenhez képest használható előrejelzést (BTC, long, T = 60/240/960, k = 4). A placebo-minimum p-érték 1/81 ≈ 0,012.
