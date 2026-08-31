---
epic: epic_058
id: t703
title: Modell tanítás és out-of-sample kiértékelés
assignee: modeling_agent
status: pr
blocks: [t704]
blocked_by: [t702]
---

## Goal

Megmérni, hogy a triple-barrier modell felülmúlja-e az 55%-os fedezeti pontot.

## Konfiguráció

```
feature-keszlet : ugyanaz a 230 (208 + 22 alt-data)
train           : 2023-01-01 -> 2025-07-31
valid           : 2025-08-01 -> 2026-07-31   (12 honap OOS)
```

Szándékosan ugyanaz az ablak, mint az `epic_057` t601-nél — így a két
megközelítés közvetlenül összevethető.

## ⚠️ A mérés szabályai

**Ne in-sample optimumot riportálj.** Ma háromszor buktunk el ezen.
A kiértékelés: a `P(+1)` küszöböt a train végén rögzítjük, és a teljes
valid ablakon **változatlanul** alkalmazzuk.

## A döntő szám

| | Elvárás |
|---|---|
| A `P(+1) > küszöb` csoport tényleges +1 aránya | **> 55%** |
| Alap-arány összevetésként | 47,8% |

55% alatt nincs értelme folytatni: a díjak után nem fedez.

## Acceptance Criteria

- [ ] Modell betanítva, predikció a teljes snapshot range-re
- [ ] Küszöbönkénti tábla: küszöb, n, +1 arány, −1 arány, 0 arány
- [ ] Negyedéves bontás — van-e olyan időszak, ahol összeomlik
- [ ] Explicit válasz: 55% fölött van-e, és mekkora mintán

## Notes
