---
epic: epic_058
id: t702
title: Multiclass trainer és osztályozási search objective
assignee: modeling_agent
status: pr
blocks: [t703]
blocked_by: [t701]
---

## Goal

A pipeline eddig **kizárólag regresszióra** épült. Most osztályozás kell.

## Scope

- `src/modeling/training/` — `lightgbm_multiclass` trainer
- `src/modeling/search/` — osztályozási objective
- `config/models.json` — `trainer: "lightgbm_multiclass"`

## Amire figyelni

**Az objective NEM sima accuracy.** A stratégia csak a legmagasabb
konfidenciájú sávban kereskedik, tehát az számít, hogy **ott** mennyire pontos.

Javasolt objective: a `P(+1)` felső decilisében mért tényleges +1 arány,
mínusz az alap-arány. Ez a lift közvetlen megfelelője, csak osztályozásra.

A nyers log-loss félrevezető lenne: optimalizálhat a 0 osztály (timeout)
jó eltalálására, ami kereskedési szempontból érdektelen.

## Kimenet

A `predict` lépés három valószínűséget írjon: `p_up`, `p_down`, `p_flat`.

## Acceptance Criteria

- [ ] `lightgbm_multiclass` trainer működik, 3 osztályra
- [ ] Search objective a felső sávban mért liftet maximalizálja
- [ ] A `predict` lépés három oszlopot ír a `model.<id>__pred` táblába
- [ ] A meglévő regressziós út **nem törik el** (a champion modellek futnak)
- [ ] `ruff check` + `pyright` tiszta

## Notes
