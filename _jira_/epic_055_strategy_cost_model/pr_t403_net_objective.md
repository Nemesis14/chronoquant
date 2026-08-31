---
epic: epic_055
id: t403
title: Nettó célfüggvény a rácskeresésbe
assignee: modeling_agent
status: pr
blocks: [t404]
blocked_by: [t401, t402]
---

## Goal

A rácskeresés a **nettó** hozamra optimalizáljon, ne a bruttóra.

Ez az epic lényege. A t401 és t402 csak addig ér valamit, amíg a kereső
ténylegesen használja is őket a döntéshez.

## Scope

- `src/strategy/strategy/search.py`
- `strategy_artifact.json` — a `search_info.best_objective` mező frissítése

## Változás

```
regi:  objective = total_fact_log_return
uj:    objective = total_fact_log_return - SUM(dij + csuszas) - funding_hatas
```

A `grid_results` tábla tartsa meg **mindkettőt** (bruttó és nettó), hogy a
különbség utólag kimutatható legyen.

## Acceptance Criteria

- [ ] A rács a nettó értékre rendez
- [ ] `grid_results`: `total_fact_log_return`, `total_cost`, `total_funding`,
      `net_log_return` oszlopok
- [ ] `strategy_artifact.json` jelzi, hogy nettó objective futott
- [ ] Regressziós teszt: nulla díjjal a nettó = bruttó, és a választott setup
      megegyezik a régivel
- [ ] `ruff check` + `pyright` tiszta

## Notes
