---
epic: epic_011
id: t1
title: Target tábla schema — fw60 logreturn oszlopok
assignee: database_agent
status: todo
blocks: [t2]
---

## Goal

A `target` tábla schema-ját átírni: régi binary oszlopok törlése, új fw60 forward outcome oszlopok hozzáadása.

## Scope

```
src/database/store/duckdb_store.py
```

## Régi oszlopok — törlendők

```
trg_l_fw60_q90   BOOLEAN
trg_s_fw60_q10   BOOLEAN
```

## Új oszlopok

| Oszlop | Típus | Definíció |
|--------|-------|-----------|
| `close` | DOUBLE | close[t] — jelenlegi bar close |
| `fw60_close` | DOUBLE | close[t+60] — raw forward close ár |
| `fw60_max` | DOUBLE | max(close[t+1:t+60]) — raw max ár |
| `fw60_min` | DOUBLE | min(close[t+1:t+60]) — raw min ár |
| `fw60_close_ret` | DOUBLE | close[t+60] / close[t] - 1 |
| `fw60_close_logret` | DOUBLE | log(close[t+60] / close[t]) |
| `fw60_max_ratio` | DOUBLE | max(close[t+1:t+60]) / close[t] |
| `fw60_min_ratio` | DOUBLE | min(close[t+1:t+60]) / close[t] |
| `long_mfe_fw60` | DOUBLE | log(max(close[t+1:t+60]) / close[t]) — LONG TARGET |
| `short_mfe_fw60` | DOUBLE | log(min(close[t+1:t+60]) / close[t]) — SHORT TARGET |

## Acceptance Criteria

- [ ] `ensure_tables` tartalmazza az új DDL-t
- [ ] Régi `trg_l_fw60_q90`, `trg_s_fw60_q10` oszlopok eltűntek a schemából
- [ ] Schema migration kezeli a meglévő DB-t (ALTER TABLE vagy DROP/RECREATE)
- [ ] `open_time` PK megmarad

## Notes
