---
epic: epic_011
id: t2
title: sync_targets — logreturn outcome-ok implementálása
assignee: database_agent
status: todo
blocked_by: [t1]
blocks: [t3, t4]
---

## Goal

A `sync_targets.py`-ból eltávolítani a régi binary target számítást, és helyette kiszámolni + persistálni az összes fw60 forward outcome oszlopot.

## Scope

```
src/database/sync_tables/sync_targets.py
```

## Törlendő logika

- `global_long_threshold = q90(future_max_return)` számítás
- `global_short_threshold = q10(future_min_return)` számítás
- `trg_l_fw60_q90` és `trg_s_fw60_q10` képzése és upsert-je

## Új számítási logika

Forward window: `t+1 .. t+60` (aktuális bar kizárva — SQL: `ROWS BETWEEN 1 FOLLOWING AND 60 FOLLOWING`)

```
close[t]         → a jelenlegi bar close ára
close[t+60]      → fw60_close
max(close[t+1:t+60]) → fw60_max
min(close[t+1:t+60]) → fw60_min

fw60_close_ret   = fw60_close / close - 1
fw60_close_logret = log(fw60_close / close)
fw60_max_ratio   = fw60_max / close
fw60_min_ratio   = fw60_min / close
long_mfe_fw60    = log(fw60_max / close)
short_mfe_fw60   = log(fw60_min / close)
```

## Null tail

Az utolsó 60 sor minden fw60 oszlopa NULL (nincs elég jövőbeli adat).

## Acceptance Criteria

- [ ] Régi binary threshold számítás eltávolítva
- [ ] Mind a 10 céloszlop ki van számolva
- [ ] Aktuális bar ki van zárva a forward windowból (`t+1`-től számít)
- [ ] Utolsó 60 sor NULL minden fw60 oszlopban
- [ ] Upsert idempotens (`open_time`-ra keyed)
- [ ] `pyright` + `ruff` átmegy

## Notes
