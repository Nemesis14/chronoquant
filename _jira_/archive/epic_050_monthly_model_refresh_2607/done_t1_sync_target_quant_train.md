---
epic: epic_050
id: t1
title: target + quant_train tábla frissítése 2026-07-31-ig
assignee: database_agent
status: done
blocks: [t2]
blocked_by: []
---

## Goal

Az OHLCV/feature szinkron már 2026-08-12-ig fut, de a `target` és
`quant_train` tábla csak 2026-06-14-ig friss. Az új modellek tréningjéhez
kell a target adat legalább 2026-07-31-ig.

## Scope

`src/data_handling/02_sync_pipeline.py` (targets), `src/data_handling/03_build_quant_train.py`.

## Acceptance Criteria

- [x] `target` tábla `max(open_time) >= 2026-07-31 23:00`
- [x] `quant_train` tábla range-rebuild a kiegészített szakaszra, sorszám konzisztens a target+feat_ohlcv_quant join-nal
- [x] Nincs időrés (gap) a kiegészített szakaszon

## Notes

`02_sync_pipeline.py --tables targets --skip-ohlcv --start "2026-06-01 00:00:00"`
majd `03_build_quant_train.py --start "2026-06-01 00:00:00"` lefutott.
- `target`: max(open_time) = 2026-08-12 09:55 (3107696 sor, csak az utolsó 60 perc NULL — fw60 purge, várt).
- `quant_train`: max(open_time) = 2026-08-12 08:55 (3107631 sor).
Mindkettő jóval túl van a 2026-07-31 igényen.
