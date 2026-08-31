---
epic: epic_050
id: t2
title: Új snapshot létrehozása (2021-01-01 – 2026-07-31)
assignee: database_agent
status: done
blocks: [t3]
blocked_by: [t1]
---

## Goal

Az új champion modellek tréningjéhez immutable snapshot kell, amely lefedi a
kibővített range-et (2021-01-01 – 2026-07-31).

## Scope

`src/data_handling/05_create_snapshot.py`.

## Acceptance Criteria

- [x] `05_create_snapshot.py --asset-id solusdt --start 2021-01-01 --end 2026-07-31` lefut
- [x] Új `snap.<snapshot_id>` tábla létrejön (vagy content-hash alapján reuse, ha egyezik)
- [x] `reg.snapshots` bejegyzés létrejön, status=candidate

## Notes

`snapshot_id = solusdt_fw60_2101_2607__49466cd2`, rows=2933276,
range=[2021-01-01 00:00:00 .. 2026-07-31 00:00:00], elapsed=586.5s (~10 perc,
háttérben futott). Ez lesz a t3-ban felveendő két új model entry snapshot_id-je.
