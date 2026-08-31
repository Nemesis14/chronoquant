---
epic: epic_050
id: t3
title: config/models.json — két új model entry (l/s, 2101_2607)
assignee: modeling_agent
status: done
blocks: [t4, t5]
blocked_by: [t2]
---

## Goal

`lgbm_solusdt_l_fw60_2101_2607` és `lgbm_solusdt_s_fw60_2101_2607` bejegyzés
felvétele `config/models.json`-ba, train_valid_split móddal, az epic-ben
eldöntött ablakokkal: train 2021-01-01 – 2026-01-31, valid 2026-02-01 –
2026-07-31, a t2-ben létrehozott snapshot_id-vel.

## Scope

`config/models.json`.

## Acceptance Criteria

- [x] Két új model entry, `active: false` (amíg élesítés nem történik)
- [x] `sampling.snapshot_id` a t2 snapshotra mutat
- [x] `feature_lookback_embargo_minutes: 240`, `target_purge_minutes: 60` megtartva
- [x] JSON valid, séma v4-nek megfelel

## Notes

`lgbm_solusdt_l_fw60_2101_2607` és `lgbm_solusdt_s_fw60_2101_2607` felvéve,
`snapshot_id=solusdt_fw60_2101_2607__49466cd2`, train 2021-01-01–2026-01-31,
valid 2026-02-01–2026-07-31. JSON parse OK, 4 model entry összesen.
