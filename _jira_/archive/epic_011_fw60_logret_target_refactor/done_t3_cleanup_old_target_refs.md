---
epic: epic_011
id: t3
title: Régi target referenciák teljes eltávolítása
assignee: database_agent
status: todo
blocked_by: [t2]
blocks: [t4]
---

## Goal

Minden `trg_l_fw60_q90` és `trg_s_fw60_q10` referenciát eltávolítani a teljes projektből — config, kód, metadata.

## Scope

```
config/targets.json           (vagy ahol a target config van)
config/models.json            (model target_col referenciák)
src/database/                 (minden további referencia)
src/modeling/                 (target col name referenciák, ha vannak)
database/solusdt/             (metadata fájlok)
```

## Keresési alap

```
rg "trg_l_fw60_q90|trg_s_fw60_q10|global_long_threshold|global_short_threshold|percentile.*target|target.*percentile"
```

Minden találatot megvizsgálni és eltávolítani / átírni.

## Acceptance Criteria

- [ ] `rg "trg_l_fw60_q90|trg_s_fw60_q10"` — nulla találat a `src/` és `config/` könyvtárakban
- [ ] config/targets.json (ha létezik) frissítve vagy törölve
- [ ] config/models.json-ban nincs régi target col referencia
- [ ] Meglévő metadata JSON-ok régi target mezői eltávolítva
- [ ] `pyright` + `ruff` átmegy

## Notes
