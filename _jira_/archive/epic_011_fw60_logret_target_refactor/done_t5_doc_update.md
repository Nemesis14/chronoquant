---
epic: epic_011
id: t5
title: Dokumentáció frissítése — fw60 target layer
assignee: doc_agent
status: todo
blocked_by: [t4]
---

## Goal

Eltávolítani a régi binary target leírásokat a dokumentációból, és leírni az új fw60 forward outcome layer fogalmait.

## Scope

```
_doc_/0000_project_overview.md          (Database → Tables szekció, ML Models szekció)
_doc_/1240_sync_targets.md              (teljes tartalom — fő target doc)
```

## Mit kell frissíteni

### `_doc_/0000_project_overview.md`

- **Database → Tables:** `target` tábla sorában frissíteni a leírást:
  - régi: `trg_l_fw60_q90`, `trg_s_fw60_q10`
  - új: `long_mfe_fw60`, `short_mfe_fw60` + a többi fw60 outcome oszlop
- **ML Models → Active models:** target sor frissítése
  - régi: `trg_l_fw60_q90` / `trg_s_fw60_q10`
  - új: `long_mfe_fw60` / `short_mfe_fw60`

### `_doc_/1240_sync_targets.md`

Átírni az egész dokumentumot az új modell szerint:

1. **Forward outcome definíciók** — mind a 10 oszlop, képlet + szemantika
2. **Forward window szabály** — `t+1..t+60`, aktuális bar kizárva, utolsó 60 sor NULL
3. **Long / short target szemantika:**
   - `long_mfe_fw60`: pozitív = felfelé ment → long kedvező
   - `short_mfe_fw60`: negatív = lefelé ment → short kedvező
4. **Régi binary target** — rövid megjegyzés: eltávolítva, volt `trg_l_fw60_q90`/`trg_s_fw60_q10`

## Acceptance Criteria

- [ ] `_doc_/0000_project_overview.md` target tábla sora, ML models sora frissítve
- [ ] `_doc_/1240_sync_targets.md` leírja az összes fw60 outcome oszlopot képlettel
- [ ] Nincs régi `trg_l_fw60_q90` / `trg_s_fw60_q10` referencia a doc fájlokban
- [ ] Forward window szabály (t+1, NULL tail) dokumentálva

## Notes
