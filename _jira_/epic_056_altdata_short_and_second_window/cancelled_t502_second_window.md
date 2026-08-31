---
epic: epic_056
id: t502
title: Második valid ablak — long, kontroll és kezelés
assignee: modeling_agent
status: todo
blocks: [t503]
blocked_by: []
---

## Goal

Megnézni, hogy a kezelés előnye egy másik időszakon is megvan-e.

## Konfiguráció

```
target       : long_mfe_fw60
train        : 2023-01-01 -> 2025-07-31
valid        : 2025-08-01 -> 2026-01-31
snapshotok   : UJ, a 2301_2601 tartomanyra
               - kezeles : teljes
               - kontroll: --exclude-altdata
model_id-k   : lgbm_solusdt_l_fw60_ctrl_2301_2601
               lgbm_solusdt_l_fw60_alt_2301_2601
```

Snapshot létrehozás:
```bash
uv run python src/data_handling/05_create_snapshot.py --asset-id solusdt \
  --start "2023-01-01 00:00:00" --end "2026-01-31 23:59:00"
uv run python src/data_handling/05_create_snapshot.py --asset-id solusdt \
  --start "2023-01-01 00:00:00" --end "2026-01-31 23:59:00" --exclude-altdata
```

## ⚠️ Amit nem szabad elfelejteni

A két ablak **nem független**: a tanítóadat nagy része közös
(2023-01 → 2025-07 mindkettőben benne van). Ha mindkettőn javul, az
megerősítés, de gyengébb, mint két külön minta. A riport ezt mondja ki.

## Acceptance Criteria

- [ ] Két új snapshot, eltérő ID-val (a hash a feature-készletet is tartalmazza)
- [ ] Mindkét modell végigfutott
- [ ] Strategy session mindkettőre, 8 bps költség-tudatos összevetéssel
- [ ] Explicit válasz: a második ablakon is megvan az előny?

## Notes
