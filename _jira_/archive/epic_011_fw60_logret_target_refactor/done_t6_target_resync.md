---
epic: epic_011
id: t6
title: Target tábla teljes újrafeltöltése DuckDB-ben
assignee: database_agent
status: todo
blocked_by: [t3]
blocks: [t4]
---

## Goal

A régi target tábla törlése és az új schema alapján teljes újrafeltöltése az OHLCV adatokból.

## Scope

```
database/solusdt/solusdt.duckdb   (target tábla)
src/database/02_sync_pipeline.py  (vagy a sync futtatás belépési pontja)
```

## Lépések

1. `DROP TABLE target` a DuckDB-ben
2. `ensure_tables` futtatás → újracsinálja a target táblát az új DDL-lel (T1)
3. `sync_targets` teljes futtatás → OHLCV-ből kiszámolja és upserteli az összes fw60 oszlopot

## Acceptance Criteria

- [ ] Target tábla újralétrejött az új schema-val (10 oszlop + open_time PK)
- [ ] Sor szám egyezik az OHLCV tábla soraival (mínusz utolsó 60 NULL tail sor)
- [ ] `long_mfe_fw60` és `short_mfe_fw60` nem NULL az első válid sorokban
- [ ] Utolsó 60 sor: minden fw60 oszlop NULL
- [ ] Nincs `trg_l_fw60_q90` / `trg_s_fw60_q10` oszlop a táblában

## Notes
