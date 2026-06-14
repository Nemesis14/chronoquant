---
epic: epic_003_target_quality
id: t2
title: Teszt — target forward ablak t+1-től számolódik, nem t+0-tól
assignee: database_agent
status: pr
blocks: []
blocked_by: [t1]
---

## Goal

Igazolni, hogy a target forward return számításban a jelenlegi bar close-ja
(t) NEM szerepel a forward ablakban — kizárólag t+1..t+60 barok számítanak.

Ez data leakage prevención alapul: ha t-beli close benne lenne a forward
maxban, a model látná a jelenlegi árat a target-ben.

## Scope

- `_tests/store/` vagy `_tests/data_pipeline/` — új tesztek: `test_target_window.py`
- `src/data_pipeline/sync_targets.py` — kódelemzés

### Tesztek tartalma

1. **Unit teszt szintetikus adaton**: 5 soros OHLCV-t tölt be, manuálisan
   kiszámít egy expected forward_max_return-t (t+1..t+k, t kizárva),
   és összehasonlítja az `insert_target` által mentett értékkel.
2. **Produkciós szúrópróba**: véletlen 100 sort választ a `target` táblából
   és ellenőrzi, hogy `future_max_close > close[t]` lehetséges (nem tautológia),
   azaz nem csak a t-beli close-t tartalmazza.
3. **Regressziós biztonság**: `ROWS BETWEEN 1 FOLLOWING` (nem `0 FOLLOWING`)
   jelen van a `_TARGET_SQL` definícióban.

## Acceptance Criteria

- [ ] Unit teszt szintetikus 5-soros adaton: forward max/min == elvárás
- [ ] `_TARGET_SQL` tartalmaz `1 FOLLOWING` kezdő boundaryt (nem `CURRENT ROW`)
- [ ] `uv run pytest _tests/store/test_target_window.py -v -s` — zöld

## Notes

A `sync_targets.py` SQL-ben már `ROWS BETWEEN 1 FOLLOWING AND {horizon} FOLLOWING`
van — ez elvileg helyes. A teszt azt igazolja, hogy ez tényleg így működik
és nem kerül visszaírt módosítással felülbírálva.
