---
epic: epic_003_target_quality
id: t3
title: Teszt — NULL sorok száma == horizon - 1 a target tábla végén
assignee: database_agent
status: pr
blocks: []
blocked_by: [t1]
---

## Goal

A t1 fix után igazolni, hogy a target tábla NULL sorai:
1. Pontosan `horizon - 1` darab (fw60 → 59 db)
2. Kizárólag a dataset legvégén találhatók (nem szóródnak el)
3. Mindkét target oszlopban egyszerre NULL-ok (szimmetria)

Ezt az állapotot a `test_features_target_overview.py`-ba és/vagy
külön fájlba kell tesztként beépíteni, hogy jövőbeli sync után
automatikusan ellenőrizhető legyen.

## Scope

- `_tests/store/test_features_target_overview.py` — meglévő tesztbe új assert-ek
- esetleg `_tests/data_pipeline/test_target_window.py` (t2 taskkal közös fájl)

### Tesztek tartalma

1. `null_count == horizon - 1` — a `target` táblában összesen
2. Null sorok mind a `MAX(open_time) - (horizon-1)` tartományban vannak
3. Mindkét target oszlop (`trg_l_fw60_q90`, `trg_s_fw60_q10`) ugyanazon sorokban NULL
4. Szintetikus unit teszt: 10 soros DB-be 60-as horizont → 9 NULL sor

## Acceptance Criteria

- [ ] `null_count == 59` assert a produkciós DB-re (config-ból olvassa a horizontot)
- [ ] NULL sorok pozíciója: mindegyik `>= MAX(open_time) - 59 perc`
- [ ] Szimmetria: `trg_l IS NULL` == `trg_s IS NULL` minden sorban
- [ ] Szintetikus teszt: `n=10`, `horizon=3` → 2 NULL sor, az utolsó 2-ben
- [ ] `uv run pytest _tests/store/ -v` — zöld (a t1 fix után)

## Notes

Ez a task t1 (SQL fix) után futtatandó. Jelenleg a produkciós DB-ben
csak 1 NULL sor van (vizsgálat: 2026-06-14), a fix után 59 kell legyen.
A horizontot `utils.load_features_config()` adja — ne hardcode-olj.
