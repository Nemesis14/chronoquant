---
epic: epic_034
id: t44
title: Strategy contract audit az uj modeling scope mellett
assignee: modeling_agent
status: pr
blocks: [t46, t47]
blocked_by: [t41, t43]
---

## Goal
Ellenőrizni, hogy a strategy scored-join, calibration és optimization továbbra is
koherens marad az új, szigorúan sample-scoped modellezési lánccal.

## Scope
- `src/strategy/`
- `model.__pred` és strategy session input szerződés

## Acceptance Criteria
- [ ] A strategy az új modeling outputtal változtatás nélkül vagy tudatosan igazított módon működik
- [ ] Egyértelmű, hogy a strategy továbbra is a `model.__pred` táblákból dolgozik
- [ ] A same-window / calibration korlátok dokumentáltak maradnak

## Notes
Itt nem új strategy koncepció a cél, hanem a modeling refaktor utáni szerződésellenőrzés.

[t44 audit — 2026-06-22]

1. model.__pred input szerződés — HELYES
   - build_table.py: build_scored_table() kizárólag model."<model_id>__pred" táblákat olvas
     (pred_table_fqn() hívással, a modeling.sampling.snapshot_sampler modulból importálva)
   - A join: snap."<snapshot_id>" ⋈ model."long_id__pred" ⋈ model."short_id__pred" on open_time
   - Explict táblaexisztencia ellenőrzés: _table_exists(conn, "model", f"{mid}__pred") raise-el ha hiányzik

2. model.__pred tábla struktúra konzisztencia — HELYES
   - A build_scored_table() a "pred" oszlopot pred_long_raw / pred_short_raw aliassal olvassa
   - A downstream calibrate.py és optimize.py pontosan ezeket a col neveket várja
   - A snap tábla adja a long_mfe_fw60 / short_mfe_fw60 target oszlopokat (JOIN-on keresztül)

3. same_window / calibration korlátok — DOKUMENTÁLVA (kisebb kiegészítés szükséges volt)
   - A strategy_artifact.json-ban az "evaluation_mode": "same_window" mező már megvolt
   - Hiányzott azonban egy magyarázó komment — hozzáadva az artifacts.py-ban (L334)
   - A build_table.py modul docstringjébe hozzáadva a "Contract note" blokk, amely
     expliciten dokumentálja: a __pred tábla a teljes snapshot range-et fedi, nem a
     training sample-t — és a __sample tábla soha nem olvasódik a strategy rétegben

4. model.__sample szivárgás — NEM TALÁLHATÓ
   - grep(__sample, src/strategy/) → no matches
   - sample_table_fqn() nem importált sehol a strategy rétegben
   - A strategy kizárólag snap.* és model.*__pred táblákra támaszkodik

Módosított fájlok:
   - src/strategy/strategy/build_table.py — "Contract note" blokk a modul docstringben
   - src/strategy/strategy/artifacts.py — komment az evaluation_mode sor előtt

Logikai gap: nem volt. Mindkét módosítás csak dokumentáció/komment jellegű pontosítás.
