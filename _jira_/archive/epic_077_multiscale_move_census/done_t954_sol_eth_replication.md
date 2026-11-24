---
epic: epic_077
id: t954
title: SOL/ETH replikáció és a Scout-események elhelyezése a katalógusban
assignee: analyst_agent
status: done
blocks: []
blocked_by: [t953]
---

## Goal
Ellenőrizni, hogy a BTC-eredmények (darabszámok, arányok, skála-invariancia, bázisráták) SOL-on és ETH-n is megjelennek-e, és hogy a Scout `research."scout_events_v1"` eseményei hova esnek a katalógus nagy mozgásaihoz képest.

## Scope
- t950–t953 pipeline ugyanazzal a kóddal SOL és ETH train-ablakán.
- Assetek közti összehasonlítás és poolozhatóság-diagnosztika (arányok hasonlósága).
- Scout-események: melyik skála/ciklusszám/állapot felel meg nekik; a katalógus hányadát fedik le.
- Riport: `_doc_/reports/move_census_cross_asset/`.

## Acceptance Criteria
- [ ] Ugyanaz a kód és rács, asset-paraméterrel.
- [ ] Asset × skála × k darabszám-tábla; arányok asset-közi hasonlósága számszerűen.
- [ ] Scout-esemény leképezés: a Scout fix skálája a katalógusban hol helyezkedik el.
- [ ] Összegző javaslat a folytatásra (short, setup-detektor, kimenet-modell) — a user dönt.
- [ ] Lezáró összefoglaló: `_doc_/research/move_census.md`.
- [ ] ruff + pyright + pytest tiszta.

## Notes

[analyst] 2026-11-24 — kész.
- Kód: `move_census_cross_asset.py` (run / scout stage). Táblák SOL és ETH lab-adatbázisban (`move_census_v1_*`), Scout-illesztés: `move_census_v1_scout_map` (solusdt_lab).
- ETH = BTC (1 / 27 darabszám-cella p<0,05 drift nélkül). **SOL eltér:** 10 / 27 cella p<0,05 drift nélkül is, arány 1,05–1,21 T ≥ 1 óránál; jelölt-bázisráta és találati arány martingál. Ok nem azonosított (hipotézisek a cikkben/md-ben).
- Volumen-kiszáradás hipotézis (BTC-n talált) SOL/ETH-n nem replikálódik.
- Scout: 2143 train esemény, 1446 (67,5%) egyezik egy (T=240, k=3) jelölttel; impulzus-arány 13% (8 óra) vs ~50% időlimit nélkül.
- Lezáró md: `_doc_/research/move_census.md` + README index. Cikk (HTML artifact) a usernek átadva.
- ruff tiszta, pyright 0 hiba, `pytest src/reports` 183 passed (új modulokhoz nincs pytest — validátorra marad).
