---
epic: epic_073
id: t930
title: Több-asset eseményadatkészlet + order flow mezők
assignee: analyst_agent
status: done
blocks: [t932, t933, t934, t935]
blocked_by: [t929, t937]
---

## Goal
A `wave_events` v2 logikával (first-touch, W4h/H8h) építsd fel a SOL + BTC + ETH eseményeit egy közös
`research."scout_events_v1"` táblába (asset_id oszlop, közös split-határok, CV-blokk). Az 5300 szerint a lazább
A-küszöb változatok is (érzékenység). Order flow / mikrostruktúra mezők a prefixre (taker buy arány, kötésszám,
volumen-profil), ha a DB-ben elérhetők. Riport: eseményszámok asset × osztály × CV-blokk, csak train példák.

## Acceptance Criteria
- [ ] Holdout-2 sorok külön jelölve, nem kerülnek riportba
- [ ] Riport `_doc_/reports/scout_events/`

## Notes

- Kód: `src/reports/builders/scout_events.py` (könyvtár), `scout_events_report.py` (riport), tesztek: `src/reports/tests/test_scout_events.py`. A `wave_events.py` NEM változott (a `build_events` asset-független; a v1/v2 SOL kimenet reprodukálható).
- Táblák (SOL lab DB): `research."scout_events_v1"` (A>=1,5 sigma_W, elsődleges), `_a125`, `_a10` (A-küszöb érzékenység, külön tábla), `scout_events_v1__funnel`, `__meta`, `scout_flow_v1` (20 order-flow jellemző, kulcs `flow_key` = `asset|t0`). Harness-kompatibilis: `wave_events_v2` oszlopai + `asset_id`, `tau_min` (oldalazás: H, cenzorált; a `scout_bench.frame` konvenciója), `first_touch_min` (oldalazás: NULL).
- Holdout-2: minden címke-függő oszlop NULL, ha `label_end > 2026-08-01` (`label_masked`; a `p_star` marad); a riport csak train. A holdout-2 sorok száma címke nélkül: SOL 41, BTC 38, ETH 39 (W4h_H8h, kifutott ablak).
- Train W4h_H8h: SOL 720 / 117 imp, BTC 718 / 92, ETH 737 / 99; pooled 2175 / 308 (14,2%). 5300 minimum: mind PASS (pooled impulzus 308 vs 300: szűk tartalék); blokkonként pooled 339-380 (min. 100).
- Asset-közi egyidejűség (+-4h, t_c): pooled 65,9% (SOL 58,9, BTC 70,1, ETH 68,7); lift ~3,8-4,7 a függetlenséghez képest; P(imp | másik imp) 42-58% vs 6-9% egyébként: 2175 esemény csak 1337 idő-klaszter -> nap-klaszter újramintavétel kötelező.
- Holdout-2 becslés (pessz./opt.): pooled 250 esemény 2027-03-26 / 03-11; 40 impulzus (train arány) 2027-04-26 / 04-07 (kötő feltétel); 60 nap 2026-12-01; assetenként 50 2026-12-26. Most 118 esemény, ~16,7 várható impulzus.
- Order flow (train, leíró): osztályok között kicsi a különbség (pooled egyváltozós AUC 0,44-0,54); legnagyobb: decline-szakasz volumen/kötésszám az impulzusnál alacsonyabb (vol_decline_rel 1,03 vs 1,29 / 1,24; trades_rel_decline 1,05 vs 1,26 / 1,21; qv_logz 0,81 vs 0,97 / 0,96), a rebound taker-buy hányad szinte azonos (0,518 / 0,515 / 0,518). A jellemzők részben az A/sigma_W-vel korreláltak (az impulzus-arány A/sigma_W 1,0-1,25-nél 30%, >=3-nál 5,5%: a skalár alap fő jele), a többlet a t931 benchmarkban dől el.
- Megjegyzés: BTC/ETH 1m adat 2021-01-01-től, ezért az első 7 nap sigma nélkül nem ad eseményt; a SOL a 2020-09-14-es adattal 2021-01-01-től teljes.
- Ellenőrzés: ruff + pyright tiszta, `pytest src/reports` 123 passed (a valós SOL `wave_events_v2` regresszió is lefutott). Státusz: nem nevezve át (`todo_`), validátorra vár.
- [validator] Done - 2026-11-21. ruff + pyright tiszta; pytest src/reports 123 passed. Fuggetlen ellenorzes: 28 veletlen BTC/ETH train cimke (osztaly + tau_min + A + P*) ujraszamolva az 1m OHLCV-bol: 28/28 egyezik; a flow jellemzok es sigma_1m a t_c utan csonkolt adaton bitre azonosak (nincs look-ahead); esemenyek asset-enkent nem atfedoek; masked oszlopok NULL minden label_end > 2026-08-01 sorra (3 valtozat, a hataratlogo sorok is); darabszamok == Notes; CV-hatarok kozosek; SOL W4h_H8h train+OOS == wave_events_v2 (790 sor, 0 elteres); flow_key join hiany nelkul. Riport: manifest ervenyes, nincs hianyzo/felesleges fajl, OOS/holdout csak darabszam, peldak train (2023). Docs 8330/0002 konzisztens.
- [validator] FIGYELEM t932+: az asset-kozi egyidejuseg (P(imp | masik imp) 42-58%) a riportban csak leiro statisztika, a tablakban NINCS ilyen oszlop - ez maradjon is igy. Barmely asset-kozi jellemzo csak a t_c-n mar ismert informacio lehet (masik asset esemenyenek cimkeje / kimenete t_c utan szivarogtatas); a masik asset legkozelebbi esemenyenek cimkezett osztalya TILOS jellemzokent.
