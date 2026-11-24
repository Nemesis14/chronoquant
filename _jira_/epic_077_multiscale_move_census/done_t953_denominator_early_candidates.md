---
epic: epic_077
id: t953
title: Nevező — korai-impulzus jelöltek és a nagy mozgássá válás aránya
assignee: analyst_agent
status: done
blocks: [t954]
blocked_by: [t951, t952]
---

## Goal
A t950–t952 hindsight katalógusából nem következik valószínűség. Itt a nevezőt építjük: az összes **valós időben azonosítható** korai-impulzus jelölt (kauzális állapotgép skálánként), és hogy ezek hányadából lett utólag nagy mozgás.

## Scope
- Kauzális jelölt-definíció skálánként (megerősített lökés + korrekció, csak a megerősítésig ismert adat), a ciklusszám szerint (0 / 1 / ≥2).
- Skála-relatív címke: first-touch a referencia-lökés többszöröse (cél) vs a korrekció/lökés kezdő szintje (stop), az idő-limit a referencia-lökés idejének többszöröse (nincs fix óra).
- Bázisráta: jelöltek száma, nagy mozgássá válás aránya, skála × ciklusszám szerint, a placebóhoz (t951) viszonyítva.
- Nap-klaszter bootstrap CI; purge-szemlélet a jelöltek átfedésére.
- Riport: `_doc_/reports/move_census_candidates/`.

## Acceptance Criteria
- [ ] Minden jelölt jellemzője és címkéje csak a megerősítésig / utána ismert adatból (egységteszt a look-ahead ellen).
- [ ] Bázisráta-tábla skála × ciklusszám, CI-vel és placebo-összevetéssel.
- [ ] A jelöltek effektív mintaszáma (független klaszterek) közölve.
- [ ] Világos következtetés: van-e ciklusszám- vagy skála-függő bázisráta-különbség, amelyre érdemes modellt építeni.
- [ ] OOS és holdout-2 érintetlen.
- [ ] ruff + pyright + pytest tiszta.

## Notes

[analyst] 2026-11-24 — kész.
- Kód: `move_census_candidates.py`, `move_census_candidates_report.py`; táblák `move_census_v1_cand/_cand_sum/_cand_placebo`; riport `_doc_/reports/move_census_candidates/`.
- 94 795 jelölt (k = 3,4,6 összesen), nap-klaszteres CI; placebo 40 + 40.
- P(nagy | jelölt) = 0,39–0,43, a sign placebóval megegyezik (e⁻¹ = 0,368 zárt alak + diszkrét overshoot). Ciklusszám (higher-low lánc), korrekciómélység, volumenarány szerint a sávok laposak (0,39–0,42).
- R-szorzós találati arány (stop = mélypont): +1R 0,50, +2R 0,35, +3R 0,26 ≈ martingál (0,5 / 0,33 / 0,25). Bruttó E[R] ≈ 0…+0,09, a driftmentesített valós útvonalon ≈ 0; költség (13 bps) után T ≤ 2 óra: −0,06…−0,24 R, T ≥ 4 óra ≈ 0.
- Placebo-összevetés: 2,6% (pooled) / 3,0% (ciklus-bucket) p<0,05 — a véletlen szintje.
- Feltáró jelzés: volumen-kiszáradás (<0,5) sávban E[R2] +0,11 [0,01; 0,22] (k=4); 9 sáv mellett nem szignifikáns.
