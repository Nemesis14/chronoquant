---
epic: epic_077
id: t951
title: Placebo-null a katalógus rácsára
assignee: analyst_agent
status: done
blocks: [t953]
blocked_by: [t950]
---

## Goal
Megmérni, mennyi a t950 katalógusból véletlenül is kijön: ugyanaz a detektor és rács placebo hozamsoron.

## Scope
- Placebo: az 1m hozamsor blokk-bootstrapje (vol-klaszterezést megőrző blokkok) és napon belüli keverés; N ≥ 200 realizáció (költség szerint, dokumentált).
- Összehasonlítás: darabszámok, efficiency ratio-k, időtartam-eloszlások skála × k szerint, a valós vs placebo eloszlás.
- Riport: `_doc_/reports/move_census_placebo/`.

## Acceptance Criteria
- [ ] Ugyanaz a detektor és paraméter-rács, mint t950 (kód-újrahasználat, nem másolat).
- [ ] A placebo megőrzi a vol-klaszterezést (diagnosztika: autokorreláció |r|-ben).
- [ ] Skálánként: valós / placebo darabszám-arány és CI.
- [ ] Világos mondat a riportban: hol van bizonyíthatóan több struktúra a valós adatban, hol nincs.
- [ ] ruff + pyright + pytest tiszta.

## Notes

[analyst] 2026-11-24 — kész.
- Kód: `move_census_placebo.py`, `move_census_placebo_report.py`; tábla `research."move_census_v1_placebo"`; riport `_doc_/reports/move_census_placebo/`. N = 200 + 200 realizáció (sign, block).
- Eredmény: a véletlen előjelű placebóhoz (azonos 1m vol-burkoló) képest a darabszám, hossz, hatékonyság, nagy-arány statisztikák megkülönböztethetetlenek (darabszám: 1 / 27 cella p<0,05). Egyetlen szisztematikus eltérés: variancia-arány 0,955 (T=15) → 0,856 (3 nap), p = 0,005 minden skálán (visszaforduló ár).
- A blokk-bootstrap eltérései (hossz ×1,25–1,45, ER −8…−15%) a vol-rezsim szétzúzásából jönnek (a trailing 30 napos σ félremér) — a sign placebo az érvényes null; a block a hét-szintű szerkezet ellenőrzése.
- Zárt alakú viszonyítás: P(nagy | jelölt) = e⁻¹ = 0,368 folytonos bolyongásban; a mért 0,39–0,43 a diszkrét overshoot miatt a placebóval egyezik.
