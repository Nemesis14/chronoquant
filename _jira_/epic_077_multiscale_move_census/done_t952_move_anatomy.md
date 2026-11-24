---
epic: epic_077
id: t952
title: Anatómia — leg-felbontás, ciklusszám, arányok, skála-invariancia
assignee: analyst_agent
status: done
blocks: [t953]
blocked_by: [t950]
---

## Goal
Minden nagy mozgást kisebb skálájú zigzaggal legekre bontani (lökés / korrekció / következő lökés), és leírni az első lökés és a korai szakasz szerkezetét; tesztelni a fraktál-feltevést.

## Scope
- Leg-felbontás (σ-normált zigzag a szülő-mozgás skálájának törtrészén).
- Ciklusszám (0 / 1 / ≥2) eloszlása; az arányok ciklusszám szerint.
- Mérők: első lökés időtartama és mérete (1m-ben, abszolút és szülőhöz viszonyítva), korrekció mélysége (retracement), időarányok, lökés-sebesség változása ciklusonként, volumen-arány (lökés vs korrekció).
- Skála-invariancia: arányok eloszlása skálánként (KS / kvantilis-összevetés), darabszám ~ skála hatványfüggvény illesztése.
- Riport: `_doc_/reports/move_census_anatomy/`.

## Acceptance Criteria
- [ ] Az első lökés időtartama 1m-ben: kvantilisek (min/p10/medián/p90/max) skálánként és szülő-hosszhoz viszonyítva — ez a „keret".
- [ ] Ciklusszám-eloszlás és ciklusonkénti arány-táblák.
- [ ] Skála-invariancia verdikt (igen / nem / részben) számokkal, nem vizuálisan.
- [ ] Nincs Elliott-hullámszámozás; a nevezéktan: lökés / korrekció / ciklusszám.
- [ ] ruff + pyright + pytest tiszta.

## Notes

[analyst] 2026-11-24 — kész.
- Kód: `move_census_anatomy.py`, `move_census_anatomy_report.py`; táblák `move_census_v1_legs/_anat_stats/_anat_placebo`; riport `_doc_/reports/move_census_anatomy/`.
- Bontási küszöb c: 0,10 (elsődleges), 0,05 / 0,20 (érzékenység) előre rögzítve; **c = 0,33 utólag lett hozzáadva** (a 0,10-es bontás medián mozgása ~4 impulzus, finom szerkezet) — jelölve a riportban.
- Ciklusszám c=0,33-nál: 86,5% tiszta lökés, 11,4% 1 ciklus, 1,9% 2 ciklus; a placebo 87,5 / 10,4 / 1,8% (gyakorlatilag azonos). Skálázási meredekség −0,91 (placebo −0,92…−0,89).
- Valós eltérés (kicsi): későbbi lökések lassabbak (2.: 0,88×, 3.+: 0,77× az első sebessége; placebo 0,96 / 0,94); block placebo ezt reprodukálja → hét-szintű szerkezet.
- Az első lökés időkerete: a tábla a riportban (percben, skálánként); nincs szűk keret: T és k² szerint skálázódik (medián hossz ≈ 3,5·T / 5,7·T / 12·T k=3/4/6-nál).
- Placebo-futás: 30 + 30 realizáció (min. p = 0,032).
