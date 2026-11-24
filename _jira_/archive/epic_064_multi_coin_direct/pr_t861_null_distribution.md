---
epic: epic_064
id: t861
title: Null-eloszlás egyetlen rotáció helyett — a t858 következtetésének javítása
assignee: modeling_agent
status: pr
blocks: [t856, t860]
blocked_by: []
---

## Goal

A `t858` és a `t860` null-tesztje is **egyetlen** címke-rotációra épült. A
`t860` (15 perces rács) futásában ez robbant ki: a rotált címke majdnem
ugyanazt a számot adta, mint az éles (IC +0,0275 vs +0,0312). Ebből először azt
a következtetést vontam le, hogy a 15 perces ág artefakt — **ez is téves volt,
ugyanabból a hibából**: a 4380 órás eltolás a 12 húzás közül a második
legmagasabb. A tanulság nem az egyik vagy másik ágról szól, hanem arról, hogy
**a mérőeszköz volt hibás**.

## Két hiba

**Egy rotáció egy húzás a null-eloszlásból.** Nem mondja meg, hogy az éles
eredmény szokatlan-e; csak annyit, hogy egy adott eltolás alatta maradt. A
`t858`-ban használt 4380 órás eltolás a 24-es listában az egyik legalacsonyabb
húzás — nem az eredmény volt tiszta, hanem az eltolás választása szerencsés.

**A rotált címkén újrafutott a regularizáció-keresés.** A null-modell így a
saját konfigurációját választhatta, vagyis hozzáigazíthatta magát ahhoz a
zajhoz, amin meg kellett volna buknia. (A 15 perces null-futás tényleg más
konfigurációt választott: `min_child 2000, λ=100`.) A javított teszt fixen
tartja az éles adaton választott konfigurációt.

## Eredmény — mindkét rács, fix konfiguráció

`z = (éles − null átlag) / null szórás`. Ez a p-vel ellentétben összevethető
különböző rotációszámú futások között.

| rács | | éles | null átl. | null szórás | ≥ éles | p | **z** |
|---|---|--:|--:|--:|--:|--:|--:|
| 1h (24 rot.) | IC | +0,0240 | +0,0051 | 0,0145 | 2/24 | 0,120 | **1,31** |
| | AUC | 0,5157 | 0,5035 | 0,0091 | 2/24 | 0,120 | 1,34 |
| | lift | +2,22 | +0,03 | 1,86 | 4/24 | 0,200 | 1,17 |
| **15m (24 rot.)** | IC | +0,0312 | +0,0046 | 0,0135 | 1/24 | 0,080 | **1,98** |
| | AUC | 0,5209 | 0,5027 | 0,0084 | **0/24** | **0,040** | **2,17** |
| | lift | +3,11 | +0,64 | 1,69 | 2/24 | 0,120 | 1,46 |

**A 15 perces rács a kettő közül az erősebb.** Egyetlen rotáció sem érte el az
éles IC-t vagy AUC-t; az órásnál 24-ből 2 igen.

Amiben az eredeti aggály viszont megalapozott: a 15 perces rács tényleg többet
gyárt a semmiből — a lift null-átlaga +1,24pp az órás +0,03pp-jével szemben.
Csak nem annyit, hogy az éles eredményt megmagyarázza.

⚠️ **A p = 0,040 nem felfedezés.** Két rács, három horizont, négy modellcsalád
futott, mindegyik három mérőszámon — hogy ebből egy 0,040 kiessen, az pontosan
a zajtól várt. Bármilyen enyhe többszörös-korrekció megeszi.

**Amit eldönt: a rácsválasztást.** Hogy a 15 perces jobb az órásnál, az
kétváltozós összevetés, nem sok hipotézis közül a legjobb kiválasztása. A
panelre a 15 perces rács viendő. A `t856` protokollja a kapu-jelentés §10.7-ben
**előre rögzítve**.

## Részletek — órás rács, 24 rotáció

| | éles | null átlag | null szórás | null p95 | null max | **p** |
|---|--:|--:|--:|--:|--:|--:|
| IC | +0,0240 | +0,0051 | 0,0145 | +0,0255 | +0,0356 | **0,120** |
| AUC | 0,5157 | 0,5035 | 0,0091 | 0,5175 | 0,5229 | **0,120** |
| p90 lift | +2,22pp | +0,03 | 1,86 | +3,25 | +3,49 | **0,200** |

**A null-eloszlás nem nullára centrált.** A rotáció megőrzi mindkét sorozat
autokorrelációját — ez a lényege —, de erős címke-átfedésnél épp ez a
blokkszerkezet gyárt látszategyüttmozgást. A nullával összevetni rossz teszt
volt.

Az éles +0,0240 a null p95-je (+0,0255) **alatt** van. A `t858`-ban jelentett
havi t = 2,00 a nullához mérve készült; a helyes viszonyítás
(0,0240 − 0,0051)/0,0145 = **1,30**.

## Mit ír felül

- **`t858`: „Statisztikailag átment, gazdaságilag nem" → nem ment át.** Ezen az
  ágon nem mutattunk ki jelet. A gazdasági mérleg (p = 0,338) és a
  null-eloszlás (p = 0,12) egymástól függetlenül ugyanazt mondja.
- **A véletlen-5 baseline nem mond ellent.** Az a kérdés, hogy „jobbak-e a
  szelektált változók öt véletlennél" (igen), nem az, hogy „van-e jel". A
  modell és az öt véletlen változó ugyanabból a forrásból szed: két
  autokorrelált sorozat látszategyüttmozgásából.
- **Minden korábbi „átment a null-teszten" állítás érintett**, mert mindegyik
  egyetlen rotációra épült.

## Kimenet

- [src/modeling/experiments/null_distribution.py](../../src/modeling/experiments/null_distribution.py)
- [_doc_/research/sol_8h_lgbm_gate.md](../../_doc_/research/sol_8h_lgbm_gate.md) §10
  + javítás-jelzés a fejlécben és a §3/§4/§8-nál

## Acceptance Criteria

- [x] Több rotáció, egyenletesen szétterítve a mintán
- [x] Konfiguráció fixen tartva minden futásban
- [x] Empirikus p-érték `(1+k)/(1+n)` alakban
- [x] `t858` és a kapu-jelentés javítva
- [x] 15 perces rács null-eloszlása, 12 rotáció
- [x] 15 perces rács, 24 rotáció
- [x] Előre rögzített panel-protokoll (§10.7)

## Notes

A `t856` panelen a null-eloszlást kell használni, nem egyetlen eltolást. A
rotációk számából adódó alsó korlát: 24 rotációval a legkisebb elérhető
p-érték 0,040, tehát p < 0,05 állításhoz legalább ennyi kell.
