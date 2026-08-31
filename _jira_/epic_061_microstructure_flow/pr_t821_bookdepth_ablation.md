---
epic: epic_061
id: t821
title: bookDepth marginális hozzájárulás — ablációs mérés
assignee: modeling_agent
status: todo
blocks: [t824]
blocked_by: []
---

## Goal

Megmondani, mennyit ér az az 5 mélység-feature, ami már benne van a
rendszerben. Ha semmit, akkor az `aggTrades`-től sem érdemes csodát várni.

## Módszer

Ugyanaz a walk-forward protokoll, egyetlen különbséggel: a `--drop-features`
kapcsolóval a `feat_depth_*` oszlopok kizárva. A paraméterek **cache-ből**,
ugyanazok, mint a teljes készletnél.

## Acceptance Criteria

- [ ] `--drop-features` kapcsoló a `walk_forward_tb.py`-ban (prefix-lista)
- [ ] A: teljes készlet · B: `feat_depth_*` nélkül — azonos paraméterekkel
- [ ] Δlift és Δbps/kötés rezsim×irány bontásban
- [ ] Az 5 feature gain-részesedése a havi illesztésekben

## Notes
[orchestrator] Elvegezve — 2026-08-26

## ⚠️ Elso megallapitas: a riport tevedett

A `triple_barrier_gate.md` §7 szerint a bookDepth "megvan es soha nem
hasznaltuk". **Ez nem igaz** — az 5 melyseg-feature bekerult a
`quant_train`-be az epic_058 alatt, tehat a +9,3pp-es eredmeny **mar
tartalmazta oket**. A kerdes nem az, hogy segitenenek-e, hanem hogy mennyit
adtak hozza.

## Modszer

`--drop-features feat_depth_` — a sorhalmaz valtozatlan, csak a modell
bemenete szukul 235 -> 230 feature-re. Parameterek cache-bol, azonosak.
Vegrehajtas: maker belepes + maker timeout (az epic_059 alapertelmezese).

## Eredmeny

| | n | talalat | bps/kotes |
|---|---|---|---|
| **bookDepth-tel — LONG** | 1046 | 47,6% | −4,93 |
| **bookDepth-tel — SHORT** | 935 | 47,1% | −5,62 |
| **egyutt** | **1981** | **47,4%** | **−5,26** |
| nelkule — LONG | 1025 | 45,2% | −10,08 |
| nelkule — SHORT | 934 | 49,1% | −1,68 |
| **egyutt** | **1959** | **47,1%** | **−6,07** |

Netto kulonbseg: **+0,81 bps/kotes** a bookDepth javara.

## ⚠️ De ez atrendezodes, nem konzisztens jel

| | Δ talalat |
|---|---|
| LONG | **+2,4pp** (45,2 -> 47,6) |
| SHORT | **−2,0pp** (49,1 -> 47,1) |

A ket irany ellentetes elojellel mozdul. Az osszesitett +0,81 bps/kotes
ket, nagysagrendben azonos, ellentetes iranyu hatas maradeka — nem az a kep,
amit egy valodi informacio-tobblettol varnank.

**Ertelmezes:** az 5 feature valtoztat a modellen, de nem lehet kimondani,
hogy hozzatesz. Egyetlen meres zajan belul van.

**Kovetkezmeny az epic_061-re:** az `aggTrades` OFI-tol sem varhato csoda a
melysegbol. A t822/t823 tovabbra is indokolt (az OFI mas informacio: KI
kezdemenyezett, nem az, hogy mennyi all a konyvben), de a varakozast
lejjebb kell venni.

## Acceptance Criteria

- [x] `--drop-features` kapcsolo a `walk_forward_tb.py`-ban
- [x] A: teljes keszlet · B: `feat_depth_` nelkul, azonos parameterekkel
- [x] Δ irany szerinti bontasban
- [ ] Gain-reszesedes a havi illeszteseknel — nem futott, a fenti alapjan nem donto
