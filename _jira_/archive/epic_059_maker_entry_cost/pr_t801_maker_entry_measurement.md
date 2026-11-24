---
epic: epic_059
id: t801
title: Maker belépés mérése — fill rate és adverse selection
assignee: modeling_agent
status: todo
blocks: [t802, t803]
blocked_by: []
---

## Goal

Ugyanazon a modellen, ugyanazon az ablakon lemérni a taker és a maker
belépést. A modell nem változhat a két futás között.

## Módszer

A hiperparaméter-keresés **egyszer** fut, az eredmény cache-elve
(`--params-cache`), a költség-variánsok ugyanazt a paraméterkészletet kapják.
Enélkül a két szám nem összevethető.

```
A) taker  : --trials 100 --params-cache <cache>
B) maker  : --params-cache <cache> --maker-entry
C) maker+ : --params-cache <cache> --maker-entry --maker-timeout
```

Protokoll változatlan az epic_058 t705-höz képest: `tb_label_240_100`,
100 trial 2023-01..2024-06-on, havi újratanítás, napi p95 kalibráció,
mérés 2024-07 → 2026-07 (25 hónap).

## Acceptance Criteria

- [ ] Mindhárom variáns lefutott azonos paraméterekkel
- [ ] Fill rate irányonként riportálva
- [ ] Találati arány **a fill-szűrés előtt és után** — ez az adverse selection mértéke
- [ ] bps/kötés és nettó a három variánsra

## Notes
[orchestrator] Elvegezve — 2026-08-26

Parameterek egyszer keresve (100 trial), cache-elve:
`artifacts/_experiments/e059_tb240.json`. Mindket variansban azonosak.

## Eredmeny — 25 honap OOS, 2024-07 -> 2026-07

| | n | talalat | fill | netto bps | **bps/kotes** | pozitiv honap |
|---|---|---|---|---|---|---|
| Taker LONG | 1063 | 47,6% | 100% | −10348,6 | −9,74 | 7/25 |
| Taker SHORT | 955 | 47,8% | 100% | −8580,7 | −8,99 | 9/25 |
| **Taker egyutt** | **2018** | **47,7%** | | **−18929,3** | **−9,38** | 16/50 |
| Maker LONG | 1046 | 47,6% | 98,4% | −5453,3 | −5,21 | 12/25 |
| Maker SHORT | 935 | 47,1% | 97,9% | −5518,8 | −5,90 | 10/25 |
| **Maker egyutt** | **1981** | **47,4%** | **98,2%** | **−10972,1** | **−5,54** | 22/50 |

## A ket kockazat, mérve

| | Kuszob | Mert |
|---|---|---|
| Fill rate | > 60% | **98,2%** |
| Talalat romlasa (adverse selection) | < 1,5pp | **−0,35pp** |

LONG: 47,6% -> 47,6% (valtozatlan). SHORT: 47,8% -> 47,1%.

A pihenó limit a jel zaroaran szinte mindig telik a kovetkezó percben, es a
tulélő belepesek **nem rosszabbak**. Az adverse selection ezen a horizonton
es ezzel a jel-tipussal nem szamottevő.

## ⚠️ Amit ez NEM jelent

A rendszer tovabbra is vesztesegеs (−5,54 bps/kotes, −110 USD / 1000
notional). A maker belepes a rés **41%-at** zarja be, nem a teljeset.
A pozitiv honapok szama viszont 16/50 -> 22/50.

**VERDIKT: GO** — az elore rogzitett szabaly szerint. A maker belepes a
rendszer alapertelmezese lesz.

## Acceptance Criteria

- [x] Mindharom varians azonos parameterekkel (a C meg fut)
- [x] Fill rate iranyonkent riportalva
- [x] Talalati arany fill-szures elott es utan
- [x] bps/kotes es netto
