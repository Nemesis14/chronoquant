---
epic: epic_060
id: t812
title: Soronkénti gazdaságtan a walk-forward mérésben
assignee: modeling_agent
status: todo
blocks: [t813]
blocked_by: [t811]
---

## Goal

A mérésben ma fix `±100 bps` a nyereség/veszteség. Skálázott korlátnál
soronként változik — és éppen ez a lényeg.

## A változás

```
regi:  net = +100 - entry - exit        (nyero)
uj:    net = +barrier_bps[i] - entry - exit
```

A fedezeti pont ezért soronként más. Az összesített mérték:
**nettó bps/kötés**, és mellé a `találat − soronkénti fedezeti pont` átlaga.

## Acceptance Criteria

- [ ] A `walk_forward()` soronkénti `barrier_bps`-szel számol
- [ ] Rezsim×irány bontás, mint a t705-ben
- [ ] Összevetés a fix ±100 bps eredménnyel (−9,12 bps/kötés) azonos ablakon
- [ ] **Az osztályeloszlás korszak-stabilitása riportálva**

## Notes
[orchestrator] Elvegezve — 2026-08-26

## Amit a merés csinal

A `walk_forward_tb.py` mostantol **soronkenti** korlattal szamol: a nyerő
kotes annyit er, amennyi a korlat volt a belepeskor, mig a dij fix. Ezert a
fedezeti pont is soronkent mas — az uj `fedezeti_%` es `tobblet_pp` oszlopok
ezt mutatjak.

`--label tb_label_240_vol` -> a `_bps` par automatikusan bejon.

## Eredmeny — azonos vegrehajtas (maker belepes + maker timeout)

| Cimke | n | talalat | fedezeti | bps/kotes |
|---|---|---|---|---|
| Fix ±100 bps — LONG | 1046 | 47,6% | 53,1% | −4,93 |
| Fix ±100 bps — SHORT | 935 | 47,1% | 53,1% | −5,62 |
| **Fix egyutt** | **1981** | **47,4%** | **53,1%** | **−5,26** |
| Vol-skalazott — LONG | 981 | 47,3% | 53,8% | −2,83 |
| Vol-skalazott — SHORT | 893 | **49,7%** | 53,9% | −3,84 |
| **Vol egyutt** | **1874** | **48,4%** | **53,8%** | **−3,31** |

**+1,95 bps/kotes a vol-skalazott javara.** A javulas nagyobb resze a
**short oldalrol** jon: 47,1% -> 49,7% (+2,6pp).

## ⚠️ Ket dolog, amit ki kell mondani

**1. A fedezeti pont ROMLOTT: 53,1% -> 53,8%.** A 2024-2026-os ertekelesi
ablakban a skalazott korlat **szűkebb** (63-100 bps a 100 helyett), mert a
volatilitas leesett. Szűkebb korlat mellett a fix dij aranyaban nagyobb.
A vol-skalazas tehat **nem a fedezeti ponton** nyer, hanem a talalati
aranyon — es tobbet nyer ott, mint amennyit itt veszit.

**2. A keresés kevesebb liftet talalt: 0,1104 -> 0,0750** (belső valid).
Ez **megerositi a fő diagnozist**: a fix cimken mert lift egy resze nem
irany-elorejelzes volt, hanem annak elorejelzese, hogy mikor van nagy
mozgas. A vol-skalazas ezt kinormalja — es ami marad, az kevesebb, de
**valodibb**: a kereskedesi eredmeny jobb lett, miközben a "lift" romlott.

## Acceptance Criteria

- [x] A `walk_forward()` soronkenti `barrier_bps`-szel szamol
- [x] Osszevetes a fix ±100 bps eredmennyel azonos ablakon
- [x] Osztalyeloszlas korszak-stabilitasa riportalva (t811)
- [ ] Rezsim x irany bontas — a t814-ben
