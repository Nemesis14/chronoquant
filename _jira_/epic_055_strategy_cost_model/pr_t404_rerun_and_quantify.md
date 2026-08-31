---
epic: epic_055
id: t404
title: Meglévő strategy sessionök újrafuttatása és a különbség kimutatása
assignee: analyst_agent
status: pr
blocks: []
blocked_by: [t403]
---

## Goal

Megmutatni, mennyit torzított a költség hiánya — és hogy melyik korábbi
következtetés marad érvényben.

## Amit újra kell futtatni

- `strat_ctrl_2602_2607` és `strat_alt_2602_2607` (epic_054)
- Az élő champion sessionjei (`config/trading.json` szerint)

## A riport kötelező tartalma

1. **Setup-váltás**: beállításonként a régi (bruttó) és az új (nettó) optimum.
   Hány esetben más setupot választ a kereső?
2. **Az élő stratégia**: a jelenleg élesben futó beállítás nettó eredménye.
   Ha veszteséges díjakkal, azt ki kell mondani.
3. **Az epic_054 verdikt felülvizsgálata**: az alt-data +34%-os éle nettó
   objectívvel is megmarad-e?
4. **Break-even díjszint** stratégiánként: milyen díj mellett megy nullába.

## ⚠️ Amire figyelni

- A `same_window` optimalizálás miatt az abszolút számok felfelé torzítottak.
  Ez a költség-modelltől függetlenül igaz — ne keverd össze a két hatást.
- Ha az élő stratégia nettó veszteséges, az **azonnali jelzés**, nem riport-lábjegyzet.

## Acceptance Criteria

- [ ] `_doc_/research/strategy_cost_impact.md` a fenti négy szekcióval
- [ ] Minden állítás mögött szám
- [ ] Explicit válasz: érvényben marad-e az epic_054 GO verdiktje

## Notes

[orchestrator] Elvegezve — 2026-08-25

Riport: `_doc_/research/strategy_cost_impact.md`

**Az eredmeny kellemetlen es fontos: a kontroll strategia dijak utan
veszteseges (-49 USD / 1000 notional), a kezelt epphogy nyereseges (+22 USD).**

- Setup-valtas: a kontrollnal 200/200, a kezelesnel 197/200 setup helyezese
  megvaltozott. A regi (brutto) rangsor semmit nem mondott a valos sorrendrol.
- A kezeles regi "gyoztes" beallitasa (1984 kotes) netto **-1,78 log return**.
- Break-even: kontroll 8,72 bps vs tenyleges 9,94 -> **-1,22 bps hianyzik**.
  Kezeles 11,66 vs 11,13 -> **+0,53 bps tartalek**.
- A korabbi 8 bps atalany OPTIMISTA volt: a tenyleges 9,9-11,1 bps, mert az
  idotullepeses kilepesek taker dijat fizetnek (a kotesek 32-48%-a).
- Funding: -4,87 / -5,16 USD per 1000 notional (BEVETEL). A t402 ticketbe irt
  horgony (-10,98) egy elszamolassal el volt csuszva; ez a javitott ertek.

**Az epic_054 verdikt modosul:** az alt-data nem "javit 34%-ot egy nyereseges
strategian", hanem **atbillenti veszteségesbol epphogy nyeresegesbe**.
Erosebb allitas, sokkal kisebb mozgasterrel.

⚠️ A 200 setupbol 198-200 netto veszteseges, es a nyertes ugyanazon az ablakon
lett kivalasztva, amin merunk. A +22 USD felso korlat, nem varhato hozam.
