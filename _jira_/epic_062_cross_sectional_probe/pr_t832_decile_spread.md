---
epic: epic_062
id: t832
title: Keresztmetszeti decilis-spread mérés
assignee: analyst_agent
status: todo
blocks: [t833]
blocked_by: [t831]
---

## Goal

Leíró statisztika, modell nélkül: **mennyi van egyáltalán elosztani való**.

## A mérés

Minden órában, minden eszközre a következő 4 órás log-hozam. Ezekből:

```
spread(t) = atlag(felso decilis hozam) - atlag(also decilis hozam)
```

## Amit külön ki kell mutatni

- A spread **eloszlása**, nem csak az átlaga (a medián a fontos)
- **Rezsim szerint bontva**: emelkedő (+21%) és eső (−57%) ablak külön
- Mennyit visz el a spreadből, ha az eszközök egyszerűen **együtt mozognak**
  (az átlagos páronkénti korreláció) — ez a valódi korlát
- ⚠️ **Likviditási szűrő**: a legnagyobb spreadet tipikusan a legvékonyabb
  párok adják, ahol a csúszás is a legnagyobb. Forgalom szerinti alsó
  harmad kizárva külön is lemérve.

## Acceptance Criteria

- [ ] Spread-eloszlás táblázat + rezsim-bontás
- [ ] Átlagos páronkénti korreláció riportálva
- [ ] Likviditási szűrővel és anélkül is
- [ ] A 26 bps kétszeres kör-költség ráhúzva a számokra

## Notes
[orchestrator] Elvegezve — 2026-08-26

## Terv-elteres, tudatos

A ticket 1 perces adatot irt. Helyette **orai** bar: a meres 4 oras elore
hozamot nez zaro-zaro alapon, amit az orai bar pontosan ad, es a
letoltes ~1000-szer kisebb. 1m csak akkor kell, ha a modell-ag indul.

Univerzum: 30 perp, a **2024-01-i** forgalom szerint valasztva a teljes
S3 archivum-listabol (829 USDT-perp, ebbol 261 letezett akkor). A
MATICUSDT benne van es 2024-09-11-en abbamarad — a survivorship bias
igy tenylegesen ki van zarva, nem csak allitva.

## 1. A lehetoseg — atmegy, negyszeresen

| | median spread | par-koltseg | fedezet |
|---|---|---|---|
| Teljes ablak | **335,9 bps** | 18,8 | 17,9× |
| Likviditasi szuroval | 328,7 bps | 18,8 | 17,5× |
| Emelkedo rezsim | 345,1 bps | 18,8 | 18,4× |
| **Eso rezsim** | **279,5 bps** | 18,8 | **14,9×** |

Elore rogzitett kuszob: 80 bps. **Teljesul, 4,2-szeresen**, es a rossz
rezsimben is tartja. Atlagos paronkenti korrelacio: **0,650**.

## 2. ⚠️ De mennyi ebbol ELERHETO — ezt utolag tettem hozza

A fenti a **tokeletes elorelatas** felso korlatja: a decilisek a kimenet
szerint vannak rendezve. Ezert lefutott ugyanaz a konyv nulla-
intelligenciaju rangsorolokkal (trailing hozam szerinti sorrend):

| jel | atlag bps | median bps | netto (−18,8) | pozitiv ora |
|---|---|---|---|---|
| **reverzal 72h** | **+4,1** | +9,2 | **−14,7** | 53,1% |
| reverzal 1h | +3,4 | +8,4 | −15,4 | 53,0% |
| reverzal 4h | +2,1 | +8,3 | −16,7 | 53,0% |
| momentum 72h | −4,1 | −9,2 | −22,9 | 46,9% |

**A legjobb naiv jel a 336 bps-bol 4,1-et fog meg — 1,2%-ot.**

Ket dolog latszik:
- **A reverzal az elojel.** Rovid horizonton a kripto keresztmetszet
  atlaghoz ter vissza. Minden lookbacken, konzisztensen.
- **atlag < median mindenhol** (+4,1 vs +9,2): balra ferde eloszlas.
  Gyakran nyer keveset, ritkan veszit sokat — a klasszikus mean-reversion
  profil. Ez onmagaban kockazati figyelmeztetes.

## 3. Ertelmezes — amit ez eldont es amit nem

**Eldonti:** van mit elosztani, a koltseg nem oli meg elore a gondolatot.
**NEM donti el:** elerheto-e. A naiv jelek a leheto legprimitivebb
rangsorolok (nyers hozam), es a modell-agnak 235 feature-je van. Az
iranyproblemanal is 0-rol indult a naiv alap es a modell +9,5pp-t hozott.

**A donto meres tehat: egy valodi rangsorolo modell.** Ezt a t833 alatt
futtatom le, minimalis feature-keszlettel az orai panelbol.
