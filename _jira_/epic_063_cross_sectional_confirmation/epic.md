# Epic 063: A cross-sectional eredmény megerősítése

> **A kiváltó ok:** az `epic_062` **+22 … +29 bps/pár** nettót mért, 22 hónap
> OOS-en, mindkét rezsimben pozitívan. Ez a projekt első pozitív eredménye —
> és pontosan ezért kell a leggyanakvóbban kezelni.
>
> ⚠️ **Tőkére vetítve: Sharpe 0,51, max drawdown −24,7%, t ≈ 0,7.**
> **Az eredmény NEM szignifikáns** — összefér azzal is, hogy a valódi él nulla.
> Ez az epic nem finomítás, hanem annak eldöntése, hogy létezik-e a hatás.
>
> Riport: [cross_sectional_probe.md](../../_doc_/research/cross_sectional_probe.md)

## Goal

Eldönteni, hogy az eredmény valós-e, vagy egy szerencsés univerzum + utólagos
paraméterválasztás terméke. **Semmi nem épül, amíg ez nem dől el.**

## Amit meg kell fogni — a t833 négy fenntartása

1. **A horizontot az eredmények ismeretében választottuk** (6 érték, a
   legjobb a leghosszabb). A 20%-os decilis és a top-20 szűrő szintén.
2. **Átfedő ablakok:** 15 984 óra ≈ 222 független ablak. A havi bontás
   (15–16/22) az őszinte szignifikancia.
3. **A 120 órás ugrás szuperlineáris és megmagyarázatlan** (+35 → +79 bruttó).
   A keresztmetszeti szórás ~√t szerint nőne. Ez leak vagy valódi — el kell dőljön.
4. **Egyszerűsített szimuláció:** záró-záró órai bar, nincs kockázat-
   kiegyenlített súlyozás, nincs margin/likvidációs modell.

## Tasks

- t841: Független univerzum — a 31–60. legforgalmasabb pár, ugyanaz a recept
- t842: A 120 órás anomália felderítése
- t843: Kockázat-kiegyenlített súlyozás és realisztikusabb szimuláció
- t844: Összesített verdikt

Sorrend: (t841 ∥ t842) → t843 → t844

## A döntési szabály — előre rögzítve

A t841 a döntő: **ugyanaz a kód, ugyanaz a horizont és decilis** (72h, 20%),
csak más 30 pár — a 31–60. legforgalmasabb 2024-01-ben. Semmit nem hangolunk.

| Kimenet | Verdikt |
|---|---|
| Nettó > **+10 bps/pár** ÉS mindkét rezsimben pozitív | **GO** — jöhet a t843 és az 1 perces backfill |
| Nettó 0 … +10 bps | gyenge — a hatás valós de kicsi; csak a legforgalmasabb párokon |
| **Nettó ≤ 0** | **Az `epic_062` eredménye univerzum-specifikus volt. Vissza a rajzasztalhoz.** |

⚠️ A t842 **blokkoló**: ha a 120 órás anomália leaknek bizonyul, a 72 órás
szám is gyanús, és az egész epic újramérendő, mielőtt bármi épülne.

## Amit NEM csinálunk

- Nem hangolunk hiperparamétert (a mostani eredmény keresés nélkül született —
  ez konzervatív, és így is kell maradnia a megerősítésig)
- Nem megyünk 72 óra fölé, amíg a t842 nem zárul
- Nem nyúlunk éles tőkéhez
