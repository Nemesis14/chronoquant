# Előrejelezhető-e a nagy lökés az első szakaszból? (epic_078)

**Időszak:** 2026-11-24 · **Asset:** BTCUSDT, long · **Státusz:** lezárt, nemleges (hat változócsoport, 48 változó: a placebón is ugyanannyi) · **Riport:** `_doc_/reports/cand_pred`

## Kérdés

Az epic_077 szerint az 1m ár alakjából a folytatás nem becsülhető a véletlennél jobban. Itt magyarázó változókat adunk a valós időben felismert jelöltekhez
(megerősített mélypont, k = 4, T = 60 / 240 / 960 perc): felismerhető-e közülük az, amelyikből nagy emelkedés lesz (cél 1: a lökés ≥ 4σ_T; cél 2: +1R a mélypont-stop előtt)?

## Időtáv (medián)

| T | σ_T | megerősítés a mélypont után | nagy mozgás hossza [p25; p75] | nettó |
|---|---|---|---|---|
| 60 perc | 0,55% | 57 perc | 305 perc [125; 643] | 3,2% |
| 240 perc | 1,09% | 209 perc | 1277 perc [539; 2472] | 6,6% |
| 960 perc | 2,13% | 886 perc | 5618 perc [2624; 9146] | 13,5% |

A jelöltet kb. egy skálányi idő után ismerjük; a teljes nagy mozgás ennek kb. ötszöröse. Jelöltek: 4 225 / 1 219 / 344.

## Módszer

- 48 változó, 6 csoport (zigzag-geometria, volumen, VWAP, kereskedési adat [taker flow, ügyletszám, funding], quant / klasszikus, Elliott-szerű alakok), mind a megerősítő bar zárásáig ismert adatból (look-ahead teszt: levágott adaton azonos).
- Idő szerinti teszt: expanding-window éves fold-ok (teszt 2023 / 2024 / 2025, purge); logisztikus regresszió (C = 0,1, rögzített), GBM összehasonlításul; forward csoport-kiválasztás belső idő-CV-n. Teszt-minta 3 530 jelölt. OOS (2026-02-től) érintetlen.
- Placebo: a teljes lánc 80 szintetikus áron (|r| marad, az előjel véletlen; volumen / flow / funding valós).

## Eredmény

- Nagy mozgás: valós AUC(mind) 0,523 [0,504; 0,544], placebo 0,527 [0,512; 0,547] (p = 0,69). A GBM 0,522 vs 0,524. A hatás a zigzag-definíció mechanikája: a legerősebb egyváltozós jel (`z_risk`, 0,543) a placebo-sávban van (0,525–0,562).
- +1R a stop előtt: minden modell ≈ 0,50 (mind 0,499 vs placebo 0,499; GBM 0,510 vs 0,500).
- Egyetlen enyhén pozitív jel: lehorgonyzott VWAP-távolság (`w_anch_low`, `w_anch_hp`: AUC 0,526 / 0,522 vs placebo ~0,497, p = 0,025; csoport-szinten p = 0,049), de 48 változó + 16 modell mellett ez a véletlen szintje, és a +1R célon nincs meg.

## Korlátok

Csak BTC, long, 1m információ. A CI félszélessége ~±0,02 AUC, ennél kisebb hatás nem mutatható ki. Nincs OI (nincs az adatbázisban), orderbook, likvidáció, hír, cross-asset vezetés. A tervtől eltérés: az eredetileg tervezett egyetlen validációs év (2025) helyett három éves fold (a T = 960 cella kis mintája miatt); az adatbázis 1m quant-oszlopai helyett záróár-alapú analógok (hogy a placebón azonosan újraszámolhatók legyenek).

## Következő lépés (javaslat)

A „jelölt + 1m ár-információ" ág lezárása; ha folytatás kell, új információ (likvidáció, OI, orderbook, BTC → alt vezetés); a VWAP-hipotézis egyszeri, előre rögzített megismétlése ETH-n és SOL-on.

Kód: `src/reports/builders/cand_pred_*.py`; táblák: `research."cand_pred_v1_*"` (btcusdt lab DB).
