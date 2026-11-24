## Cél

Az epic_077 szerint az ár geometriája önmagában nem ad a véletlennél jobb folytatás-becslést. Itt **magyarázó változókat** adunk a valós időben felismerhető
jelöltekhez (megerősített mélypont, k = 4, T = 60 / 240 / 960 perc), és megmérjük, hogy egy csoport **idő szerinti** (out-of-time) előrejelzése jobb-e a véletlennél.

* **Célok:** (1) a jelöltből nagy mozgás lesz (≥ 4σ_T), (2) +1R a strukturális stop (a mélypont) előtt.
* **Mérce:** AUC és lift a legjobb tizedben; a véletlen: az alap (csak skála-dummy) és a véletlen előjelű placebo (**80** realizáció, a teljes lánccal újrafuttatva).
* **Validáció:** expanding-window éves fold-ok, teszt 2023 / 2024 / 2025; a tanítás csak azokat a jelölteket látja, amelyek címkéje a tesztév előtt ismert (purge). Az OOS (2026-02-től) érintetlen.
* **Modell:** L2 logisztikus regresszió (C = 0,1, fix), winsorizált + standardizált változók; a GBM csak összehasonlítás. A „forward-kiválasztás" belső idő-CV-n választ csoportot, a külső tesztévet nem látja.

## Mekkora időtávon élnek a modellek?

A jelöltet a mélypont után kb. egy skálányi idővel ismerjük meg (a megerősítés pillanata), a teljes nagy mozgás kb. 5 skálányi.

{{chart:timescale}}

{{table:timescale_table}}

## Mit próbáltunk?

Hat csoport, összesen 48 változó, mind a megerősítő bar zárásáig ismert adatból (look-ahead teszt: a levágott adaton azonos).

{{table:groups}}

## Eredmény — cél: nagy mozgás lesz (≥ 4σ_T)

Pont = valós AUC (95% nap-klaszteres CI), narancs = a placebo átlaga és 95%-os sávja.

{{chart:auc_big}}

## Eredmény — cél: +1R a stop előtt

Pont = valós AUC (95% nap-klaszteres CI), narancs = a placebo átlaga és 95%-os sávja.

{{chart:auc_hit1}}

## Melyik változó?

Egyváltozós AUC a 2023–25-ös tesztidőszakon (a nyers változó mint pontszám), a placebo-sávval; a 14 legnagyobb eltérés.

{{chart:univar_big}}

{{chart:univar_hit1}}

{{table:metrics}}

{{table:compare}}

{{table:compare_univar}}

{{table:selected}}

## Értelmezés

* A valós modellek AUC-ja a „nagy mozgás" célnál ~0,52, de a placebón (ahol nincs irány-információ) ugyanennyi: a zigzag-definíció mechanikus hatása
  (minél nagyobb a megerősítésig megtett visszapattanás, annál közelebb a 4σ cél). A forward-kiválasztás a placebón is szinte mindig a zigzag-csoportot választja.
* A +1R célnál minden modell a placebo sávjában van (≈ 0,50).
* Egyetlen enyhén pozitív jel: a VWAP-csoport (a mélyponttól / csúcstól lehorgonyzott VWAP-távolság), p ≈ 0,03–0,05, de 16 modell és 48 változó mellett ez a véletlen szintje, és a +1R célon nem jön elő.
* A nem-kimerítő keret miatt a negatív eredmény csak erre a változókészletre, BTC-re, long irányra és 1 perces információra érvényes.
