## Cél

A t950–t952 katalógus utólagos: csak a már befejezett nagy mozgásokat látja. Itt a **nevezőt** építjük: azokat a helyzeteket, amelyek **valós időben** felismerhetők
(egy megerősített mélypont, ami után a ár a skálájához tartozó σ-egység felét visszapattanta), és megnézzük, hány lett belőlük nagy mozgás, és mennyit hoz az R-szorzós belépés.

* **Jelölt:** a (T, k) zigzag megerősített mélypontja; a megerősítés pillanatáig ismert adat az egyetlen bemenet.
* **Ciklusszám (c):** egymást követő *emelkedő mélypontok* száma (0 = friss mélypont, a Scout-féle „1-2" indulás; 1 = egy ciklus után; ≥ 2 = később).
* **Címke 1:** a jelöltből indul-e nagy mozgás (a t950 definícióval).
* **Címke 2 (R-szorzó):** belépés a megerősítő bar zárásán, stop = a mélypont (strukturális), cél = +1R / +2R / +3R, ahol R = belépő − mélypont. Zárás alapú, nincs időlimit. Martingálnál `P(+nR) = 1/(n+1)`, `E[R] = 0` költség előtt.
* **Költség:** 13 bps (taker be + ki: 2 × 5; csúszás be + ki: 2 × 1,5), R-egységben `13 bps / R`.
* **Placebo:** 40-40 véletlen-előjeles és blokk-bootstrap útvonal, ugyanazon a kódon.
* **CI:** nap-klaszteres bootstrap (egy nap jelöltjei együtt mintavételezve).

## Mennyi a bázisráta?

{{chart:p_big_k3}}

{{chart:p_big_k4}}

## R-szorzós találati arányok és él

{{chart:hits_k4}}

{{chart:edge_r1}}

{{chart:edge_r2}}

{{chart:edge_r3}}

## Van-e mintázat a jelölt jellemzőiben? (leíró, k = 4, T ≥ 1 óra)

A sávok és szempontok száma miatt ez feltáró, nem megerősítő elemzés; a CI-k nap-klaszteresek.

{{chart:strata_retr}}

{{chart:strata_vol}}

{{table:strata}}

## Effektív mintaméret

{{table:effective_n}}

{{table:summary}}

{{table:summary_demeaned}}

{{table:placebo_compare}}

## Értelmezés

Lásd az epic zárójelentését. Ez a riport a t953 mérése.
