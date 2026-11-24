# Elliott-hullámok tesztje: becsülhető-e a 3. és az 5. hullám? (epic_080)

**Időszak:** 2026-11-24 · **Asset:** BTCUSDT 1m, csak long, csak BTC · **Státusz:** lezárt, nemleges (az Elliott-szabályokat teljesítő setupok nem jobbak a véletlen bolyongásnál; díj után mind negatív) · **Kód:** `src/reports/builders/elliott_test.py`, `elliott_null.py` · **Táblák:** `elliott_*` (lab DB, research séma)

## Kérdés

Korai Elliott-setupból, kauzálisan felismerve, becsülhető-e (A) a 3. hullám egy beágyazott 1-2-1-2 után, illetve (B) az 5. hullám 1-2-3-4 után? Milyen gyakran követi a setupot jelentős hullám a szerkezeti stop előtt, és ez több-e a véletlennél?

## Módszer

- **Pivotok:** az epic_077 σ-normált zigzagja, T = 15 / 30 / 60 / 120 / 240 / 480 / 960 perc × k ∈ {3, 4} (ρ = 0,5). Minden öt egymást követő megerősített pivot (alsó-felső-alsó-felső-alsó, p0 … p4) jelölt; a belépés a p4 megerősítő barjának zárása (kauzális; look-ahead teszt: a levágott adaton azonos ablakok).
- **A család** (p3 < p1): kemény szabályok p2 > p0, p4 > p2. **B család** (p3 > p1): p2 > p0, p4 > p2, p4 > p1 (a 4. hullám nem fedi át az 1.-et), W3 ≥ W1. **Fib:** a 2. hullám visszahúzása [0,382; 0,786], a 4. hullámé A-nál [0,382; 0,786], B-nél [0,236; 0,5].
- **Kohorszok:** `fib` (kemény + Fibonacci), `hard` (csak kemény), `control` (a kemény szabályt nem teljesítő, ugyanilyen pivot-ablakok, ugyanazokkal a képletekkel).
- **Célok:** A elsődleges p2 + 1,618·W1, másodlagos p1; B elsődleges p4 + W1, másodlagos p3. **Stop:** p4 (szoros) vagy p2 (tág). Időkorlát 60·T bar; gap-szűrés (> 2% nulla-volumenű perc az utolsó 2T-ben).
- **Mérce:** a driftmentes bolyongás találati aránya, `p_rw = D/(U + D)` (D = log(belépő/stop), U = log(cél/belépő)). A statisztika a lezárt (cél vagy stop) eseményeken a `hit − p_rw` átlaga („többlet"), nap-klaszteres bootstrap CI-vel; emellett a különbség a kontrollhoz képest („delta") és a díj (13 bps) utáni nettó átlag.
- **Előre rögzített döntési szabály** (epic_080): találat, ha a többlet a p_rw és a kontroll felett is a CI alsó határán > 0, a placebo-null maximumának 95. percentilise fölött, azonos előjel ≥ 2 évben és ≥ 2 skálán, és díj után a nettó átlag pozitív.

## Gyakoriság

Setup-ablakok (mind a 7 skála és k = 3 / 4 együtt, a két k átfedő pivotokat ad):

| Család | fib | hard (csak kemény) | control |
|---|---|---|---|
| A (3. hullám) | 821 | 2 779 | 34 806 |
| B (5. hullám) | 2 101 | 2 877 | 31 018 |

A lezárt eseményeken az A család találati aránya az elsődleges célnál (p2 + 1,618·W1, stop p4) 23% (kemény) és 18% (fib); a B családé (p4 + W1) 53% és 51%. A p_rw ezeknél 24% / 20% és 55% / 51%. A lezáratlan (időkorlát) események aránya az A családban 5–7%, a B-ben 1% alatt.

## Eredmény

Elsődleges cél, szoros stop (a többi változat a `elliott_summary` táblában):

| Család | Szint | n (lezárt) | Találat | p_rw | Többlet [95% CI] | Nettó bp (kontroll) |
|---|---|---|---|---|---|---|
| A | hard | 3 379 | 23,1% | 24,0% | −0,009 [−0,032; +0,015] | −19,3 (−13,5) |
| A | fib | 766 | 18,1% | 20,0% | −0,019 [−0,059; +0,021] | −21,3 (−13,5) |
| B | hard | 4 942 | 52,5% | 54,8% | −0,022 [−0,045; −0,001] | −17,0 (−17,2) |
| B | fib | 2 085 | 50,6% | 50,7% | −0,001 [−0,034; +0,032] | −14,5 (−17,2) |

- A 16 előre rögzített változat közül a legnagyobb többlet **+0,009** (B, tág stop, másodlagos cél, fib), CI-vel a nullát tartalmazza. Az A család másodlagos céljánál a fib-setup a véletlennél szignifikánsan **rosszabb** (−0,058 / −0,061, CI a nulla alatt).
- A kontrollhoz képesti különbség a B fib kohorszban +0,02–0,03, de ez abból jön, hogy a szabályt nem teljesítő ablakok a véletlennél rosszabbak; a véletlen bolyongáshoz képest nincs többlet.
- Skálánként és évenként a többlet a 0 körül ingadozik, a B családnál túlnyomóan negatív; a legnagyobb mintájú skálákon (T = 15 / 30 / 60) is negatív vagy ≈ 0.
- **Díj után minden változat negatív** (−9,5 … −31 bp / kötés a 13 bps díjjal).
- **Erő:** a többlet kimutatható minimuma (MDE, 2,8·SE) 0,025–0,069, vagyis kb. 2,5–7 százalékpontos eltérést látnánk meg. Az Elliott-élre nincs ennél kisebb bizonyíték.

## Placebo-null (40 ismétlés)

A teljes lánc (σ, zigzag, ablakok, szabályok, kimenetel) véletlen előjelű áron újrafuttatva (|r| marad, volumen valós), a statisztika a 16 változat maximuma:

| Statisztika | Valós | Null átlag | Null 95. perc. | Null max. | p |
|---|---|---|---|---|---|
| max többlet a p_rw felett | 0,0088 | 0,0148 | 0,0314 | 0,0370 | 0,68 |
| max különbség a kontrollhoz | 0,0262 | 0,0269 | 0,0442 | 0,0573 | 0,54 |

A valós legjobb változat a placebo-maximumok átlaga alatt van, vagyis a „legjobb a 16-ból" válogatás önmagában többet ad a véletlenen, mint amit az Elliott-szabály ad.

## Ítélet

**Nincs találat.** A szigorú Elliott-szabályok szerint, kauzálisan felismert 3. és 5. hullám-setupok BTC-n 1m-ből nem becsülhetők a véletlen bolyongásnál jobban: a találati arány a p_rw körül (néhol alatta) van, a null nem utasítható el, a díj után minden változat negatív. A szabályrendszer (Fibonacci-sávok, nem-átfedés) a kontrollhoz képest csak azért tűnik jobbnak, mert a szabályt nem teljesítő ablakok rosszabbak a véletlennél. Az OOS (2026-02-től) érintetlen maradt.

## Korlátok

- Az Elliott-számlálás szubjektív; egy rögzített gépi operacionalizálást mértünk (σ-zigzag, kemény szabályok, Fibonacci-sávok, két cél, két stop). Más pivot-motor, korrekciótípusok (zigzag, lapos, háromszög) és több fokozat együtt vizsgálata nem történt meg.
- A k = 3 és k = 4 ablakok, valamint a skálák átfedő pivotokat adnak; a nap-klaszteres CI ezt kezeli, a nyers n nem független.
- Csak BTC, csak long, csak 1m-ből számolt ár; SOL/ETH replikáció nem készült (nincs kiemelkedő jel, amit replikálni kellene).
- Az 5. hullám bevégződése (a 3. hullám utáni teljes 1-2-3-4-5 számlálás szubjektív megerősítése) nem szerepel; a mérce az első érintés a cél és a stop közt.

## Kód

`uv run python -m reports.builders.elliott_test` (valós futás, táblák), `uv run python -m reports.builders.elliott_null --n 40 --workers 2` (placebo-null). A régi spec és a 4. hullám-tanulmány: `_doc_/research/elliott_waves/`.
