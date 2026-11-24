# Kitörés-előrejelezhetőség v2: a lehető legjobb modellezés (epic_079)

**Időszak:** 2026-11-24 · **Asset:** BTCUSDT 1m, csak long, csak BTC-adat (nincs cross-asset) · **Státusz:** lezárt, nemleges (a nagy mozgás nem becsülhető használható mértékben; az egyetlen valós jel rövid távú visszafordulás, ami a díj alatt van) · **Kód:** `src/reports/builders/cand_v2_{sample,features,model,null}.py` · **Táblák:** `cand_v2_*` (lab DB, research séma)

## Kérdés

Az epic_078 (lásd [cand_pred](cand_pred.md)) lineáris és hangolatlan GBM próbával, kevés mintán, pillanatkép-változókkal futott. Itt a modellezést a lehető legjobbra állítottuk, hogy vagy találjunk placebón túli előrejelzést, vagy **erővel együtt** lezárjuk: BTC-n quant változókkal (1m ár, volumen, flow, funding) a kitörés nem becsülhető.

## Mintakészlet

- Jelölt = az σ-normált zigzag megerősített mélypontja (visszapattanás ρ·k·σ_T, ρ = 0,5, k = 4), skálák T = 15 / 30 / 60 / 120 / 240 / 480 / 960 perc együtt (a szerkezet skálafüggetlen, epic_077). 30 087 jelölt, 13 604 klaszter.
- Kauzális kizárások a megerősítő baron (train és teszt egyaránt): `flash` (V-alakú, a visszapattanás ≤ max(2, T/20) bar), `gap` (> 2% nulla-volumenű perc az utolsó 2T-ben), `trivial` (risk_z ≥ 3,8, a címke szinte biztos). Teszt-minta 17 387 jelölt (2023H1–2025H2).
- **Analitikus offset:** `p_rw = exp(−(k − risk_z)/(ρ·k))` — a véletlen bolyongás szerinti P(nagy | visszapattanás mérete). Ez leveszi a zigzag-mechanikát (epic_078 AUC ≈ 0,52-ét), és jól kalibrált. M0 = offset + rekalibráció (meredekség, tengelymetszet).
- Célok (előre rögzítve): `y_big` (a lökés ≥ 4σ), `y_hit1` (+1R a szerkezeti stop előtt), `fr_h05/h1/h2` (előre vett hozam σ_T-ben, 0,5T / 1T / 2T).
- **Erő (MDE):** a teszt-ablak effektív mintája ≈ a klaszterek száma; az AUC-hiba ≈ 0,007, a kimutatható minimum (α 0,05, erő 0,8) **≈ 0,019 AUC**.

## Változók (118)

Az epic_078 48 változója, plusz: dinamika (RSI / VWAP / flow / volumen / volatilitás változása 0,25T / 0,5T / 1T alatt), zigzag-sorozat (utolsó 6 láb), nagyobb (4T) skála állapota, Wyckoff-szerű (spring, teszt, erőfeszítés/eredmény), rezsim (30 napos trend, vol-percentilis, napszak / hét napja, idő a legutóbbi nagy mozgás óta, funding-változás). Mind a megerősítő bar zárásáig ismert adatból (`lookahead_check`: levágott adaton azonos).

## Modellezési protokoll (túltanulás ellen)

- **Külső teszt:** féléves blokkok 2023H1–2025H2; a tanító halmaz csak olyan jelöltet tartalmaz, amelynek a címkéje a blokk kezdete előtt már ismert (purge).
- **Előfeldolgozás** csak a külső tanítón: 0,5 / 99,5% vágás, QuantileTransformer (rang-gauss), korrelációs szűrés |ρ| > 0,92.
- **Modellek:** M0 offset + rekalibráció; M1 ridge-logit / ridge offsettel; M2 sekély LightGBM (`init_score` = offset); M3 ExtraTrees az offset-reziduumra; M4 a három eltolás átlaga.
- **Hiperparaméter és változószám** csak belső purge-olt, táguló idő-CV-n (≤ 3 féléves blokk), kis rácsról; ha semmi sem veri az offsetet, a tanuló az offsetre esik vissza.
- **Metrikák:** log-loss nyereség M0 felett, AUC, felső tized lift; hozamnál rang-IC és felső tized átlag; nap-klaszteres bootstrap CI.
- **Tanulság (hiba, javítva):** az 1 / klaszterméret súly utólagos információt tartalmaz (a pozitívak nagyobb klaszterekben vannak), és az AUC-t 0,606-ra fújta fel. Tanításhoz és kiértékeléshez **nem használható**; a függőséget a nap-klaszteres bootstrap és a skálánkénti bontás kezeli.

## Eredmény (külső teszt, 17 387 jelölt)

| Cél | Modell | Nyereség / IC | CI alsó határ | AUC (offset) |
|---|---|---|---|---|
| nagy mozgás | M4 | +0,00043 log-loss | +0,00006 | 0,537 (0,531) |
| nagy mozgás | M3 (legjobb) | +0,00054 | −0,00002 | 0,541 (0,531) |
| +1R a stop előtt | M4 | +0,00003 | −0,00050 | 0,506 (0,492) |
| hozam 0,5T | M4 | IC 0,039 | 0,020 | |
| hozam 1T | M4 | IC 0,035 | 0,018 | |
| hozam 2T | M4 | IC 0,027 | 0,009 | |

- **Nagy mozgás:** a javulás az offset felett statisztikailag épphogy kimutatható, de elhanyagolható (AUC +0,006, az MDE 0,019 alatt). Blokkonként 4 pozitív, 1 nulla, 1 negatív (2023H1), skálánként 6/7 pozitív. A **+1R-n nincs jel** (a CI nullát tartalmaz), ezért gazdasági haszon nincs.
- **Előre vett hozam:** a rang-IC ≈ 0,03–0,04, a jel mindenhol a „már megnyúlt visszapattanás után alacsonyabb a hozam" (átlaghoz visszatérés). Blokkonként 6/6 pozitív, skálánként 6/7. A felső és az alsó tized különbsége 3–6 bps, a költség 13 bps → díj után nem kereskedhető.

## Nullok

Mindkettő a teljes láncot újrafuttatja (előfeldolgozás, belső CV, visszaesés, M4), a statisztika a tanulók maximuma.

| Null | Cél | Valós | Null 95. perc. | Null max. | p |
|---|---|---|---|---|---|
| Időeltolt címke-permutáció (40) | nagy mozgás, nyereség | 0,0004 | 0,00017 | 0,00035 | 0,024 |
| | 1T hozam, IC | 0,0346 | 0,0203 | 0,0239 | 0,024 |
| Véletlen előjelű placebo (12) | nagy mozgás, nyereség | 0,0004 | 0,00027 | 0,00029 | 0,077 |
| | 1T hozam, IC | 0,0346 | 0,0237 | 0,0252 | 0,077 |

A p alsó határa a futásszámtól függ (40 → 0,024; 12 → 0,077), vagyis a valós érték mindkét nullban a legnagyobb. A placebo átlag-maximuma az IC-nél 0,011: a gépezet önmagában is ad némi látszat-IC-t.

## Ítélet az előre rögzített szabály szerint

Találat csak akkor, ha mind a négy teljesül: (1) a CI alsó határa > 0, (2) a null 95. percentilise fölött, (3) azonos előjel ≥ 2 évben és ≥ 2 skálán, (4) a felső tizedben gazdaságilag számít.

- Nagy mozgás: (1) csak M4-re, (2) igen, (3) igen, **(4) nem**: a +1R-n nincs jel, a log-loss nyereség 0,0004.
- Hozam: (1)–(3) igen, **(4) nem**: 3–6 bps a 13 bps díj alatt.

**Nincs találat.** A BTC-n az 1m ár, volumen, flow és funding változókból a kitörés kezdete nem becsülhető használható mértékben. Az egyetlen valós jel a rövid távú visszafordulás, ami a díjnál kisebb. Az OOS (2026-02-től) érintetlen maradt.

## Az epic_078 verdiktjének pontosítása

Az epic_078 „nem jósolható" megállapítása lineáris / sekély próbára vonatkozott, és az AUC ≈ 0,52 a zigzag-mechanika volt (a placebón is megvolt). Az epic_079 az offsettel ezt leveszi, jobb modelltérrel és 118 változóval is ugyanide jut.

## Korlátok

- Csak BTC, csak long, csak 1m-ból számolt változók; hírek, order book és cross-asset kívül esnek (a Scout és a hírek külön forrásként futnak).
- A placebo-null 12 ismétlés; ha valaki ezt a jelet tovább vizsgálja, a null szélesebb mintával finomítható.
- Az HTML riport (`_doc_/reports/cand_v2`) nem készült el; a számok a `cand_v2_*` táblákban és ebben a lapban vannak.

## Kód a git historyban

A builderek a repóban vannak (`src/reports/builders/cand_v2_*.py`); futtatás: `uv run python -m reports.builders.cand_v2_sample|cand_v2_features|cand_v2_model` és `cand_v2_null --mode perm|placebo`.
