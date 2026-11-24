---
epic: epic_064
id: t862
title: Az éles champion mint több napos trendjelző — jel-sorozatos belépés tesztje
assignee: modeling_agent
status: pr
blocks: []
blocked_by: []
---

## Goal

Az éles SOL champion 60 perces horizontra készült, de a gyakorlatban nem így
használjuk: ha a long jelek **sorozatban** jönnek, belépés longra; short
sorozatnál shortra; tartás napokig. A tapasztalat szerint a nagyobb trendeket
jól jelzi. Ez más állítás, mint amire a modell készült — és tesztelhető.

## Mérési elv

**Nem a nullához mérünk.** Ha a SOL emelkedik, minden long szabály nyereséges.
Minden kötés hozamát az ugyanabban az időszakban vett **összes azonos hosszú
ablak** átlagához hasonlítjuk. A különbség a jel.

**A kötés nem független megfigyelés.** Egy jelsorozat sok belépést nyit egyetlen
epizódon belül; egy beállításnál a 75 kötésből 32 egyetlen napra esett. A
tartási időnél közelebbi belépéseket egy epizódba vonjuk, és az **epizódok
egyenlő súlyú átlaga** a fő mérőszám.

## Eredmény — nem mérhető ki

Valid időszak (2026-02 … 2026-07), a 21 beállítás közül a legjobb:

| tartás | oldal | mérce | legjobb többlet | epizód | **t** |
|--:|---|--:|--:|--:|--:|
| 1n | LONG | −27 | +31 | 32 | 0,49 |
| 3n | LONG | −55 | +98 | 15 | 0,92 |
| 3n | SHORT | +35 | +235 | 5 | 0,58 |
| 7n | SHORT | +66 | +277 | 5 | **1,19** |
| 14n | SHORT | +91 | +180 | 5 | 0,58 |

**Egyetlen tartási időn, egyetlen oldalon sincs |t| ≥ 2** — a legjobb 1,19, és
az is 21 beállításból kiválasztva.

**Miért látszott működni:** a valid időszakban a SOL 118,1 → 74,5, azaz −37%.
Shortolni bármikor nyert. A szabály azért hozott, mert short volt, nem mert a
sorozat informatív. Ez magyarázza az előjelváltást is: trainen (emelkedő piac) a
long oldal néz jól ki, validon (eső piac) a short. Valódi hatás nem fordul meg.

## Két melléklelet

**A kötés-szintű bootstrapom is túl engedékeny volt.** 14 napos blokkokat
mintavételezett, de kötésenként súlyozott, így egy sok-kötéses epizód uralta:

| konfiguráció | kötés | epizód | kötés-szintű p | epizód-szintű t |
|---|--:|--:|--:|--:|
| 0,80 / 4 | 50 | 14 | 0,004 | **0,19** |
| 0,90 / 2 | 75 | 16 | 0,009 | **0,46** |

**A short modell in-sample ellen-prediktív több napos horizonton.** Trainen, 1
napos tartásnál 21 beállításból 15 éri el a |t| ≥ 2-t, mind negatív irányban.
Oka: a `short_mfe_fw60` a 60 perces lefelé *kilengés nagyságát* becsli, nem az
irányt — nagy becsült kilengés = magas volatilitás, ami emelkedő piacon gyakran
visszapattanó mélypont. Ugyanaz a magnitúdó-vs-irány probléma, mint a
`sol_8h_lgbm_gate.md` §6-ban.

## Adathibák, amikbe belefutottunk

- `predictions.close` a sorok **98,5%-ában NULL** (csak az éles farok hordozza)
  — az ár az `ohlcv` táblából jön.
- `short_pred` **negatív**, az erősebb jel a negatívabb; előjelváltás nélkül a
  percentilis-rangsor kifordítja az egész short oldalt.

## Kimenet

- [src/modeling/experiments/champion_streak_test.py](../../src/modeling/experiments/champion_streak_test.py)
- [_doc_/research/champion_streak_gate.md](../../_doc_/research/champion_streak_gate.md)

## Acceptance Criteria

- [x] Oksági jel-definíció (gördülő percentilis, nem a tárolt lookup)
- [x] Sorozathossz és küszöb söprés
- [x] Tartási idő 1–14 nap
- [x] Mérce: az összes azonos hosszú ablak, nem a nulla
- [x] Epizód-szintű aggregálás
- [x] Train / valid / 2026-08 külön, sosem összevonva

## Notes

**A 2026-08-i megfigyelés a hipotézist generálta, ezért nem erősítheti meg.** A
teljes hónap 1–6 független epizódot ad. Nem azért nem bizonyíték, mert a
benyomás hamis, hanem mert egy epizódból nem választható szét a jel a
szerencsétől.

**A valódi korlát a minta.** A champion train ablaka 2021-01 – 2026-01, tehát
out-of-sample történet csak 2026-02-től van: 6 hónap, ami 3 napos tartásnál ~15
független epizód. Feloldás: ugyanezt a modell-tervet korábbi vágásponttal
újratanítva, gördülő újratanítással végigvezetve ~4 év OOS rekord keletkezne.
Enélkül a kérdés eldönthetetlen, és további szabály-változatok mind ugyanazon a
15 epizódon futnának.
