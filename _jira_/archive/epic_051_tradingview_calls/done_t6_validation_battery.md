---
epic: epic_051
id: t6
title: Validációs battery — permutációs placebo + FDR korrekció
assignee: —          # főszálon fut, agent nélkül (a t1-t5 folytatása)
status: done
blocks: [t7]
blocked_by: []
---

## Goal

Eldönteni, hogy a `t5` rangsora **valódi jel vagy overfit**. A scoring akkor is
gyárt rangsort, ha az adatban nincs semmi — a rangsor önmagában nem bizonyít.

## Scope

```
src/data_handling/store/analyst_signal_store.py  ← migráció v4: oos_validation
src/modeling/analyst_signal/validate.py          ← a battery
```

## A három teszt + FDR

Mind a valódi adatot **permutált szerző-címkékhez** hasonlítja, 5000
permutációval. **A permutáció megőrzi a szerzőnkénti call-számot**, tehát egy
pusztán mintaméret-vezérelt rangsor nem álcázhatja magát képességnek — ez volt
a `t5` Notes-ában rögzített aggály.

| teszt | mit kérdez |
|---|---|
| `beat_benchmark` | hány szerző veri a buy-and-holdot a véletlenhez képest |
| `significant` | hány szerző ér el egyénileg `t > 2`-t |
| `best_author` | a legjobb `t` jobb-e, mint egy véletlen particionálás legjobbja (family-wise, egzakt) |

Plusz **Benjamini-Hochberg FDR** a szerzőnkénti egyoldalú p-értékeken — ez
válaszolja meg a gyakorlati kérdést **név szerint**.

## Acceptance Criteria

- [x] `oos_validation` tábla migrációval
- [x] Permutáció **call-szám megőrzésével**
- [x] Family-wise egzakt teszt a legjobb szerzőre
- [x] BH-FDR a teljes szerző-családon
- [x] Minden horizont, minden eredmény kiírva (nincs válogatás)
- [x] `ruff check` + `pyright` tiszta

---

## EREDMÉNY — 2026-08-20 — **nincs jel**

138 szerző (≥30 call), 5000 permutáció, 5 horizont, 15 teszt.

| h | teszt | valódi | véletlen | p |
|---|---|---:|---:|---:|
| 1h | veri a BH-t (db) | 67 | 70,3 | 0,816 |
| 1h | egyénileg szign. | 0 | 3,5 | 1,000 |
| 1h | legjobb `t` | 1,92 | 2,65 | 0,989 |
| **1d** | veri a BH-t (db) | **60** | **62,6** | 0,760 |
| **1d** | egyénileg szign. | **2** | **2,4** | 0,708 |
| **1d** | legjobb `t` | **2,50** | **2,52** | **0,476** |
| 3d | veri a BH-t (db) | 54 | 54,0 | 0,543 |
| 7d | veri a BH-t (db) | 52 | 53,4 | 0,662 |

**A nyertesek száma minden horizonton a véletlen szintjén vagy alatta.**

### FDR — egyéni szerzők

| h | nyers `p<0,05` | véletlenül várható | FDR 10% | FDR 5% |
|---|---:|---:|---:|---:|
| 1h | 5 | 6,9 | **0** | **0** |
| 4h | 10 | 6,9 | **0** | **0** |
| 1d | 6 | 6,9 | **0** | **0** |
| 3d | 4 | 6,9 | **0** | **0** |
| 7d | 6 | 6,9 | **0** | **0** |

**Egyetlen szerző sem éli túl a korrekciót.**

## Notes

**A legjobb jelölt:** `P_S_trade`, 1d, +1,359%/call, `t=2,50`, nyers
`p=0,0062`. Family-wise permutációs `p = 0,476` — a 138 *hamis* szerző
legjobbika az esetek 48%-ában ilyen jó vagy jobb.

**Két menet közben felmerült, majd elvetett lelet** — mindkettőt rögzítem,
hogy ne kelljen újra felfedezni:

1. *Split-half perzisztencia* 1d-n, 2024-07 vágással, ≥20 call: Spearman
   **0,534, p=0,0004**. Elvetve: 16 konfiguráció végigpróbálása után jött ki,
   és nem robusztus (más vágóponttal 0,248, p=0,11).
2. *„A rosszak megmaradnak rossznak"* — BOTTOM10 hátrány `p=0,006`.
   Elvetve: 10 horizont/vágópont kombinációból **1** szignifikáns, pontosan
   annyi, amennyi véletlenül jár.

**Erő-elemzés (a T7 riportba):** egy 76 callos szerzőnél az átlag standard
hibája ≈ 0,34%, tehát csak **~0,9%/call** valódi él lett volna kimutatható.
A következtetés helyes olvasata: *nincs azonosítható szerző*, nem pedig
*senkinek nincs képessége*.
