# Az éles champion mint több napos trendjelző — kapu-jelentés

**Kérdés.** Az éles SOL champion 60 perces horizontra készült, és azon a
horizonton a díj felemészti a keveset, amit hoz. De nem így használjuk: ha a
long jelek **sorozatban** jönnek, beszállunk longra, ha a short jelek, akkor
shortra, és tartjuk napokig. Működik-e ez?

**Válasz.** Nem mérhető ki. A valid időszakban egyetlen beállítás sem veri
azt, hogy egyszerűen tartjuk ugyanazt az irányt — a legjobb `t = 1,19`.

Kód: `champion_streak_test.py`.

---

## 1. Beállítás

| | |
|---|---|
| Modell | `lgbm_solusdt_l_fw60_2101_2607` / `..._s_...` (éles champion) |
| Jel | a nyers score **oksági** gördülő percentilise, 30 napos ablak |
| Belépés | K egymást követő órán jel → belépés a K. óra záróárán |
| Tartás | 1 / 2 / 3 / 5 / 7 / 14 nap |
| Küszöb | percentilis ≥ 0,80 / 0,90 / 0,95 |
| Költség | 10 bps/kötés (maker be, taker ki, slippage) |

⚠️ **A percentilis gördülő ablakból jön, nem a tárolt kalibrációs lookupból.** A
lookup egy rögzített időszakon készült; visszafelé alkalmazva a belépési küszöb
tudna olyan adatról, amit az adott pillanat nem láthatott.

⚠️ **A `predictions.close` a sorok 98,5%-ában NULL** (csak az éles farok hordozza),
ezért az ár az `ohlcv` táblából jön.

⚠️ **A `short_pred` negatív** — az erősebb short jel a negatívabb. Előjelet
váltva rangsoroljuk, hogy a magasabb percentilis mindkét oldalon erősebb jelet
jelentsen.

---

## 2. A mérce: nem a nulla

Ha a SOL emelkedik, minden long szabály nyereségesnek látszik. Ezért egyetlen
kötés hozamát sem önmagában közöljük: az **ugyanabban az időszakban vett összes
azonos hosszú ablak** átlagához mérjük. Ez a különbség a jel, nem a nyers hozam.

---

## 3. ⚠️ A kötés nem független megfigyelés

Egy jelsorozat sok belépést nyit **egyetlen epizódon belül**, és azok mind
ugyanabba a mozgásba futnak bele. Egy beállításnál a 75 kötésből **32 egyetlen
napra esett** (2026-05-10).

A tartási időnél közelebbi belépéseket ezért egyetlen epizódba vonjuk össze, és
a fő mérőszám az **epizódok egyenlő súlyú átlaga**.

Ez nem elméleti finomság. Az első futásban kötés-szintű blokk-bootstrapot
használtam, ami 14 napos blokkokat mintavételezett — de az átlagot kötésenként
súlyozta, így egy sok-kötéses epizód uralta. Ugyanaz az eredmény:

| konfiguráció | kötés | epizód | kötés-szintű p | **epizód-szintű t** |
|---|--:|--:|--:|--:|
| 0,80 / 4 | 50 | 14 | 0,004 | **0,19** |
| 0,90 / 2 | 75 | 16 | 0,009 | **0,46** |
| 0,90 / 3 | 35 | 11 | 0,035 | **0,93** |
| 0,95 / 2 | 40 | 11 | 0,035 | **0,05** |

A `p = 0,004` a kötésszámból jött, nem bizonyítékból.

---

## 4. Eredmény — a legjobb konfiguráció minden tartási időre

A „legjobb többlet" az adott időszak/oldal 21 beállítása közül a legnagyobb —
tehát felfelé torzított, mégis ez a szám.

| időszak | tartás | oldal | mérce | legjobb többlet | epizód | **t** |
|---|--:|---|--:|--:|--:|--:|
| **VALID** | 1n | LONG | −27 | +31 | 32 | 0,49 |
| | 1n | SHORT | +7 | +229 | 5 | 0,87 |
| | 2n | LONG | −40 | +47 | 26 | 0,66 |
| | 2n | SHORT | +20 | +208 | 6 | 0,66 |
| | 3n | LONG | −55 | +98 | 15 | 0,92 |
| | 3n | SHORT | +35 | +235 | 5 | 0,58 |
| | 5n | LONG | −77 | +25 | 10 | 0,13 |
| | 5n | SHORT | +57 | +166 | 8 | 0,76 |
| | 7n | LONG | −86 | −42 | 8 | −0,13 |
| | 7n | SHORT | +66 | +277 | 5 | **1,19** |
| | 14n | LONG | −111 | −72 | 5 | −0,16 |
| | 14n | SHORT | +91 | +180 | 5 | 0,58 |

**A valid időszakban egyetlen tartási időn, egyetlen oldalon sincs `|t| ≥ 2`** —
a legjobb 1,19, és az is a 21 beállítás közül kiválasztva.

---

## 5. Miért látszott mégis működni

**A valid időszakban a SOL 118,1-ről 74,5-re esett: −37%.** Ott shortolni
bármikor nyert. A short-sorozat szabály azért hozott pénzt, mert **short volt**,
nem mert a sorozat informatív lett volna. A többlet a „shortolj bármikor"
mércéhez képest t ≈ 0–1,2.

Ugyanez magyarázza az előjelváltást a két időszak között: trainen (emelkedő
piac) a long oldal néz jól ki, validon (eső piac) a short. Egy valódi hatás nem
fordul meg — a piaci irány igen.

---

## 6. Egy figyelmeztető jel a train oldalon

| időszak | tartás | oldal | legjobb többlet | t | `|t| ≥ 2` db / teszt |
|---|--:|---|--:|--:|--:|
| TRAIN | 1n | SHORT | −30 | −1,55 | **15 / 21** |
| TRAIN | 2n | SHORT | −31 | −0,95 | 5 / 21 |
| TRAIN | 3n | SHORT | −40 | −0,59 | 4 / 21 |

A train **in-sample** — ott a modellnek jól kellene teljesítenie. A short oldal
mégis **ellen-prediktív**: 1 napos tartásnál 21 beállításból 15 éri el a
`|t| ≥ 2`-t, és mind negatív irányban. Shortolni short-jelre rosszabb, mint
shortolni véletlenszerűen.

Ennek van értelme: a short modell a következő 60 perc **lefelé kilengésének
nagyságát** becsli (`short_mfe_fw60`). Nagy becsült lefelé kilengés = magas
volatilitás, nem lefelé irány. Emelkedő piacon ezek gyakran visszapattanó
mélypontok. Ugyanaz a magnitúdó-vs-irány probléma, ami a `sol_8h_lgbm_gate.md`
§6-ban is előjött.

---

## 7. A 2026-08-i megfigyelés

Ez az a hónap, amiből a hipotézis származik — a kitörés, ami „zölddel kezdte az
elejét, közepét és az utolsó hullámot is". A teljes hónap **1–6 független
epizódot** ad bármelyik beállításnál. Egyetlen megfigyelés.

**A generáló megfigyelés nem lehet egyben a megerősítés.** Nem azért, mert a
benyomás hamis, hanem mert egy epizódból nem lehet megkülönböztetni a jelet a
szerencsétől — bármi is történt.

---

## 8. Mi a valódi korlát, és mi oldaná fel

Nem a modell és nem a szabály: a **minta**. A valid 6 hónap, ami 3 napos
tartásnál ~15 független epizód. Ennyiből egy közepes hatás sem mérhető ki.

Az éles champion train ablaka 2021-01 – 2026-01, tehát az out-of-sample
történet mindössze 2026-02-től van. Ez feloldható: **ugyanezt a modell-tervet
korábbi vágásponttal újratanítva és gördülő újratanítással végigvezetve** a
teljes történeten ~4 év out-of-sample rekord keletkezne 6 hónap helyett. A
`sol_lgbm_8h.py` gördülő gépezete pontosan ezt csinálja, csak másik célváltozóra.

Enélkül a kérdés eldönthetetlen, és semmilyen további szabály-változat nem
segít — az mind ugyanazon a 15 epizódon futna.

---

## Kód a git historyban

A kísérleti kód a repóból törlődik (epic_068, t894/t895). Törlés előtti állapot: commit `73f2156`, útvonalak: `src/modeling/experiments/champion_streak_test.py`.
