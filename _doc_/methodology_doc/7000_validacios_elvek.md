# 7000 — Validációs elvek és mérési fegyelem

> Ez a dokumentum azt rögzíti, **hogyan mérünk** — nem azt, hogy mit találtunk.
> A konkrét eredmények a `_doc_/research/` alatt vannak.
>
> Az itt leírt elvek nem elméletből származnak: mindegyik mögött egy konkrét
> hiba áll, amit a projektben elkövettünk, és egy szám, ami megmutatta.

---

## 1. Az alapelv

> **Az in-sample szelekció nem eredmény.**

Ha N beállítás közül kiválasztod a legjobbat ugyanazon az adaton, amin méred,
akkor nagyrészt azt választod ki, **melyiknek volt szerencséje** — nem azt,
melyik jó.

### A bizonyíték

Az `epic_054`-ben mért **+34%-os él** a kontroll modellhez képest, in-sample
szelekcióval. Ugyanez gördülő teszten, ahol a választás és a mérés szét volt
választva: az előny eltűnt, majd megfordult.

Az `epic_057`-ben a 3 hónapos ablakon nyerő setup 12 hónapon **−14,15%**-ot hozott.

### A szabály

Minden mérésnél a **kiválasztás** (paraméter, küszöb, setup) és a **mérés**
külön adaton történik. Ha ez nem teljesül, a szám felső korlát, nem várható érték.

---

## 2. Előre rögzített döntési szabály

A siker feltételét **az eredmények ismerete előtt** kell leírni, a ticketbe.

Rossz: "megnézzük, mi jön ki, és eldöntjük, jó-e."
Jó: "a felső sáv találati aránya legyen > 55% mindkét rezsimben; alatta a
megközelítés lezárul."

Ez azért kell, mert egy tetszőleges eredményhez utólag mindig található
magyarázat. Az `epic_058`-ban ezt előre rögzítettük, és emiatt lehetett a
negatív eredményt vita nélkül elfogadni.

**Minden variánst közölni kell, nem a legjobbat.** Ha három küszöböt teszteltél,
mindhárom szerepeljen a riportban.

---

## 3. Gördülő mérés — az élessel egyező üzemeltetés

Egy évig eleresztett modellt mérni **nem ugyanaz a rendszer**, mint amit élesben
üzemeltetnél.

### A protokoll

```
hiperparameter kereses  EGYSZER, a legkorabbi ablakon, belso hasitassal
                        soha nem latja az ertekelesi honapokat

utana havonta:          ujratanit minden addigi adaton
                        ujrakalibralja a kuszobot
                        elorejelzi az adott honapot
                        nyugdijba megy
```

Az így kapott egy-hónapos out-of-sample szeletek összefűzve adnak folyamatos
idősort. **Egy modell soha nem fut egy hónapnál tovább.**

### Kalibráció

A belépési küszöb **gördülő kvantilis** a modell saját score-eloszlásából
(pl. 30 napos trailing p95), nem fix abszolút érték. A modell kimeneti
eloszlása elcsúszik; fix küszöbnél a kiválasztási arány vele együtt vándorol.

---

## 4. A költség nem utólagos korrekció

> **A rácskeresés célfüggvénye legyen nettó.**

### A bizonyíték

Bruttó célfüggvénnyel a kereső azt a beállítást választotta, amely 1000 USD
notionalon:

```
brutto nyereseg    +649 USD
dij (1984 kotes)  -1587 USD
netto              -938 USD
```

Nettó célfüggvényre váltva **a 200 setupból 197-200 helyezése megváltozott.**
A régi rangsor semmit nem mondott a valós sorrendről.

### A költség szerkezete

A díj a **notionalra** megy, nem a tőkeáttételes részre, és **kilépés-típusonként
eltér**:

| Esemény | Ordertípus | Díj |
|---|---|---|
| Belépés | limit (1 perc) | maker |
| TP kilépés | limit, pihen | maker |
| **SL kilépés** | **stop → market** | **taker** |
| Timeout kilépés | limit (1 perc) | maker, ha telik |

Az SL szándékosan taker: egy nem teljesült stop-limit pont abban a mozgásban
hagy bent, ami ellen véd.

**A limit teljesülését szimulálni kell, nem feltételezni.** (Mérve: ~98% a
következő percen belül.)

---

## 5. A target szemantikája dönti el, mit tanul a modell

> **Amit a targetbe írsz, azt fogja megtanulni — akkor is, ha nem azt akartad.**

### A bizonyíték

A `long_mfe_fw240` **csak a felfelé kilengést** méri. A modell ezt tanulta meg,
és mivel a nagy felfelé kilengés együtt jár nagy lefelé kilengéssel,
**volatilitás-előrejelző** lett irány-előrejelző helyett.

Mért adat a felső 1%-on:

```
max felfele   +258,8 bps
max lefele    -350,6 bps    <- NAGYOBB
4h zaro        +20,7 bps
```

### A szabály

A címke tegye fel **ugyanazt a kérdést, amit a kereskedés**. TP/SL-lel kereskedő
stratégiához: *melyik korlátot éri el előbb az ár* (triple barrier).

A címke konvenciói **egyezzenek a szimulátoréval** — intrabar érintés, same-bar
ütközés feloldása.

---

## 6. Körkörös címkézés — a leggyakoribb csapda

> **Ha a címkét a jövőbeli mozgás definiálja, akkor a "mi történt utána" kérdés
> tautológia.**

### A bizonyíték

Zigzag fordulópontokra mérve a triple-barrier kimenetet: **98-99% találat.**
Értéktelen szám: a zigzag mélypontot az definiálja, hogy onnan az ár X%-ot
emelkedett; a `+1` címke pedig azt, hogy a +100 bps TP teljesült. Ugyanaz a
mennyiség kétszer.

### A szabály

"Tökéletes detektor felső korlátja" típusú méréshez a detektornak **kauzálisnak**
kell lennie — csak a `t`-ig látó adatból. Egyébként bármely target 100%-ot ad.

---

## 7. Rezsim-függőség

Egyetlen ablakon mért eredmény nem általánosítható. A projektben háromszor
fordult elő, hogy a következtetés rezsimtől függött:

- egy hónapban **0/200 setup** volt nyereséges (2026-06)
- ablakonként más setup nyert
- a nyerő **irány a trenddel fordult**: eső piacon short, emelkedőn long

### A szabály

Minden mérés fusson **legalább két ellentétes rezsimen**, és a riport bontsa
külön. **Egy rezsimben pozitív, másikban negatív = negatív eredmény**, nem
"a jó rezsimben működik".

---

## 8. Feature-mennyiség és FDR

A lag/delta/z-score expanzió multiplikátor: 15 nyers sorozat × 7 lag ×
4 transzformáció = 400+ oszlop. FDR-korrekció mellett ez önmagában megöli a
szignifikanciát.

**A tesztek nevezőjét előre dokumentálni kell**, és nem szabad utólag szűkíteni.

---

## 9. Sizing — a tőkeáttétel nem szabad paraméter

> **Több tőkeáttétel egy ponton túl KEVESEBB végső tőkét ad, likvidálás nélkül is.**

Mért adat 140 kötésen, kamatos szimulációval:

| Tőkeáttétel | Végső tőke | Max visszaesés |
|---|---|---|
| 2x | 1,56× | −42,9% |
| **5x** | **2,31×** | −77,4% |
| 10x | 1,92× | −96,4% |
| 20x | **0,02×** | −100% |

A mechanizmus: a leghosszabb vesztes sorozat **4 egymás utáni SL** volt. Az SL
tökéletesen működött (−2,50% kötésenként), de 20x-en ez **−95%**.

⚠️ **Az SL egyetlen kötés veszteségét korlátozza, nem a sorozatét.** 72%-os
nyerő aránynál a négyes vesztes sorozat 140 kötésen **várható**, nem szélsőség.

A sizing az adatból következik (a Kelly-korlát fele), nem preferenciából.

---

## 10. Csendes adathibák

Két konkrét eset, mindkettő hibaüzenet nélkül rontott volna:

**Snapshot tartalom-hash.** Csak a sorokat fingerprintelte, az oszlopokat nem.
Új target-oszlop hozzáadása után a rendszer "reused" üzenettel a régi, oszlop
nélküli snapshotot adta vissza. Javítva: az oszloplista része a hashnek.

**Archívum-formátumváltás.** A Binance spot kline timestamp 2025 folyamán
**milliszekundumról mikroszekundumra** váltott, a futures fájlok nem. Fejléc is
csak az újabb fájlokban van. Mindkettőt fájlonként detektálni kell, nem
feltételezni.

**Elv:** minden külső adatforrásnál az első lépés a **formátum ellenőrzése
több korszakból**, nem a dokumentáció elolvasása.

---

## 11. Ellenőrzőlista új kísérlethez

- [ ] A döntési szabály a ticketben van, az eredmények előtt
- [ ] A kiválasztás és a mérés külön adaton történik
- [ ] Gördülő újratanítás, az élessel egyező gyakorisággal
- [ ] A küszöb a train adatból jön, a validra változatlanul alkalmazva
- [ ] A célfüggvény nettó (díj + csúszás + funding)
- [ ] A limit orderek teljesülése szimulálva, nem feltételezve
- [ ] Legalább két ellentétes rezsim, külön bontva
- [ ] A címke nem tartalmaz jövőbeli információt a definíciójában
- [ ] Minden tesztelt variáns közölve, nem csak a legjobb
- [ ] A tesztek nevezője (feature × lag × target) dokumentálva
