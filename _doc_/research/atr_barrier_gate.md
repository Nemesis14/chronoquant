# Vol-skálázott triple-barrier — a rezsim mint a feladat része (epic_060)

> **Verdikt: RÉSZLEGES.** A skálázott korlát pontosan azt csinálta, amiért
> megépült — **a rezsim-függés összeomlott** (cellák szórása 14,9 → 5,9 bps),
> és az eredmény javult (−5,26 → −3,31 bps/kötés). De **egyetlen cella sem
> pozitív**, tehát az `epic.md` GO-feltétele nem teljesül.
>
> Dátum: 2026-08-26 · SOLUSDT · 240 perces horizont · 25 hónap OOS

---

## 1. A probléma, ami idevezetett

Fix ±100 bps korlát mellett a címke jelentése korszakonként elcsúszik:

| év | timeout arány | (padló nélküli) median korlát |
|---|---|---|
| 2021 | **1,6%** | 153,5 bps |
| 2023 | 18,4% | 91,8 bps |
| 2026 | **39,3%** | **64,5 bps** |

2021-ben a címke azt kérdezi „melyik korlátot éri el előbb", 2026-ban azt,
hogy „elér-e egyáltalán bármelyiket". A modell **korszak-proxyt** tanul.

---

## 2. A címke

```
sigma_1m       = EWM std(logret, span=1440), t-1-re shiftelve
korlat_bps(t)  = clip(k * sigma_1m * sqrt(240) * 1e4, 60, 400)
```

**A `k` nem hangolt paraméter.** `normalising_k()` úgy választja meg, hogy a
korlát **teljes-historiás mediánja pontosan 100 bps** legyen — így a fix
változattal közvetlenül összevethető, és a különbség tisztán az adaptivitás,
nem a szélesség. Mért érték: **k = 0,5242**.

⚠️ Az ATR_14 helyett `sigma·√t`: az ATR egy 1 perces bar tartományát méri, a
korlát viszont egy 240 perces ablakra vonatkozik, és a kettő között nincs
rögzített arány.

### Rezsim-stabilitás — a belépési feltétel

| timeout arány | fix ±100 bps | vol-skálázott |
|---|---|---|
| 2021 | 1,6% | 6,5% |
| 2026 | 39,3% | 19,6% |
| **tartomány** | **37,7pp** | **13,1pp** |

**Háromszor stabilabb**, de az előre rögzített 10pp-es feltételt 3,1pp-vel
nem hozza. **A maradék instabilitás teljes egészében a 60 bps-es padlóból
jön**: padló nélkül 2026-ban a barok **42,7%-a** kerülne 60 bps alá.

Ez nem címkézési hiba, hanem strukturális állítás:

> **2026-ban a SOL 4 órás volatilitása annyira leesett, hogy egy
> rezsim-semleges korlát a barok 42,7%-án a díjfedezeti szint alá kerülne.**

A padló nem vehető el: 40 bps-es korlátnál a 13 bps kör-költség a nyereség
harmada lenne.

---

## 3. Eredmény

Azonos végrehajtás (maker belépés + maker timeout), 25 hónap OOS:

| Címke | n | találat | fedezeti | **bps/kötés** |
|---|---|---|---|---|
| Fix ±100 bps | 1981 | 47,4% | 53,1% | **−5,26** |
| **Vol-skálázott** | 1874 | **48,4%** | 53,8% | **−3,31** |

**+1,95 bps/kötés.** A javulás nagyobb része a **short** oldalról jön:
47,1% → 49,7% (+2,6pp).

### A lényeg: a rezsim-függés

| cella | fix ±100 bps | vol-skálázott |
|---|---|---|
| emelkedő LONG | +2,43 | −2,55 |
| emelkedő SHORT | −4,12 | −6,14 |
| eső LONG | **−12,51** | **−2,03** |
| eső SHORT | −6,23 | **−0,23** |
| **cellák szórása** | **14,9 bps** | **5,9 bps** |

*(mindkét oszlop a legjobb végrehajtással: maker + BNB)*

**A legrosszabb cella −12,51 → −6,14, a szórás 2,5-szeresére csökkent.**
Ez pontosan a tervezett hatás: a rezsim kikerült a feladatból.

---

## 4. ⚠️ Két dolog, amit ki kell mondani

### 4a. A fedezeti pont ROMLOTT: 53,1% → 53,8%

A 2024–2026-os értékelési ablakban a skálázott korlát **szűkebb** (74–96 bps
a fix 100 helyett), mert a volatilitás leesett. Szűkebb korlát mellett a fix
díj arányában nagyobb.

A vol-skálázás tehát **nem a költségen nyer, hanem a találati arányon** — és
többet nyer ott, mint amennyit itt veszít. Ez fontos: azt jelenti, hogy a
javulás valódi előrejelzés, nem könyvelési trükk.

### 4b. A keresés KEVESEBB liftet talált: 0,1104 → 0,0750

Ez **megerősíti a fő diagnózist**. A fix címkén mért lift egy része nem
irány-előrejelzés volt, hanem annak előrejelzése, mikor van nagy mozgás. A
vol-skálázás ezt kinormálja — és ami marad, az kevesebb, de **valódibb**:
a kereskedési eredmény javult, miközben a „lift" romlott.

---

## 5. Az összesített állás a direkciós ágon

| Konfiguráció | bps/kötés | / 1000 USD |
|---|---|---|
| Kiindulás (fix címke, taker) | −9,38 | −189 USD |
| + maker belépés | −5,54 | −110 USD |
| + maker timeout | −5,26 | −104 USD |
| + BNB-kedvezmény | −4,72 | −94 USD |
| **+ vol-skálázott címke** | **−2,78** | **−52 USD** |

**A veszteség 70%-a eltűnt.** De továbbra is veszteség, és az `epic.md`
előre rögzített GO-feltétele (`találat − fedezeti > 0` mind a négy cellában)
**nem teljesül** — a legjobb cella −0,23 bps/kötés.

---

## 6. Verdikt

**RÉSZLEGES — nem GO, de nem is zárul le.**

A hipotézis (a rezsim a feladat része, nem szűrő) **igazolódott**: a
rezsim-szórás 2,5-szeresére csökkent, és mindkét irány javult. A rendszer
mégsem lett nyereséges, mert a maradék hiány már nem rezsim-eredetű, hanem
egyszerűen **kevés a jel**: a modell ~+9pp liftet ad, ahol ~+15pp kellene.

### Ami ebből következik

- **A címke marad.** A `tb_label_240_vol` innentől az alapértelmezett — jobb
  eredményt ad, és rezsim-függetlenebb. A fix ±100 bps változat legacy.
- **Ami hátravan ezen az ágon:** a long/short osztályok egymáshoz kalibrálása
  (t813) és az `aggTrades` order flow (`epic_061`). Egyik sem ígér +6pp-t.
- ⚠️ **A 4b. pont miatt óvatosan:** ha a lift nagy része volatilitás-
  előrejelzés volt, és a vol-skálázás ezt kiveszi, akkor a maradék valódi
  irány-jel **strukturálisan kicsi** ezen a horizonton. Ezt az `epic_062`
  (cross-sectional) kerüli meg: ott nem abszolút irányt kell eltalálni,
  hanem relatív sorrendet — és a modell dokumentáltan azt tudja.

---

## Kód a git historyban

A kísérleti kód a repóból törlődik (epic_068, t894/t895). Törlés előtti állapot: commit `73f2156`, útvonalak: `src/modeling/experiments/{triple_barrier_eval,walk_forward_tb}.py`.
