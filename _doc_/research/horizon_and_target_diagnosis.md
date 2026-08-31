# Horizont- és target-diagnózis — miért nem keresett pénzt a rendszer

> **A végkövetkeztetés:** a modell nem irányt jósol, hanem **volatilitást**.
> A hiba nem a feature-ökben, nem a kalibrációban és csak részben a díjban van
> — a **target teszi fel a rossz kérdést**.
>
> Dátum: 2026-08-25 · Eszköz: SOLUSDT · Irány: long

---

## 1. A vizsgálati lánc

Négy egymásra épülő kérdést jártunk végig. Mindegyik pozitív részeredményt
adott, és mindegyik elbukott a következő, szigorúbb teszten.

| # | Kérdés | Eredmény | A szigorúbb teszten |
|---|---|---|---|
| 1 | Ad-e többletet 22 Binance derivatíva-feature? | +34% él/kötés | gördülő teszten megfordult |
| 2 | Mennyit torzít a hiányzó költség-modell? | **döntő** — a keresés a tőkét felszámoló setupot választotta | megmaradt |
| 3 | Jobb-e a 4 órás horizont az 1 órásnál? | +148 USD vs +23 (3 hó OOS) | 12 hónapon **−14,15%** |
| 4 | Miért? Stabil-e a kalibráció? | **igen, stabil** | — a hiba máshol van |

---

## 2. A költség-modell (epic_055) — ez maradandó eredmény

A `src/strategy/` sehol nem számolt díjjal. Következmény:

```
A kezelt modell "gyoztes" beallitasa (brutto objective):
  1984 kotes
  brutto nyereseg                    +649 USD
  dij (1984 × 0,80 USD @ 8 bps)     −1587 USD
  ──────────────────────────────────────────
  netto                              −938 USD   (1000 USD notionalon)
```

A nettó célfüggvény bevezetése után a rácsban **a kontrollnál mind a 200, a
kezelésnél 197 setup helyezése megváltozott.** A régi rangsor semmit nem
mondott a valós sorrendről.

Mért díjszint (Binance VIP0, nincs érvényes BNB-kedvezmény): maker 2,0 bps,
taker 5,0 bps, csúszás ~1,5 bps → kör-költség 7–13 bps a kilépés típusától
függően.

**Funding:** −4,87 USD / 1000 notional / 6 hónap, azaz **bevétel** (2026-ban a
funding negatív, a long kapja). Elhanyagolható a díjhoz képest.

---

## 3. A horizont-hipotézis (epic_057) — helyes volt, mégis megbukott

Az érvelés jó: a díj kötésenként fix, a mozgás a horizonttal nő.

| Horizont | Átlagos MFE | A 10 bps díj hányszorosa |
|---|---|---|
| 60 perc | 49,5 bps | 5,0× |
| 4 óra | 100,3 bps | 10,0× |
| 24 óra | 252,7 bps | 25,3× |

És a 4 órás modell tényleg jobb lett: él **11,5 → 27,5 bps/kötés**, kötésszám
401 → 140, díj a bruttó 96%-áról 36%-ára.

**De 12 hónapos out-of-sample ablakon a győztes setup −14,15%-ot hozott.**
A 3 hónapos +148 USD nem volt reprezentatív.

---

## 4. A diagnózis — hol vész el a pénz

### 4a. A kalibráció NEM hibás

Az A ablakon illesztett score-küszöbök B ablakon változatlanul működnek:

| Küszöb | A (2025-08..2026-01) | B (2026-02..2026-07) |
|---|---|---|
| >p50 | 127,0 | 126,9 |
| >p90 | 165,2 | 164,1 |
| >p99 | 211,7 | **251,2** |
| összes | 102,0 | 90,7 |

A top 1% out-of-sample **2,77× liftet** ad. A modell rangsorol.

### 4b. Nulla díjjal is veszteséges a setupok többsége

Éves ablak, 200 setup:

| Díjszint | Nyereséges setup |
|---|---|
| **0 bps** | **59 / 200** |
| 10 bps (valós) | 4 / 200 |

A bruttó él kötésenként: **medián −0,53 bps**, p90 +1,01 bps.
Vagyis a tipikus beállítás díjak nélkül sem keres pénzt.

### 4c. A tényleges ok: a modell volatilitást jósol

B ablak, out-of-sample, óránkénti minta:

| Csoport | n | max FEL | max LE | **4h záró** | záró>0 |
|---|---|---|---|---|---|
| mind | 4320 | 90,7 | −98,6 | −3,3 | 48,3% |
| >p90 | 432 | 160,8 | −188,0 | **−3,9** | 48,8% |
| >p98 | 87 | 229,8 | −279,0 | +14,9 | 58,6% |
| >p99 | 44 | 258,8 | **−350,6** | +20,7 | 63,6% |

**A top 1%-ban a lefelé kilengés (−350,6) NAGYOBB, mint a felfelé (+258,8).**

A `long_mfe_fw240` target csak a felfelé kilengést méri. A modell pontosan ezt
tanulta meg — és mivel a nagy felfelé kilengés együtt jár nagy lefelé
kilengéssel, a modell **volatilitás-előrejelző** lett.

Ezt a feature-importance végig mutatta: a top 10 mind volatilitás
(`gk_vol_*`, `natr_14`, `hist_vol_20`, `returns_std_14`).

Amikor TP-t teszünk a jósolt MFE-re, valójában arra fogadunk, hogy a felfelé
mozgás **előbb jön**, mint a lefelé. Erről a modell semmit nem mond.

---

## 5. Sizing — amit a tőkeáttételről megtudtunk

A 140 kötés kamatos szimulációja (fw240, in-sample):

| Tőkeáttétel | Végső tőke | Max visszaesés |
|---|---|---|
| 2x | 1,56× | −42,9% |
| **5x** | **2,31×** | −77,4% |
| 10x | 1,92× | −96,4% |
| 15x | 0,44× | −99,6% |
| 20x | **0,02×** | −100% |

**Az 5x fölött több tőkeáttétel kevesebb végső tőkét ad** (volatilitás-húzás),
15x fölött veszteséget, 20x-en gyakorlatilag teljes elvesztést.

A mechanizmus: a leghosszabb vesztes sorozat **4 egymás utáni SL**. Az SL
tökéletesen működött (−2,50% kötésenként), de 20x-en ez −95%. 72%-os nyerő
aránynál a négyes sorozat 140 kötésen **várható**, nem szélsőség.

⚠️ Az SL egyetlen kötés veszteségét korlátozza, **nem a sorozatét**.

---

## 6. Következtetés és irány

**Ami megmarad és értékes:**
- Költség-modell a strategy rácskeresésben (epic_055)
- Alt-data infrastruktúra: 496k soros `altdata_5m`, 22 feature, szigorú leak-tesztek
- A validációs fegyelem: in-sample szelekció ≠ eredmény
- Likvidációs collector (a t302 fut)

**Ami lezárul:**
- A folytonos MFE-regresszió mint kereskedési alap. Nem azért, mert a modell
  rossz — hanem mert a target rossz kérdést tesz fel.

**A folytatás:** `epic_058` — triple-barrier címke (+1 / −1 / 0), fix ±100 bps
korláttal, 240 perces függőleges korláttal. Ez azt kérdezi, amit a kereskedés:
**melyik korlátot éri el előbb az ár.**
