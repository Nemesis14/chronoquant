# Triple-barrier gate — az irány-előrejelzés lezárása

> **Verdikt: NO-GO.** A `t705`-ben előre rögzített szabály szerint (a felső sáv
> találati aránya > 55% tartósan) egyetlen konfiguráció sem teljesít mindkét
> piaci rezsimben. Az irány-előrejelzési hipotézis lezárul.
>
> Dátum: 2026-08-26 · SOLUSDT · 4 órás horizont · ±100 bps korlát

---

## 1. A címke

`tb_label_240_100`: melyik korlátot éri el előbb az ár a belépéstől számítva.

```
+1  felso (+100 bps) eloszor       -1  also (-100 bps) eloszor       0  egyik sem 240 perc alatt
```

Intrabar érintés dönt (`high` / `low`), same-bar ütközésnél az alsó nyer —
ugyanaz a konvenció, mint a strategy szimulátorban.

**Ellenőrzött eloszlás** (2025-08 → 2026-07, óránkénti minta):
32,9% / 36,0% / 31,0% — pontosan a t701-ben előre rögzített horgony.

⚠️ Az eloszlás **erősen rezsimfüggő**: 2021-ben csak 1,6% volt a timeout,
2026-ban 39,3%. Fix bázispontos korlát mellett a címke jelentése korszakonként
változik.

---

## 2. A mérés fegyelme

- Óránkénti minta, seed 42
- 240 perces feature-embargó a train elején, 240 perces target purge a végén
- Belső early stopping a train utolsó 15%-án (438 fa lett a 3000-es korlátból)
- **A küszöb a trainen rögzítve, változatlanul alkalmazva a validon**

Az utolsó pont a lényeg: ma háromszor dőlt meg egy pozitív eredmény azon, hogy
a küszöböt a mérési ablakon választottuk ki.

---

## 3. Eredmény — két ellentétes rezsimben

| Valid ablak | SOL | LONG p99 | SHORT p95 | SHORT p98 |
|---|---|---|---|---|
| 2025-08 → 2026-07 | **−56,7%** | 48,1% | **59,2%** ✅ | **74,1%** ✅ |
| 2024-07 → 2025-07 | **+21,2%** | **61,8%** ✅ | 50,9% | 53,6% |

**A nyerő irány a trenddel fordul.** Eső piacon a short, emelkedőn a long lépi
át az 55%-ot.

### A lift viszont valós, és mindkét rezsimben hasonló

| | Találat | Alap | Lift |
|---|---|---|---|
| Eső, SHORT p95 | 59,2% | 36,0% | **+23,2pp** |
| Emelkedő, LONG p99 | 61,8% | 41,7% | **+20,1pp** |

Az emelkedő ablakban a két alap-arány gyakorlatilag azonos (41,7% vs 41,9%),
tehát a trend nem látszik az alap-eloszlásban — a modell mégis eltalálja az
irányt. Nem egyszerű bázisráta-másolás.

---

## 4. Két elvetett megoldási kísérlet

### 4a. Trend-szűrő — ROSSZABB lett

7 napos fix trend-szabály (nem hangolt), csak a trenddel egyező irány:

| | Csak trend | Trend + modell p95 |
|---|---|---|
| Eső piac | 34,7% | 44,8% (+10,0pp) |
| Emelkedő | 42,0% | 50,4% (+8,4pp) |

A modell konzisztensen hozzáad +8–12pp-t, **de a 7 napos trend olyan rossz
kiindulás** (eső piacon 34,7%), hogy a javítás sem elég.

És a döntő: eső piacon a **csak-modell SHORT p95 59,2%-ot** hozott, a
trend-szűrős változat 44,8%-ot. **A szűrő elvette azt, amit a modell tudott.**

### 4b. A modell válassza az irányt — NEM MŰKÖDIK

| Ablak | p95 | p98 | p99 |
|---|---|---|---|
| Eső | 47,3% | 50,7% | 51,3% |
| Emelkedő | 51,0% | 54,2% | 63,5% |

Ok: **a P(+1) szisztematikusan magasabb, mint a P(−1)** — a modellnek beépített
long-elfogultsága van. Amikor a két irány verseng, a long szinte mindig nyer
(eső piacon is 108 long jel 5 short mellett a p99-en), és elnyomja a jó short
jeleket. A két osztály valószínűsége **nincs egymáshoz kalibrálva**.

---

## 5. Verdikt és ami megmarad

**A `t705` szabálya szerint: NO-GO.** Az irány-előrejelzés a jelenlegi
feature-készlettel és 4 órás horizonton nem oldható meg rezsim-függetlenül.

### Ami viszont bebizonyosodott, és értékes

- **A modellnek van valós lift-je**: ~20pp a felső sávban, mindkét rezsimben.
  Ez nem zaj. Csak nem elég ahhoz, hogy díjak után megbízhatóan fedezzen.
- **A triple-barrier címke helyes eszköz** — a diagnózis vele lett tiszta.
  Az MFE-regresszió volatilitást tanított; ez legalább a valódi kérdést kérdezi.
- **A rezsim a rendszer hiányzó dimenziója.** Ma háromszor futottunk bele
  ugyanabba: a júniusi hónap (0/200 nyereséges setup), az ablakonként változó
  optimum, és most a trenddel forduló irány.

### Ami nem oldható meg ezen az úton

A rezsim-felismerés nem szűrőként pótolható — ezt a 4a. szakasz megmutatta.
Ha valaha folytatódik, akkor a rezsim **magának a modellnek a bemenete** kell
legyen, nem utólagos kapu. De az már más rendszer, nem ennek a finomítása.

---

## 6. Kiegészítés — a tisztességes teszt (2026-08-26)

Az 5. szakasz NO-GO verdiktje **egyetlen, kézzel választott hiperparaméter-
készleten** állt, miközben a regressziós út 100 Optuna trialt kapott. A user
jogos kifogása volt továbbá, hogy a mérés **túl szigorú**: egy évig eleresztett
modellt mértünk, miközben élesben havonta újratanítana.

Mindkettőt orvosoltuk.

### A javított protokoll

| | |
|---|---|
| Címke | `tb_label_1440_100` — ±100 bps, **24 órás** függőleges korlát |
| Osztályeloszlás | 48,5% / 51,1% / **0,4%** — a timeout eltűnt, a feladat bináris |
| Hiperparaméter-keresés | **100 Optuna trial**, egyszer, 2023-01 → 2024-06-on, belső 80/20 hasítással |
| Újratanítás | **havonta**, minden addigi adaton (25 illesztés) |
| Kalibráció | havonta, az adott illesztés train-predikcióiból (p95) |
| Mérés | 2024-07 → 2026-07, 25 hónap folyamatos OOS |
| Költség | belépés 6,5 bps · TP kilépés 2,0 (maker) · SL kilépés 6,5 · funding 3 elszámolás/24h |

Fedezeti találati arány: `113,0 / (91,5 + 113,0)` = **55,3%**.

A 24 órás korlát a feladatot **könnyebbé** tette: az alap-arány 32,6%-ról
~48,5%-ra nőtt, tehát +22,7pp helyett csak **+6,8pp** lift kellett volna.

### Eredmény

| Rezsim | Irány | n | Találat | Alap | Lift | bps/kötés |
|---|---|---|---|---|---|---|
| Emelkedő (+21%) | LONG | 575 | 53,2% | 48,5% | +4,7pp | −4,48 |
| Emelkedő (+21%) | SHORT | 323 | 49,2% | 51,1% | −1,9pp | −10,79 |
| Eső (−57%) | LONG | 482 | 47,9% | 48,5% | −0,6pp | −12,55 |
| Eső (−57%) | SHORT | 232 | 42,7% | 51,1% | **−8,4pp** | −24,42 |

**Együtt: 1612 kötés, −11,03 bps/kötés, −178 USD / 1000 notional.**

### Következtetés

- **Egyetlen cella sem éri el az 55,3%-ot.** A legjobb (emelkedő + long) 53,2%.
- **A short oldal a véletlennél is rosszabb**: eső piacon 42,7% az 51,1%-os
  alappal szemben, 232 kötésen. Nem mintavételi zaj.
- **A havi újratanítás nem változtatott az előjelen.** A realisztikusabb mérés
  ugyanazt adta.

### Miért tűnt jónak a 4 órás verzió

A 4 órás címkénél az alap-arány 32,6% volt, és ott +20pp liftet mértünk.
Kiegyensúlyozott osztályoknál ez eltűnik. A magyarázat: 4 órán a modell főként
azt tanulta meg, **mikor van egyáltalán nagy mozgás** — nem azt, hogy merre.
Ez visszamenőleg megerősíti a fő diagnózist: **a modell volatilitást tud
előrejelezni, irányt nem.**

**Az irány-előrejelzési hipotézis ezzel lezárul** — most már azonos mércével
mérve, mint a regressziós út.

---

## 7. A végleges mérés — 4 órás címke, gördülő üzemeltetés (2026-08-26)

A 6. szakasz 24 órás korláttal mért, ami **más feladattá tette a problémát**:
ott az alap-arány ~48,5%, tehát tisztán az irányt kell eltalálni. A 4 órás
címkénél az alap 37,7%, ahol a "mikor van egyáltalán nagy mozgás" kérdés is
benne van. A user kérésére visszatértünk a 4 órásra, és hozzávettük a napi
kalibrációt.

### Protokoll

| | |
|---|---|
| Címke | `tb_label_240_100` — ±100 bps, 4 órás korlát, 3 osztály |
| Keresés | 100 Optuna trial, egyszer, 2023-01 → 2024-06-on, belső 80/20 |
| Újratanítás | **havonta** (25 illesztés) |
| Kalibráció | **naponta**, gördülő 30 napos ablakból, a modell saját score-jaiból |
| Mérés | 2024-07 → 2026-07, 25 hónap folyamatos OOS |
| Költség | belépés 6,5 · TP kilépés 2,0 (maker) · SL/timeout 6,5 · funding 0,5 elszámolás |

### Eredmény

| Rezsim | Irány | n | Találat | Alap | Lift | bps/kötés |
|---|---|---|---|---|---|---|
| Emelkedő (+21%) | LONG | 567 | 50,6% | 37,7% | **+12,9pp** | −4,84 |
| Emelkedő (+21%) | SHORT | 501 | 48,1% | 39,1% | **+9,0pp** | −11,65 |
| Eső (−57%) | LONG | 484 | 42,8% | 37,7% | **+5,1pp** | −18,00 |
| Eső (−57%) | SHORT | 434 | 49,3% | 39,1% | **+10,2pp** | −1,86 |

**Együtt: 1986 kötés, −9,12 bps/kötés, −181 USD / 1000 notional.**

Összesített lift: LONG **+9,3pp**, SHORT **+9,6pp**.

### A kulcsszám: félúton vagyunk

```
alap-arány        37,7% / 39,1%
fedezeti pont     55,3%
szukseges lift   +17,6pp / +16,2pp
a modell hozza    +9,3pp /  +9,6pp     ->  a szukseges ~55%-a
```

**Mind a négy rezsim×irány cellában pozitív a lift.** Ez valós, reprodukálható
előrejelző erő — nem szelekciós zaj. De a költségstruktúra kétszer ennyit kér.

### Miért 55,3% a fedezeti pont

1. **Díj-aszimmetria:** nyerő +91,5 bps (TP maker), vesztes −113 bps (SL taker).
2. **22% timeout:** nagyjából nullát hoznak, de fizeted rájuk a 13 bps-t.

Más korlát-geometria nem segít: TP 150 / SL 100 mellett a fedezeti pont 44,4%-ra
esne, de az alap-arány is ~25%-ra — a szükséges lift **19pp** lenne, rosszabb.
A szimmetrikus 100/100 közel optimális.

### A két formuláció szembeállítva

| | Lift | Szükséges | bps/kötés |
|---|---|---|---|
| 24 órás korlát | ~0pp | 6,8pp | −11,03 |
| 4 órás korlát | **+9,5pp** | 17,6pp | −9,12 |

A 24 órásnál **kevés kell, de nincs lift**; a 4 órásnál **van lift, de sok kell**.
Nem a paraméterezés a probléma: a jel ott van, ahol a mozgás nagy és ritka,
de éppen ott drága a belépés is.

### Verdikt

**Nem nyereséges — de nem is reménytelen.** A modell a szükséges él felét hozza,
konzisztensen. A hiányzó ~8 százalékpont nem paraméterezéssel szerezhető meg
(ma sokszor próbáltuk), hanem:

- **jobb feature-ök** — a `bookDepth` orderbook-mélység 2023-tól megvan és
  soha nem használtuk; ez a legkézenfekvőbb kiaknázatlan adat
- **alacsonyabb belépési költség** — ha a belépés is maker lenne (limit order
  a jel utáni percben), a fedezeti pont 55,3% -> **51,8%**-ra esne, a szükséges
  lift 17,6 -> **14,1pp**-re. A szakadék harmada eltűnne.

---

## 8. Helyesbítés és utólagos mérés (2026-08-26, epic_059 / epic_061)

A §7 két állítását le kellett mérni. Az egyik téves volt, a másik igaz.

### 8a. ❌ „a `bookDepth` megvan és soha nem használtuk" — TÉVES

Az 5 mélység-feature (`feat_depth_imb_1/5`, `feat_depth_slope`,
`feat_depth_total_z_240`, `feat_depth_drain_60`) az `epic_058` alatt bekerült
a `quant_train`-be. **A §7-ben riportált +9,3pp lift már tartalmazza őket.**

Az ablációs mérés (`--drop-features feat_depth_`, azonos paraméterek):

| | LONG találat | SHORT találat | bps/kötés |
|---|---|---|---|
| bookDepth-tel | 47,6% | 47,1% | **−5,26** |
| nélküle | 45,2% | 49,1% | −6,07 |

Nettó +0,81 bps/kötés — de a két irány **ellentétes előjellel** mozdul
(LONG +2,4pp, SHORT −2,0pp). Ez átrendeződés, nem informácio-többlet.
Részletek: `_jira_/epic_061_microstructure_flow/pr_t821_bookdepth_ablation.md`

### 8b. ✅ „maker belépéssel a fedezeti pont 51,8%-ra esne" — IGAZ, sőt mérve

A `--maker-entry` ág implementálva volt és sosem futott le. Lefuttatva,
őszinte fill-modellel (a limit csak akkor telik, ha a következő perc
átüti — tehát a long belépés adverse-selected):

| | fedezeti pont | bps/kötés |
|---|---|---|
| Taker (a §7 száma) | 55,3% | −9,38 |
| **Legjobb végrehajtás** | **52,8%** | **−4,72** |

Fill rate **98,2%**, adverse selection **−0,35pp**. A §7 „félúton vagyunk"
számai ezzel:

```
alap-arany        37,7%
fedezeti pont     52,8%      (55,3% helyett)
szukseges lift   +15,1pp     (+17,6pp helyett)
a modell hozza    +9,5pp
hianyzo           -5,6pp     (-8,1pp helyett)
```

Teljes riport: [execution_cost_gate.md](execution_cost_gate.md)

### 8c. A rezsim-cella, ami átbillent

Legjobb végrehajtás mellett az **emelkedő rezsim long oldala pozitív lett**
(+2,43 bps/kötés, 568 kötés). A másik három cella továbbra is negatív. A
mintázat változatlan: **a nyerő irány a trenddel fordul** — ezt kezeli az
`epic_060` (a rezsim a címke része) és az `epic_062` (a rezsim kiejtése).

---

## Kód a git historyban

A kísérleti kód a repóból törlődik (epic_068, t894/t895). Törlés előtti állapot: commit `73f2156`, útvonalak: `src/modeling/experiments/triple_barrier_eval.py`.
