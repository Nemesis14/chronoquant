# Végrehajtási költség — a fedezeti pont leszállítása (epic_059)

> **Verdikt: GO.** A maker belépés valós, mérve, nem feltételezve: a limit
> **98,2%-ban telik**, és az így kiszűrt belépések **nem rosszabbak**
> (adverse selection −0,35pp). A rendszer vesztesége **−9,38 → −4,72
> bps/kötés** — a végrehajtás önmagában a **fele**, modell-változtatás nélkül.
>
> Dátum: 2026-08-26 · SOLUSDT · `tb_label_240_100` · 25 hónap folyamatos OOS

---

## 1. A kérdés

A `triple_barrier_gate.md` §7 azzal zárult, hogy a modell a szükséges él
~55%-át hozza, és a rés két úton zárható: több lift, vagy alacsonyabb
fedezeti pont. A doksi megbecsülte, hogy maker belépéssel 55,3% → 51,8%
lenne — **de ez számítás volt, nem mérés**.

A `walk_forward_tb.py`-ban a `--maker-entry` ág implementálva volt, és
**soha nem futott le.**

---

## 2. A mérés fegyelme

A hiperparaméter-keresés **egyszer** futott (100 Optuna trial,
2023-01 → 2024-06, belső 80/20), az eredmény cache-elve. **Minden variáns
pontosan ugyanazt a modellt kapja** — különben a költség hatását nem lehetne
elválasztani a keresés zajától.

Protokoll változatlan: havi újratanítás (25 illesztés), napi p95 kalibráció
gördülő 30 napos ablakból, mérés 2024-07 → 2026-07.

### A fill-modell — ez a lényegi rész

```
long limit a jel zaroaran  ->  akkor telik, ha a KOVETKEZO perc low <= jel_close
short limit                ->  akkor telik, ha a kovetkezo perc high >= jel_close
```

Ez **nem kedvezmény, hanem szűrő**: egy long csak akkor lép be, ha az ár
előbb lefelé tickel. A túlélő belépések tehát szisztematikusan
adverse-selected. Épp ezt kellett megmérni.

---

## 3. Eredmény — a költséglétra

| Variáns | fedezeti pont | n | találat | fill | **bps/kötés** | netto / 1000 USD |
|---|---|---|---|---|---|---|
| Taker belépés (market) | 55,3% | 2018 | 47,7% | 100% | **−9,38** | −189 USD |
| + maker belépés (limit) | 53,1% | 1981 | 47,4% | 98,2% | **−5,54** | −110 USD |
| + maker timeout-kilépés | 53,1% | 1981 | 47,4% | 98,2% | **−5,26** | −104 USD |
| **+ BNB-kedvezmény (−10%)** | **52,8%** | 1981 | 47,4% | 98,2% | **−4,72** | **−94 USD** |

**A veszteség 50%-a eltűnt** — 1981 ténylegesen szimulált kötésen, nulla
modell-kockázattal. A pozitív hónapok száma 16/50 → 22/50.

### A két előre rögzített kockázat

| | Küszöb | Mért |
|---|---|---|
| Fill rate | > 60% | **98,2%** |
| Találat romlása (adverse selection) | < 1,5pp | **−0,35pp** |

LONG: 47,6% → 47,6% (**változatlan**). SHORT: 47,8% → 47,1%.

A pihenő limit a jel záróárán a következő percben szinte mindig teljesül, és
a túlélő belépések nem rosszabbak. Az adverse selection ezen a horizonton és
ezzel a jel-típussal **nem számottevő**.

---

## 4. ⚠️ A stop loss market order maradt

Nem spóroltunk rajta, tudatosan. Egy nem teljesülő stop-limit nyitva hagyja a
pozíciót pontosan abban a mozgásban, ami elől menekülne. A 4,5 bps nem ér
annyit, mint egy korlátlan farok.

A timeout kilépés viszont **nem sürgős** — lejárt az óra, semmi nem szalad —
ezért az pihenhet limitként. Ez hozta a −5,54 → −5,26-ot.

---

## 5. A rezsim-bontás — és itt jön a fontos rész

Legjobb végrehajtás mellett, rezsim × irány:

| Rezsim | Irány | n | találat | fedezeti | **bps/kötés** |
|---|---|---|---|---|---|
| **Emelkedő (+21%)** | **LONG** | 568 | 51,2% | 52,8% | **+2,43** ✅ |
| Emelkedő (+21%) | SHORT | 510 | 48,8% | 52,8% | −4,12 |
| Eső (−57%) | LONG | 478 | 43,3% | 52,8% | −12,51 |
| Eső (−57%) | SHORT | 425 | 45,2% | 52,8% | −6,23 |

**Az emelkedő rezsim long oldala megfordult pozitívba.** A találati arány
(51,2%) még a fedezeti pont alatt van — a pluszt a **timeout-kötések** hozzák,
amiket a fedezeti képlet nullának vesz, de emelkedő piacon ténylegesen
pozitívak.

Ez pontosan ugyanaz a mintázat, amit a `triple_barrier_gate.md` háromszor
megtalált: **a nyerő irány a trenddel fordul.** Most viszont már valós
végrehajtási költséggel, és az egyik cella átbillent.

---

## 6. Következtetés

| | Előtte | Utána |
|---|---|---|
| Fedezeti pont | 55,3% | **52,8%** |
| Szükséges lift (alap 37,7%) | +17,6pp | **+15,1pp** |
| A modell hozza | +9,5pp | +9,5pp |
| **Hiányzó** | **8,1pp** | **5,6pp** |
| bps/kötés | −9,38 | **−4,72** |

A rendszer **továbbra is veszteséges**. De a hiányzó rés a bps-oldalon
megfeleződött, és a négy rezsim×irány cellából **az egyik átbillent
pozitívba**.

A maradék rés nem szerezhető meg további költség-optimalizálással — a
fedezeti pont 52,8%-nál gyakorlatilag padlón van (a stop taker marad, a
csúszást nem lehet kedvezményezni). Innentől a **lift oldala** a kérdés:

- `epic_060` — a rezsim mint a címke része (ATR/vol-skálázott korlát)
- `epic_061` — mikrostruktúra: `bookDepth` abláció + `aggTrades` order flow
- `epic_062` — cross-sectional: a rezsim kiejtése béta-semlegesítéssel

---

## 7. Amit át kell vezetni a rendszerbe

⚠️ **`config/strategies.json` → `costs` még nem tükrözi ezt.** A
`src/strategy/` költség-modellje (epic_055) külön modul, és ma taker
belépéssel számol. A `src/trading/live/` belépési logikája szintén market
order. Mindkettőt maker-alapúra kell állítani — de **csak akkor**, ha a
lift-oldali epic-ek valamelyike átbillenti a rendszert pozitívba. Addig ez
egy mérési eredmény, nem üzemeltetési változás.

---

## Kód a git historyban

A kísérleti kód a repóból törlődik (epic_068, t894/t895). Törlés előtti állapot: commit `73f2156`, útvonalak: `src/modeling/experiments/{triple_barrier_eval,walk_forward_tb}.py`.
