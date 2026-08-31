# Cross-sectional rangsor — az első pozitív eredmény (epic_062)

> **Verdikt: GO a további kutatásra — NEM kereskedhető rendszer.**
>
> 30 alt-perp, piac-semleges könyv, 72 órás tartás, valós díjjal és **mért
> fundinggal**: **+22,5 bps / páronként**, 16/22 nyereséges hónap, mindkét
> rezsimben pozitív. Ez az első nettó pozitív, rezsim-robusztus eredmény a
> projektben.
>
> ⚠️ **De tőkére vetítve: +11,2% évesítve, 22,0% volatilitás mellett —
> Sharpe 0,51, max drawdown −24,7%, és t-statisztika ≈ 0,7.**
> **Ez nem szignifikáns.** Az eredmény összefér azzal is, hogy a valódi él
> nulla, és 22 hónapon szerencsénk volt. Részletek a 6a. szakaszban.
>
> Dátum: 2026-08-26 · 30 USDT-perp · 2024-10 → 2026-07 (22 hónap OOS)

---

## 1. Miért ez a kérdés

Három lezárt mérés mondta ugyanazt:

- A modell **jól rangsorol** — 2,77× lift a saját top 1%-án, out-of-sample,
  ablakok között stabil kalibrációval
- A modell **bukik az abszolút irányon** — *„a nyerő irány a trenddel
  fordul"* ([triple_barrier_gate.md](triple_barrier_gate.md))

Egy cross-sectional könyv (top decilis long / alsó decilis short) **csak a
rangsorolást használja**, és kiejti a bétát — vagyis pontosan azt a
dimenziót, ami háromszor megölte a direkciós ágat.

---

## 2. Az univerzum — survivorship bias nélkül

A „ma legnagyobb 30 perp" lista visszamenőleg torzít, és épp a legmozgékonyabb
párokat ejti ki. Ezért:

1. A Binance Vision S3 archívum **teljes enumerációja** — 986 szimbólum, a
   delistelteket is beleértve (nem `exchangeInfo`, ami csak az élőket adja)
2. Rangsor a **2024-01-i** forgalom szerint, napi barokból (829 USDT-perp,
   ebből 261 létezett akkor)
3. A top 30 órai bara, 2024-01 → 2026-07

Az eredmény ellenőrizhetően torzításmentes: a **MATICUSDT benne van**, és az
adata 2024-09-11-én megszakad — pontosan ahogy a valóságban is.

⚠️ **Órai bar, nem 1 perces.** A mérés 4/24/72 órás záró-záró hozamot néz,
amit az órai bar pontosan ad, ~1000-szer kisebb letöltésből. 1 perces adat
csak a modell-ágnak kell.

---

## 3. Lépcső 1 — van-e mit elosztani?

Óránkénti felső-alsó decilis spread a 4 órás előre hozamban:

| | median spread | pár-költség | fedezet |
|---|---|---|---|
| Teljes ablak | **335,9 bps** | 18,8 bps | 17,9× |
| Likviditási szűrővel | 328,7 bps | 18,8 bps | 17,5× |
| Eső rezsim | 279,5 bps | 18,8 bps | 14,9× |

Előre rögzített küszöb: 80 bps. **Teljesül, 4,2-szeresen.**
Átlagos páronkénti korreláció: **0,650**.

⚠️ De ez a **tökéletes előrelátás** felső korlátja — a decilisek a kimenet
szerint vannak rendezve.

---

## 4. Lépcső 2 — mennyi ebből elérhető?

Ugyanaz a könyv, nulla-intelligenciájú rangsorolókkal:

| jel | bruttó | nettó (−18,8) | pozitív óra |
|---|---|---|---|
| **reverzál 72h** | **+4,1** | −14,7 | 53,1% |
| reverzál 1h | +3,4 | −15,4 | 53,0% |
| momentum 72h | −4,1 | −22,9 | 46,9% |

**A legjobb naiv jel a 336 bps-ból 4,1-et fog meg — 1,2%-ot.** Minden naiv
változat veszít a költséggel szemben.

Két dolog látszik: a **reverzál az előjel** (rövid horizonton a kripto
keresztmetszet átlaghoz tér vissza, minden lookbacken konzisztensen), és
**átlag < median mindenhol** — balra ferde eloszlás, gyakran nyer keveset,
ritkán veszít sokat.

---

## 5. Lépcső 3 — a tanított rangsoroló

26 feature az órai panelből (hozamok 1–168h, realizált volatilitás,
range-pozíció, forgalom z-score) **plusz mindegyik keresztmetszeti rangja** —
utóbbi a lényeg: egy adott órában minden szimbólumra azonos érték nulla
keresztmetszeti információt hordoz.

Target: a 4/72 órás előre hozam **szimbólumok közti átlagtól megtisztítva** —
a piaci mozgás pont az, amit egy semleges könyv nem keres meg, tehát nem
hagyjuk, hogy a modell újra bétát tanuljon.

Havi újratanítás, LightGBM, **hiperparaméter-keresés nélkül** (kézzel
beállított, konzervatív paraméterek).

### 5a. A tartási idő a döntő változó

A díj páronként fix, a befogott mozgás a tartással nő:

| tartás | bruttó | funding | **nettó** | emelkedő | eső |
|---|---|---|---|---|---|
| 4h | +4,55 | — | −14,25 | | |
| 12h | +13,26 | — | −5,54 | −5,39 | −5,67 |
| 24h | +22,63 | −1,43 | **+2,41** | +13,65 | −6,98 |
| 48h | +29,76 | −2,43 | **+8,54** | +1,15 | +14,72 |
| **72h** | **+35,14** | **−3,72** | **+12,62** | **+10,97** | **+14,00** |

**48 órától mindkét rezsim pozitív.**

### 5b. A funding kicsi — mérve, nem feltételezve

Letöltve mind a 30 párra (102 729 elszámolás). A két láb nagyrészt kioltja
egymást: **−1,4 … −3,7 bps** a teljes tartásra. A modell nem áll
szisztematikusan a drága funding oldalára.

### 5c. Likviditási szűrő — JAVÍT

A költség SOLUSDT-re van kalibrálva (9,4 bps/láb). Vékony alton ez optimista,
ezért lemértük csak a legforgalmasabb 20 páron:

| univerzum | bruttó | funding | **nettó** | emelkedő | eső |
|---|---|---|---|---|---|
| mind a 30 | +35,14 | −3,72 | +12,62 | +10,97 | +14,00 |
| **legforgalmasabb 20** | **+49,39** | −1,88 | **+28,71** | +16,53 | +38,94 |

**A likvid részhalmazon jobb, nem rosszabb.** Ez kizárja a legfőbb
aggodalmat: az él nem a kereskedhetetlen, vékony párokból jön.

### 5d. Koncentráció — a szélesebb decilis kiegyenlít

| decilis | pár/oldal | **nettó** | emelkedő | eső | pozitív hónap |
|---|---|---|---|---|---|
| 10% | 2 | **+28,71** | +16,53 | +38,94 | 15/22 |
| **20%** | **4** | **+22,49** | **+21,39** | **+23,41** | **16/22** |
| 30% | 6 | +21,40 | +22,80 | +20,23 | 15/22 |

Szélesebb decilisnél a teljes hozam kissé csökken, de a **rezsimek
kiegyenlítődnek** (+21,4 / +23,4) és több a nyereséges hónap. Ez egy valódi,
diverzifikálható hatás aláírása — nem néhány szerencsés koncentrált fogadásé.

### 5e. Költség-tűrés

A fedezeti lábankénti költség **20,6–23,8 bps**, azaz a feltételezett 9,4
bps **2,1–2,5-szerese**. A végrehajtásnak több mint kétszer ilyen rosszra
kellene romlania, hogy nullára jöjjön.

---

## 6a. ⚠️ A döntő szám: tőkére vetítve nem szignifikáns

A bps/pár jól hangzik, de a kérdés az, mit csinál a tőkével. Nem átfedő
könyvekkel (72 óránként egy új), tőkeáttétel nélkül, bruttó kitettségre:

| | |
|---|---|
| Teljes hozam (23 hónap) | +22,5% |
| **Évesítve** | **+11,2%** |
| Évesített volatilitás | 22,0% |
| **Sharpe** | **0,51** |
| **Max drawdown** | **−24,7%** |
| Legrosszabb hónap | **−20,2%** (2024-11) |

```
t = Sharpe x sqrt(ev) = 0,51 x sqrt(1,92) = 0,7
```

**A 0,7 nem szignifikáns** (a küszöb ~2). Ekkora Sharpe mellett ~15 év adat
kellene ahhoz, hogy kimondható legyen: az él nem nulla.

Két további nyugtalanító részlet:

- **A legrosszabb hónap a legelső** (2024-11, −20,2%), amikor a modellnek a
  legkevesebb tanítóadata volt. Élesben pontosan ott kezdenél.
- A −24,7% **tőkeáttétel nélkül** van. A `horizon_and_target_diagnosis.md`
  §5 szerint 5× fölött a tőkeáttétel már csökkenti a végtőkét; itt már 2× is
  −50% körüli visszaesést jelentene.

⚠️ **Éles tőke szóba sem jöhet.** Ez kutatási nyom, nem stratégia.

---

## 6. ⚠️ Amit NEM szabad elfelejteni

**1. A horizontot az eredmények ismeretében választottuk.** Hat értéket
futtattunk (4/12/24/48/72/120), a legjobb a leghosszabb. A trend monoton és
mechanikusan magyarázható (fix díj, növekvő mozgás) — de ez akkor is
utólagos szelekció hat érték felett. A 20%-os decilis és a top-20 szűrő
szintén utólagos, bár mindkettő a **konzervatív** irányba.

**2. Átfedő ablakok.** 15 984 órai könyv 72 órás tartással ≈ **222 független
ablak**, nem 15 984. A tényleges szignifikancia a **havi bontásból** (15–16 /
22 pozitív) olvasható ki, nem az óraszámból.

**3. A 120 órás ugrás szuperlineáris és megmagyarázatlan** (+35 → +79 bruttó).
A keresztmetszeti szórás ~√t szerint nőne; ez gyorsabb. **72 óra fölött nem
szabad hinni a számoknak**, amíg ez nincs megértve.

**4. Egyszerűsített szimuláció.** Záró-záró órai barok, nincs intrabar,
nincs TP/SL, nincs pozícióméretezés, nincs margin/likvidációs modell. Az
egyenlő súlyozás nem kockázat-kiegyenlített (a volatilis párok dominálják a
P&L-t).

**Amit ez viszont NEM tartalmaz — és felfelé mutat:** a mérés **26 feature-t**
használ az órai panelből. A meglévő 235-ös készlet (derivatíva-pozicionáltság,
orderbook mélység, prémium, bázis) **egyáltalán nem szerepel benne**.

---

## 7. Verdikt és következő lépés

Az `epic.md` előre rögzített küszöbe (median decilis-spread > 80 bps)
**teljesült, 4,2-szeresen**. A hozzátett rangsoroló-mérés szerint az él nem
csak elméleti: **+22 … +29 bps páronként, nettó, mért fundinggal, 22 hónap
folyamatos out-of-sample-ön, mindkét rezsimben.**

**De a 6a. szakasz miatt ez GO a kutatásra, nem GO a kereskedésre.** Sharpe
0,51 és t ≈ 0,7 mellett az eredmény nem különböztethető meg a nullától. A
helyes olvasat nem az, hogy „nyereséges rendszerünk van", hanem hogy
**van egy nyom, ami nem esett szét azonnal** — szemben a direkciós ággal,
ahol tudjuk, hogy veszteséges (−2,78 bps/kötés).

Összevetésül a direkciós ág legjobb konfigurációja ugyanezen a napon:
**−2,78 bps/kötés** ([atr_barrier_gate.md](atr_barrier_gate.md)).

### A következő epic — sorrendben

1. **Független megerősítés.** Más univerzum (31–60. legforgalmasabb),
   más selection-hónap, más seed. Ha ott is +20 bps körül van, valós.
2. **A 120 órás anomália megértése** — leak vagy valódi?
3. **1 perces backfill + a meglévő feature-készlet** a 30 párra. A mostani
   26 feature a minimum; a derivatíva-réteg itt még kipróbálatlan.
4. **Kockázat-kiegyenlített súlyozás** és a margin/likvidációs modell.

⚠️ Éles tőke egyik lépés előtt sem.
