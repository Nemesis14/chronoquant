# Alt-data gate — Binance derivatíva-változók (epic_054)

> **Verdikt: GO, fenntartásokkal.** A 22 Binance derivatíva-változó mérhető
> többletet ad a meglévő 208 feature-höz képest — a kereskedési eredményben
> markánsan, a modell-metrikákban alig. A döntő szám: azonos kereskedési
> gyakoriság mellett **+34% él kötésenként**.
>
> Riport dátuma: 2026-08-22

---

## 1. A kísérlet

Kontroll és kezelés, azonos mintán, azonos ablakon, **egyetlen különbséggel**.

| | Modell | Snapshot | Kandidáns feature |
|---|---|---|---|
| **C** | `lgbm_solusdt_l_fw60_ctrl_2301_2607` | `..._2301_2607__948cc228` | 208 |
| **T** | `lgbm_solusdt_l_fw60_alt_2301_2607` | `..._2301_2607__de542d6b` | 230 (208 + 22) |

- Train: 2023-01-01 → 2026-01-31 · Valid: 2026-02-01 → 2026-07-31
- Minta: 31 387 sor (train 27 043 / valid 4 344), seed 42, embargó 240p, purge 60p
- Irány: **long only**
- Keresés: 100 Optuna trial mindkettőn

A train 2023-tól indul, mert a `metrics` archívum top trader oszlopai csak
2023-01-tól teljesek. A kontroll pontosan ezt a rövidebb ablakot semlegesíti.

---

## 2. Feature engineering — 20 a 22-ből túlélt

| | Kiválasztott | Ebből alt |
|---|---|---|
| Kontroll | 137 | 0 |
| Kezelés | **156** | **20** |

Kiesett: `feat_ls_count_z_240`, `feat_ls_top_conc_60`.
Küszöbök: MI > 0,001, korreláció < 0,98.

**A 20 alt-változó nem szorított ki meglévő feature-t** — a kezelés 156-ot
választott, a kontroll 137-et. Vagyis nem redundánsak a meglévő készlettel.

De a top 10 univariáns prediktor **kizárólag volatilitás-feature**
(`gk_vol_*`, `natr_14`, `hist_vol_20`, `returns_std_14`). Egyetlen alt-változó
sincs köztük.

---

## 3. Feature importance — a 20 változó a súlya alatt teljesít

| | Érték |
|---|---|
| Alt-változók együttes gain-részesedése | **8,97%** |
| Részesedés, ha egyenlő súlyúak lennének | 12,82% |
| Legjobb alt-változó helyezése | **12. / 156** (`feat_ls_count_delta_60`, 1,11%) |

A top 4 feature (mind volatilitás) egyedül **43,9%** gaint visz. A teljes
alt-csoport kevesebbet, mint bármelyik a három legerősebb volatilitás-feature
közül külön-külön.

**Legerősebb alt-változók:**

| # | Változó | gain |
|---|---|---|
| 12 | `feat_ls_count_delta_60` | 1,11% |
| 14 | `feat_prem_std_60` | 1,00% |
| 15 | `feat_oi_to_volume_60` | 0,73% |
| 24 | `feat_funding_countdown` | 0,57% |
| 25 | `feat_oi_accel_60` | 0,56% |
| 28 | `feat_ls_retail_vs_top_60` | 0,52% |

Érdemes látni, hogy a felső mezőnybe a **pozicionáltság változása**
(`ls_count_delta`) és a **prémium szórása** (`prem_std`) került — nem a
szintek, hanem a dinamika. A `feat_ls_retail_vs_top_60`, amit előzetesen a
legígéretesebbnek tartottunk, csak a 28. helyre jött be.

---

## 4. Modell-metrikák — a javulás elhanyagolható

| Metrika | Kontroll | Kezelés | Δ |
|---|---|---|---|
| valid lift (top 7,5%) | 2,2757 | 2,2995 | +0,024 |
| valid alsó 7,5% | 0,4830 | 0,5070 | +0,024 |
| **spread (top/alsó)** | **4,712** | **4,536** | **−0,176** |
| Spearman ρ | 0,3062 | 0,3088 | +0,003 |
| **train–valid gap** | **0,0019** | **0,0736** | **+0,072** |
| valid RMSE | 0,00895 | 0,00854 | −0,0004 |
| decile monotonitás | 1,000 | 1,000 | 0 |

**A modell-metrikák alapján a kezelés NEM jobb.** A lift +1%, a spread pedig
romlott, és a train–valid gap 38-szorosára ugrott. Ha itt megálltunk volna, a
verdikt „nincs jel" lett volna.

---

## 5. Kereskedési eredmény — itt fordul meg a kép

Strategy session mindkét modellre, azonos ablakon (2026-02 → 2026-07),
200 setup rácskeresés, long irány.

### 5a. Beállításonkénti összevetés (nem csak a maximum)

| | Érték |
|---|---|
| Összevetett setupok | 200 |
| Átlagos különbség | **+0,0465** |
| Medián különbség | +0,0435 |
| Setupok, ahol a kezelés jobb | **122 / 200 (61%)** |
| Átlagos hozam (kontroll / kezelés) | 0,1162 / **0,1626** |

### 5b. ⚠️ Költség-korrekció — ez a döntő

**A `src/strategy/` sehol nem számol díjjal, csúszással vagy jutalékkal.**
Minden riportált hozam bruttó. Ez azért kritikus, mert a két bruttó optimum
gyökeresen eltérő forgalmú (672 vs 1984 kötés).

Kötésenkénti él a bruttó optimumon:

| | kötés | bruttó | bps/kötés | win% | tartás |
|---|---|---|---|---|---|
| Kontroll | 672 | 0,4541 | 6,76 | 56,5% | 49,6 p |
| Kezelés | 1984 | 0,5003 | **2,52** | 36,6% | 14,9 p |

A kezelés bruttó optimuma **2,52 bps/kötés** — reális Binance futures taker
díj (kb. 5 bps oldalanként) mellett ez mélyen veszteséges lenne.

**Ha viszont mindkét oldal a saját költség-tudatos optimumát választhatja:**

| Kör-költség | Kontroll nettó | Kezelés nettó | Nyertes |
|---|---|---|---|
| 0 bps | 0,4541 | 0,5003 | kezelés |
| 2 bps | 0,3197 | 0,3822 | kezelés |
| 4 bps | 0,1910 | 0,3020 | kezelés |
| 6 bps | 0,1080 | 0,2218 | kezelés |
| **8 bps** | **0,0250** | **0,1416** | **kezelés (5,7×)** |
| 10 bps | −0,0457 | 0,0614 | kezelés |

A kezelés **minden költségszinten nyer, és a fölénye a költséggel nő.**

A 8 bps-es optimumnál a két setup **azonos gyakoriságú** — kontroll: cutoff
0,98, 415 kötés; kezelés: cutoff 0,98, 401 kötés. Tehát a különbség tiszta
jelminőség:

- kontroll: **8,60 bps / kötés**
- kezelés: **11,53 bps / kötés** → **+34% él azonos forgalomból**

---

## 6. Fenntartások — amit nem szabad elfelejteni

1. **A díj nincs modellezve, csak utólag számoltuk.** A 8 bps feltételezés,
   nem mért érték. A `src/strategy/` költség-modellje **hiányzó funkció** —
   enélkül a rácskeresés szisztematikusan a túlkereskedő setupokat választja.
   Ez a legfontosabb kimenete ennek a vizsgálatnak, függetlenül az alt-adattól.
2. **Egyetlen 6 hónapos valid ablak.** Egy ablakon mért javulás lehet zaj.
   A kereskedési szám (2026-02 → 2026-07) nem független megerősítés.
3. **A rácskeresés ugyanazon az ablakon optimalizál, amin mérünk**
   (`same_window`). Mindkét oldalt egyformán érinti, tehát az összevetés fair,
   de az abszolút számok optimisták.
4. **A 122/200 nominálisan szignifikáns, valójában kevésbé.** A setupok erősen
   korreláltak (ugyanaz az adat, átfedő konfigurációk), az effektív mintaméret
   jóval kisebb 200-nál.
5. **Csak long irány.** A short nem futott le.
6. **A train–valid gap ugrása valós figyelmeztetés.** 0,0019 → 0,0736.

---

## 7. Döntés

| Kérdés | Válasz |
|---|---|
| Ad-e többletet az alt-data? | **Igen, a kereskedési eredményben.** Modell-metrikában alig. |
| Tier 3 (`bookDepth`, 2023-01-től) | **GO** — az orderbook-likviditás a legközvetlenebb magnitúdó-prediktor, és a mostani eredmény indokolja a ~600 MB-ot. |
| Tier 4 (`aggTrades`, 10–15 GB) | **VÁRJ** — előbb a short irány és egy második valid ablak. |
| t302 likvidációs collector | **FUSSON TOVÁBB** — az adat értéke idővel nő, a leállítás visszafordíthatatlan. |
| Fizetős szolgáltató | **NEM** — semmi nem indokolja. |

### Következő lépések, prioritás szerint

1. **Költség-modell a `src/strategy/`-be.** Enélkül minden rácskeresés
   torzított. Ez nem alt-data kérdés, hanem alaprendszer-hiányosság.
2. **Short irány** ugyanezzel a recepttel.
3. **Második valid ablak** (pl. 2025-08 → 2026-01) a stabilitás ellenőrzésére.
4. Tier 3 bookDepth.
