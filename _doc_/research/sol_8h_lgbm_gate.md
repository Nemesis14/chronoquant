# SOL, 8 órás horizont, LightGBM — kapu-jelentés

**Kérdés.** Egyetlen coinon (SOLUSDT), rövid (1–8 órás) feature-ablakokkal és 8
órás triple-barrier célváltozóval, regularizált LightGBM-mel, havi
újratanítással: van-e olyan jel, ami fedezi a kereskedési díjat?

**Válasz.** Nem. Sem statisztikailag, sem gazdaságilag.

> ⚠️ **JAVÍTVA — lásd §10.** Ez a jelentés eredetileg azt állította, hogy a
> modell „átmegy a null-teszten és veri a véletlen baseline-t". **Ez téves
> volt.** A null-teszt egyetlen rotációra épült, ami szerencsés húzás volt. 24
> rotációval a null-eloszlás nem nullára centrált (IC átlag +0,0051, p95
> +0,0255), és az éles +0,0240 **egyik mérőszámon sem éri el a 95.
> percentilist** (p = 0,12 / 0,12 / 0,20). A §3, §4 és §8 alábbi szövege a
> javítás előtti állapotot tükrözi; olvasd a §10-zel együtt.

---

## 1. Beállítás

| | |
|---|---|
| Eszköz | SOLUSDT perpetual |
| Célváltozó | triple barrier, 480 perc, `k = 0,648 × EWM_std × √h`, medián 137 bps |
| Feature-ök | 235 db, 1/2/4/8 órás ablakok, max 8h visszatekintés, előzetes szűrés nélkül |
| Minta | 33 423 órás sor |
| Train ablak (regularizáció-keresés) | 18 888 sor, 2024-07-31-ig |
| Mérés | 2024-08 … 2026-07, **havi újratanítás** bővülő ablakon, 24 hónap |
| Költség | 4,72 bps/kötés (maker be + ki, `execution_cost_gate.md`) |

Kód: [sol_lgbm_8h.py](../../src/modeling/experiments/sol_lgbm_8h.py),
[sol_lgbm_8h_econ.py](../../src/modeling/experiments/sol_lgbm_8h_econ.py).

---

## 2. Regularizáció

Nyolc rácspont, purge-olt idősoros CV a train ablakon belül (a fold-varratokból
8 óra kivágva, mert a címke ennyit fog át). A keresés soha nem látta a
gördülő hónapokat.

| levelek | min_leaf | λ | colsample | fák | CV IC | CV AUC |
|--:|--:|--:|--:|--:|--:|--:|
| **3** | **4000** | **200** | **0,3** | **200** | **+0,0309** | **0,5195** |
| 3 | 2000 | 100 | 0,5 | 300 | +0,0166 | 0,5093 |
| 4 | 4000 | 200 | 0,3 | 200 | +0,0309 | 0,5195 |
| 4 | 2000 | 50 | 0,5 | 300 | +0,0163 | 0,5090 |
| 4 | 1000 | 20 | 0,7 | 400 | +0,0209 | 0,5124 |
| 8 | 2000 | 200 | 0,5 | 300 | +0,0176 | 0,5098 |
| 8 | 1000 | 50 | 0,7 | 400 | +0,0192 | 0,5116 |
| 16 | 2000 | 100 | 0,5 | 300 | +0,0171 | 0,5096 |

A keresés a rács **legszigorúbb** sarkát választotta, és a 3, illetve 4 leveles
változat azonos eredményt adott: 4000-es `min_child_samples` mellett a fa
amúgy sem tud négy levélig nőni. A választott modell tehát gyakorlatilag
additív döntési tuskók összege — minden lazítás rontott.

---

## 3. Gördülő mérés

|  | train | valid | t | pozitív hónap |
|---|--:|--:|--:|--:|
| IC | +0,1019 | **+0,0240** | 2,00 | 15/24 |
| AUC | 0,5622 | 0,5157 | 1,99 | — |
| p90 lift | +12,31pp | +2,22pp | 1,03 | 15/24 |

**Véletlen 5 feature, ugyanez a modell (10 húzás):** valid IC +0,0134,
tartomány [−0,0058, +0,0288], AUC 0,5084, p90 lift +1,25pp.

A modell mindhárom mérőszámon veri a baseline-t, nagyjából 1,8×-osan. A
baseline szórása viszont jelentős: egy húzás +0,0288-at adott, ami a modell
fölött van.

---

## 4. Null-teszt

A címke 4380 órával elforgatva — a feature-ök szerkezete és a címke
autokorrelációja megmarad, a köztük lévő párosítás elvész.

|  | train | valid | t |
|---|--:|--:|--:|
| IC | +0,3086 | **−0,0041** | −0,33 |
| AUC | 0,6927 | 0,4958 | −0,53 |
| p90 lift | +28,81pp | −0,31pp | −0,25 |

Véletlen baseline ugyanitt: IC −0,0051, AUC 0,4971.

**Két dolog következik ebből.** Egy: a gépezet nem gyárt pozitív valid számot,
tehát a +0,0240 nem artefakt — a train IC +0,3086 volta megmutatja, hogy a
train-valid szakadék önmagában semmit nem jelent, mert tiszta zajon is
kialakul. Kettő: az éles futás baseline-ja (+0,0134) itt nullára esik, vagyis
**a véletlen változók is valódi jelet hordoznak** — mind árból származnak, és
elég korreláltak ahhoz, hogy bármelyik öt ugyanazokat a komponenseket vigye. A
modell érdemi többlete így nem +0,0240, hanem a kettő különbsége, ~+0,011.

---

## 5. Gazdasági mérleg

Küszöb-szabály: long, ha a score átlépi a **train**-eloszlás 90. percentilisét,
short a 10. alatt. A küszöb soha nem a kereskedett hónapból jön.

| szabály | kötés | gyakoriság | találat | bruttó | nettó | 95% CI | p(≤0) |
|---|--:|--:|--:|--:|--:|---|--:|
| long (≥ p90) | 1187 | 8,2% | 52,9% | +8,09 | **+3,37** | [−16,54, +19,31] | 0,338 |
| short (≤ p10) | 1252 | 8,6% | 53,2% | +5,09 | **+0,37** | [−11,05, +10,33] | 0,484 |

Referencia: 11 837 irányos óra, ebből +1 49,0%.

A konfidencia-intervallum 30 napos blokk-bootstrapból jön. A kötések 8 órában
átfednek, így 1187 független kötést feltételezni nagyjából az átfedés
gyökével csökkentené a hibát, és érmedobásból felfedezést csinálna.

**Havi konzisztencia (long):** 12/24 pozitív hónap, legrosszabb −151 bps,
legjobb +100 bps, a havi átlagok t-je **−0,07**. A poololt +3,37 bps tehát
néhány sok-kötéses hónapból jön; egyenlő súlyú hónapokkal a hatás eltűnik.

---

## 6. Mit használ a modell

| átl. vágás | feature |
|--:|---|
| 37,8 | `feat_adx_neg_8` |
| 17,7 | `feat_natr_rank_4` |
| 14,3 | `feat_rsi_8` |
| 12,5 | `feat_wick_imbalance_sma_8` |
| 12,3 | `feat_hist_vol_8` |
| 10,4 | `feat_natr_8` |
| 9,9 | `feat_bb_width_rank_8` |
| 9,2 | `feat_adx_neg_4` |
| 9,0 | `feat_returns_std_8` |
| 6,9 | `feat_lower_high_count_8` |

235-ből 58 változó soha nem vágott.

A lista a `direction_features.py`-ban dokumentált problémát ismétli: a top 10-ből
öt tiszta **magnitúdó** (`natr` ×2, `hist_vol`, `bb_width`, `returns_std`), az
első helyezett `adx_neg` pedig trenderősség. Ezek azt mondják meg, hogy
*lesz-e* nagy mozgás — vagyis a timeout-osztályt választják szét —, nem azt,
hogy merre. Ez magyarázza, miért esik szét az IC és a pénz: a rangsorolás
nagyrészt a „történik-e valami" tengelyen jó, a nyereséghez viszont az „merre"
tengely kell.

---

## 7. Miért nem mond ellent az IC és a pénz

Az IC minden órán mérődik, a timeoutokat is beleértve, és a döntés
szempontjából érdektelen tömeg uralja. A kereskedett farokban a találat 52,9%
a 49,0%-os alaparányhoz képest, azaz +3,9pp. Szimmetrikus ±137 bps barrier
mellett ez ~10,7 bps bruttót ígér; mérve +8,09 lett, amiből a díj 4,72-t elvisz.

A maradék nem azért nem szignifikáns, mert kicsi, hanem mert a szórás nagy: a
mért hatás és szórás mellett **~10 400 kötés** kellene t = 2-höz. Egyetlen
coinon ez ~18 év. Ugyanez a szabály ~9 coinon két év alatt összejön.

---

## 8. Döntés

**A SOL egycoinos ág lezárva.** Nem cáfoltuk a jelet — épp ellenkezőleg, ez az
első ág, ami a null-tesztet és a baseline-t is átlépi. Az egycoinos minta
egyszerűen nem elég nagy ahhoz, hogy a kérdést eldöntse, és semmilyen további
modellezés nem hoz több adatot.

**A folytatás az `epic_064` eredeti terve**, most már számmal alátámasztva:
ugyanez a szabály a 30 coinos panelen, `t856` szerint. A ~9 coinos becslés
felső korlát, mert egyidejű coinok jelei korreláltak — ezért kéri a `t856` a
jel-klaszterezési diagnosztikát: ha a jelek ugyanarra a néhány napra esnek,
akkor nem szelektivitást vettünk, hanem rezsim-detektort építettünk, és a
tényleges független kötésszám jóval kevesebb, mint a nyers darabszám.

Két megkötés a panelre nézve, ebből a futásból:

1. **A magnitúdó-feature-ök dominanciáját kezelni kell.** A `direction_features.py`
   irány-blokkja (funding, OI, crowding, taker-imbalance) ott van, de ebben a
   futásban nem szerepelt — a `feat_sol_short_1h` tábla nem tartalmazza.
   A panelen a két blokkot együtt kell mérni.
2. **A küszöb maradjon abszolút.** A p90 percentilis coinonként garantáltan
   kötést csinál, akkor is, ha a modell semmit nem lát. A `t856` abszolút
   küszöböt ír elő — ez a különbség dönti el, hogy „nincs kötés" valóban
   lehetséges kimenet-e.

---

## 9. Audit: miért +12,31pp a trainen és +2,22pp a validon?

Az 5,6-szoros szakadékot nem elég in-sample optimizmusnak nevezni — meg kellett
nézni, hogy a két oldal egyáltalán ugyanazt a számot számolja-e. Négy magyarázat
adja ugyanezt a képet, és mind a négy mérhető.

Kód: [sol_lgbm_8h_audit.py](../../src/modeling/experiments/sol_lgbm_8h_audit.py).

### 9.1 Ugyanazt a mennyiséget mérjük? — Igen

|  | kiválasztott hányad | n_sel | alaparány (+1) | timeout |
|---|--:|--:|--:|--:|
| train | 0,100 | 1892 | 49,05% | 17,38% |
| valid | 0,101 | 50 | 49,56% | 17,80% |

Ez volt a legkomolyabb gyanú: a modell szándékosan durva (3 levél, 4000-es
`min_child_samples`), tehát a predikciója lépcsős, és a `pred >= quantile(0,90)`
holtverseny esetén a tizednél többet választ ki — méghozzá 25 000 train soron
másképp, mint 600 valid soron. **Nem ez történik:** a realizált hányad 0,100 vs
0,101. Az alaparány, amihez a liftet mérjük, gyakorlatilag azonos (49,05% vs
49,56%), a timeout-arány szintén. A mérőszám konzisztens.

### 9.2 Ugyanaz a becslő? — Lényegében igen

| | lift |
|---|--:|
| valid-kor, havi liftek átlaga | +2,22pp |
| valid-kor, poololt (11 837 irányos sor) | +2,77pp |
| teljes OOS, poololt | +2,74pp |

A train lift egyetlen nagy mintán számolódik, a valid 24 db ~600 soros hónap
átlaga — ezek különböző becslők. A különbség viszont csak 0,55pp, tehát az
aggregáció sem magyarázza a szakadékot.

### 9.3 A döntő mérés: OOS lift a train korszakban is

A gördülő mérést visszanyújtottuk 2023-10-ig, így a **tanítási korszak hónapjai
is out-of-sample** pontot kapnak.

| korszak | hónap | lift IN-SAMPLE | lift OUT-OF-SAMPLE | szórás |
|---|--:|--:|--:|--:|
| train-kor | 10 | +11,49pp | +5,13pp | 6,05 |
| valid-kor | 24 | +12,31pp | +2,22pp | 10,60 |

**Ez a táblázat adja meg a választ.** Az in-sample lift a *két korszakban
gyakorlatilag azonos* (+11,49 vs +12,31). Vagyis a +12pp nem a tanítási időszak
tulajdonsága, hanem a modellé: ennyit ér el bármilyen adaton, amit már látott.
Ez a modell kapacitásának mérőszáma, nem a jelé.

A szakadék felbontása:

- **in-sample → OOS (ugyanazon a korszakon): −6,36pp** — tiszta in-sample
  optimizmus
- **train-kor OOS → valid-kor OOS: −2,91pp, Welch t = −1,01** — a korszakok
  közti további esés **nem válik el a zajtól**

⚠️ A visszanyújtás diagnosztika, nem tiszta eredmény: a regularizációs rácsot
2024-07-31-ig terjedő adaton választottuk, tehát a train-kori hónapok ettől
kontaminaltak. Arra jó, hogy „változott-e a világ", nem arra, hogy „mennyi pénz
van benne".

### 9.4 Ami viszont tényleg elmozdult: a barrier

| negyedév | +1 arány | timeout | barrier bps | OOS lift |
|---|--:|--:|--:|--:|
| 2023Q4 | 54,6% | 13,9% | **205** | +6,63pp |
| 2024Q2 | 45,6% | 18,6% | 152 | +8,13pp |
| 2025Q1 | 48,0% | 16,8% | 163 | +0,35pp |
| 2025Q4 | 48,0% | 22,0% | 138 | −4,98pp |
| 2026Q2 | 47,1% | 20,2% | **111** | −6,76pp |
| 2026Q3 | 52,6% | 17,8% | **93** | +18,22pp |

A címke eloszlása stabil (+1 arány 45–55%, timeout 14–24%), de a **barrier
szélessége 205 bps-ről 93 bps-re esett** — a SOL volatilitása megfeleződött. Ez
a liftet nem torzítja (a lift címke-arány, nem hozam), de a **gazdaságtant
igen**: 205 bps-es barrier mellett a 4,72 bps költség a kifizetés 2,3%-a, 93
bps mellett 5,1%-a. Ugyanaz a találati arány ma feleannyit ér.

### 9.5 Ami marad: a havi zaj

A havi OOS liftek szórása **10,60pp**, az egyedi hónapok −15,64 és +21,39 között
szórnak. Egyetlen hónap liftje gyakorlatilag tiszta zaj; 24 hónap átlagának
standard hibája 2,16pp, ami a +2,22-es átlaggal t ≈ 1,03-at ad — pontosan az a
szám, ami a 3. szakaszban szerepel.

### 9.6 Következtetés

**Nincs számítási inkonzisztencia a train és a valid oldal között.** A p90
küszöb ugyanazt a hányadot választja, az alaparány ugyanaz, az aggregáció nem
torzít. A +12,31pp a modell in-sample kapacitása, ami a tanítási korszakban is
ugyanennyi; a valódi out-of-sample teljesítmény ~+2 és +5pp között van, és a két
korszak különbsége nem szignifikáns.

Ez megerősíti a 8. szakasz döntését, egy kiegészítéssel: a panelre lépéskor a
**csökkenő barrier-szélességet** figyelembe kell venni, mert a fix bps-ben mért
költség egyre nagyobb hányadát viszi el a kifizetésnek. A `t856` vol-normalizált
fedezeti pontja pontosan ezt kezeli — ez a mérés megmutatta, miért nem
elhagyható.

### 9.7 Segítene-e hosszabb értékelési blokk?

Kézenfekvő ellenvetés, hogy a havi lift ~50 soron mérődik, és 12 hónapos
blokkokkal pontosabb lenne. Ugyanazokat az out-of-sample predikciókat
újracsoportosítva:

| blokk | db | sor/blokk | n_sel | átlag lift | szórás | SE | t |
|---|--:|--:|--:|--:|--:|--:|--:|
| 1 hónap | 24 | 493 | 50 | +2,22pp | 10,60 | 2,16 | 1,03 |
| 3 hónap | 8 | 1480 | 148 | +2,40pp | 7,87 | 2,78 | 0,86 |
| 6 hónap | 4 | 2959 | 296 | +2,76pp | 4,56 | 2,28 | 1,21 |
| 12 hónap | 2 | 5918 | 592 | +2,27pp | 2,24 | 1,59 | 1,43 |
| poololt | 1 | 11 837 | 1184 | +2,77pp | — | — | — |

**Nem segít.** A pontbecslés +2,2 és +2,8pp között marad — a havi szeletelés
tehát nem torzít. A standard hiba pedig nem csökken (2,16 → 2,78 → 2,28 →
1,59), mert hosszabb blokknál ugyan kisebb a blokkon belüli zaj, de arányosan
kevesebb blokk is van: `SE = szórás / √blokkszám`. Az összes bizonyíték
ugyanaz a 11 837 irányos sor, akárhogy csoportosítjuk; az aggregálás nem gyárt
információt.

A 12 hónapos sor t = 1,43-ja két blokkból, egy szabadságfokkal számolt —
értelmezhetetlen. A havi felbontás azért a helyes választás, mert 23
szabadságfokot ad a hibabecsléshez.

⚠️ A blokkon belüli p90 más kereskedési szabály: a 12 hónapos blokk *az év*
legjobb 10%-át választja, nem minden hónap saját legjobb 10%-át.

**Melléktermék, ami a 9.3-at erősíti:** a szórás nagyjából `1/√k` szerint
csökken (10,60 → 7,87 → 4,56 → 2,24, szemben a 10,60/√k = 6,12 / 4,33 / 3,06
elvárttal). A hónapok közti ingadozás tehát javarészt mintavételi zaj, nem
tartós rezsim-instabilitás.

**Következmény:** a mintaméret valóban a korlát, de a megoldás nem az időbeli
átcsoportosítás, hanem **több független megfigyelés** — vagyis több coin. Ez a
7. szakasz ~9 coinos becslésével egybevág, és a `t856`-ra mutat.

---

## 10. JAVÍTÁS — a null-teszt egyetlen rotációra épült

A §4 null-tesztje **egyetlen** eltolással (4380 óra) futott, és tisztának
látszott (IC −0,0041). Ez volt az alapja annak a §8-beli állításnak, hogy „ez az
első ág, ami a null-tesztet és a baseline-t is átlépi". **Az állítás téves.**

Kód: [null_distribution.py](../../src/modeling/experiments/null_distribution.py).

### 10.1 Két hiba a mérőeszközben

**Egy rotáció egy húzás.** Egyetlen eltolás nem mondja meg, hogy +0,0240
szokatlan-e; csak annyit mond, hogy egy adott rotáció alatta maradt. A 4380
órás eltolás a 24-es listában az egyik legalacsonyabb húzás.

**A rotált címkén újrafuttattam a regularizáció-keresést.** A null-modell így a
saját konfigurációját választhatta, vagyis hozzáigazíthatta magát ahhoz a
zajhoz, amin meg kellett volna buknia. A javított teszt fixen tartja az éles
adaton választott konfigurációt — csak így összemérhető a kettő.

### 10.2 A null-eloszlás (24 rotáció, fix konfiguráció)

| | éles | null átlag | null szórás | null p95 | null max | **p** |
|---|--:|--:|--:|--:|--:|--:|
| IC | +0,0240 | +0,0051 | 0,0145 | +0,0255 | +0,0356 | **0,120** |
| AUC | 0,5157 | 0,5035 | 0,0091 | 0,5175 | 0,5229 | **0,120** |
| p90 lift | +2,22pp | +0,03 | 1,86 | +3,25 | +3,49 | **0,200** |

`p = (1 + ahány rotáció eléri az éles eredményt) / (1 + rotációk)` — az éles
futás maga is egy húzás a null alól, ezért nulla találat sem jelenthet p = 0-t.

**A null-eloszlás nem nullára centrált.** A rotáció megőrzi mindkét sorozat
saját autokorrelációját — ez a lényege, hiszen a párosítást bontja el, nem a
blokkszerkezetet. Erős címke-átfedésnél viszont épp ez a blokkszerkezet gyárt
látszategyüttmozgást: az IC null-átlaga +0,0051, egyes rotációk +0,0356-ig
felmennek. **A nullával összevetni tehát rossz teszt volt.**

Az éles +0,0240 a null p95-je (+0,0255) **alatt** van. A §3-ban jelentett havi
t = 2,00 a nullához mérve készült; a helyes viszonyítás
(0,0240 − 0,0051)/0,0145 = **1,30**.

### 10.3 Miért nem mond ellent a véletlen baseline

A két kontroll nem ugyanazt kérdezi, és a gyengébbet néztem.

- **Véletlen-5 baseline:** „jobbak-e a szelektált változók öt véletlennél?" —
  igen (+0,0240 vs +0,0134).
- **Rotáció:** „van-e egyáltalán jel?" — és a válasz az, hogy amit mérünk, azt a
  modell *és* az öt véletlen változó is ugyanabból a forrásból szedi: két
  autokorrelált sorozat látszategyüttmozgásából. A modell ezt csak valamivel
  hatékonyabban aknázza ki.

### 10.4 Következmény

**A §8 döntése áll, de más indokkal.** Korábban úgy szólt: „nem cáfoltuk a
jelet, csak kicsi a minta". A helyes megfogalmazás: **ezen az ágon nem
mutattunk ki jelet.** A gazdasági mérleg (§5: p = 0,338) és a null-eloszlás
(p = 0,12) ugyanazt mondja, egymástól függetlenül.

Ez a mérőeszköz-javítás **minden korábbi „átment a null-teszten" állítást
érint** a projektben, mert mindegyik egyetlen rotációra épült. A `t856` panelen
a null-eloszlást kell használni, nem egyetlen eltolást.

### 10.5 A 15 perces rács — és egy második javítás

A 15 perces ágat (`t860`) először artefaktnak nyilvánítottam, mert a 4380 órás
rotáció majdnem az éles eredményt adta. **Ez ugyanabból a hibából eredt:** az a
rotáció a 12 húzás közül a második legmagasabb volt. Teljes eloszlással:

| rács | | éles | null átl. | null szórás | ≥ éles | p | **z** |
|---|---|--:|--:|--:|--:|--:|--:|
| 1h (24 rot.) | IC | +0,0240 | +0,0051 | 0,0145 | 2/24 | 0,120 | **1,31** |
| | lift | +2,22 | +0,03 | 1,86 | 4/24 | 0,200 | 1,17 |
| 15m (12 rot.) | IC | +0,0312 | +0,0055 | 0,0131 | 0/12 | 0,077 | **1,96** |
| | lift | +3,11 | +1,24 | 1,41 | 2/12 | 0,231 | 1,33 |

**A 15 perces rács az erősebb**, nem a gyengébb. A sűrűbb rács valóban gyárt
több látszatliftet (null-átlag +1,24 vs +0,03), de nem annyit, hogy az éles
eredményt megmagyarázza.

### 10.6 A 15 perces rács, 24 rotációval — végleges

A `p = 0,077` nem a felbontás alja volt; 24 rotációval is megáll.

| | éles | null átl. | null szórás | null p95 | null max | ≥ éles | **p** | **z** |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| IC | +0,0312 | +0,0046 | 0,0135 | +0,0293 | +0,0319 | 1/24 | 0,080 | 1,98 |
| AUC | 0,5209 | 0,5027 | 0,0084 | 0,5180 | 0,5190 | **0/24** | **0,040** | 2,17 |
| p90 lift | +3,11 | +0,64 | 1,69 | +3,16 | +5,17 | 2/24 | 0,120 | 1,46 |

Az órás rács ugyanezen: 0,120 / 0,120 / 0,200, z = 1,31 / 1,34 / 1,17.

**A 15 perces rács következetesen jobb** — mindhárom mérőszámon, mindkét
rotációszámnál. Az órán belüli hipotézis nem dőlt meg.

**De a p = 0,040 nem felfedezés.** Ebben a munkaszakaszban két rács, három
horizont és legalább négy modellcsalád futott, mindegyik három mérőszámon. Hogy
ebből egy 0,040 kiessen, pontosan az, amit tiszta zajtól várnánk. A három
mérőszám erősen korrelált (ugyanazon a predikción mérjük), tehát a Bonferroni
túl szigorú lenne — de bármilyen enyhe korrekció is megeszi.

**Amit viszont eldönt: a rácsválasztást.** Hogy a 15 perces rács jobb az
órásnál, az kétváltozós összevetés, nem sok hipotézis közül a legjobb
kiválasztása — erre nem vonatkozik ugyanúgy a többszörös összehasonlítás
problémája. A panelre a 15 perces rács viendő.

### 10.7 Következő lépés — előre rögzített protokoll

A SOL innentől **hipotézis-generáló minta**, a 30 coinos panel a
**megerősítő minta**. Ez az egyetlen mód kilépni a többszörös összehasonlítás
csapdájából: a döntéseket most kell rögzíteni, mérés előtt.

Előre rögzítve, a `t856`-hoz:

1. **Rács:** 15 perces bar, ablakok 2/4/8/16/32 bar (30p – 8h), 2880 bares
   gördülő percentilis.
2. **Modell:** LightGBM, `num_leaves=3, min_child_samples=4000, reg_lambda=200,
   colsample_bytree=0.3, n_estimators=200`, havi újratanítás.
3. **Elsődleges mérőszám: AUC.** Egy, előre kiválasztva — ez adta a legnagyobb
   z-t a generáló mintán, és egyetlen mérőszámmal nincs válogatás. Az IC és a
   lift másodlagos, leíró célra.
4. **Teszt:** null-eloszlás ≥ 24 rotációval, fix konfigurációval, coinonként.
5. **Küszöb:** p < 0,05 az elsődleges mérőszámon. Nem „valamelyik metrikán".

Ha a panel ezt nem hozza, az ág lezárul — nem próbálunk újabb változatot.
