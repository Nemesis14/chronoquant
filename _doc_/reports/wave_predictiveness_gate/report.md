## Cél és döntés

**t920 (epic_072):** a `_doc_/methodology_doc/5200_wave_start_image_model.md` (v2) go/no-go szabálya szerint megmérjük, hogy a prefix-kép (AE-beágyazás) inkrementális előrejelző erőt ad-e a skalár alap (S0 + C) fölött az impulzus-címkére (bináris fő teszt). Cella: `W4h_H8h` (W = 4h, H = 8h), **720 train esemény**, ebből **718 vesz részt a CV-ben** (nem lóg át blokkhatáron), 117 impulzus (16.3%).

> **Cella-döntés: NO-GO.** Az első nem teljesülő feltétel: **3. Permutációs p (nap-blokkos, 1 cella → Holm = nyers p)** (0.2617; szabály: < 0.01).

> ⚠️ **OOS-tisztaság:** az elemzés kizárólag a train eseményeit használja (`load_events(split="train")`, az 1m árfolyam a train végén levágva); az OOS és a holdout-2 sorok nem kerültek a memóriába. Az OOS-előjel nem lett lefuttatva (egyedi OOS t0 = 75 < 100, t925).

## Előre rögzített terv (az eredmények megtekintése előtt)

- **Modell A** = `S0 + C` (a_sigma, velocity_1h, pretrend_3d, log_sigma_1m, drop_depth, volume_ratio; fold-train átlaggal / szórással standardizálva). **Modell B** = A + reprezentáció. Azonos tanuló: L2-regularizált logisztikus regresszió, **rögzített erősség C = 0.1** (belső hangolás nincs: címke-függő lenne, és minden permutációban újra kellene futnia).
- **Elsődleges (döntési) teszt: AE latens embedding** (16 dim). Az 5200 nem nevez meg reprezentációt, ezért az alapértelmezés az AE-latens, ez rögzítve lett az eredmények megtekintése előtt. **Érzékenység (nem döntési, nincs cherry-pick):** klaszter one-hot k = 10 (t919 választása), klaszter one-hot k = 5 (legstabilabb partíció), C = 0,01 / 1, gradiens-boosting (csak pont-becslés).
- **Statisztika:** a 6 blokkos purged + embargós (`H`) CV összevont out-of-fold `ΔAUC = AUC(B) − AUC(A)`; az AE, a k-means és a kép-tengely **foldonként, csak a fold-train eseményein** illesztett (`wave_image_model.fit_fold`); a végső modell 17,16 ATR-es konstansa nem használt.
- **p-érték:** naptári nap-blokkos permutáció, **N = 1000**, `p = (1 + #{null ≥ megfigyelt}) / (N + 1)`; 1 cella → Holm = nyers p.
- **Feltételek sorrendben:** (1) minimum eseményszám, (2) ΔAUC ≥ 0,02, (3) p < 0,01, (4) ΔAUC > 0 legalább 4 / 6 blokkban, (5) OOS előjel (csak ≥ 100 egyedi t0; itt 75 → nem fut, a legjobb kimenet: feltételes GO).
- **Permutáció — pontos újraillesztés:** az AE és a k-means felügyelet nélküli, a per-fold beágyazások nem függnek a címkéktől, ezért egyszer készülnek el (és gyorsítótárba kerülnek). Minden permutációban **csak a címkék** permutálódnak (egy nap címke-vektora érintetlenül egy másik, azonos esemény-számú napra kerül, így a blokk-szerkezet és a címke-összeg pontosan megmarad), és **minden címke-függő komponens** — A és minden B modell fold-train illesztése — újra tanul ugyanazokon a foldokon. Ez a teljes CV-pipeline egzakt újrafuttatása.

{{table:criteria}}

## 1. Fő eredmény: összevont OOF ΔAUC

Az elsődleges (AE) Modell B összevont OOF AUC-ja **0.6934**, a Modell A-é **0.6647**, így **ΔAUC = +0.0287** (95% nap-blokk bootstrap CI [-0.0117; +0.0685]). A permutációs null átlaga +0.0078, 99. percentilise +0.0983; **p = 0.2617** (N = 1000, a legkisebb elérhető érték 0.0010). Blokk-előjel: **5 / 6** blokkban pozitív a ΔAUC.

{{chart:null}}

{{table:summary}}

## 2. Blokkonkénti stabilitás

Minden blokk egyszer teszt; a modellek a többi blokk (purge + embargó után) eseményein tanulnak. A blokkonkénti ΔAUC kis mintán (108–132 esemény, 15–26 impulzus) zajos; a 4. feltétel azt védi, hogy egyetlen időszak ne hordozza az eredményt.

{{chart:blocks}}

{{table:blocks_table}}

## 3. A skalár alap (Modell A) erőssége

A Modell A önmagában **AUC = 0.6647** (összevont OOF; 0,5 = véletlen); a nap-blokkos permutációs null szerint p = 0.0010. Log-loss: A **0.4184** vs konstans (fold-train prior) **0.4458** (Δ = -0.0274); Brier A 0.1301. Az S0 + C tehát érdemben előrejelez az impulzus-arány fölött — ez a mérce, amit a képnek meg kell haladnia. (A t919 szerint a klaszterek nagyrészt a rebound méretét, `A/σ_W`-t különítik el, ami az S0 része.)

## 4. Kalibráció (összevont OOF)

Kvantilis-bin (5 bin) megbízhatósági tábla. Kalibrációs meredekség (a logit-becslésre illesztett logisztikus): A **1.26**, B **0.91** (1 = tökéletes, < 1 = túl magabiztos); átlagos becslés vs megfigyelt arány: A 0.158 / 0.163, B 0.158 / 0.163. Log-loss: A 0.4184, B 0.4122; Brier: A 0.1301, B 0.1284.

{{chart:calibration}}

{{table:calibration_table}}

## 5. Érzékenységi vizsgálatok (nem döntési)

Az alábbiak a döntést **nem** befolyásolják; azt mutatják, hogy az elsődleges eredmény függ-e a reprezentációtól vagy a tanulótól. A klaszter-reprezentációk permutációs p-értékei is a táblában vannak (ugyanaz a N = 1000), de a döntés kizárólag az AE-teszten alapul; a három teszt között nincs szelekció.

{{chart:sensitivity}}

{{table:sensitivity_table}}

{{table:runtime}}

## Értelmezés

**Cella-döntés (W4h_H8h): NO-GO.** Elsődleges, előre rögzített teszt (AE latens, logisztikus C = 0.1): ΔAUC = +0.0287 [-0.0117; +0.0685], p = 0.2617, pozitív blokkok 5/6; AUC A = 0.6647, AUC B = 0.6934.

**A kép nem ad kimutatható inkrementális információt a skalár alap fölött** ezen a cellán: az első nem teljesülő feltétel a 3. (Permutációs p (nap-blokkos, 1 cella → Holm = nyers p): 0.2617).

**Érzékenység (nem döntési):** klaszter one-hot k = 10 (t919): ΔAUC +0.0131, p = 0.312, 3/6 blokk; klaszter one-hot k = 5: ΔAUC -0.0028, p = 0.574, 2/6 blokk. A gradiens-boosting érzékenység (AE): ΔAUC +0.0163.

**Mit jelent ez:** a ΔAUC pont-becslése (+0.0287) eléri a 0,02-es küszöböt, de a permutációs p (0.262) a 0.01 felett van és a bootstrap CI a nullát is tartalmazza, így az eredmény nem különböztethető meg a zajtól. A permutációs null szélesebb és pozitív irányba eltolt (átlag +0.0078, 99. percentilis +0.0983): a többlet-jellemzők zaj-illesztése is hoz pozitív összevont ΔAUC-ot, a megfigyelt értéknek ehhez képest kell kiemelkednie. **Blokk-szerkezet:** a ΔAUC a B5 blokkban a legnagyobb (+0.1240), a pozitív blokkok közül 2 gyakorlatilag nulla (≤ 0,005); a 5/6-os előjel-szám tehát gyengébb bizonyíték, mint amilyennek látszik. Érvényes NO-GO nem hiba (5200: erős skalár alap kockázat). **Korlátok:** egyetlen cella, 718 CV-esemény és 117 impulzus (blokkonként 15–26) — a blokkonkénti ΔAUC zajos; a logisztikus tanuló rögzített erősséggel fut; az embargó-érzékenység (2H) nem futott; az AE egy seeddel illesztett. A holdout-2 és az OOS érintetlen maradt.
