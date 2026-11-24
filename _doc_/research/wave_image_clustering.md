# Hullám-kezdetek kép-klaszterezése — előrejelző-erő gate (epic_072 / t921)

**Időszak:** 2026-11-19 → 2026-11-20 · **Asset:** SOLUSDT · **Cella:** W4h_H8h · **Státusz:** lezárt, nemleges. **Képkoncepció: NO-GO** (a 3. feltétel, a permutációs p bukott). A folytatásról a user dönt.

Riportok (UI Elemzések): `_doc_/reports/wave_event_dataset/` (t918 v1 → t925 v2), `_doc_/reports/wave_image_clusters/` (t919), `_doc_/reports/wave_predictiveness_gate/` (t920).
Módszertan és küszöbök: `_doc_/methodology_doc/5200_wave_start_image_model.md` (v2).

## Cél

A kérdés, hogy egy megerősített lokális mélypontból induló visszapattanás *képe* (alakja) előre jelzi-e, hogy **impulzus** következik-e. Ha igen, az új, a LightGBM quant modelltől független modell-koncepció alapja lehetne (Jiang–Kelly–Xiu 2023 kép-alapú megközelítése, de felügyelet nélküli klaszterezéssel indulva). A siker mércéje nem a kép nyers hasznossága, hanem az, hogy a **skalár alap fölött** többet ad-e, mint a zaj (epic_071 tanulsága).

## Módszer

- **Esemény:** `t0` lokális mélypont, ha `low[t0]` a minimum a `[t0 − B, t0 + W]` ablakban. Megerősítés `t_c = t0 + W`-kor, a jóslás csak innen indul. Érvényes, ha `A = log(close[t_c]/low[t0]) ≥ 1,5·σ_W`, ahol `σ_W` a t0 előtti 7 nap 1 perces hozamszórásából, `√W`-re skálázva. A jellemzők csak a `[t0 − B, t_c]` bar-okból épülnek.
- **v1 → v2 váltás és oka:** a v1 négy cellát (4h/4h, 4h/8h, 8h/8h, 12h/12h) és bináris címkét tartalmazott, az OOS volt a döntő. A t918 után a cellánkénti OOS csak 21–73 esemény volt (a 150-es minimum sehol sem teljesült), a 8h/12h cellák train-száma az 500-as minimum alatt maradt, és a minták 84–87%-a „oldalazás” volt (a W4h_H8h cellában 68%), ami elmosta a valódi hamis indulásokat. A user döntése (A opció, 2026-11-20): v2 = csak W = 4h, H ∈ {4h, 8h}; háromosztályos címke; elsődleges bizonyítás a train-en belüli purged CV; az OOS csak megerősítő előjel. A v2 szabályait az adatok és a modell-értékelés előtt rögzítettük, a küszöböket nem csökkentettük.
- **First-touch címke:** `P* = close[t_c]·exp(A)`; a `(t_c, t_c + H]` ablakban az 1 perces bar-okon az első elért szint dönt: **impulzus** (`high > P*` előbb), **hamis indulás** (`low < low[t0]` előbb), **oldalazás** (egyik sem). Ha egy baron mindkettő teljesül: hamis indulás (konzervatív). A train-en ez 0 / 720 esetben fordult elő. A fő teszt bináris (impulzus vs nem).
- **Nem-átfedés és split:** az események greedy időrendben, a teljes `[t0 − B, t_c + H]` intervallum nem fedheti egymást. Train 2021-01-01 → 2026-01-31, OOS 2026-02-01 → 2026-07-31 (champion valid), holdout-2 2026-08-01 → (érintetlen). Purge: az eseménynek teljes egészében egy splitben kell lennie.
- **6 blokkos purged CV (train):** egész napos blokkhatárok, purge a teljes intervallumra, plusz embargo `H` a teszt-blokk szélein. Modell A és B azonos foldokon.
- **Kép, AE, klaszterezés:** JKX-stílusú bináris kép a 15 perces prefixből (64 × 96 px, ár-panel ATR-egységben fix tengellyel + volumen-panel), konvolúciós autoencoder (torch CPU, 16 dim latens, seed 42), k-means a standardizált latensen, k ∈ {4…10} választása OOF log-lossal. Baseline: k-Shape és DTW k-means a nyers záróár-útvonalon. Minden illesztés (AE, k-means, tengely) foldonként, csak a fold-train eseményein.
- **Előre rögzített elsődleges teszt:** Modell A = `S0 + C` (A/σ_W, 1 órás sebesség, 3 napos előtrend, log σ_1m, esésmélység, volumen-arány); Modell B = A + AE latens (16 dim). Azonos tanuló: L2 logisztikus regresszió, rögzített C = 0,1, belső hangolás nincs. A döntés kizárólag az AE-n alapul, a többi (klaszter one-hot k = 10 / 5, C = 0,01 / 1, gradiens-boosting) érzékenység.
- **Permutációs próba:** nap-blokkos, N = 1000. Egy nap címke-vektora érintetlenül egy azonos esemény-számú napra kerül. Az AE és a k-means felügyelet nélküli, ezért foldonként egyszer illesztett; minden permutációban A és B (minden címke-függő komponens) újratanul ugyanazokon a foldokon, vagyis ez egzakt teljes refit. `p = (1 + #{null ≥ megfigyelt}) / (N + 1)`; 1 cella, így Holm = nyers p.
- **GO feltételei (5200 v2, sorrendben):** (1) minimum eseményszám; (2) ΔAUC ≥ 0,02; (3) permutációs p < 0,01; (4) ΔAUC > 0 legalább 4 / 6 blokkban; (5) OOS előjel (csak ≥ 100 egyedi t0 esetén).

## Eredmény

### t918 — v1 eseményszámok (train / OOS / holdout-2)

| Cella | Train | Impulzus / t0 alá / oldalazott (train) | OOS | Holdout-2 |
|---|---|---|---|---|
| W4h_H4h | 779 | 8,6% / 7,3% / 84,1% | 73 | 44 |
| W4h_H8h | 720 | 16,5% / 15,7% / 67,8% | 70 | 41 |
| W8h_H8h | 429 | 6,8% / 6,3% / 86,9% | 39 | 22 |
| W12h_H12h | 306 | 8,8% / 6,5% / 84,6% | 21 | 19 |

A train ≥ 500 csak a két 4h cellában teljesült, az OOS ≥ 150 egyikben sem (legjobb érzékenységi változat: 148). Az impulzus-arány az A/σ_W-vel erősen csökken (W4h_H4h Q1 15,4% → Q5 1,9%), mert `P*` az A-val nő; erős skalár alap volt várható.

### t925 — v2 eseményszámok

| Cella | Train | Impulzus / hamis indulás / oldalazás | OOS | Holdout-2 | CV-minimum (≥ 100 imp. és ≥ 100 nem-imp.) |
|---|---|---|---|---|---|
| W4h_H4h | 779 | 66 (8,5%) / 58 (7,4%) / 655 (84,1%) | 73 | 44 | **FAIL** (66 impulzus) → **NO-GO** |
| W4h_H8h | 720 | 117 (16,3%) / 115 (16,0%) / 488 (67,8%) | 70 | 41 | PASS (117 / 601) |

- A **W4h_H4h NO-GO** a 5200 szabálya szerint (összevont CV-ben 66 impulzus < 100), nem küszöb-csökkentés. A t919 és a t920 csak a W4h_H8h cellán futott, a Holm-család így 1 cella.
- **OOS:** a cella-összeg 143, de a két 4h cella t0-i szinte azonosak, ezért az **egyedi t0 szám 75 < 100**. A user döntése szerint az OOS-minimum az egyedi t0-ra vonatkozik; az OOS előjel-ellenőrzés így nem futott, a legjobb elérhető kimenet „feltételes GO” (CV-GO, OOS-megerősítetlen) lett volna.
- 6 CV-blokk, W4h_H8h teszt-események blokkonként 130 / 108 / 120 / 113 / 115 / 132; átlógó esemény összesen 2.

### t919 — klaszterek (W4h_H8h, csak train)

- Választott **k = 10** (AE + k-means): OOF log-loss 0,4366 vs prior 0,4458, ΔLL = −0,0092, 5 / 6 blokkban jobb a priornál. A minimum a tartomány szélén van, és k = 7 / 8 / 10 ΔLL-je 0,001-en belül van (zajszint). A legstabilabb partíció k = 5 (ARI 0,71, ΔLL −0,0057).
- Baseline: k-Shape ΔLL −0,0039, DTW k-means +0,0057. Az AE valamivel jobb, de a különbségek zajszintűek; a stabilitás hasonló (ARI 0,38–0,44).
- **A klaszterek főleg a visszapattanás méretét kódolják.** A magas A/σ_W mediánú klaszterek (C5–C9, 2,3–3,5) többnyire alacsony impulzus-arányúak (C7: 2,4%, C5: 6,3%, C6: 8,1%, C9: 8,0%; kivétel C8: 18,4%), a legmagasabbak C4 (24,6%) és C1 (23,1%); a train átlag 16,2%. Ez leíró, in-sample, nem bizonyíték, de pontosan azt jelzi, amit a t920-nak a skalár alap ellen kellett mérnie.

### t920 — előrejelző-erő teszt (döntő)

**Cella-döntés: NO-GO.** Feltételek: (1) teljesül (train 720, CV 718 esemény, min. blokk 108, 117 impulzus / 601 nem-impulzus); (2) ΔAUC +0,0287 ≥ 0,02 teljesül; (3) **p = 0,2617 ≥ 0,01 — NEM teljesül**; (4) 5 / 6 blokk pozitív, teljesül; (5) nem fut (OOS egyedi t0 = 75).

| Mutató | Érték |
|---|---|
| AUC A (S0 + C) | **0,6647** |
| AUC B (+ AE latens) | **0,6934** |
| ΔAUC | **+0,0287**, 95% nap-blokk bootstrap CI [−0,0117; +0,0685] |
| Permutációs p (N = 1000) | **0,2617** |
| Permutációs null | átlag +0,0078, 99. percentilis +0,0983 |

- **Blokkonkénti ΔAUC (B1..B6):** −0,0089, +0,0019, +0,0011, +0,0440, +0,1240, +0,0268. Az 5 / 6 gyengébb bizonyíték, mint amilyennek látszik: két blokk ≈ 0 (≤ 0,005), és a B5 (+0,124) hordozza az átlagot.
- **Skalár alap:** AUC 0,6647, nap-blokkos permutációs p = 0,001 (az elérhető minimum); log-loss 0,4184 vs prior 0,4458 (Δ −0,0274). Jól kalibrált (átl. becslés 0,158 vs megfigyelt 0,163; meredekség 1,26, enyhén alulmagabiztos). A B kalibrációs meredeksége 0,91, log-loss 0,4122, Brier 0,1284 vs 0,1301.
- **Érzékenység (nem döntési):**

| Változat | ΔAUC | Pozitív blokk | p |
|---|---|---|---|
| Klaszter one-hot k = 10 | +0,0131 | 3 / 6 | 0,312 |
| Klaszter one-hot k = 5 | −0,0028 | 2 / 6 | 0,574 |
| AE, C = 0,01 | +0,0457 | 3 / 6 | — |
| AE, C = 1 | +0,0240 | 4 / 6 | — |
| AE, gradiens-boosting | +0,0163 | 4 / 6 | — |

Az irány a reprezentációtól és a tanulótól függően ingadozik, robusztus jel nincs. A permutációs p-t csak a logisztikus C = 0,1 három reprezentációjára számoltuk.

## Verdikt

**Képkoncepció: NO-GO a W4h_H8h cellán** (3. feltétel bukott: p = 0,2617). A W4h_H4h cella már a t925-ben NO-GO volt, a W8h/W12h cellák a v2-ben kiestek. Az összesített döntés tehát NO-GO.

## Értelmezés

- A **skalár alap `S0 + C` valódi jelet hordoz**: AUC 0,665, permutációs p 0,001, jól kalibrált. A jel nagyrészt a visszapattanás relatív mérete (A/σ_W) és a volatilitás-kontextus: minél nagyobb a már megtett visszapattanás, annál kisebb az esély, hogy a még egyszer ekkora (`P*`) szint előbb jön, mint az új mélypont.
- A **kép ezen felül semmit nem ad, ami a zajtól megkülönböztethető**. A +0,029 ΔAUC kisebb, mint a permutációs null 99. percentilise (+0,098), a CI tartalmazza a nullát, és a null átlaga is pozitív (+0,008), mert a 16 többlet-jellemző zajillesztése is hoz némi ΔAUC-ot. A klaszterek is elsősorban a skalár `A/σ_W`-t másolják le.
- Az 5 / 6 blokk-előjel nem mentő érv: egy blokk hordozza az átlagot, az érzékenységi változatok előjele és blokk-aránya ingadozik.
- A tanulság megegyezik az epic_071-ével: a nyers vagy leíró különbségek (klaszterenkénti impulzus-arányok) félrevezetők, amíg nincs azonos szerkezetű skalár baseline és permutációs null.

## Korlátok

- Egyetlen asset (SOLUSDT), egyetlen paraméterkészlet (W = 4h, B = 4h, szorzó 1,0, küszöb 1,5σ_W).
- Kis minta: 117 impulzus a W4h_H8h train-en, blokkonként 15–26 impulzus; a blokkonkénti ΔAUC zajos.
- Egyetlen AE seed (42) és egyetlen architektúra; a seedek közti szórás nem mért.
- Az embargo `2H` érzékenység nem futott.
- Az OOS nem lett felhasználva (egyedi t0 = 75 < 100), így nincs független megerősítés — de a NO-GO-t az OOS nem is tudná megfordítani.
- A holdout-2 érintetlen: jelenleg 44 egyedi esemény (cella-összeg 85); tiszta próbához kevés.
- A felügyelet nélküli klaszterezés k-választása a tartomány szélén van és zajszintű.

## Lehetséges folytatás (opciók, nem döntött)

- **(a) Lezárás:** a képkoncepció NO-GO, az epic negatív eredménnyel zárul. Ez az alapértelmezés.
- **(b) A skalár visszapattanás-modell (S0 + C) mint jelölt gate / feature a fw60 LGBM-hez.** Az alap valós jelet hordoz, de külön, előre rögzített tesztet igényel: baseline (a champion saját feature-jei), permutáció, és tiszta OOS. A **holdout-2 érintetlen és továbbra is elérhető**; ide elég esemény gyűlésekor tehető a tiszta próba (most 44 egyedi esemény, ez kevés). A champion valid-ablakát (2026-02 → 2026-07) a champion már használta, az itteni OOS tehát nem teljesen tiszta.

## OOS leíró kiértékelés (post-hoc)

*Leíró, post-hoc, egyszeri kiértékelés a user kérésére (ticket: `pr_t928_oos_descriptive_eval.md`, epic_072). Nem gate-eredmény: az OOS ≥ 100 egyedi t0 feltétel nem teljesül (75), a NO-GO döntés változatlan. Szabályok az OOS megtekintése előtt rögzítve: A/B modell egyszer illesztve az összes train eseményen (C = 0,1), küszöb = train-OOF 80. / 90. percentilis, kvintilis-vágópontok train-OOF-ból. Holdout-2 nem töltődött be.*

W4h_H8h, OOS: 70 esemény, 10 impulzus (14,3%), 8 hamis indulás, 52 oldalazás.

| Mutató | Train OOF | OOS |
|---|---|---|
| AUC A (S0+C) | 0,665 [0,615; 0,716] | 0,717 [0,518; 0,895] |
| AUC B (+AE) | 0,693 [0,644; 0,741] | 0,695 [0,490; 0,873] |
| ΔAUC (B−A) | +0,029 [−0,012; +0,069] | −0,022 [−0,142; +0,103] |
| A top 20%: jelzés / találat / precizitás / lift / recall | 144 / 39 / 27,1% / 1,66× / 33% | 13 / 5 / 38,5% / 2,69× / 50% |
| B top 20%: jelzés / találat / precizitás / lift / recall | 144 / 45 / 31,2% / 1,92× / 38% | 17 / 6 / 35,3% / 2,47× / 60% |
| A top 10%: jelzés / találat / precizitás | 72 / 23 / 31,9% | 8 / 4 / 50,0% |
| B top 10%: jelzés / találat / precizitás | 72 / 22 / 30,6% | 10 / 4 / 40,0% |

A felső score-kvintilis impulzus-aránya OOS-on A: 38,5% (5/13), B: 35,3% (6/17) (train 27% / 31%). A kép OOS-on sem ad többet a skalár alapnál (ΔAUC negatív, CI ±0,12). Kis minta (10 impulzus, 5–6 találat), széles CI-k: irányjelző, nem bizonyíték. A W4h_H4h másodlagos cella (73 esemény, 6 impulzus) hasonló képet ad (AUC A 0,749, B 0,759); a teljes táblák a ticketben.

## Újragenerálás

Repo gyökérből:

- t918/t925 adatkészlet + riport: `uv run python -m reports.builders.wave_event_dataset` (alap v2, ~25 s; `--version v1` a v1-hez, `--no-store`, `--no-sensitivity`). Tábla: `database/solusdt/solusdt_lab.duckdb` → `research."wave_events_v2"` (1815 sor, hash `2c9934c5a11abb60`), v1: `wave_events_v1` (2693 sor, hash `f258c528a0e36273`).
- t919 klaszterek: `uv run --group research python -m reports.builders.wave_image_clusters` (~26 perc CPU, a DTW dominál).
- t920 gate: `uv run --group research python -m reports.builders.wave_predictiveness_gate` (per-fold AE + k-means 434 s egyszer, gyorsítótárból ~136–154 s; a permutációs null ~100 s).

Kód: `src/reports/builders/wave_events.py` (detektálás, címke, split, CV), `wave_image_model.py` (kép, AE, klaszter, `fit_fold`), `wave_event_dataset.py`, `wave_image_clusters.py`, `wave_predictiveness_gate.py`; tesztek: `src/reports/tests/test_wave_events.py`, `test_wave_image_clusters.py`, `test_wave_predictiveness_gate.py`. A függőségek a `research` csoportban vannak (`torch` CPU, `tslearn`; `uv sync --group research`).
