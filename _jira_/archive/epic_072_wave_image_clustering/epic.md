# Epic 072: Hullám-kezdetek kép-klaszterezése — új modell-koncepció (SOLUSDT)

**Created:** 2026-11-19
**Status:** OPEN (koncepció / gate kutatás)

## Goal
Feltáró kutatás: egy megerősített lokális mélypontból induló visszapattanás *képe* (alakja) előre
jelzi-e, hogy **impulzus** vagy **korrekció** következik? Ha igen, ez egy új, a LightGBM quant
modelltől független modell-koncepció alapja. Jiang–Kelly–Xiu (2023) kép-alapú megközelítés,
de nem felügyelt klaszterezéssel indulva.

## Scope
- Dependency: `torch` (+ esetleg `tslearn`) — code_doc_agent
- Kutatási kód / builder: `src/reports/builders/` + `_doc_/reports/` és `_doc_/research/wave_image_clustering.md`
- Módszertan: `_doc_/methodology_doc/`

## Tasks
- t916: Függőségek (torch, tslearn) felvétele (code_doc_agent)
- t917: Módszertan — esemény, címke, képformátum, go/no-go (methodology_agent)
- t918: Esemény-adatkészlet: lokális mélypontok, prefixek, címkék (analyst_agent)
- t919: Képgenerálás + autoencoder embedding + klaszterezés, DTW/k-Shape baseline (analyst_agent)
- t920: Előrejelző-erő teszt: skalár alap vs kép modell, permutáció, OOS (analyst_agent)
- t921: Kutatási összefoglaló + go/no-go a modell-koncepcióra (analyst_agent)
- t924: Módszertan v2 (5200) — 4h rács, 3-osztályos first-touch címke, train CV (methodology_agent)
- t925: wave_events_v2 adatkészlet + riport (analyst_agent)
- t926: research séma a 0002 doksiban (code_doc_agent)

Sorrend: t916 ∥ t917 → t918 → (t924 ∥ t926) → t925 → ⛔ → t919 → t920 → t921

## Tanulságok az epic_071-ből (kötelező)
- **Baseline/placebo kötelező:** a nyers arányok (pl. "66% bounce") félrevezetők; minden jelet azonos szerkezetű véletlen/skalár baseline-hoz kell mérni.
- **OOS-tisztaság:** a leíró/vizuális riportok (t918) csak a **train** időszakból mutassanak példákat — az OOS-t csak a t920 nézze.
  OOS határ: a t917 rögzítse és indokolja (a champion train 2021-01 → 2026-01, valid 2026-02 → 2026-07 a `config/models.json` szerint).
- **⛔ Vizuális ellenőrzőpont:** a t918 után a user az UI Elemzések oldalán megnézi a detektált mélypont-eseményeket és példa-képeket, mielőtt a t919 (embedding/klaszterezés) indul.

## Key Decisions
- **2026-11-20 (t920 eredmény, W4h_H8h): képkoncepció NO-GO.** AUC_A (S0+C) 0,6647, AUC_B (+AE) 0,6934, ΔAUC +0,0287 (CI [−0,0117; +0,0685]), nap-blokkos permutációs p = 0,2617 ≥ 0,01 → a 3. feltétel bukott; OOS-ellenőrzés nem futott (egyedi t0 75 < 100). A skalár alap (S0+C) valódi jelet hordoz (perm. p 0,001), a kép nem ad többet. Összefoglaló: `_doc_/research/wave_image_clustering.md` (t921). Az epic státuszát a user dönti; holdout-2 érintetlen.
- **2026-11-20 (user, v2 vizuális ellenőrzőpont jóváhagyva):** a W4h_H4h cella NO-GO (66 train impulzus < 100) → a t919/t920 csak a **W4h_H8h** cellán fut.
  Az OOS-minimum az **egyedi t0** számra vonatkozik (75 < 100) → az OOS előjel-ellenőrzés nem fut; a legjobb elérhető eredmény "feltételes GO" (CV-GO, OOS-megerősítetlen). Tiszta próba később a holdout-2-n (most 44 egyedi esemény).
- **2026-11-20 (user, A opció — v2):** a t918 után cellánként csak 21–73 OOS esemény, a 8h/12h cellák a train-minimum alatt.
  v2: csak W=4h, H∈{4h,8h}; háromosztályos first-touch címke (impulzus / hamis indulás / oldalazás), a fő teszt bináris;
  elsődleges bizonyítás purged blokkolt CV a train-en (6 blokk); OOS csak megerősítő előjel (≥100 **egyedi t0** esemény; a 2026-11-20 döntés pontosította, nem cella-összeg); holdout-2 érintetlen.
- **2026-11-19 (t917 nyitott pontok, orchestrátor default, user felülbírálhatja):** OOS = 2026-02-01→2026-07-31 (champion valid),
  holdout-2 2026-08-01→ érintetlen; Holm 8 cellán elsődleges + White RC másodlagos; min. eseményszám 500 train / 150 OOS
  (≥50 pos/neg) feltételezés, t918 után felülvizsgálva; permutáció 1000 teljes refit (ha túl drága → OOS-only, jelezve); doksi: `5200`.
- **Nem átfedés:** a teljes `[t0 − B, t_c + H]` esemény-intervallumra (validátor javaslat), hogy egy esemény kép-ablaka se nyúljon az előző címke-ablakába.
- Esemény: t₀ lokális mélypont, ha low[t₀] = min a [t₀−B, t₀+W] ablakban; **csak t_c = t₀+W-kor ismert**, jóslás t_c-től.
- Min. visszapattanás t_c-ig: ≥ 1.5·σ_W (W-re skálázott volatilitás).
- (W, H) rács előre rögzítve: (4h,4h), (4h,8h), (8h,8h), (12h,12h).
- Kezdő címke bináris: impulzus = H-n belül ár > close[t_c]·exp(1.0·A), ahol A = log(close[t_c]/low[t₀]); különben korrekció
  (al-jelölés: t₀ alá esett / oldalazott — későbbi 3. osztályhoz).
- Siker = a kép/alak modell szignifikánsan többet tud, mint a skalár alapmodell (A/σ, sebesség, előzmény-trend, vol), OOS-on is.
- Kapcsolódik: epic_065 (klaszter tanulságok), `_doc_/research/elliott_waves/`, epic_071 (S/R távolság opcionális bemenet).
