---
epic: epic_072
id: t920
title: Mérd ki a kép/alak előrejelző erejét a skalár alapmodellhez képest
assignee: analyst_agent
status: done
blocks: [t921]
blocked_by: [t919]
---

## Goal
Klaszter ↔ címke mutual information permutációs teszttel; skalár alapmodell vs alap + klaszter/embedding modell
(AUC, train-en CV, OOS-on fix); White Reality Check a (W,H) rács és a reprezentációk többszörös tesztelésére.

## Acceptance Criteria
- [ ] Go/no-go a t917 küszöbei szerint, (W,H)-ként
- [ ] Riport `_doc_/reports/wave_predictiveness_gate/`

## Notes
- 2026-11-20 (orchestrátor, user döntés): csak W4h_H8h; OOS-minimum egyedi t0-ra (75 < 100) → OOS előjel-ellenőrzés nem fut, legjobb eredmény "feltételes GO".

**2026-11-20 — analyst_agent (kész, a ticket nincs átnevezve; W4h_H8h, csak train; a Goal v1-es szövege helyett az 5200 v2 go/no-go a mérvadó)**

Fájlok:
- `src/reports/builders/wave_predictiveness_gate.py` (új): per-fold label-független tervezési adat (`prepare_folds` = `wave_image_model.fit_fold`, AE + k-means csak fold-train-en, gyorsítótárazható), címke-függő rész (`oof_predictions`), nap-blokkos permutáció (`permute_day_blocks`, `permutation_null`), nap-blokk bootstrap, kalibráció, `evaluate_gate` (5200 v2 feltételek sorrendben); CLI `uv run --group research python -m reports.builders.wave_predictiveness_gate`.
- `src/reports/tests/test_wave_predictiveness_gate.py` (új, 11 teszt): fold-train-only illesztés (spy a `fit_embedding`-en, a design adat címke-független, purge/embargó), permutáció megőrzi a nap-blokk szerkezetet és a címke-összeget, az OOF nem használja a teszt-fold címkéit, null == explicit teljes refit, determinizmus, cache, gate-logika, riport.
- Riport: `_doc_/reports/wave_predictiveness_gate/` (manifest + report.md + 4 chart + 6 tábla; a scratchpadben épült, majd bemásolva).

Előre rögzített terv (az eredmények megtekintése előtt): Modell A = S0 + C, Modell B = + reprezentáció; azonos tanuló: L2 logisztikus, rögzített C = 0,1 (belső hangolás nincs). **Elsődleges (döntési) = AE latens (16 dim)** (az 5200 nem nevez meg reprezentációt → alapértelmezés). Érzékenység (nem döntési): klaszter one-hot k = 10 és k = 5, C = 0,01 / 1, gradiens-boosting (csak pont-becslés). Permutáció: N = 1000, egy nap címke-vektora érintetlenül egy azonos esemény-számú napra kerül; az AE / k-means felügyelet nélküli → per-fold egyszer illesztve, minden permutációban A és minden B (az összes címke-függő komponens) újratanul ugyanazokon a foldokon (egzakt teljes refit).

**Cella-döntés: NO-GO** (W4h_H8h). A 3. feltétel bukik (permutációs p), az 5. (OOS) nem fut (egyedi OOS t0 = 75 < 100).
- Feltételek sorrendben: (1) minimum eseményszám teljesül (train 720; 718 CV-esemény, min. blokk 108, 117 impulzus / 601 nem-impulzus); (2) ΔAUC = **+0,0287** ≥ 0,02 teljesül; (3) **p = 0,2617** (N = 1000, nap-blokkos, 1 cella → Holm = nyers p) ≥ 0,01 **NEM teljesül**; (4) blokk-előjel 5 / 6 teljesül; (5) OOS-ellenőrzés nem fut.
- Összevont OOF AUC: A (S0 + C) = **0,6647**, B (+ AE) = **0,6934**; ΔAUC 95% nap-blokk bootstrap CI = [−0,0117; +0,0685] (tartalmazza a nullát). Permutációs null: átlag +0,0078, 99. percentilis +0,0983 (a 16 többlet-jellemző zajillesztése is pozitív ΔAUC-ot ad).
- Blokkonkénti ΔAUC (B1..B6): −0,0089, +0,0019, +0,0011, +0,0440, +0,1240, +0,0268 → 5 / 6 pozitív, de két blokk ≈ 0 (≤ 0,005), és a B5 (+0,124) hordozza az átlagot (az 5/6 gyengébb bizonyíték, mint amilyennek látszik).
- Skalár alap erőssége: AUC A = 0,6647 (nap-blokkos permutációs p = 0,001, a minimum), log-loss 0,4184 vs prior 0,4458 (Δ = −0,0274) → az S0 + C érdemben előrejelez; a kép ezt nem haladja meg szignifikánsan.
- Kalibráció (5 kvantilis-bin): A meredekség 1,26 (alulmagabiztos), B 0,91; log-loss A 0,4184, B 0,4122; Brier A 0,1301, B 0,1284; mindkettő jól kalibrált (átl. becslés 0,158 vs megfigyelt 0,163).
- Érzékenység (nem döntési): klaszter k = 10: ΔAUC +0,0131, p = 0,312, 3/6 blokk; klaszter k = 5: ΔAUC −0,0028, p = 0,574, 2/6 blokk; AE C = 0,01: +0,0457 (3/6), C = 1: +0,0240 (4/6); AE gradiens-boosting: +0,0163 (4/6). Az irány a reprezentációtól / tanulótól függően ingadozik, nincs robusztus jel. A permutációs p-t csak a logisztikus C = 0,1 három reprezentációjára számoltam; a döntés kizárólag az AE-n alapul.
- Futási idő (CPU, 1 szál, foglalt gép): per-fold AE + k-means (6 fold, k = 5 és 10) 434 mp egyszer (gyorsítótárba mentve), permutációs null (N = 1000, 6 fold × A + 3 B) ~101–108 mp, bootstrap + érzékenység ~5 mp; a gyorsítótárból futó teljes build ~136 mp.
- Nem futott: embargó-érzékenység (2H); OOS-előjel (feltétel nem teljesül); több AE-seed.

Ellenőrzés: `uv run --group research ruff check src/reports` OK, `pyright src/reports` 0 hiba, `pytest src/reports/tests` 86 passed (11 új). Manifest érvényes; a tartalomban nincs ≥ 2026-02-01 dátum a generálás dátumán kívül. Az OOS és holdout-2 sorok nem kerültek betöltésre (`load_events(split="train")`, 1m árfolyam a train végén levágva).

**[validator] 2026-11-20 — PASS.** ruff OK, pyright 0 hiba, pytest src/reports 86 passed (11 új lefutott, nincs skip). Szivárgás-audit: csak train, per-fold AE/k-means/standardizálás fold-train-en, A+B minden permutációban újratanul, nap-blokkos permutáció (azonos méretű napok közt), p=(1+#)/(N+1), nap-blokk bootstrap; az elsődleges AE a modul docstringben előre rögzítve; feltételek 5200 sorrendben; NO-GO (p=0,2617) helyes. Gyorsítótárból újrafuttatva (~154 mp): AUC_A 0,6647, AUC_B 0,6934, ΔAUC +0,0287, p 0,2617 reprodukálódik. Riport manifest érvényes, nincs elavult fájl/dátum. Doc: 8330_reports.md frissítve (builder + teszt).
