---
epic: epic_072
id: t919
title: Generálj prefix-képeket, tanulj embeddinget és klaszterezz
assignee: analyst_agent
status: done
blocks: [t920]
blocked_by: [t916, t918, t925]
---

## Goal
Prefixekből képek (Jiang–Kelly–Xiu stílus: OHLC bar + volumen, fekete-fehér pixelmátrix), konvolúciós
autoencoder embedding **csak train adaton**, kis k-jú klaszterezés (k ∈ 4..10, választás train-en belüli CV-vel).
Baseline: k-Shape / DTW k-means a nyers normalizált útvonalon. Riport: klaszter-centroid képek, klaszterenkénti
kimenet-tölcsér (p10–p90 kúp) vs feltétel nélküli kúp.

## Acceptance Criteria
- [ ] Embedding és klaszter illesztés kizárólag train-en; OOS-ra csak transform/assign
- [ ] Riport `_doc_/reports/wave_image_clusters/` centroidokkal és tölcsérekkel

## Notes
- 2026-11-20 (orchestrátor, user döntés): csak W4h_H8h cella, forrás `research."wave_events_v2"`; train-en belüli kiértékelés a 6 blokkos purged CV-vel (`cv_fold`).

**2026-11-20 — analyst_agent (kész, a ticket nincs átnevezve; W4h_H8h, csak train)**

Fájlok:
- `src/reports/builders/wave_image_model.py` (új, könyvtár): `build_prefixes` (prefix, look-ahead guard `prefix_end <= t_c`), `render_images` (JKX-stílusú bináris kép, 3 px/15m bar, 64 × 96 px, ár-panel 48 sor ATR-egységben FIX tengellyel + volumen-panel 15 sor), konvolúciós AE (torch CPU, 1 szál, seed 42, determinisztikus), `fit_embedding` / `fit_clusterer` / `assign`, **`fit_fold`** (a t920 számára: AE + k-means egy purged/embargózott fold TRAIN eseményein, a teszt-fold csak transzformálódik; minden `fit_*` elutasít nem-train sort), k-Shape / DTW baseline (`fit_raw_clusterer`), `select_k` (6 blokkos purged CV, OOF log-loss + stabilitás).
- `src/reports/builders/wave_image_clusters.py` (új): riport-builder; CLI `uv run --group research python -m reports.builders.wave_image_clusters`. A train-sorokat SQL-szűréssel tölti (`load_events(split="train")`), az 1m árfolyamot a train végén levágja: OOS / holdout-2 sor és bar sosem kerül a memóriába.
- `src/reports/tests/test_wave_image_clusters.py` (új, 12 teszt; `importorskip` torch/tslearn, `uv run --group research pytest ...`).
- Riport: `_doc_/reports/wave_image_clusters/` (manifest + report.md + 9 chart + 7 tábla; a scratchpadben épült, majd bemásolva, mint a t925-nél).

Módszer-döntések: ár-tengely fix (0 … 17,16 ATR, train 99,5. pct), így a rebound mérete megmarad a képen; AE 3 conv réteg (8/16/32 csatorna) → 16 dim latens, BCE pos-súllyal, 30 epoch; k-means (n_init 10) a standardizált latensen; k-választás az 5200 szerint: OOF log-loss, prediktor = klaszter-impulzusarány a fold-train prior felé zsugorítva (erősség 20). Determinizmus: threadpool 1 szál (k-means, k-Shape), k-Shape hozzárendelés saját SBD-vel (a tslearn `predict` nem bitre-reprodukálható).

Eredmények (train 720 esemény, 718 OOF teszt-esemény; fold-train 586–610):
- **Választott k = 10** (AE + k-means, 5200 argmin szabály): OOF log-loss 0,4366 vs prior 0,4458 → ΔLL = −0,0092, 5/6 blokkban jobb a priornál; ARI 0,38, párosított Jaccard 0,44. Figyelem: a minimum a tartomány szélén van, k = 7 / 8 / 10 ΔLL-je ≤ 0,001-en belül (−0,0083 / −0,0083 / −0,0092) → zajszint. A legstabilabb AE-partíció k = 5 (ARI 0,71, ΔLL −0,0057, 5/6 blokk). A t920-nak érdemes érzékenységi futást adni k = 5-ön is.
- Klaszter-méretek (k = 10): 119 / 117 / 112 / 74 / 69 / 63 / 62 / 41 / 38 / 25.
- Train osztályarányok klaszterenként (impulzus / hamis indulás / oldalazás, **leíró, in-sample, nem bizonyíték**): C0 21,8 / 16,0 / 62,2; C1 23,1 / 13,7 / 63,3; C2 15,2 / 19,6 / 65,2; C3 14,9 / 16,2 / 68,9; C4 24,6 / 30,4 / 44,9; C5 6,3 / 7,9 / 85,7; C6 8,1 / 11,3 / 80,6; C7 2,4 / 2,4 / 95,1; C8 18,4 / 23,7 / 57,9; C9 8,0 / 12,0 / 80,0 (train átlag: 16,2 / 16,0 / 67,8). A magas A/σ_W mediánú klaszterek (C5–C9: 2,3–3,5) a legalacsonyabb impulzus-arányúak → a klaszterezés nagyrészt a rebound méretét (S0 `A/σ_W`) különíti el, ezt a t920-nak a skalár alap ellen kell mérnie.
- Baseline (k = 10, ugyanaz a CV): AE ΔLL −0,0092 (ARI 0,38); k-Shape −0,0039 (ARI 0,44; saját legjobb k = 9: −0,0080); DTW k-means +0,0057 (ARI 0,39; saját legjobb k = 4: −0,0019). Az AE valamivel jobb az OOF log-lossban, de a különbségek kicsik és zajszintűek; stabilitásban nincs lényeges eltérés (ARI 0,38–0,44).
- Futási idő (CPU, 1 szál, foglalt gép): teljes 1567 mp (k-választás 6 fold × AE + 7 k + baseline-ok 1449 mp, ebből a DTW dominál; végső AE + k-means 82 mp).
- Kúp: A-egységben (+1 = P*, −1 = low[t0]); feltétel nélkül a H végén a medián-érték a riport 4. szekciójában (cones chart, `cluster_summary` tábla).

Ellenőrzés: `uv run ruff check src/reports` OK, `uv run --group research pyright src/reports` 0 hiba, `uv run --group research pytest src/reports/tests` 75 passed (12 új). Riport: manifest érvényes (`reports.schema.Manifest`), nincs ≥ 2026-02-01 dátum a tartalomban.

**[validator] Validated - 2026-11-20 - PASS.** ruff OK, pyright 0 errors, pytest src/reports 75 passed (12 new ran, not skipped). Leakage: train-only SQL load, 1m truncated at train end, `_require_train` in every fit_*, fit_fold fits AE + k-means on cv_fold train only; render axis (`fit_render`, 99.5 pct price / 99 pct volume) is computed inside `fit_embedding` on the fit set, so it is fold-safe (the 17.16 ATR constant applies only to the final all-train model; no fold leak). k-selection by OOF log-loss with labels is exactly what 5200 prescribes (train-CV only, stability reported) - no mismatch. Report: manifest valid, all 16 referenced files exist, none stale, no dates >= 2026-02-01 except generation date, class shares labelled descriptive. Doc fix: 8330_reports.md now lists wave_image_clusters, wave_image_model and test_wave_image_clusters.py.
For t920: use fit_fold/fit_embedding per fold (do not reuse the fixed 17.16 constant from the final model); k=10 minimum is at range edge and k=7/8/10 are within 0.001 LL, so add a k=5 sensitivity run (most stable, ARI 0.71); beat the scalar S0 baseline (clusters mostly encode rebound size A/sigma_W); the validated run did not re-run the 26 min build.
