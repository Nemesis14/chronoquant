---
epic: epic_073
id: t931
title: Közös benchmark keret (CV, metrikák, kereskedési szimuláció)
assignee: analyst_agent
status: done
blocks: [t932, t933, t934, t935]
blocked_by: [t929]
---

## Goal
Újrahasznosítható keret a `src/reports/builders/` alatt: közös foldok (purge + embargo), modell-interfész
(fit / predict_proba 3 osztályra), metrikák az 5300 szerint, triple-barrier PnL-szimuláció költséggel,
leaderboard tábla (lab DB `research` séma + riport). Baseline: skalár logit (S0+C) regisztrálva.

## Acceptance Criteria
- [x] Tesztek: fold-szivárgás, determinizmus, metrikák helyessége (`src/reports/tests/test_scout_bench.py`, 17 teszt)

## Notes

**Állapot:** kész, `todo_` marad (a rename az orchestrátoré). Nincs commit.

**Kód:** `src/reports/builders/scout_bench/` csomag (`frame`, `folds`, `models`, `metrics`, `stats`, `trades`, `runner`, `report`); teszt: `src/reports/tests/test_scout_bench.py`.
Újrahasznosítva az epic_072-ből: `wave_events.cv_block_bounds` / `to_minute_grid` / `_epoch_min`, `wave_oos_eval.assert_no_holdout`; a permutáció / bootstrap az 5300 szerint multi-asset-re újraírva (a `wave_predictiveness_gate` változata egy-assetes).

**Modell regisztrálása (t932-t935):**
```python
from reports.builders import scout_bench as sb
class M(sb.ScoutModel):
    def fit(self, train, prefix_data, *, seed): ...        # train: polars keret (label_class, tau_min, S0+C, ... , row_id)
    def predict_proba(self, df, prefix_data): ...          # (n, 3) CLASSES = impulse / false_start / sideways
sb.REGISTRY.register(sb.ModelSpec("scout_lgbm_r1", M, family="P1", hypothesis="H1", round=1, config={...}))
res = sb.run_benchmark(frame, sb.REGISTRY, bars, n_perm=1000); sb.persist(res)
```
- `hypothesis` H1/H2/H3 = előre regisztrált elsődleges (Holm család); csak `round=1` és hipotézisenként egy modell (epic döntés 2026-11-21: a 2. kör nyertesei csak SPA-val); `hypothesis=None` = kísérleti. A `config` hash-elődik (`config_hash`).
- `prefix_data` átlátszatlan objektum, a keret változtatás nélkül adja át; indexkulcs a keret `row_id` oszlopa (pozíció a rendezett, használható eseményeken). Véletlen csak a `seed`-ből.
- A bináris score alapból `proba[:, 0]`; felülírható: `ScoutModel.score`. Hazard modellnél a `tau_min` a tanító kereten elérhető (a permutációban együtt mozog a címkével).
- Keret: `sb.load_wave_events_v2()` (SOL adapter) vagy `sb.load_scout_events()` (t930 `scout_events_v1`; `a_variant` szűrés `a15`; a tábla saját string `event_id`-ját nem bántja). Oszlop-szerződés: `sb.REQUIRED_COLS`. Extra jellemző-oszlopok (order flow) a keretben maradnak, a modell használja.
- A permutáció a primaryk teljes refitjét futtatja: olcsó, gyors modellt igényel (a baseline N=1000 ~35 s SOL-on).
- Egyetlen futás eredménye a lab DB `research."scout_leaderboard_v1"` (append, `run_id`) + `__runs`; riport: `report.build(res, root)` / `python -m reports.builders.scout_bench.report`.

**Baseline-reprodukció (SOL W4h_H8h, 718 CV-esemény / 117 impulzus, 6 blokk, embargó 8 h, fold-hash `8f49999ae1b48a33`):** pooled OOF AUC **0,6647** (t920 Modell A: 0,6647; tűrés ±5e-4 → rendben). Felső 20% (fold-train 80. percentilis): precizitás 0,305 / lift 1,87 (128 jelzés); felső 10%: 0,351 / 2,15 (57). 3-oszt. log-loss 0,8285; kalibráció meredekség 1,26. Baseline AUC nap-csomagos permutációs p = 0,0010 (N = 1000, teljes refit; a minimum).
**Baseline szimuláció (költségek `config/strategies.json`: maker 2 / taker 5 / csúszás 1,5 bp):** felső 20%: +18,3 bp/esemény, CI [-29,8; +71,0], hit 30,5% (break-even 26,4%); felső 10%: +42,2 bp, CI [-33,2; +120,6], hit 35,1% (break-even 25,6%); szelekció nélkül (718): +21,0 bp, CI [-3,7; +45,5], hit 16,3% (break-even 12,0%). Tehát a CI alsó határa mindhárom sorban negatív, és a baseline szelekció nem javít a szelekció nélküli referencián (az AUC-többlet itt nem jelenik meg nettó hozamként). Same-bar kettős érintés: 0 az eseményeken.

**Döntések / feltételezések (t929-hez illeszkedő, megjelölve):**
- Felső-q% küszöb = a fold saját tanító halmazán számolt score-percentilis (in-sample; a ticket "küszöb a train foldból"); flexibilis modelleknél túlillesztett train-score magasabb küszöböt adhat → a jelzésszám eltérhet 20% / 10%-tól, ezért a leaderboard `n_top20` / `n_top10` oszlopot is közöl.
- Baseline 3-osztályos valószínűség: P(impulzus) bináris logitból, a maradékot a fold-train P(false_start | nem impulzus) állandóval osztja (a bináris score így azonos a t920-szal).
- Szimuláció kitöltés: cél = max(P*, bar nyitó) (maker), stop = min(low[t0], bar nyitó) (taker + csúszás), időkorlát = az ablak utolsó bar-jának záróára; belépő taker + csúszás; az entry bar is az ablak része; funding nincs.
- SPA: nap-összegzett bináris log-loss különbség (baseline mínusz modell), stacionárius bootstrap, várható blokk 5 nap, 5000 újramintavétel (5300 feltételezés); a futásban csak a baseline van → SPA nincs, a gép szintetikus adaton tesztelt (zaj: p nagy; valódi többlet: p < 0,02).
- Permutáció: azonos assetenkénti szerkezetű napok cseréje; a magányos napok összes-darabszám szerinti tartalék-csoportja; ha ott sincs pár, a nap címkéi fixek. A tesztelt egy-assetes SOL-on a szerkezet = napi darabszám.
- Pyright: a builder fájlok tetején a repo más fájljaihoz hasonló lazítás (pandas/polars típuszaj).
- A `scout_events_report.py` / `test_scout_events.py` (t930, párhuzamosan készül) ruff/pyright hibái nem ehhez a tickethez tartoznak.

**Ellenőrzés:** `ruff check src/reports` a scout_bench fájlokra tiszta (a t930 `scout_events_report.py`-ban 2 saját hiba van), `pyright src/reports/builders/scout_bench` 0 hiba, `pytest src/reports/tests` 123 passed (17 új). Riport: `_doc_/reports/scout_leaderboard/` (manifest, 3 chart, 6 tábla). Lab DB: `research."scout_leaderboard_v1"` 1 sor (baseline, run `19aa261212b9`), `__runs` 1 sor. Dokumentáció: `8330_reports.md`, `0002_data_architecture.md`.

[validator] Validated - 2026-11-21: ruff clean, pyright 0 errors, `pytest src/reports/tests` 123 passed. Folds: cross-asset purge/embargo verified on the real 3-asset t930 frame (no train interval within H of any test block; 6 shared blocks); baseline re-run AUC 0.6647 (== t920), top20 prec 0.305 / n 128; trade sim hand-checked on 3 real SOL events (tp / sl / timeout, net matches to 1e-12); SPA matches Hansen (recentring mu_c/mu_l/upper, omega from bootstrap) and detects a synthetic superior model (p=0.0) vs noise (p=0.92); `load_scout_events()` loads 2175 a15 events (SOL/BTC/ETH), REQUIRED_COLS satisfied, sideways tau_min censored at H=480 as in the harness.
Recommendation (non-blocking): top-q thresholds come from fold-train scores (no OOF leakage), but flexible models over-fit train scores -> inflated threshold, n_top below 20/10%. Before t932-t935 comparisons add a threshold-matched view: rank-based top-q within each test fold (label-free) or inner-CV OOF train scores; keep n_top columns.
