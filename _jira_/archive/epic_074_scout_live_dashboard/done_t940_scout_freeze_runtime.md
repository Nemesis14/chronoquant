---
epic: epic_074
id: t940
title: Scout modell befagyasztása (artifact) + futtató modul
assignee: modeling_agent
status: done
blocks: [t941, t942]
blocked_by: []
---

## Goal
Az S0+C baseline (scout_bench baseline spec, a15, SOL+BTC+ETH train split) betanítása egyszer, és mentése `artifacts/scout_s0c_r1/` alá: model.pkl, manifest.json (feature lista, train range, fold hash, train OOF AUC), küszöbök (top-20%/10% a train OOF score-ból), szabályok (default: P*/low[t0]/8h; R2_03: +3σ_W/−1,5σ_W/8h), költségek. Futtató modul (pl. `src/scout/`): inkrementális eseményfelismerés (v2 definíció, ugyanaz a kód mint `wave_events`/`scout_events`) egy asset 1m OHLCV-jén, score a befagyasztott artifacttal, kimenet-feloldás (first-touch) amikor a H ablak lezárult. Kiindulás: `src/reports/builders/scout_live_score.py` (t938) — a duplikációt kerüld, a kutatási kódot importáld/emeld át. Determinisztikus: ugyanaz a bemenet → ugyanaz a kimenet; a predikció csak t_c-ig ismert adatot használhat. Szabályok: a worker-t ne állítsd le; az API csak read-only DuckDB; minden írás worker job. Holdout-2 (≥ 2026-08-01) adat nem kerülhet modell-illesztésbe.

## Scope
`artifacts/scout_s0c_r1/`, új `src/scout/` modul (vagy a modeling alatt), tesztek

## Acceptance Criteria
- [x] Artifact + manifest, a train OOF AUC egyezik a leaderboarddal (0,636)
- [x] A t938 SOL nov. 21-i esemény score-ja reprodukálódik (P(imp) 0,096)
- [x] Tesztek: no look-ahead, determinizmus, feloldás

## Notes

**2026-11-23 (modeling_agent) — kész, validálásra vár.**

### Döntés: modul helye
Új top-level csomag: `src/scout/` (a `reports`, `modeling` mellett; a `pyproject` `packages.find where=src` miatt
konfig-változás nem kellett). Indok: a Scout nem a quant `src/modeling/` életciklusán megy (nincs snapshot / registry /
deploy), és a t941 (`data_handling`) + t942 (`worker`) egyaránt importálja — egy kicsi, DB-mentes, tiszta API-jú csomag.
A kutatási kódot importálja (`wave_events.build_events`, `scout_events.scout_params/event_id`, `scout_bench.rules`,
`scout_bench.trades.Costs`), nem másolja.
- `src/scout/runtime.py` — `load_artifact`, `detect`, `score`, `resolve`, `run`, `OUTPUT_SCHEMA`, `IMMUTABLE_COLUMNS`
- `src/scout/freeze.py` — `fit_baseline` (a t938 `scout_live_score`-ból emelve; a `scout_live_score` most innen
  re-exportálja a `Baseline`/`fit_baseline`/`SEED`-et és az `R2_03`-at → nincs duplikáció), artifact író CLI:
  `uv run python -m scout.freeze [--out] [--overwrite]`
- `src/scout/tests/test_runtime.py`

### Artifact: `artifacts/scout_s0c_r1/`
`manifest.json` (eseménydefiníció + `params_json`, feature lista, `mu`/`sd`/`coef`/`intercept`/`q_false_start`,
küszöbök, szabályok, költségek, `detect_anchor` 2026-07-24, `live_start` 2026-11-01, train tartomány, `frame_hash`,
`fold_hash`, fold határok, OOF AUC, fájl-sha256-ok, `freeze_hash`), `model.pkl` (sklearn `ScalarLogit`, audit),
`train_oof.parquet` (percentilis / küszöb forrása).
- train n = 2170 (308 impulzus), SOL + BTC + ETH, `split='train'`, label_end_max ≤ 2026-02-01 (holdout guard)
- **train OOF AUC 0,6361** (= leaderboard 0,636) ✔; top20 = 0,1879, top10 = 0,2106
- `fold_hash` 0d90521c16be85de és `frame_hash` 0a723719fc86eedc — egyezik a `research."scout_leaderboard_v1__runs"`
  pooled futásaival ✔; `freeze_hash` f0541e6d37ecfdab
- újrafagyasztás scratch-be: bitre azonos `model.pkl` / `train_oof.parquet` / freeze hash (determinista)
- `load_artifact()` ellenőrzi a fájl-hash-eket, a freeze hash-t és hogy a kód detektálási paraméterei = befagyasztottak

### API (t941 / t942-nek)
```python
from scout import load_artifact, run, OUTPUT_SCHEMA, IMMUTABLE_COLUMNS
art = load_artifact()                       # egyszer / folyamat
df = run(asset_id, ohlcv_1m, art)           # since alapért. = art.live_start (2026-11-01); since < live_start → ValueError
```
- `ohlcv_1m`: polars, `open_time` (naiv UTC) + `open, high, low, close, volume`, **`art.detect_anchor`-tól (2026-07-24)**
  a legutolsó bárig (korábbi sorokat levágja; ha > 1 órával az anchor után kezdődik → ValueError). Duplikált
  `open_time` → utolsó marad; rendezetlen input OK.
- kimenet: eseményenként 1 sor, `t0` szerint; oszlopok = `OUTPUT_SCHEMA`:
  `event_id, asset_id, model_id, cell_id, t0, t_c, label_end, low_t0, close_tc, a_log, sigma_w, a_sigma, p_star, stop,
  velocity_1h, pretrend_3d, log_sigma_1m, drop_depth, volume_ratio, scorable, p_impulse, p_false_start, p_sideways,
  oof_percentile, top20, top10, entry, default_{tp,sl,outcome,outcome_at,exit_price,net_bps},
  r2_03_{tp,sl,outcome,outcome_at,exit_price,net_bps}, status, label_class, as_of`
- `IMMUTABLE_COLUMNS` = minden az `entry` előtt (azonosítás, geometria, feature-ök, predikció) → t941: soha nem
  frissül. `entry`, `r2_03_tp`, `r2_03_sl`: set-once (null, amíg a `t_c`-kor nyíló bár nincs meg). Outcome mezők +
  `status` + `label_class`: `status='open'` amíg `label_end > as_of`, utána egyszer `resolved` vagy `invalid`.
- `scored_at`, `backfilled` a t941 író feladata (a runtime determinista, nincs benne falióra). Kulcs: `event_id`
  (`asset|W4h_H8h|YYYYmmddTHHMM`).
- Teljesítmény: anchor óta ~173 ezer 1m sor / asset, `run` ≈ 0,3 s (BTC mérés).

### Élő ellenőrzés (read-only, 2026-11-23, anchor 2026-07-24 → most)
- **SOL `solusdt|W4h_H8h|20261121T0617`: P(imp) = 0,0964** (t938: 0,096) ✔, percentilis 22,2, top20 nem; resolved,
  sideways / timeout.
- Nov. 1-jétől: SOL 7, ETH 8, BTC 9 esemény, mind resolved; `run` 0,1–0,3 s / asset (~176 ezer 1m sor).
- A SOL live DB-t a worker (PID `python -m worker`) percekig tartotta; az olvasás újrapróbálással ment át — a t942 job
  a worker-en belül fut, ott ez nem gond, de külső olvasónál számolni kell vele.
- Tesztek: `uv run --group research pytest src/reports/tests src/scout/tests -q -m "not integration"` → 197 passed,
  1 skipped; `src/scout/tests` 14 passed + 1 integration passed (élő SOL reprodukció; DB-lock esetén skip). ruff + pyright
  (src/scout, scout_live_score.py) tiszta.

### Gap-kezelés (t938 validátor megjegyzése)
Egységesítve a `rules.py`-jal: kimenet csak a teljes H ablak lezárulásakor, `simulate_rule_event`-tel; bármely hiányzó
bár az ablakban → `status='invalid'`, `*_outcome='invalid'`, `label_class` null (ugyanez a 3-osztályos címke
érvényessége). Nincs „eddigi érintés” részleges kimenet → a t938 `first_touch` eltérés megszűnt (a t938 CLI leíró
eszközként változatlanul megmaradt). Prefix-rés nem fordulhat elő (a jelölt teljes `[t0−B, t_c)` ablakot követel);
nem véges feature (pl. hiányzó `t0−3d` bár → `pretrend_3d`) → `scorable=false`, predikció null, esemény megmarad.
Float NaN → null a kimenetben.

### Determinizmus / look-ahead
Rögzített `detect_anchor` (holdout-2 kezdet − 8 nap) → a greedy non-overlap lánc mindig ugyanott indul; több régi
történet / más futási idő nem változtat az eseményeken. Tesztek: csonkolás `t_c`-nél (ugyanaz a predikció, egy bárral
korábban nincs esemény), jövőbeli árak skálázása nem változtat az immutable oszlopokon, ismételt / bővített / kevert
input azonos, korábbi „most” futás = a későbbi prefixe, nyitott → lezárt átmenet, rés → invalid, kimenet egyezik a
`trades.simulate_event` / `rules.simulate_rule_event` / kutatási címkével.

### Doksi
- új: `_doc_/database_and_code_doc/5800_scout_runtime.md`; `0004_model_lifecycle.md`: „Impulse Scout (külön
  életciklus)” szekció
- **code_doc_agent-nek:** a `0000_project_overview.md` modullistájába felvenni a `src/scout/`-ot (5800 hivatkozással)
  és az `artifacts/scout_s0c_r1/`-et; a CLAUDE.md delegation table-be: `src/scout/` → modeling_agent.

**2026-11-23 (validator_agent) — VALIDÁLVA → done.**
- ruff + pyright (`src/scout`, `scout_live_score.py`): tiszta (0 hiba).
- `uv run --group research pytest src/reports/tests src/scout/tests -q` → **198 passed, 1 skipped** (a skip a Chronos-Bolt
  letöltés, nem releváns). Az élő SOL integrációs teszt most **lefutott** (nem skip): 2026-11-21 esemény P(imp) ≈ 0,096 ✔.
- Leaderboard egyezés (lab DB, read-only): a pooled futások (`78a7589a381e`, `ab75270600ae`, `b98ecd97217b`,
  `6406880ad101`) `data_hash` = 0a723719fc86eedc = manifest `frame_hash`, `fold_hash` = 0d90521c16be85de ✔;
  `base_auc` 0,6360584903817986 = manifest `oof_auc` (bitre) ✔; `config_hash` d23e9208110d ✔.
- Train split: a loader csak `split='train'`-t olvas, `label_end_max` 2026-01-31T06:23 < 2026-02-01 ✔; küszöbök 0,1879 / 0,2106 ✔.
- Újrafagyasztás scratch-be: `model.pkl` és `train_oof.parquet` sha256 azonos, freeze hash f0541e6d37ecfdab azonos,
  a manifest a `created_at`-on kívül bájtra egyezik ✔ (determinista).
- Kód-review: a detektálás a kutatási `wave_events.build_events` / `scout_events.scout_params|event_id` importja (nincs
  másolat); a lezárási feltétel (`t_c_min + h <= m0 + n`) ekvivalens a kutatási címke `tc + h < n` feltételével; a
  címkeablak (`tc+1..tc+H`) rése → `simulate_rule_event` 'invalid' = címke érvénytelen; `since` < live_start → ValueError,
  anchor-guard (1 h slack) rendben; `load_artifact` fájl-sha + freeze hash + params_json ellenőrzés (tamper teszt) ✔.
- **Javítás (validator):** a `train_oof.parquet`-et a globális `*.parquet` `.gitignore` szabály kizárta volna → tiszta
  checkoutnál a `load_artifact()` elhasalna. A többi modell-artifact (`manifest.json`, `model.pkl`) git-követett, ezért
  `.gitignore`-ba felvéve: `!artifacts/scout_s0c_r1/train_oof.parquet` (38 KB). Commit nem történt.
- Megjegyzések (nem blokkoló): (1) a rögzített `detect_anchor` miatt az élő greedy lánc eltérhet a 2021-től számolt kutatási
  holdout-2 eseményektől — tudatos döntés, determinisztikus; (2) a t938 CLI (`scout_live_score.score_events`) továbbra is
  saját R2_03 szint-számolást tartalmaz (leíró eszköz, a runtime nem használja); (3) a code_doc_agent-teendők (overview
  modullista, CLAUDE.md delegation `src/scout/`) nyitottak.
