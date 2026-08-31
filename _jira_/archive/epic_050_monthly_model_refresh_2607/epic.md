# Epic 050: Havi modell-frissítés — 2101_2607 champion + rekalibráció + redeploy

## Goal

A 7200-as signal-frekvencia elemzés (epic_049) kimutatta, hogy a bevezetett
champion (`lgbm_solusdt_l/s_fw60_2101_2605`) kalibrációs ablaka (2025-05 –
2026-05) elavult a piaci rezsimváltás (volatilitás-csökkenés) miatt — ez
okozta a live-on tapasztalt szokatlanul ritka long/short signalt. Cél: friss
adaton (2026-07-31-ig) újratanítani mindkét irányú champion modellt, a
kalibrációs ablakot a jelenlegi rezsimhez igazítani, és élesíteni — havi
rendszerességű újrafuttatási ciklus első köre.

## Scope

- `database/solusdt/solusdt.duckdb` — target + quant_train frissítés
- `database/solusdt/solusdt_lab.duckdb` — új snapshot, sample, pred, strat táblák
- `config/models.json` — két új model entry (`lgbm_solusdt_l/s_fw60_2101_2607`)
- `config/trading.json` — új strategy_session path-ok
- `artifacts/lgbm_solusdt_l_fw60_2101_2607/`, `artifacts/lgbm_solusdt_s_fw60_2101_2607/`
- `database/_registry/registry.duckdb` — új deployment (aktív modellcsere)
- `_doc_/models_doc/` — analízis riport frissítés

## Tasks

- t1: target + quant_train frissítés 2026-07-31-ig (database_agent)
- t2: új snapshot 2021-01-01 – 2026-07-31 (database_agent)
- t3: config/models.json két új model entry (modeling_agent)
- t4: long modell teljes pipeline (modeling_agent)
- t5: short modell teljes pipeline (modeling_agent)
- t6: strategy kalibráció mindkét irányra + config/trading.json átállítás (modeling_agent)
- t7: éles csere — régi deployment kivezetése, új becseréslése (database_agent) — megerősítés után
- t8: analízis riportok frissítése (analyst_agent)
- t9: validáció — config diff, ruff/pyright/pytest (validator_agent)

## Key Decisions

- **Kalibrációs/opt ablak: 6 hónap görgetve (2026-02-01 – 2026-07-31)** a korábbi
  13 hónap helyett. Indoklás: elég friss a jelenlegi (alacsonyabb
  volatilitású) rezsimhez, de elég hosszú a grid search statisztikai
  stabilitásához (90 napos ablak becslés szerint csak ~2 long trade-et adna).
- Train ablak marad a teljes historikus adat (2021-01-01-től) — csak a
  kalibrációs/valid ablak tolódik.
- Új modell-id: `lgbm_solusdt_l_fw60_2101_2607` / `lgbm_solusdt_s_fw60_2101_2607`
  (train 2021-01-01 – 2026-01-31, valid 2026-02-01 – 2026-07-31).
- t7 (éles csere) explicit user-megerősítést igényel a végrehajtás előtt, a
  terv jóváhagyása ellenére is — magas blast-radius akció.
- **Bugfix (nem tervezett, t4 futás közben derült ki):** `src/modeling/pipeline.py`
  `step_search()` a `run_search()`-t explicit `feature_selection`/`feature_key`
  nélkül hívta, így a függvény `feature_selection="joint"` defaultja aktiválódott,
  ami egy sosem futtatott `run_gain_rank()` előlépést igényelt volna
  (`gain_ranked` kulcs a feature_set.json-ban) — ez minden standard pipeline
  search lépését eltörte volna. A meglévő champion (2101_2605) `search/`
  mappája (nincs `search_joint` variáns) igazolja, hogy az eredetileg
  `feature_selection="fixed"` móddal épült. Javítás: `step_search()` explicit
  `feature_selection="fixed", feature_key="selected"` átadása — visszaállítja
  az eredeti, dokumentált (nem-experimentális) pipeline-viselkedést.
- **Második bugfix (ugyanabban a vizsgálatban):** `_select_best_trial()` és
  `_check_patience()` a `src/modeling/search/lgbm_search.py`-ban egy soha
  nem létező `valid_top10_lift` mezőt szűrt (a tényleges objektív mező neve
  `objective_score`, lower=better) — emiatt `_select_best_trial` mindig
  `None`-t adott vissza, így 100 sikeresen lefutott trial ellenére sosem
  íródott ki `search_best.json` ("No completed trials." az összegzésben,
  holott minden trial rendben lefutott). Mindkét függvényt javítottam a
  helyes `objective_score` mezőre (ascending sort, mert lower=better).
  `_check_patience` jelenleg nem hívott sehonnan (dead code), de a
  konzisztencia és a jövőbeli újrafelhasználhatóság miatt azt is javítottam.
  Ez azt jelenti, hogy **a jelenleg éles 2101_2605 champion modellek search
  lépése is ezzel a hibával futott** — vagyis a search valószínűleg nem a
  ténylegesen legjobb hiperparamétert választotta, hanem egy korábbi/eltérő
  kódútvonalon (esetleg manuális `_select_best_trial` hívás vagy régebbi
  kódverzió) készült. Ez nem blokkolja ezt az epicet, de érdemes megjegyezni
  jövőbeli auditra.
- **Harmadik bugfix (súlyosabb, korrektségi hiba):** `pipeline.py` `step_search()`
  sosem adta át a `direction` paramétert a `run_search()`-nek, ami emiatt
  mindig a default `"long"`-ra esett — **a short irányú modellek keresése
  eddig mindig a hosszú irányhoz tartozó `valid_ratio_p925` metrikát
  optimalizálta**, ami short target-re (`short_mfe_fw60`, negatív értékek)
  szisztematikusan 0.0-t ad (`_compute_ratio_p925` guard: `if overall <= 0:
  return 0.0`). Emiatt a short modell 100 trial-ja mind `objective_score=-0.0`
  volt — a TPE sampler gyakorlatilag vak volt, a "legjobb" trial esetlegesen
  lett kiválasztva. Javítás: `step_search()` a `target_name`-ből
  (`config/models.json`) levezeti a helyes `direction`-t és átadja. A
  `lgbm_solusdt_s_fw60_2101_2607` `search/` mappáját törölve (érvénytelen
  volt) és a search-öt újrafuttatva helyes iránnyal (100 friss trial,
  objective_score most valós, változó negatív értékek).
  Mellékesen javítva: `_persist_completed()` a `search_trials.jsonl`-be eddig
  csak a `valid_ratio_p925`/`train_ratio_p925` mezőket írta ki, a
  `valid_ratio_p075`/`train_ratio_p075`-t sosem — most mindkettő perzisztálva
  van (kellett a javított elemzési notebook irány-alapú mezőválasztásához is).
  **⚠️ Ez azt jelenti, hogy a jelenleg éles `lgbm_solusdt_s_fw60_2101_2605`
  short champion modell hiperparaméter-keresése valószínűleg ugyanezzel a
  hibával futott** — érdemes lenne egy külön jövőbeli epicben ellenőrizni/
  újrafuttatni, de ez jelen epic hatókörén kívül esik (ez az epic az új
  2101_2607 modellt korrigáltan építi fel).
