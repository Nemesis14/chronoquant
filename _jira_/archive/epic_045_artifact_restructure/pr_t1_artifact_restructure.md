---
epic: epic_045_artifact_restructure
id: t1
title: Artifact mappa szerkezet átszervezése — strategy in model, notebooks numbered
assignee: code_doc_agent
status: pr
---

## Goal

Az artifact mappák strukturális rendberakása: a strategy artifactok kerüljenek a modell mappájába (egy modell = egy mappa), a notebookok kapjanak számozott névkonvenciót, és a HTML fájlok a modell gyökérbe kerüljenek.

## Elvégzett munka

### 1. Strategy artifactok áthelyezése

| Régi hely | Új hely |
|---|---|
| `artifacts/strat_solusdt_fw60_long_2101_2605/` | `artifacts/lgbm_solusdt_l_fw60_2101_2605/strategy/` |
| `artifacts/strat_solusdt_fw60_short_2101_2605/` | `artifacts/lgbm_solusdt_s_fw60_2101_2605/strategy/` |

Régi standalone `strat_*` mappák törölve.

### 2. Notebook/HTML reorganizáció (long model)

| Régi hely | Új hely |
|---|---|
| `analysis/sampling_analysis.ipynb` | `analysis/01_sampling.ipynb` |
| `analysis/sampling_analysis.html` | `01_sampling.html` (root) |
| `feature_engineering/01_feature_engineering.ipynb` | `analysis/02_feature_engineering.ipynb` |
| `feature_engineering/01_feature_engineering.html` | `02_feature_engineering.html` (root) |
| `search/search_report.ipynb` | `analysis/03_hyperparameter_search.ipynb` |
| `search/search_report.html` | `03_hyperparameter_search.html` (root) |

### 3. Notebook/HTML reorganizáció (short model)

| Régi hely | Új hely |
|---|---|
| `feature_engineering/01_feature_engineering.ipynb` | `analysis/02_feature_engineering.ipynb` |
| `feature_engineering/01_feature_engineering.html` | `02_feature_engineering.html` (root) |

### 4. config/trading.json frissítés

```json
"strategy_session_long_id":  "lgbm_solusdt_l_fw60_2101_2605/strategy"
"strategy_session_short_id": "lgbm_solusdt_s_fw60_2101_2605/strategy"
```

Path-like session ID — a kód meglévő `Path("artifacts") / session_id` logikája automatikusan a helyes mappát oldja fel. Kódváltoztatás nem szükséges.

### 5. Registry (DuckDB) frissítés

`registry.duckdb` — `artifacts` tábla: a long és short strategy session (`strat_solusdt_fw60_long/short_2101_2605`) file-path artifactjainak abszolút path-jai az új helyre frissítve:
- `strategy_artifact`, `isotonic_long`, `isotonic_short`, `rank_lookup_long`, `rank_lookup_short`

DuckDB table ref artifactok (strat_trades, strat_equity stb.) érintetlenek — azok nem fájlpathok.

## Acceptance Criteria

- [x] `artifacts/strat_solusdt_fw60_long_2101_2605/` nem létezik
- [x] `artifacts/strat_solusdt_fw60_short_2101_2605/` nem létezik
- [x] `artifacts/lgbm_solusdt_l_fw60_2101_2605/strategy/strategy_artifact.json` létezik
- [x] `artifacts/lgbm_solusdt_s_fw60_2101_2605/strategy/strategy_artifact.json` létezik
- [x] Trading service path-resolution tesztelve: minden artifact megtalálható
- [x] Registry paths frissítve
- [x] Numbered HTML fájlok a modell gyökérben
- [x] Numbered ipynb fájlok az `analysis/` mappában
