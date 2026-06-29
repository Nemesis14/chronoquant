---
epic: epic_044_methodology_doc_restructure
id: t1
title: methodology_doc teljes átstrukturálása és átírása
assignee: methodology_agent
status: pr
---

## Goal

A `_doc_/methodology_doc/` könyvtár tartalma nem tükrözte a helyes módszertani határokat és a pipeline sorrendjét. A feladat: helyrerakni a struktúrát, ellenőrizni a tartalmakat, és az egész zónát egységes módszertani specifikációként átírni.

## Elvégzett munka

### Struktúra átrendezés

- **Módszertanba NEM való fájlok áthelyezve:**
  - `1400_snapshots.md`, `1500_registry.md` → `_doc_/database_and_code_doc/`
  - `4000_quant_train.md`, `5000_modelling.md`, `5010_sampling_yearly.md` → `_doc_/_plans_/`
  - `7000_trading.md`, `7100_live_trading.md`, `8000_ui.md`, `8100_dashboard.md` → `_doc_/database_and_code_doc/`
  - `2011_feature_analysis_price_action.md` → `research/`
  - `5700_offline_prediction.md` → `_doc_/database_and_code_doc/`
  - `6200_strategy_optimization.md` → törölve (legacy Optuna sweep, lecserélve grid search-re)

- **Fájlok átszámozva** a pipeline sorrendjének megfelelően:
  - `2000_features.md` → `1000_features.md`
  - `3000_targets.md` → `2000_targets.md`
  - `5400_sampling.md` → `3000_sampling.md`
  - `2010_feature_engineering.md` → `4000_feature_engineering.md`
  - `2015_mutual_information.md` → `4100_mutual_information.md`
  - `5500_hyper_param_search.md` → `5000_hyper_param_search.md`
  - `5600_model_training.md` → `5100_training.md`
  - `6150_short_score_semantics.md` → `6200_short_score_semantics.md`

### Tartalmi átírás

Mind a 12 fájl teljeskörűen átírva az alábbi elvek szerint:
- Kód-mentes módszertani specifikáció
- Mermaid diagramok minden fogalomhoz (min. 2-3 per fájl)
- Indoklások: miért ezt a megközelítést, milyen alternatívákat fontoltunk meg
- Kötelező szekciók: Miért kritikus / Miért ezt / Kulcsfogalmak / Paraméterek / Kockázatok / Validációs checklist
- Egységes dokumentum hangvétel: 3 féle olvasónak (quant, data scientist, fejlesztő)

### Új fájl

- `0000_overview.md` — teljes módszertani specifikáció áttekintője: pipeline flowchart, fejezetek összefoglalói, keresztmetszetű elvek (data leakage policy, reprodukálhatóság)

## Végső struktúra

```
methodology_doc/
  0000_overview.md
  1000_features.md
  2000_targets.md
  3000_sampling.md
  4000_feature_engineering.md
  4100_mutual_information.md
  5000_hyper_param_search.md
  5100_training.md
  6000_strategy.md
  6100_strategy_calibration.md
  6200_short_score_semantics.md
  6300_strategy_grid_search.md
```

## Acceptance Criteria

- [ ] 12 fájl létezik, mind a pipeline sorrendjében számozva
- [ ] Nincs kód, SQL, implementációs referencia egyetlen fájlban sem
- [ ] Minden fájlban legalább 2 Mermaid diagram
- [ ] Minden fájlban van alternativa táblázat
- [ ] Minden fájlban van validációs checklist (min. 5 pont)
- [ ] `0000_overview.md` tartalmaz pipeline flowchartot és fejezet-összefoglalókat
