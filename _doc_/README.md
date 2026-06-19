# _docs — Module Documentation

Flat file structure with compound numbering. Main sections: `NN_module.md`, subsections: `NNSS_topic.md`.

See `.agent/skills/docs_skill.md` for conventions and the doc file template.

**Ordering rule: within every topic block, methodology/concept docs have lower numbers than
their corresponding code docs.**

---

## Structure

```
_doc_/
  0000_project_overview.md

  # Database Infrastructure (1xxx)
  1000_database.md
  1001_database_module.md
  1100_store.md + 1110–1150 (duckdb_store, query, stats, validate, toolkit)
  1200_sync_tables.md
  1210_sync_ohlcv.md
  1230_sync_predictions.md
  1300_tests.md + 1310–1320 (store, pipeline)

  # Features (2xxx) — methodology BEFORE code
  2000_features.md           ← feature layer: 25 groups, lag, warmup (METHODOLOGY)
  2010_feature_engineering.md ← feature selection: quality, redundancy, stability (METHODOLOGY)
  2100_sync_features.md      ← sync_features.py (CODE)
  2200_features_polars.md    ← _features_polars.py (CODE)

  # Targets (3xxx) — methodology BEFORE code
  3000_targets.md            ← fw60 logreturn outcomes, MFE (METHODOLOGY)
  3100_sync_targets.md       ← sync_targets.py (CODE)

  # Quant Train (4xxx) — methodology BEFORE code
  4000_quant_train.md        ← INNER JOIN handoff, rebuild semantics (METHODOLOGY)
  4100_quant_train.md        ← schema, rebuild modes, CLI (CODE)

  # Sampling / Modelling (5xxx)
  5000_modelling.md          ← modeling domain overview / TOC
  5010_sampling_yearly.md    ← yearly random-hour sampling (METHODOLOGY, active)
  5100_sampling_config.md    ← YearlySamplingConfig dataclass
  5200_sampling_artifacts.md ← write_yearly_artifacts / load_yearly_sample
  5300_create_sample.md      ← create_yearly_sample orchestrator + CLI
  5400_sampling.md           ← ARCHÍV: expanding window CV
  5410_sampling_splits.md    ← ARCHÍV: expanding window splits
  5420_sampling_audit.md     ← ARCHÍV: feature table audit

  analysis/                  ← analyst_agent: EDA, specs, sample quality notebooks
```
