# 5300 — Sampling Orchestratorok: create_model_sample

A `src/modeling/sampling/create_sample.py` valósítja meg a snapshot-native sampling
path orchestrátorát. Sampling mode: **`train_valid_split`** (aktív,
champion modellek) az **egyetlen** támogatott mód. A korábbi `walk_forward` és `yearly_random_hour`
módok (és a `sampling/config.py`, `artifacts.py`, `audit.py`, `splits.py`) epic_068-ban törölve (git history: `73f2156`);
más `sampling_mode` `ValueError`-t dob.

| Mode | Output oszlop | Mikor aktív |
|------|---------------|-------------|
| `train_valid_split` | `split` TINYINT (0=train, 1=valid) | Minden modell (`config/models.json`) |

Forrás:
- [sampling/create_sample.py](../../src/modeling/sampling/create_sample.py)
- [sampling/snapshot_sampler.py](../../src/modeling/sampling/snapshot_sampler.py)

→ _doc_/methodology_doc/5400_sampling.md

---

## Overview

```mermaid
flowchart TD
  CM[create_model_sample\nmodel_id, snapshot_id]
  CFG[config/models.json\nsampling_mode]
  TV[create_snapshot_sample_train_valid_split\ntrain/valid split path]
  SNAPS[snapshot_sampler\nSQL builder]
  DB["lab.duckdb\nmodel.__sample CTAS"]
  REG[reg.feature_sets + reg.models]

  CM --> CFG
  CFG -- train_valid_split --> TV
  TV --> SNAPS
  SNAPS --> DB
  TV --> REG
```

A kimenet: `model."<model_id>__sample"` DuckDB tábla —
`open_time` + target oszlop(ok) + `split` indicator.
A `feat_*` oszlopok a snapshotban maradnak, downstream lépések
`snap."<snapshot_id>" ⋈ model."<model_id>__sample"` JOIN-nal dolgoznak.

---

## `create_model_sample(model_id, snapshot_id)`

Config-vezérelt belépési pont. Feloldja a modell paramétereit (`config/models.json`),
megnyitja a lab connectiont, majd `sampling_mode` alapján delegál.

| Paraméter | Típus | Leírás |
|-----------|-------|--------|
| `model_id` | `str` | Modell kulcs a `config/models.json`-ból |
| `snapshot_id` | `str` | Immutable snapshot id (`snap."<id>"`) |

**Visszatérés:** Summary dict (lásd `create_snapshot_sample_train_valid_split`).

**Raises:** `ValueError` ha a modell ismeretlen, a snapshot nem létezik, üres a sample,
vagy ismeretlen `sampling_mode`.

```mermaid
sequenceDiagram
  participant P as pipeline.step_sample
  participant CM as create_model_sample
  participant CFG as config/models.json
  participant TV as create_snapshot_sample_train_valid_split
  participant SNAPS as snapshot_sampler
  participant DB as lab.duckdb
  participant REG as reg.feature_sets + reg.models

  P ->> CM: model_id, snapshot_id
  CM ->> CFG: meta (asset_id, target_name, sampling_mode, params)
  Note over CM: sampling_mode != train_valid_split -> ValueError
  CM ->> TV: conn, model_id, snapshot_id, params
  TV ->> SNAPS: build_train_valid_split_ctas_sql
  TV ->> DB: CTAS model."model_id__sample"
  TV ->> REG: upsert feature_sets + link_model
  TV -->> CM: summary dict (split_row_counts)
  CM -->> P: summary dict
```

---

## `create_snapshot_sample_train_valid_split(conn, model_id, snapshot_id, ...)`

Train/valid split orchestrator. Egyetlen kronológiai felosztás: train és valid
időablak, embargo szabályokkal a train oldalon.

```mermaid
flowchart TD
  CHK[snapshot_exists?\nValueError ha nem]
  COLS[_snapshot_feature_columns\nfeat_* superset]
  SQL[build_train_valid_split_ctas_sql\nCTAS SQL generálás]
  EXEC["conn.execute\nCREATE OR REPLACE\nmodel.__sample"]
  CNT[COUNT + _split_counts\nn_rows ellenőrzés]
  FSID[build_feature_set_id\nfeature_set_id deriválás]
  REG[registry.upsert\nfeature_sets + models\nstatus=sampled]

  CHK --> COLS --> SQL --> EXEC --> CNT --> FSID --> REG
```

| Paraméter | Típus | Default | Leírás |
|-----------|-------|---------|--------|
| `conn` | `DuckDBPyConnection` | — | Nyitott lab connection |
| `model_id` | `str` | — | Owning model id (tábla token + reg link) |
| `snapshot_id` | `str` | — | Immutable forrás snapshot |
| `asset_id` | `str` | — | Asset kulcs (feature_set_id és reg linkekhez) |
| `target_cols` | `list[str]` | — | Target oszlop(ok) a samplebe |
| `horizon` | `int` | `60` | Forward-window bar count (feature_set_id token) |
| `direction` | `str` | `"l"` | `l`/`s`/`combo` (feature_set_id token) |
| `seed` | `int` | `42` | Reproducibility seed per-hour pickhoz |
| `train_start` | `str` | `"2021-01-01"` | Train ablak kezdete (YYYY-MM-DD, inclusive) |
| `train_end` | `str` | `"2025-04-30"` | Train ablak vége (YYYY-MM-DD, inclusive) |
| `valid_start` | `str` | `"2025-05-01"` | Valid ablak kezdete (YYYY-MM-DD, inclusive) |
| `valid_end` | `str` | `"2026-05-31"` | Valid ablak vége (YYYY-MM-DD, inclusive) |
| `feature_lookback_embargo_minutes` | `int` | `240` | Train elejéről kizárt percek (feature warmup) |
| `target_purge_minutes` | `int` | `60` | Train végéről kizárt percek (target leak megelőzés) |
| `selected_cols` | `list[str] \| None` | `None` | Logikai feature_set; None = összes feat_* |

**Visszatérési kulcsok:**

| Kulcs | Típus | Leírás |
|-------|-------|--------|
| `model_id` | `str` | Modell azonosító |
| `snapshot_id` | `str` | Forrás snapshot |
| `sample_table` | `str` | `model."<model_id>__sample"` FQN |
| `n_rows` | `int` | Összes sample sor |
| `split_row_counts` | `dict[str, int]` | Per-split sorok (`{"0": n, "1": n}`) |
| `feature_set_id` | `str` | Regisztrált `feature_set_id` |
| `n_input` | `int` | Snapshot feat_* oszlopok száma |
| `n_selected` | `int` | Kiválasztott feature-ök száma |

---

## SQL logika — `build_train_valid_split_select_sql`

`src/modeling/sampling/snapshot_sampler.py` — IO-free SQL builder.

### Két embargo szabály (csak train oldalon)

```mermaid
flowchart LR
  TS[train_start] --> FLE[+ feature_lookback_embargo_minutes\nlegkorábbi megtartott sor]
  TE[train_end] --> TP[- target_purge_minutes\nlegkésőbbi megtartott sor]
  VS[valid_start] --> VE[valid_end\nnincsen embargo]
```

1. **Feature lookback embargo** (`feature_lookback_embargo_minutes = 240`): a train
   ablak első N perce kizárva — garantálja, hogy minden megtartott sornak teljes
   feature lookback ablaka legyen a snapshot start előtt.
2. **Target purge** (`target_purge_minutes = 60`): a train ablak utolsó N perce
   kizárva — ezeknek a soroknak a target értéke a valid periódusban lévő árból
   számítódik, így data leak megelőzés.

**Valid oldalon nincs embargo** — a valid feature-számítás visszanéz a train
történelembe, ami korrekt és elvárt viselkedés.

### QUALIFY ROW_NUMBER (hourly select)

```sql
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY date_trunc('hour', open_time)
    ORDER BY hash(CAST(epoch_ms(open_time) AS BIGINT) + {seed}), open_time
) = 1
```

Minden órablokkból pontosan egy sor kerül a samplebe. A `hash(epoch_ms + seed)` content-addressed
determinizmust biztosít: azonos snapshot + seed → bit-azonos tábla (`CREATE OR REPLACE`).

### `split` indicator oszlop

```sql
CAST(CASE
    WHEN CAST(open_time AS DATE) BETWEEN DATE '{valid_start}' AND DATE '{valid_end}'
        THEN 1
    ELSE 0
END AS TINYINT) AS split
```

| Érték | Jelentés |
|-------|----------|
| `0` | Train sor |
| `1` | Valid sor |

Az output tábla sémája: `open_time`, target oszlop(ok), `split TINYINT`.

---

## Kapcsolódó fájlok

| Fájl | Tartalom |
|------|----------|
| [5400_sampling.md](../methodology_doc/3000_sampling.md) | Sampling metodológiai háttér |
| [5530_pipeline_predict_provenance.md](5530_pipeline_predict_provenance.md) | Pipeline orchestrator (step_sample hívja ezt) |
| [1510_registry_code.md](1510_registry_code.md) | registry.upsert — reg.feature_sets és reg.models írás |
