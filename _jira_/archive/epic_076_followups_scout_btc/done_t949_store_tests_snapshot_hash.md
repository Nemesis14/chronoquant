---
epic: epic_076
id: t949
title: Store tesztek lock-retry + snapshot tartalom-hash
assignee: database_agent
status: done
blocks: [t950]
blocked_by: []
---

## Goal
1) `src/data_handling/tests/store/conftest.py` read-only live DB fixture: rövid retry DuckDB lockra (a worker írása miatt flaky setup error). 2) Snapshot `content_sha256` csak az időrácsot/sorszámot/oszlopkészletet fedi, az értékeket nem (BTC és BCH hash azonos). Javaslat: ADDITÍV érték-hash (új mező, pl. `values_sha256` / mintavételes érték-hash), a meglévő `content_sha256` és snapshot id-k NEM változhatnak (registry/artifact hivatkozások). Döntés + implementáció + backfill a meglévő snapshotokra, ha olcsó. 3) A 3 régóta bukó SOL sanity teszt (`test_feat_ohlcv_quant_no_time_gap` 28 gap, `test_target_row_count_matches_features`, `test_predictions_score_range`) okának feltárása: valódi adathiba → javítás/pótlás, vagy elavult teszt-elvárás → teszt igazítása, indoklással.

## Acceptance Criteria
- [x] Store tesztek lock alatt sem error-oznak
- [x] Érték-hash bevezetve, meglévő id-k változatlanok
- [x] A 3 SOL sanity teszt zöld vagy indokoltan igazítva

## Notes

### 2026-11-24 — database_agent

**1) Lock-retry** — `src/data_handling/tests/store/conftest.py`: `connect_read_only_with_retry`
(30 × 1.5 s ≈ 45 s, `duckdb.IOException`-re). Ha a lock a keret után is fennáll → `pytest.skip`
(környezeti állapot), nem setup error.

**2) Érték-hash (döntés: additív, teljes scan, nem mintavételes)**
- Új mező: `reg.snapshots.values_sha256 VARCHAR` — registry migráció **v3**
  (`_migration_003_snapshot_values_hash`, `ADD COLUMN IF NOT EXISTS`).
- `snapshots.compute_values_sha256(conn, table_fqn)`: oszloponként `SUM(hash(open_time, col))`
  + `COUNT(*)`, rendezett oszlopnevek, `VALUES_HASH_VERSION="v1"` címke → sha256. Az értéket a
  sor `open_time`-jához köti (sorok közti csere is detektált), a fizikai sorrend nem számít.
  Egy scan, OOM-mentes. Caveat: a DuckDB `hash()` fő-verziók között nem garantáltan stabil →
  azonosság-/változás-detektálásra való; verzióváltás után `backfill_values_sha256(force=True)`.
- `create_snapshot`: CTAS után a befagyasztott táblán számol, reg-upsertbe kerül, és
  `SnapshotResult.values_sha256` (default `None`). Reuse-ágon, ha hiányzik, lustán kitölti
  (csak UPDATE). A `content_sha256`, a `snapshot_id` és a reuse-kulcs **változatlan**.
  `backfill_values_sha256(conn, asset_id, force)` helper; a CLI logolja a hasht.
- Költség: ~100–290 s / snapshot (1.9–2.9M sor, 211 oszlop). Egy snapshot-építés mellett
  ez elfogadható, ezért nincs mintavétel.
- **Backfill kész (mind a 8 snapshot)**: a hasheket read-only lab connectionnel számoltam, a
  registryt egy rövid tranzakcióban frissítettem (az id-k és a content hashek előtte-utána
  egyeznek, assert):
  BTC `c120e35d…`, BCH `666e2e2e…`, ETH `b88b6b0b…` (mindhárom content `51471332` → most
  értékben **különböznek**), SOL `546bcfa8…`/`2223a3ff…`/`fe19b6bd…`/`1ea87102…`/`3e6d406c…`.
- Tesztek: `test_values_sha256_registered_and_value_sensitive`,
  `test_values_sha256_backfill_and_lazy_fill_on_reuse` (smoke).

**3) A 3 SOL sanity teszt**
- `test_feat_ohlcv_quant_no_time_gap` (28→29 gap): **valódi adathiba volt** (belső lyukak:
  OHLCV beírva, a feature-írás a lock/újraindítás miatt elmaradt; az append-only feature sync
  ezeket nem tölti). A javítást a párhuzamos **t947** végezte (`repair_gaps.py`,
  `02_sync_pipeline.py --repair-gaps` → `sync_features(backfill=True)`). Mérés most: 0 gap,
  `feat_ohlcv_quant` = `ohlcv` = 3 257 121 sor. Nem duplikáltam, a teszt változatlan és zöld.
- `test_target_row_count_matches_features`: **elavult elvárás**. A worker `sync_full` lánca
  (ohlcv → funding → scout → features → predictions) a `target`-et szándékosan nem frissíti
  (retrainkor `--tables targets`). A target 2026-08-21 05:40-ig tart, a feature-ök
  2026-11-24-ig. Az új invariáns: azonos kezdet, `target.max <= feat.max`, azonos sorszám
  `<= target.max`-ig (3 120 401 = 3 120 401), és nincs feature nélküli target sor (0).
- `test_predictions_score_range`: **elavult elvárás**, nem a gapekhez kötődik (nincs t947
  átfedés). A champion modellek LightGBM regresszorok (`objective: regression`, fw60 MFE
  logreturn): long ∈ [0.0025, 0.026], short ∈ [-0.032, -0.0027]. Az új feltétel: véges és
  `|x| <= 2` (a target MFE korlátaival egyezik; predict_proba modellnél is érvényes).

**Ellenőrzés**: `uv run pytest src/data_handling -q` → 252 passed; `ruff check src/data_handling`
OK; `pyright src/data_handling` 0 hiba.

**Doksi**: 1410_snapshots_code (values_sha256, compute/backfill, content-hash korlát,
sequence diagram javítva), 1510_registry_code (v3 migráció), 1310_store_tests (lock-retry, 3 teszt
új elvárása), 1100_store (target-frissítés módja, snap/reg írási mód). Nyitott: a
`methodology_doc/1400_snapshots.md` (methodology_agent zóna) a „két hash” leírást még nem
említi; t950 lezárásban érdemes jelezni.

### 2026-11-24 — validator_agent — VALIDÁLVA (done)

- Kód-review: `connect_read_only_with_retry` (30 × 1.5 s, `duckdb.IOException` → retry, utána `pytest.skip`) rendben. `compute_values_sha256`: oszloponként `SUM(hash(open_time, col))` + `COUNT(*)`, rendezett oszlopnevek, `VALUES_HASH_VERSION` címke — sorrend-független, `open_time`-hoz kötött, minden oszlopot (NULL-t is) lefed. `create_snapshot` (CTAS utáni számítás + upsert, reuse-ágon lusta UPDATE), `backfill_values_sha256(force)`, `05_create_snapshot` logolás rendben. A migráció v3 additív (`ADD COLUMN IF NOT EXISTS`, nullable).
- Registry (read-only, rövid kapcsolat): `_schema_migrations` v3 alkalmazva; mind a 8 snapshot `snapshot_id` hash8 == `content_sha256[:8]`; `models` / `feature_sets` → 0 árva `snapshot_id` hivatkozás. BTC/BCH/ETH content `51471332` közös, `values_sha256` különböző (`c120e35d…` / `666e2e2e…` / `b88b6b0b…`). Spot-check: `solusdt_fw60_2301_2607__948cc228` újraszámolva read-only lab connectionnel → `1ea8710227ba…`, egyezik (107 s).
- SOL sanity tesztek: `test_target_row_count_matches_features` és `test_predictions_score_range` módosítása indokolt és nem üres (kezdet/sorszám/árva-sor invariáns; véges + |x| ≤ 2, a NULL külön tesztben).
- `uv run pytest src/data_handling -q`: 251 passed, 1 failed — `test_feat_ohlcv_quant_no_time_gap` (változatlan teszt) **1 új, valódi lyukat** talál: SOL `feat_ohlcv_quant` 2026-11-24 04:22–04:28 (7 sor; az `ohlcv` hiánytalan). Ez a t947-ben dokumentált üzemi állapot (a futó worker még a régi `sync_features`-t tölti be, nincs újraindítva), nem t949-hiba; a teszt helyesen jelez. Teendő (user): worker-újraindítás, majd `02_sync_pipeline.py --repair-gaps --asset-id solusdt`. A validátor nem írt az éles DB-be.
- `ruff check src/data_handling`: OK; `pyright src/data_handling`: 0 hiba.
- Docs (1410, 1510, 1310, 1100) pontosak.
