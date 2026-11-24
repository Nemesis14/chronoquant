---
epic: epic_074
id: t941
title: `scout_signals` tábla a live sémában
assignee: database_agent
status: done
blocks: [t942]
blocked_by: [t939, t940]
---

## Goal
Új tábla minden asset live DB-jében: kulcs event_id (asset|cell|t0), t0, low_t0, t_c, close_tc, a, sigma_w, a_sigma, p_star, stop, p_impulse/p_false_start/p_sideways, percentile, top20/top10, model_id (scout_s0c_r1), scored_at (írás ideje), rule tervek (TP/SL árak mindkét szabályra), outcome (null amíg nyitott), outcome_at, realized bp szabályonként. A predikciós mezők immutable-ek (csak az outcome mezők frissülhetnek). Sync függvény a t940 futtató modullal (store/sync_tables minta), backfill opció a nov. 2026 elejétől (csak a live forward-log kezdetéhez; a backfill sorok `backfilled=true` jelöléssel).

## Scope
`src/data_handling/` (store, sync_tables), séma doksi

## Acceptance Criteria
- [x] Tábla + idempotens sync, immutable predikció mezők tesztelve
- [x] Doksi: 1000_database, 1200_sync_tables (+ 1100, 1110, 5800)

## Notes

**2026-11-23 (database_agent) — kész, validálásra vár.**

### Séma — migráció v8 `create_scout_signals`
- `duckdb_store.py`: `_m008_create_scout_signals` (`CREATE TABLE IF NOT EXISTS`), PK `event_id`. Oszlopok =
  `scout.OUTPUT_SCHEMA` (egyetlen forrás) + `scored_at TIMESTAMP` + `backfilled BOOLEAN`; a típusokat a
  `scout_signals_columns()` képzi (`_sql_type_from_polars`). A `scout` import **lusta** (~1,9 s; a store / read-only API
  importja nem drágul).
- Séma-drift védelem: `sync_scout.check_table_schema` → ha a tábla oszlopai ≠ `scout_signals_columns()` (pl. új
  artifact-verzió más `OUTPUT_SCHEMA`-val), `RuntimeError` — új migráció kell, csendes írás nincs.
- Élő állapot: SOL v6 → v7 + v8 (a v7 `IF NOT EXISTS`, a régi 2-oszlopos SOL `funding_rate` változatlan); BTC/ETH
  v7 → v8 (a BTC v8-at 08:20-kor egy másik folyamat `ensure_tables`-e már alkalmazta — valószínűleg a párhuzamos t944,
  az új kóddal; ártalmatlan). **BCH érintetlen (v6)** — ott a következő `ensure_tables` hozza létre az üres táblát.
  Megjegyzés: a futó worker a régi kódot tölti (v6-ig ismeri a migrációkat) — a már alkalmazott v7/v8-at kihagyja, ez nem gond.

### Sync — `src/data_handling/sync_tables/sync_scout.py`
- `sync_scout(asset_id=None, *, since=None, art=None, now=None, attempts=24, wait_s=5.0) -> ScoutSyncResult`:
  RW kapcsolat zárolás-újrapróbálással → `ensure_tables` → drift check → `ohlcv` `art.detect_anchor`-tól →
  `scout.run(asset, ohlcv, art, since=since)` → `upsert_scout_signals` **egy rövid tranzakcióban** (~0,7 s / asset).
- `upsert_scout_signals(conn, df, asset_id, now)` szabályai:
  - új esemény: INSERT, `scored_at = now`, `backfilled = t_c < first_live_sync_at`;
  - `IMMUTABLE_COLUMNS` eltérés → a sor kimarad, WARNING, `immutable_mismatch` (a tárolt nyer; float tolerancia
    rel 1e-9 csak fp-zajra);
  - set-once (`entry`, `default_tp/sl`, `r2_03_tp/sl`): csak NULL → érték; eltérő nem-NULL → WARNING, tárolt marad;
  - kimenet (`{rule}_outcome/_outcome_at/_exit_price/_net_bps`, `status`, `label_class`): csak `open` →
    `resolved`/`invalid` átmenetkor, egyszer; lezárt sor később sem változik;
  - `as_of`: amíg `open`, minden futás lépteti; lezáráskor fagy;
  - tárolt, de a futásból hiányzó esemény: megmarad (WARNING) — **törlés nincs**.
- **`backfilled` definíció:** `first_live_sync_at` = `MIN(scored_at)` az asset táblájában (az első sikeres írás
  ideje; üres táblánál a futás `now`-ja). Ami ennél korábban zárult (`t_c`), backfill; utána forward predikció.
  Írási késés soronként: `scored_at − t_c` (pl. worker-leállás utáni pótlás így is látszik).
- `02_sync_pipeline.py`: új `scout` tábla-lépés (`--tables scout [--start ...]`), **opt-in** — a `--tables` nélküli
  alapfutás (`DEFAULT_TABLES`) nem tartalmazza, hogy BCH-ra ne induljon írás user-döntés nélkül.

### API a t942-nek (ui_agent)
```python
from scout import load_artifact
from data_handling.sync_tables.sync_scout import sync_scout
art = load_artifact()                       # egyszer / worker folyamat
res = sync_scout("solusdt", art=art)        # btcusdt / ethusdt ugyanígy; sync_ohlcv után
payload = res.to_dict()                     # asset_id, model_id, events, inserted, inserted_backfilled, resolved,
                                            # filled, refreshed, immutable_mismatch, missing_in_run, open_events,
                                            # total_rows, as_of, first_live_sync_at
```
- A worker-en belül futva a zárolás-újrapróbálás gyakorlatilag nem aktív (`attempts`/`wait_s` állítható).
- `immutable_mismatch > 0` vagy `missing_in_run > 0` = riasztásra érdemes (determinista runtime mellett 0 várt).
- Olvasás az API-ban: `SELECT * FROM scout_signals WHERE t0 >= ? ORDER BY t0` (read-only).

### Élő futás (2026-11-23 07:33 UTC, since = 2026-11-01)
| asset | események | backfilled | resolved | open | top20 |
|---|---|---|---|---|---|
| solusdt | 8 | 8 | 7 | 1 (`20261123T0242`, P(imp) 0,160) | 0 |
| btcusdt | 9 | 9 | 9 | 0 | 1 |
| ethusdt | 8 | 8 | 8 | 0 | 1 |
- SOL `20261121T0617`: P(imp) **0,0964**, percentilis 22,2 (= t940 ✔).
- Második futás: inserted 0, mismatch 0, missing 0; csak a nyitott SOL esemény `as_of`-ja lépett (refreshed 1) → idempotens ✔.

### Tesztek
- új: `src/data_handling/tests/sync_tables/smoke/test_sync_scout.py` (14): migráció friss / v6 (régi funding tábla) /
  v7 DB-n, `IF NOT EXISTS`, PK; insert + `scored_at`/`backfilled`; idempotencia; immutabilitás; set-once;
  open → resolved / invalid; hiányzó esemény megmarad; idegen asset elutasítva; `sync_scout` wiring (OHLCV ablak az
  anchortól); séma-drift → hiba.
- `test_sync_pipeline_helpers.py`: default tábla-halmaz = `ALL_TABLES − {scout}` + új opt-in teszt.
- ruff + pyright (érintett fájlok): tiszta.

### Fájlok
`src/data_handling/store/duckdb_store.py`, `src/data_handling/sync_tables/sync_scout.py` (új),
`src/data_handling/02_sync_pipeline.py`, `src/data_handling/tests/sync_tables/smoke/test_sync_scout.py` (új),
`src/data_handling/tests/sync_pipeline/smoke/test_sync_pipeline_helpers.py`; doksi: `1000_database.md` (ER +
`scout_signals` szekció), `1100_store.md`, `1110_duckdb_store.md` (v7 + v8 sor — a v7 eddig hiányzott),
`1200_sync_tables.md`, `5800_scout_runtime.md`.

**2026-11-23 (validator_agent) — validálva, DONE.**
- Kód-review: v8 `_m008_create_scout_signals` `CREATE TABLE IF NOT EXISTS`, PK `event_id`, oszlopok = `scout.OUTPUT_SCHEMA`
  + `scored_at` + `backfilled` (44 oszlop), lusta `scout` import ✔. `upsert_scout_signals`: `IMMUTABLE_COLUMNS` eltérésnél
  sor kihagyva (tárolt nyer), set-once csak NULL → érték, kimenet csak `open` → `resolved`/`invalid` átmenetkor, törlés
  nincs, egy tranzakció (ROLLBACK hibánál), RW connect zárolás-újrapróbálással, séma-drift → `RuntimeError` ✔.
  `02_sync_pipeline`: `scout` opt-in (`DEFAULT_TABLES` nem tartalmazza) ✔.
- ruff + pyright (5 érintett fájl): 0 hiba. Sorvégek: minden érintett kód- és doksifájl LF ✔.
- Tesztek: `test_sync_scout.py` (14) + `test_sync_pipeline_helpers.py` → 25 passed. Teljes futás
  (`src/data_handling src/worker src/scout`): 291 passed, 3 failed = a 3 ismert, korábbi SOL sanity hiba
  (`test_feat_ohlcv_quant_no_time_gap`, `test_target_row_count_matches_features`, `test_predictions_score_range`), 0 error.
  A korábban jelentett 2 store/perf error nem jött elő: a `tests/store/conftest.py` fixture újrapróbálás nélkül nyitja
  read-only a live SOL DB-t → író (worker/sync) zárolásakor setup-error. Zárolás miatti flakiness, nem t941-hiba
  (javaslat későbbre: retry a fixture-ben).
- Élő DB-k (read-only): SOL v8 / BTC v8 / ETH v8 / BCH v6 (BCH-ban nincs `scout_signals`) ✔; sorok `t0 >= 2026-11-01`:
  SOL 8 / BTC 9 / ETH 8, mind `backfilled = true`, SOL 1 nyitott ✔; SOL `20261121T0617` P(imp) 0,0964, `oof_percentile` 22,2 ✔.
- Doksi 1000 / 1100 / 1110 / 1200 / 5800 egyezik a kóddal és a sémával ✔.
- Megjegyzés (nem blokkoló): a v8 DDL a futásidejű `OUTPUT_SCHEMA`-ból képződik (nem befagyasztott lista, mint a v6-nál);
  a drift-ellenőrzés ezt fedi — új artifact-verzió = új migráció.
