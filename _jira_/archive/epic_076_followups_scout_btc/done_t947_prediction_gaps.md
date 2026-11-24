---
epic: epic_076
id: t947
title: SOL/BCH predikciók elszórt 1 perces gapjei
assignee: database_agent
status: done
blocks: [t950]
blocked_by: []
---

## Goal
A t946 validátor talált: SOL/BCH `predictions`-ben szórványos 1 perces gapek (BCH 4 db, legutóbbi 2026-11-23 07:15 UTC), az OHLCV-ben ugyanott nincs gap. Okfeltárás (sync_predictions / worker lánc, lock/timeout, feature késés?), javítás, a meglévő gapek pótlása (backfill), és ellenőrzés mind a 3 modellezett assetre (SOL, BCH, BTC). A worker-t ne állítsd le; ha a javítás worker-kódot érint (ui_agent domain), minimálisan és a Notes-ban dokumentálva.

## Acceptance Criteria
- [x] Gyökérok dokumentálva
- [x] Gapek pótolva, 0 gap SOL/BCH/BTC predictions-ben
- [x] Regressziós teszt

## Notes

### 2026-11-24 — database_agent

**Gyökérok:** a predikció-lyukak mind `feat_ohlcv_quant`-lyukak (az OHLCV-ben 0 gap). A `feat_ohlcv_quant` és a `predictions` írása append-only (`_insert_append_only`: csak `open_time > MAX`). A worker `sync_full` a feature-ablakot a futás *előtti* OHLCV-tailből számolja (`start = next(ohlcv tail)`). Ha egy job beírta az OHLCV bárt, majd a feature-írás előtt elbukott (tipikusan DuckDB fájlzár: Windows-on egy másik processz — pl. az API (uvicorn, PID 4768) read-only kapcsolata, vagy kézi lekérdezés — kizárja az írót; ill. crash / `worker_restart`), a következő job már az *új* OHLCV-tail után kezdte a feature-t → az adott perc feature-sora (és így predikciója) végleg kimaradt. A predikció-lépés hibája önjavító volt (a pred-tailtől indul), a feature-lépésé nem. Bizonyíték: BCH 2026-11-23 07:16 UTC — a worker.log szerint a 08:17:29 (helyi) BCH job OHLCV-insert után nem jutott el a feature-ig; a 2026-11-24 04:02/04:07 UTC jobok ugyanilyen `IOException: ... being used by another process` hibával buktak (ott a pred-lépésen, ami önjavító).

**Javítás (worker-kód NEM változott):**
- `sync_features`: catch-up kezdés — ha `start_time` > feature-tail + 1 perc, a kezdés tail + 1 perc (WARNING `Feature catch-up`). Ezzel a CLI és a worker is önjavító; új lyuk nem keletkezik.
- `sync_features(backfill=True)` + `duckdb_store.backfill_feat_ohlcv_quant` (belső lyuk kitöltése, meglévő `open_time` kihagyva, egy tranzakció).
- Új `sync_tables/repair_gaps.py` (`find_missing_ranges`, `repair_gaps`; lépésenkénti lock-retry 30 × 2 s) + CLI: `02_sync_pipeline.py --repair-gaps --asset-id <id> [--dry-run]`.

**Backfill (2026-11-24, élő worker mellett):**
- solusdt: 29 lyuk / 417 perc (2026-06-24 → 2026-11-23; köztük 2026-11-06 12:03–18:27 385 perc) → 417 feature + 418 predikció sor pótolva (1 extra predikció egy futás közbeni lock miatt), maradék 0.
- bchusdt: 4 lyuk / 4 perc (11-07 09:34, 11-07 11:25, 11-18 11:12, 11-23 07:16) → 4 feature + 4 predikció, maradék 0.
- btcusdt: 0 lyuk.
- Végellenőrzés: ohlcv / feat_ohlcv_quant / predictions belső lyuk = 0 mindhárom assetre a teljes tartományon (predictions 2021-01-01-től); csak az élő 1 perces tail-késés látszik.

**Tesztek:** új `src/data_handling/tests/sync_tables/smoke/test_repair_gaps.py` (3 teszt: félbeszakadt job után nincs lyuk — catch-up nélkül bukik, ellenőrizve; catch-up határesetek; repair dry-run + feature→predikció backfill, determinisztikus értékek, idempotens). `uv run pytest src/data_handling src/worker -q`: 292 passed, 4 failed — mind élő-DB teszt, nem t947: `store/sanity/test_features_target.py::test_target_row_count_matches_features` (target nem része az élő láncnak) és `store/sanity/test_predictions.py::test_predictions_score_range` (már korábban is bukott, t949), valamint 2 `store/perf/test_query_timing.py` (lock/időzítés flake, újrafuttatva átmegy). ruff + pyright: 0 hiba a módosított fájlokon.

**Docs:** 1200_sync_tables.md (catch-up, `repair_gaps.py`, idempotencia tábla), 0003_runtime_flow.md (hibatűrés), 8310_worker.md (sync_full hibatűrés sor).

**Worker restart:** a futó worker még a régi `sync_features`-t tölti be — amíg nincs újraindítva, egy feature-lépésnél bukó job még okozhat új lyukat. Újraindítás ajánlott (nem végeztem el); utána, ill. ha addig új lyuk jelenik meg: `02_sync_pipeline.py --repair-gaps --asset-id <id>`.

### 2026-11-24 — validator_agent — PASS → done

**Ellenőrizve:**
- Kód-review: `sync_features` catch-up (`_catch_up_start`: min(start, feature-tail + 1 perc), backfill módban kikapcsolva), `backfill_feat_ohlcv_quant` (NOT EXISTS anti-join, egy tranzakció, ROLLBACK hibánál), `repair_gaps.py` (gaps-and-islands, előbb feature, aztán predikció, lépésenkénti lock-retry, dry-run a predikciókat az `ohlcv`-hez méri), `02_sync_pipeline.py --repair-gaps [--dry-run]` önálló mód. Duplikáció nem keletkezhet (append-only `> MAX`, ill. anti-join).
- Élő DB, rövid read-only lekérdezések: solusdt / bchusdt / btcusdt — `ohlcv`, `feat_ohlcv_quant`, `predictions`: 0 belső lyuk, 0 duplikált `open_time`. ethusdt `ohlcv`: 0 lyuk.
- CLI dry-run mind a 4 assetre lefut; SOL/BCH/BTC: 0 / 0.
- 1 soros út teljesítménye: a catch-up plusz egy read-only `MAX(open_time)` lekérdezés, ~0,1 s (hidegen ~1,2 s) — elfogadható.
- Paritás: a backfillelt sorok újraszámolása 1 soros (élő) módon: BCH mind a 4 pótolt perc és SOL 11-06 12:03 (tartomány eleje) **bitre azonos**. SOL 11-06 15:00 és 18:27 (a 385 perces tartomány belseje): 4 oszlop eltér — `feat_obv`, `feat_obv_roc_14`, `feat_ad_line`, `feat_weekly_open_return`. Ezek a feature-ök a lekért ablak elejéhez kötöttek (kumulatívak, ill. a hét nyitóára a 2880 perces ablakon belül), így az értékük a batch-mérettől függ. Ugyanez az élő úton is így van: a worker lemaradásnál maga is 4-5 soros batch-eket ír (worker.log 05:27, 05:32). Ez a feature-ök már meglévő tervezési tulajdonsága, nem a t947 hibája; t947-blokkernek nem minősül. Mind a 6 champion (SOL/BCH/BTC long+short) használja ezeket → **follow-up javasolt** (modeling/analyst): ablakfüggő kumulatív feature-ök → train/serve skew (a tanítás 3 hónapos chunkokban, élőben 2 napos ablakkal számol). Opcionálisan a `repair_gaps` percenkénti backfillje közelebb hozná az értékeket a tipikus élő 1 soros eredményhez.
- Tesztek: `test_repair_gaps.py` 3/3 átmegy; `uv run pytest src/data_handling src/worker -q`: 294 passed, 2 failed (`store/perf/test_query_timing.py` időzítés-flake, újrafuttatva 19/19 átmegy). ruff + pyright: 0 hiba.
- Docs (1200_sync_tables.md, 0003_runtime_flow.md, 8310_worker.md) pontos.
- Kisjavítás: a `backfill_feat_ohlcv_quant` docstringje pontosítva (az anti-join a teljes táblára fut, nem csak a batch időtartományára). Csak ez az egy sor változott a megosztott `duckdb_store.py`-ban.

**Nyitott (nem blokkoló):** ethusdt `feat_ohlcv_quant`-ban 1 új lyuk (2026-11-23 22:02 UTC), mert a futó worker még a régi `sync_features`-t tölti be. Teendő: worker-újraindítás (user), utána `02_sync_pipeline.py --repair-gaps --asset-id ethusdt`. Ethusdt-nek nincs aktív champion modellje, így predikció-lyuk nincs.
