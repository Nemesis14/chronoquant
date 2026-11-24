---
epic: epic_071
id: t912
title: Hozd létre a feat_levels feature táblát és a sync-et (user jóváhagyás után)
assignee: database_agent
status: done
blocks: [t913, t914]
blocked_by: [t911]
---

## Goal
⛔ **Csak a t911 riport user általi jóváhagyása után indul.** A detektor kimenetéből bar-szintű (1m)
feature-ök egy külön `feat_levels` táblában (live main séma), t-1 laggal, idempotens upsert sync-kel,
és bekötés a `quant_train`-be a challenger feature-profilhoz. A champion `feat_ohlcv_quant` érintetlen.

## Scope
- `src/data_handling/sync_tables/` (új sync), `config/features.json` (challenger profil), `03_build_quant_train.py` / `sync_quant_train.py`
- Live DB séma migráció (`store/migrations.py`)

## Feature-jelöltek (a t911 user-feedback alapján véglegesítendő)
`feat_sr_dist_res_atr_{s,m,l}`, `feat_sr_dist_sup_atr_{s,m,l}`, `feat_sr_strength_res_{s,m,l}`, `feat_sr_strength_sup_{s,m,l}`,
`feat_sr_touches_*`, `feat_sr_is_round_*`, `feat_sr_role_flip_*`, `feat_tl_dist_atr_{s,m}`, `feat_tl_break_age_*`, `feat_tl_retest_flag_*`

## Acceptance Criteria
- [ ] `feat_levels` tábla, PK `open_time`, upsert sync, t-1 lag
- [ ] Leakage teszt a bar-szintű feature-ökre
- [ ] Champion feature-profil és `feat_ohlcv_quant` változatlan
- [ ] `ruff` + `pyright` + pytest zöld

## Notes
- [orchestrátor 2026-11-19] User jóváhagyta a t911 riportot → indítható. A detektor config v1 befagyasztva (ne módosítsd a `levels_detector` paramétereit).
- ⚠️ A detektor `levels` / `trendlines` DataFrame-jeinek több mezője (`n_touches`, `last_touch`, `strength`, `role`, `n_role_flips`, `broken_at`, `expired_at`) az objektum ÉLETE VÉGI állapota = jövő információ. Bar-szintű feature-höz KIZÁRÓLAG a `levels_as_of(res, T)` / `trendlines_as_of(res, T)` (vagy ezzel ekvivalens, kauzális inkrementális számítás) használható. A leakage teszt ezt fedje le mindhárom léptéken (az `l`-t is).

### t912 — database_agent (2026-11-19)

**Fájlok**
- új: `src/data_handling/sync_tables/_levels_features.py` — tiszta, vektorizált feature-motor (nincs DB). API: `compute_level_features(df_1m, params=None, scales=None, start=None, end=None)`, `level_features_from_results(df_1m, results, ...)`, `feature_columns()`, `FEATURE_DOCS`.
- új: `src/data_handling/sync_tables/sync_levels.py` — DB sync + CLI (`python -m data_handling.sync_tables.sync_levels --asset-id solusdt [--full | --start | --end]`), `upsert_feat_levels`.
- módosítva: `store/duckdb_store.py` — v6 migráció `create_feat_levels` (`_feat_levels_v1_columns()` = befagyasztott oszloplista); `rebuild_quant_train(..., extra_feature_tables=None, table_name="quant_train")`.
- módosítva: `store/snapshots.py` — `create_snapshot(..., source_table="quant_train")` (+ a belső hash/range függvények), `_live_source()`.
- módosítva: `sync_tables/sync_quant_train.py` (`features_profile` param), `03_build_quant_train.py` és `05_create_snapshot.py` (`--features-profile`), `02_sync_pipeline.py` (új `levels` tábla-lépés, benne az `ALL_TABLES`-ben, a predictions után).
- módosítva: `src/utils.py` — `apply_feature_profile` `base_profile` támogatás (challenger nem definiálhat saját indikátort → ValueError); új `feature_profile_extra_tables()`, `quant_train_table()`.
- módosítva: `config/features.json` — új profil `solusdt_fw60_sr` (`base_profile: solusdt_fw60`, `extra_feature_tables: ["feat_levels"]`). A `levels_detector` blokk és a champion `solusdt_fw60` profil érintetlen; `config/assets.json` nem változott (a SOL élő profilja továbbra is `solusdt_fw60`).
- új tesztek: `tests/sync_tables/sanity/test_levels_features.py` (8), `tests/sync_tables/smoke/test_sync_levels.py` (5); `tests/sync_pipeline/smoke/test_sync_pipeline_helpers.py` — a `--skip-ohlcv` elvárt halmazába bekerült a `levels`.

**Tábla: `feat_levels`** (live main, migráció v6): `open_time TIMESTAMP PRIMARY KEY`, `available_ts TIMESTAMP` (= open_time, a feat_ohlcv_quant konvenció szerint), 50 db `feat_*` DOUBLE oszlop. Írás: `INSERT OR REPLACE ... BY NAME` (idempotens upsert open_time-ra). Minden feature DOUBLE (az egész jellegűek is), hogy a pandas/numpy konverzió NULL-lal se adjon nullable int dtype-ot.

**Look-ahead / t-1**: a `t` sor a `T = t` falióra-időpontban (az 1m `t-1` bar zárása után) ismert állapotot írja le. Aktív objektum = `confirmed_at <= T < expired_at` (pontosan a `levels_as_of` / `trendlines_as_of(active_only=True)` szabálya). Érintésszám / erő / szerepcsere = lépcsőfüggvény a `confirmed_at` / `event_time <= T` érintés- és break-pontokon. A lépték-ATR és a vonal-ár a `T`-ig lezárt utolsó lépték-barból jön; a referenciaár `ref = close[t-1]` (előző sor). A `levels` / `trendlines` élete-végi mezőit (n_touches, strength, role, n_role_flips, broken_at) a kód nem olvassa. Az `expired_at`-ot csak arra használja, hogy "T-ben még él-e", ami egy csonkolt futásban is ugyanaz. Megvalósítás: eseményvezérelt és vektorizált. Az aktív objektumok slot-okba kerülnek (intervallum-színezés, ~20 slot), a sorokat chunkonként (sor × slot) mátrixban dolgozza fel, az állapotot kompozit-kulcsos `searchsorted` keresi ki. Nincs perces `*_as_of` hívás.

**Feature lista** (x = lépték; "aktív" = T-ben aktív; ATR_x = az utolsó lezárt lépték-bar ATR14-e)
| oszlop | léptékek | definíció | NULL ha |
|---|---|---|---|
| `feat_sr_dist_res_atr_{x}` | s,m,l | (a legközelebbi aktív szint ára `> ref` − ref) / ATR_x, >0 | nincs aktív szint ref felett, vagy ATR ismeretlen |
| `feat_sr_dist_sup_atr_{x}` | s,m,l | (ref − a legközelebbi aktív szint ára `<= ref`) / ATR_x, >=0 | nincs aktív szint ref alatt |
| `feat_sr_strength_{res,sup}_{x}` | s,m,l | ugyanannak a szintnek az ereje T-ben: Σ exp(−(T−pivot_time)/τ) a `confirmed_at<=T` érintésekre (= `levels_as_of.strength`) | mint fent |
| `feat_sr_touches_{res,sup}_{x}` | s,m,l | érintésszám (`confirmed_at<=T`) | mint fent |
| `feat_sr_flips_{res,sup}_{x}` | s,m,l | eddigi szerepcserék száma = a szint break eseményei `<=T` | mint fent |
| `feat_sr_n_active_{x}` | s,m,l | aktív szintek száma (0 = nincs) | csak a 0. sor |
| `feat_sr_is_round_{res,sup}_{x}` | s,m | 1, ha a szint tűrésen belül 10 többszöröse (detektor `is_round`), különben 0; az 5-ös másodlagos flag kimaradt | nincs szint |
| `feat_sr_break_age_{x}` / `feat_tl_break_age_{x}` | sr: s,m,l; tl: s,m | a legutóbbi szint- / vonal-break óta eltelt idő lépték-barokban (`event_time<=T`) | nincs break a lookback-en belül (7 / 30 / 180 nap) |
| `feat_sr_break_dir_{x}` / `feat_tl_break_dir_{x}` | mint fent | +1 = ellenállás felfelé tört, −1 = támasz lefelé (ugyanaz a break) | mint fent |
| `feat_sr_retest_flag_{x}` / `feat_tl_retest_flag_{x}` | mint fent | 1, ha a megtört objektumnak volt retest eseménye a (break, T] szakaszban, különben 0 | mint fent |
| `feat_tl_dist_sup_atr_{x}` | s,m | (ref − a legközelebbi aktív támasz-vonal ára) / ATR_x, előjeles (ε-zónán belül negatív is lehet) | nincs aktív támasz-vonal |
| `feat_tl_dist_res_atr_{x}` | s,m | (a legközelebbi aktív ellenállás-vonal ára − ref) / ATR_x, előjeles | nincs aktív ellenállás-vonal |

Vonal-ár: `anchor1_price + slope_per_bar·(bar_num_T − anchor1_bar_num)`, ahol bar_num_T az utolsó lezárt lépték-bar (mint a `trendlines_as_of.price_at`). Legközelebbi vonal = a legkisebb |távolság| az adott típusú aktív vonalak közül. A szintek támasz/ellenállás besorolása a ref 1m záróárhoz képest történik (nem a lépték-bar záróárához, mint a `levels_as_of.role`-nál); ez is T-ben ismert, csak finomabb.

**Leakage / helyesség tesztek** (`test_levels_features.py`; 200 napos szintetikus 1m sor; valós config v1; mind a 3 lépték: s=1h, m=4h, l=1d)
- A vektorizált feature megegyezik a `levels_as_of` / `trendlines_as_of` és a T-ig ismert események alapján épített referenciával: 120 véletlen sor × 50 oszlop, több mint 1000 nem-NULL érték.
- Csonkolt (open_time ≤ T) és teljes futás: minden ≤T sor egyezik, 3 vágási ponton (61,3 / 123,7 / 187,0 nap, perc-közép). Az utolsó 20 napban mindhárom léptéken van aktív szint.
- t-1: a `t` bar (és minden későbbi) sokkolása nem változtatja a `t` sort, a `t+1`-et igen.
- NULL-szemantika; az oszloplista azonos a migrációéval; nincs `forward_*` oszlop.

**quant_train / profil döntés (a t914-nek)**: a `quant_train` egyetlen közös tábla, ezért a challenger **külön táblát** kap: `quant_train__solusdt_fw60_sr` (`utils.quant_train_table(profile)`). Építés = a champion join (`feat_ohlcv_quant ⋈ target`, ugyanazzal a NULL-target szűrővel) + `LEFT JOIN feat_levels`. Így pontosan a champion sorhalmaza marad, kiegészítve 50 `feat_*` oszloppal (a `feat_levels.available_ts` nem kerül át). A champion build (profil nélkül) változatlanul a `quant_train`-be ír. Ha challenger profillal a `quant_train`-be írnánk, az ValueError.
- **Felépítve a SOL live DB-ben** (2026-11-19): `03_build_quant_train.py --features-profile solusdt_fw60_sr` → 3 120 336 sor, 495 s, 267 oszlop. A champion `quant_train` nem változott (3 120 336 sor, max 2026-08-21 04:40; a feat_levels-t is ekkora tartományra joinolja).
- **Egyezés a champion snapshottal** (`solusdt_fw60_2101_2607__49466cd2`, 2021-01-01 00:00 → 2026-07-31 00:00):
  - sorszám 2 933 276, `sum(hash(open_time))` azonos;
  - a challenger tábla nem-feat_levels `feat_*` készlete pontosan a snapshot 208 feature-je;
  - mind a 208 oszlop értéke egyezik mind a 2 933 276 soron (0 eltérés).
  → Tiszta A/B alap.
- ⚠️ **A t914-nek tudnia kell**: a jelenlegi champion `quant_train` (élő tábla) 27 régi alt-data oszlopot is hordoz (`feat_oi_*`, `feat_basis_*`, `feat_depth_*`, `feat_ls_*`, `feat_prem_*`, `feat_funding_*`, `feat_spot_*`), mert az epic_068 előtt épült. Ezek a `feat_ohlcv_quant`-ban már nincsenek, ezért a challenger táblában sincsenek. A champion snapshotban és a champion modellekben sem szerepelnek, így az A/B-t ez nem érinti; a champion `quant_train` újraépítésekor ezek el fognak tűnni. A challenger tábla 6 extra target oszlopot is hordoz (`long/short_mfe_fw240`, `tb_label_*`), ahogy a champion `quant_train` is.
- Snapshot parancs: `uv run python src/data_handling/05_create_snapshot.py --features-profile solusdt_fw60_sr --start "2021-01-01 00:00:00" --end "2026-07-31 00:00:00"`. A `create_snapshot` `exclude_features` nélkül `SELECT *`-ot csinál, így a target oszlopok is bekerülnek. Az oszlopkészlet része a content hash-nek, ezért a snapshot_id eltér a championétól. Ha a t914 a champion snapshot oszlopkészletét akarja + feat_levels-t, hívhatja `exclude_features`-szel is (az csak `long/short_mfe_fw60`-at tartja meg).
- A modell a snapshot összes `feat_*` oszlopát látja (az FE-lépés szűr); a NULL-ok a LightGBM-ben NaN-ként mennek.

**Bekötés**: `02_sync_pipeline.py --tables levels` (az alapértelmezett "all"-ban is benne van). Nem chunkolt, mert a detektor útfüggő, ezért mindig a teljes történeten fut; a `--start` / `--end` csak az írt sorokat korlátozza, `--start` nélkül inkrementális. A worker `sync_full` jobja (ui_agent domain) **nem** számol `feat_levels`-t, mert a champion nem igényli. Ha a challenger élesbe menne, a workerbe kell egy lépés (~27 s / futás, a teljes detektor miatt).

**Backfill (SOL live DB, 2026-11-19)**: `sync_levels --asset-id solusdt --full` → 3 250 525 sor (2020-09-14 07:00 → 2026-11-19 14:24), 76 s (53 s detektor + feature, 23 s írás). Utána az inkrementális futás 1 sort írt, 27 s alatt. A DB nem volt lockolva (a worker csak a sync idejére nyitja); a workert nem kellett leállítani. A v6 migráció alkalmazva.

NULL arány (teljes / 2021-től):
- dist / strength / touches / flips / is_round: res_s 15,6 / 16,0%, sup_s 14,9 / 14,5%, res_m 17,4 / 17,8%, sup_m 14,6 / 13,4%, res_l 27,0 / 25,3%, sup_l 13,3 / 10,1%;
- `n_active_*`: 0%;
- sr break_age / dir / retest: s 0,2 / 0,0%, m 0,7 / 0,0%, l 3,8 / 0,0%;
- tl_dist: sup_s 40,4 / 39,7%, res_s 40,7 / 41,0%, sup_m 35,8 / 34,3%, res_m 42,6 / 43,3%;
- tl break_age / dir / retest: s 0,1 / 0,0%, m 0,6 / 0,0%.

Mediánok: dist_atr ~0,68–0,85, touches 2–3, n_active ~11. Az is_round=1 aránya s-en ~7%, m-en ~17%; a retest_flag=1 aránya ~30%.

**Ellenőrzés**
- `uv run ruff check src/data_handling/ --fix` → All checks passed (`src/utils.py` is tiszta).
- `uv run pyright src/data_handling/ src/utils.py` → 0 errors.
- `uv run pytest src/data_handling/tests/ -v` → 226 passed, 3 failed: a 3 ismert DB-állapot hiba (`test_feat_ohlcv_quant_no_time_gap`, `test_target_row_count_matches_features`, `test_predictions_score_range`); ezek nem változtak.

**Eltérések / megjegyzések**
- A ticket `feat_sr_role_flip_*` jelöltje `feat_sr_flips_{res,sup}_{x}` lett (darabszám, nem flag). A `touches` és az `is_round` a legközelebbi támasz- / ellenállás-szintre vonatkozik. Extra: `feat_sr_n_active_{x}`, `*_break_dir_*`, valamint a szint-break kora és retestje (s, m, l). A `feat_tl_*` csak s és m léptéken van (a ticket szerint; l-en ~140 vonal van 6 év alatt).
- A `levels_detector` v1 változatlan. Ha módosul, a `feat_levels`-t `--full`-lal újra kell tölteni (a tábla nem tárol detektor-verziót), utána pedig a challenger quant_train-t is újra kell építeni.
- A DB-fájl ~9,8 GB-ról ~12,6 GB-ra nőtt (feat_levels + a challenger quant_train).
- A code_doc_agent felé: a `_doc_/database_and_code_doc/` alatt frissíteni kell a referenciát (1200_sync_tables, 4100/4110 quant_train, 1400 snapshots: feat_levels, `--features-profile`, `source_table`). A doc-zónába nem írtam.

[validator] Validated — 2026-11-19 → done
- ruff (`src/data_handling/ src/utils.py`): All checks passed; pyright: 0 errors.
- pytest `src/data_handling/tests/`: 221 passed + a 3 ismert DB-állapot hiba (`test_feat_ohlcv_quant_no_time_gap`, `test_target_row_count_matches_features`, `test_predictions_score_range`). 1 perf fail + 2 perf error (`test_timing_target_label_groupby`, `test_quant_train_timing`) a validátor párhuzamos read-only DB-lekérdezésével ütközött; izoláltan 3/3 zöld.
- pytest `src/modeling src/strategy/tests src/api src/worker`: 168 passed.
- Look-ahead: a `_levels_features.py` nem olvassa a n_touches/strength/role/n_role_flips/broken_at mezőket; a szint `price`/`is_round` a megerősítéskor rögzül, a vonal anchor/slope a definiáló pivot-párból; `expired_at` csak "T-ben él-e" célra (egy lejárt klaszter nem éled újra → csonkolás-invariáns); ref = close[t-1]; a csonkolt == teljes teszt s/m/l-en fut.
- Champion: default `source_table="quant_train"` mellett a `_live_source()` == `LIVE_SOURCE`, a hash SQL-je és fingerprintje bájtra azonos → a meglévő champion snapshot hash-e változatlanul reprodukálható. (Közvetlen újraszámolás nem futott, mert a `reg` séma nem a lab DB-ben van.) A `rebuild_quant_train` default útvonala ugyanazt a SQL-t adja (üres join_sql, `quant_train`).
- Read-only DB: feat_levels 3 250 526 sor (egyedi open_time); `quant_train` = `quant_train__solusdt_fw60_sr` = 3 120 336 sor, a sorhalmaz-különbség mindkét irányban 0; 267 oszlop, 50 feat_levels feature, `available_ts` nincs.
- Config gateway: csak `utils.load_levels_detector_config` / `utils._load_json`, nyers JSON olvasás nincs.
- Nem-blocker: a `config/assets.json` (btc→bch) és a `utils.trading_asset_ids` / `strategy_session_ids` diff korábbi, még nem commitolt munkából való, nem a t912-ből. A `levels` bekerült a pipeline "all" halmazába (+~27 s inkrementálisan), de a worker és a `scripts/` nem hívja a `02_sync_pipeline.py`-t.
